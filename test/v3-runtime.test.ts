/**
 * v3-runtime.test.ts
 *
 * v3 runtime 主循环集成测试 —— 跑设计稿 §4 的最小闭环 research→summarize，
 * 用 codex 的【真实】manifest validator（readAndValidateManifest）+ stub runNode
 * （写真实 manifest 文件）。验证：调度循环 / 文件 IPC 契约 / inputs.json 相对转绝对 /
 * journal 事件流 / fail-fast。不 spawn 真实 CLI（那条 seam 由 ephemeral-pool 自测 +
 * 后续 daemon e2e 覆盖）。
 */

import { describe, it, expect } from 'vitest';
import { mkdtempSync, rmSync, writeFileSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, isAbsolute } from 'node:path';
import { createHash } from 'node:crypto';

import { validateDag } from '../src/workflows/v3/dag.js';
import { appendEvent, readJournal } from '../src/workflows/v3/journal.js';
import { runWorkflow, type V3RuntimeDeps } from '../src/workflows/v3/runtime.js';
import { readAndValidateManifest, ManifestValidationError } from '../src/workflows/v3/manifest.js';
import {
  createFileGate,
  writePendingWait,
  readWait,
  resolveWait,
  listPendingWaits,
} from '../src/workflows/v3/human-gate.js';
import {
  GOAL_ENV,
  type BotSnapshot,
  type GoalInputs,
  type Manifest,
  type RunNode,
  type ValidateManifest,
} from '../src/workflows/v3/contract.js';

const TWO_NODE = {
  runId: 'demo-001',
  nodes: [
    { id: 'research', type: 'goal', goal: '调研 X', depends: [], inputs: [] },
    { id: 'summarize', type: 'goal', goal: '写摘要', depends: ['research'], inputs: [{ from: 'research' }] },
  ],
};

// codex 的 throw-based 校验器 → 适配成 runtime 期望的 result-style（注入边界做）
const validateManifest: ValidateManifest = async (manifestPath, outputDir) => {
  try {
    const manifest = await readAndValidateManifest(manifestPath, outputDir);
    return { ok: true, manifest };
  } catch (e) {
    return { ok: false, problems: e instanceof ManifestValidationError ? e.problems : [String(e)] };
  }
};

const resolveBotSnapshot = (): BotSnapshot => ({
  larkAppId: 'cli_test',
  cliId: 'claude-code',
  workingDir: '/tmp',
});

/** 写一个真实产物 + 返回它的 manifest file 条目（相对 path + 真实 sha256/bytes）。 */
function product(outputDir: string, name: string, content: string): Manifest['files'][number] {
  writeFileSync(join(outputDir, name), content);
  return {
    name,
    path: name, // 相对 outputDir
    kind: 'markdown',
    bytes: Buffer.byteLength(content),
    sha256: createHash('sha256').update(content).digest('hex'),
    mime: 'text/markdown',
  };
}

function writeManifest(req: Parameters<RunNode>[0], manifest: Manifest): string {
  const p = req.env[GOAL_ENV.MANIFEST_PATH]!;
  writeFileSync(p, JSON.stringify(manifest));
  return p;
}

describe('runWorkflow — research→summarize 最小闭环', () => {
  it('happy path：两节点成功 + inputs.json 相对转绝对 + journal 完整', async () => {
    const base = mkdtempSync(join(tmpdir(), 'v3-rt-ok-'));
    try {
      let summarizeSawResearch = false;
      const runNode: RunNode = async (req) => {
        if (req.node.id === 'summarize') {
          // 下游能从 inputs.json 拿到上游产物的绝对路径并 Read
          const inputs = JSON.parse(readFileSync(req.inputsPath, 'utf-8')) as GoalInputs;
          const fromResearch = inputs.inputs.find((i) => i.from === 'research');
          summarizeSawResearch = !!fromResearch && isAbsolute(fromResearch.path)
            && readFileSync(fromResearch.path, 'utf-8').includes('RESEARCH-PRODUCT');
        }
        const content = `# ${req.node.id}\nRESEARCH-PRODUCT`;
        const file = product(req.outputDir, 'out.md', content);
        const manifestPath = writeManifest(req, {
          schemaVersion: 1, status: 'ok', summary: `done ${req.node.id}`, files: [file],
        });
        return { status: 'ok', manifestPath };
      };

      const deps: V3RuntimeDeps = { runNode, validateManifest, resolveBotSnapshot };
      const outcome = await runWorkflow(validateDag(TWO_NODE), deps, { baseDir: base });

      expect(outcome).toMatchObject({ reason: 'terminal', runStatus: 'succeeded' });
      if (outcome.reason !== 'terminal') throw new Error('expected terminal outcome');
      expect(summarizeSawResearch).toBe(true);

      const events = readJournal(join(outcome.runDir, 'journal.ndjson'));
      expect(events.filter((e) => e.type === 'nodeSucceeded').map((e) => (e as any).nodeId).sort())
        .toEqual(['research', 'summarize']);
      expect(events.some((e) => e.type === 'runSucceeded')).toBe(true);

      const inputs = JSON.parse(
        readFileSync(join(outcome.runDir, 'summarize', 'attempts', '001', 'inputs.json'), 'utf-8'),
      ) as GoalInputs;
      expect(inputs.inputs).toHaveLength(1);
      expect(isAbsolute(inputs.inputs[0]!.path)).toBe(true);
      expect(inputs.inputs[0]!.path).toContain(join('research', 'attempts', '001', 'work', 'out.md'));

      // goal.txt carries the user goal + the full execution/manifest contract
      // (it is NOT the bare goal string) so the short `/goal` command can just
      // point the agent here without tripping TUI paste-detection.
      const goalFile = readFileSync(join(outcome.runDir, 'research', 'attempts', '001', 'goal.txt'), 'utf-8');
      expect(goalFile).toContain('调研 X');                         // the user goal
      expect(goalFile).toContain(GOAL_ENV.MANIFEST_PATH);           // contract references the manifest env
      expect(goalFile).toContain('"schemaVersion": 1');             // rendered manifest shape
      expect(goalFile).toContain('markdown | json | text');         // file-kind enum from contract.ts
    } finally {
      rmSync(base, { recursive: true, force: true });
    }
  });

  it('fail-fast：research 进程失败 → 整 run 失败，summarize 不派', async () => {
    const base = mkdtempSync(join(tmpdir(), 'v3-rt-fail-'));
    try {
      const runNode: RunNode = async (req) => {
        // 写一个 fail manifest（带 error）模拟节点自报失败
        const manifestPath = writeManifest(req, {
          schemaVersion: 1, status: 'fail', summary: 'boom',
          error: { code: 'E_RESEARCH', message: '调研失败' }, files: [],
        });
        return { status: 'fail', manifestPath };
      };
      const deps: V3RuntimeDeps = { runNode, validateManifest, resolveBotSnapshot };
      const outcome = await runWorkflow(validateDag(TWO_NODE), deps, { baseDir: base });

      expect(outcome).toMatchObject({ reason: 'terminal', runStatus: 'failed' });
      if (outcome.reason !== 'terminal') throw new Error('expected terminal outcome');
      expect(outcome.failedNodeId).toBe('research');

      const events = readJournal(join(outcome.runDir, 'journal.ndjson'));
      const failed = events.find((e) => e.type === 'nodeFailed') as any;
      expect(failed.nodeId).toBe('research');
      expect(failed.message).toContain('E_RESEARCH'); // 优先展示 manifest.error
      // summarize 从未被派（无依赖满足）
      expect(events.some((e) => e.type === 'nodeDispatched' && (e as any).nodeId === 'summarize')).toBe(false);
    } finally {
      rmSync(base, { recursive: true, force: true });
    }
  });

  it('manifest 非法（绝对 path 越权）→ manifestInvalid', async () => {
    const base = mkdtempSync(join(tmpdir(), 'v3-rt-bad-'));
    try {
      const runNode: RunNode = async (req) => {
        // 进程 ok 但 manifest 写了绝对路径 —— codex validator 必须拒
        const manifestPath = writeManifest(req, {
          schemaVersion: 1, status: 'ok', summary: 'x',
          files: [{ name: 'p', path: '/etc/passwd', kind: 'text', bytes: 1, sha256: 'x', mime: 'text/plain' }],
        });
        return { status: 'ok', manifestPath };
      };
      const deps: V3RuntimeDeps = { runNode, validateManifest, resolveBotSnapshot };
      const outcome = await runWorkflow(validateDag(TWO_NODE), deps, { baseDir: base });

      expect(outcome).toMatchObject({ reason: 'terminal', runStatus: 'failed' });
      if (outcome.reason !== 'terminal') throw new Error('expected terminal outcome');
      const events = readJournal(join(outcome.runDir, 'journal.ndjson'));
      const failed = events.find((e) => e.type === 'nodeFailed') as any;
      expect(failed.nodeId).toBe('research');
      expect(failed.errorClass).toBe('manifestInvalid');
    } finally {
      rmSync(base, { recursive: true, force: true });
    }
  });
});

describe('runWorkflow — humanGate suspend mode', () => {
  it('suspend 模式：派 gate 后写 pending wait 并返回 awaitingGate；批准后 redrive 继续跑 work', async () => {
    const base = mkdtempSync(join(tmpdir(), 'v3-rt-gate-'));
    try {
      const dag = validateDag({
        runId: 'gate-run',
        nodes: [{
          id: 'deploy',
          type: 'goal',
          goal: 'deploy',
          depends: [],
          inputs: [],
          humanGate: { prompt: '批准部署？' },
        }],
      });
      let runNodeCalls = 0;
      const runNode: RunNode = async (req) => {
        runNodeCalls++;
        const file = product(req.outputDir, 'deploy.md', '# deployed');
        const manifestPath = writeManifest(req, {
          schemaVersion: 1, status: 'ok', summary: 'deployed', files: [file],
        });
        return { status: 'ok', manifestPath };
      };
      const deps: V3RuntimeDeps = { runNode, validateManifest, resolveBotSnapshot };

      const first = await runWorkflow(dag, deps, { baseDir: base, gateMode: 'suspend' });

      expect(first).toEqual({
        reason: 'awaitingGate',
        runDir: join(base, 'gate-run'),
        pendingWaits: [{ nodeId: 'deploy', waitId: 'deploy-gate', prompt: '批准部署？' }],
      });
      expect(runNodeCalls).toBe(0);
      expect(readWait(first.runDir, 'deploy-gate')).toMatchObject({
        status: 'pending',
        nodeId: 'deploy',
        prompt: '批准部署？',
      });
      let events = readJournal(join(first.runDir, 'journal.ndjson'));
      expect(events.some((e) => e.type === 'gateDispatched' && e.nodeId === 'deploy')).toBe(true);
      expect(events.some((e) => e.type === 'nodeDispatched' && e.nodeId === 'deploy')).toBe(false);

      resolveWait(first.runDir, 'deploy-gate', 'approved', 'ou_reviewer');
      appendEvent(join(first.runDir, 'journal.ndjson'), {
        type: 'gateResolved',
        nodeId: 'deploy',
        waitId: 'deploy-gate',
        resolution: 'approved',
        by: 'ou_reviewer',
      });

      const second = await runWorkflow(dag, deps, { baseDir: base, gateMode: 'suspend' });
      expect(second).toMatchObject({ reason: 'terminal', runStatus: 'succeeded' });
      expect(runNodeCalls).toBe(1);
      events = readJournal(join(second.runDir, 'journal.ndjson'));
      expect(events.some((e) => e.type === 'nodeSucceeded' && e.nodeId === 'deploy')).toBe(true);
    } finally {
      rmSync(base, { recursive: true, force: true });
    }
  });

  it('suspend 模式：并发 work 先 settle，之后才返回 awaitingGate', async () => {
    const base = mkdtempSync(join(tmpdir(), 'v3-rt-gate-par-'));
    try {
      const dag = validateDag({
        runId: 'gate-parallel',
        nodes: [
          {
            id: 'approval',
            type: 'goal',
            goal: 'approval',
            depends: [],
            inputs: [],
            humanGate: { prompt: '批准？' },
          },
          { id: 'research', type: 'goal', goal: 'research', depends: [], inputs: [] },
        ],
      });
      const runNode: RunNode = async (req) => {
        await new Promise((resolve) => setTimeout(resolve, 10));
        const file = product(req.outputDir, 'research.md', '# facts');
        const manifestPath = writeManifest(req, {
          schemaVersion: 1, status: 'ok', summary: 'facts', files: [file],
        });
        return { status: 'ok', manifestPath };
      };
      const deps: V3RuntimeDeps = { runNode, validateManifest, resolveBotSnapshot };

      const outcome = await runWorkflow(dag, deps, { baseDir: base, gateMode: 'suspend', globalConcurrency: 2 });

      expect(outcome).toMatchObject({ reason: 'awaitingGate' });
      if (outcome.reason !== 'awaitingGate') throw new Error('expected awaitingGate outcome');
      expect(outcome.pendingWaits).toEqual([{ nodeId: 'approval', waitId: 'approval-gate', prompt: '批准？' }]);
      const events = readJournal(join(outcome.runDir, 'journal.ndjson'));
      expect(events.some((e) => e.type === 'nodeSucceeded' && e.nodeId === 'research')).toBe(true);
      expect(readWait(outcome.runDir, 'approval-gate')?.status).toBe('pending');
    } finally {
      rmSync(base, { recursive: true, force: true });
    }
  });

  it('默认 blocking 模式保持 CLI/dev 语义：resolveGate 批准后同一次 run 继续执行', async () => {
    const base = mkdtempSync(join(tmpdir(), 'v3-rt-gate-block-'));
    try {
      const dag = validateDag({
        runId: 'gate-block',
        nodes: [{
          id: 'deploy',
          type: 'goal',
          goal: 'deploy',
          depends: [],
          inputs: [],
          humanGate: { prompt: '批准部署？' },
        }],
      });
      const runNode: RunNode = async (req) => {
        const file = product(req.outputDir, 'deploy.md', '# deployed');
        const manifestPath = writeManifest(req, {
          schemaVersion: 1, status: 'ok', summary: 'deployed', files: [file],
        });
        return { status: 'ok', manifestPath };
      };
      const resolveGate = createFileGate({
        awaitDecision: async () => ({ resolution: 'approved', by: 'ou_cli' }),
      });
      const deps: V3RuntimeDeps = { runNode, validateManifest, resolveBotSnapshot, resolveGate };

      const outcome = await runWorkflow(dag, deps, { baseDir: base });

      expect(outcome).toMatchObject({ reason: 'terminal', runStatus: 'succeeded' });
      expect(readWait(outcome.runDir, 'deploy-gate')).toMatchObject({ status: 'approved', by: 'ou_cli' });
    } finally {
      rmSync(base, { recursive: true, force: true });
    }
  });
});

describe('human-gate 文件等待存储', () => {
  it('pending → resolve 持久化 + listPendingWaits 只列未决', () => {
    const dir = mkdtempSync(join(tmpdir(), 'v3-gate-'));
    try {
      writePendingWait(dir, { waitId: 'g1', nodeId: 'a', prompt: '批 a？' });
      writePendingWait(dir, { waitId: 'g2', nodeId: 'b', prompt: '批 b？' });
      expect(readWait(dir, 'g1')!.status).toBe('pending');
      expect(listPendingWaits(dir).map((w) => w.waitId).sort()).toEqual(['g1', 'g2']);

      resolveWait(dir, 'g1', 'approved', 'ou_x');
      const g1 = readWait(dir, 'g1')!;
      expect(g1.status).toBe('approved');
      expect(g1.by).toBe('ou_x');
      expect(typeof g1.resolvedAt).toBe('number');
      expect(listPendingWaits(dir).map((w) => w.waitId)).toEqual(['g2']); // 已决不再列入
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it('resolveWait 找不到等待时抛错', () => {
    const dir = mkdtempSync(join(tmpdir(), 'v3-gate-x-'));
    try {
      expect(() => resolveWait(dir, 'ghost', 'approved', 'u')).toThrow(/no pending wait/);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it('createFileGate：决策前已 pending，决策后落盘 resolved 并返回结果', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'v3-gate-f-'));
    try {
      let statusAtDecision: string | undefined;
      const gate = createFileGate({
        awaitDecision: async (wait) => {
          statusAtDecision = readWait(dir, wait.waitId)!.status;
          return { resolution: 'approved', by: 'ou_z' };
        },
      });
      const res = await gate({ nodeId: 'a', prompt: '批？', waitId: 'g1', runDir: dir });
      expect(res).toBe('approved');
      expect(statusAtDecision).toBe('pending');
      expect(readWait(dir, 'g1')!.status).toBe('approved');
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe('runtime CLI 白名单守卫', () => {
  it('节点 bot 解析到非 claude-code/codex 的 CLI → run 启动即报错', async () => {
    const base = mkdtempSync(join(tmpdir(), 'v3-cli-guard-'));
    try {
      const deps: V3RuntimeDeps = {
        runNode: async () => ({ status: 'ok', manifestPath: '' }),
        validateManifest,
        resolveBotSnapshot: () => ({ larkAppId: 'a', cliId: 'gemini', workingDir: '/tmp' }),
      };
      await expect(runWorkflow(validateDag(TWO_NODE), deps, { baseDir: base }))
        .rejects.toThrow(/not supported by v3 goal-mode/);
    } finally {
      rmSync(base, { recursive: true, force: true });
    }
  });

  it('codex CLI 放行', async () => {
    const base = mkdtempSync(join(tmpdir(), 'v3-cli-ok-'));
    try {
      const runNode: RunNode = async (req) => {
        const file = product(req.outputDir, 'o.md', '# ok');
        const mp = writeManifest(req, { schemaVersion: 1, status: 'ok', summary: 's', files: [file] });
        return { status: 'ok', manifestPath: mp };
      };
      const deps: V3RuntimeDeps = {
        runNode, validateManifest,
        resolveBotSnapshot: () => ({ larkAppId: 'a', cliId: 'codex', workingDir: '/tmp' }),
      };
      const dag = validateDag({ runId: 'codex-run', nodes: [{ id: 'n', type: 'goal', goal: 'g', depends: [], inputs: [] }] });
      const outcome = await runWorkflow(dag, deps, { baseDir: base });
      expect(outcome).toMatchObject({ reason: 'terminal', runStatus: 'succeeded' });
    } finally {
      rmSync(base, { recursive: true, force: true });
    }
  });

  it('seed CLI 放行（claude-code 家族 fork，原生 /goal）', async () => {
    const base = mkdtempSync(join(tmpdir(), 'v3-cli-seed-'));
    try {
      const runNode: RunNode = async (req) => {
        const file = product(req.outputDir, 'o.md', '# ok');
        const mp = writeManifest(req, { schemaVersion: 1, status: 'ok', summary: 's', files: [file] });
        return { status: 'ok', manifestPath: mp };
      };
      const deps: V3RuntimeDeps = {
        runNode, validateManifest,
        resolveBotSnapshot: () => ({ larkAppId: 'a', cliId: 'seed', workingDir: '/tmp' }),
      };
      const dag = validateDag({ runId: 'seed-run', nodes: [{ id: 'n', type: 'goal', goal: 'g', depends: [], inputs: [] }] });
      const outcome = await runWorkflow(dag, deps, { baseDir: base });
      expect(outcome).toMatchObject({ reason: 'terminal', runStatus: 'succeeded' });
    } finally {
      rmSync(base, { recursive: true, force: true });
    }
  });
});
