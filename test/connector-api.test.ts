import { createServer, type Server } from 'node:http';
import { mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

let server: Server | null = null;
let baseUrl = '';
let dataDir = '';
let prevDataDir: string | undefined;

async function startConnectorApi(): Promise<void> {
  vi.resetModules();
  const { handleConnectorApi } = await import('../src/dashboard/connector-api.js');
  server = createServer(async (req, res) => {
    const url = new URL(req.url ?? '/', `http://${req.headers.host ?? '127.0.0.1'}`);
    if (await handleConnectorApi(req, res, url)) return;
    res.writeHead(404, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: 'not_found' }));
  });
  await new Promise<void>(resolve => server!.listen(0, '127.0.0.1', resolve));
  const addr = server.address();
  if (!addr || typeof addr === 'string') throw new Error('bad test server address');
  baseUrl = `http://127.0.0.1:${addr.port}`;
}

async function json(res: Response): Promise<any> {
  return res.json();
}

beforeEach(async () => {
  dataDir = mkdtempSync(join(tmpdir(), 'botmux-connector-api-'));
  prevDataDir = process.env.SESSION_DATA_DIR;
  process.env.SESSION_DATA_DIR = dataDir;
  await startConnectorApi();
});

afterEach(async () => {
  if (server) await new Promise<void>(resolve => server!.close(() => resolve()));
  server = null;
  if (prevDataDir === undefined) delete process.env.SESSION_DATA_DIR;
  else process.env.SESSION_DATA_DIR = prevDataDir;
  vi.restoreAllMocks();
});

describe('connector-api write routes', () => {
  it('creates a connector with a generated one-time secret, then lists it without plaintext', async () => {
    const res = await fetch(`${baseUrl}/api/connectors`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        name: 'Generic alerts',
        target: { mode: 'dynamic', kind: 'turn', botId: 'app1', allowChats: ['oc_1'] },
        promptEnvelope: { sourceName: 'generic', headerAllowlist: ['x-event-id'] },
      }),
    });
    expect(res.status).toBe(201);
    const created = await json(res);
    expect(created.ok).toBe(true);
    expect(created.secret).toMatch(/^[A-Za-z0-9_-]+$/);
    expect(created.connector.verify.secretRef).toMatch(/^whsec_/);
    expect(created.connector.verify.signatureHeader).toBe('x-botmux-signature');

    const list = await json(await fetch(`${baseUrl}/api/connectors`));
    expect(list.connectors).toHaveLength(1);
    expect(JSON.stringify(list)).not.toContain(created.secret);

    const raw = readFileSync(join(dataDir, 'connectors.json'), 'utf-8');
    expect(raw).not.toContain(created.secret);
    expect(raw).toContain(created.connector.verify.secretRef);
  });

  it('updates enabled state and rotates an existing connector secret', async () => {
    const created = await json(await fetch(`${baseUrl}/api/connectors`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        name: 'Fixed alerts',
        secret: 'provided-secret',
        target: { mode: 'fixed', kind: 'turn', botId: 'app1', chatId: 'oc_1' },
      }),
    }));
    expect(created.secret).toBeUndefined();
    const id = created.connector.id;
    const ref = created.connector.verify.secretRef;

    const patch = await json(await fetch(`${baseUrl}/api/connectors/${encodeURIComponent(id)}`, {
      method: 'PATCH',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ enabled: false }),
    }));
    expect(patch.connector.enabled).toBe(false);

    const rotated = await json(await fetch(`${baseUrl}/api/connectors/${encodeURIComponent(id)}`, {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ name: 'Fixed alerts v2', rotateSecret: true }),
    }));
    expect(rotated.connector.name).toBe('Fixed alerts v2');
    expect(rotated.secretRef).toBe(ref);
    expect(rotated.secret).toBeTruthy();
    expect(JSON.stringify(await json(await fetch(`${baseUrl}/api/connectors/${encodeURIComponent(id)}`)))).not.toContain(rotated.secret);
  });

  it('manages standalone webhook secrets as metadata-only reads', async () => {
    const created = await json(await fetch(`${baseUrl}/api/webhook-secrets`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({}),
    }));
    expect(created.secretRef).toMatch(/^whsec_/);
    expect(created.secret).toBeTruthy();

    const listed = await json(await fetch(`${baseUrl}/api/webhook-secrets`));
    expect(listed.secrets).toHaveLength(1);
    expect(JSON.stringify(listed)).not.toContain(created.secret);

    const rotated = await json(await fetch(`${baseUrl}/api/webhook-secrets/${encodeURIComponent(created.secretRef)}`, {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ secret: 'manual-rotation' }),
    }));
    expect(rotated.secret).toBe('manual-rotation');

    const deleted = await json(await fetch(`${baseUrl}/api/webhook-secrets/${encodeURIComponent(created.secretRef)}`, { method: 'DELETE' }));
    expect(deleted.deleted).toBe(true);
  });
});
