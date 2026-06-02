/**
 * Regression: the pipe-mode web-terminal seed must restore the pane cursor.
 *
 * Claude Code (and other Ink TUIs) repaint their bottom block with height-
 * RELATIVE cursor moves (`\x1b[<n>A` + `\r\n`). When a fresh web client is
 * seeded from `tmux capture-pane` and then resumes the live pipe-pane stream,
 * the FIRST relative redraw assumes the cursor is exactly where the pane's
 * cursor is. Raw capture output has no cursor position and a trailing newline,
 * which scrolls the receiving xterm a row past the content and parks the cursor
 * on the bottom row — so every redraw drifts down a row (status-line update
 * bleeds into the line below). `composeSeedBody` strips the trailing newline
 * and restores the cursor with a viewport-relative CUP.
 *
 * This renders a seed + a few live relative-redraw frames through a real
 * xterm-headless and asserts the status line updates IN PLACE with no ghost.
 *
 * Run: pnpm vitest run test/web-terminal-seed-cursor.test.ts
 */
import { describe, it, expect } from 'vitest';
import xtermHeadless from '@xterm/headless';
import {
  composeSeedBody,
  normaliseCaptureLineEndings,
} from '../src/adapters/backend/tmux-pipe-backend.js';

const { Terminal } = xtermHeadless;
const COLS = 60;
const ROWS = 12;

// A capture-pane-style snapshot: 8 scrollback lines, then a 4-line bottom block
// (STATUS / TIP / INPUT / HINT). tmux separates rows with bare `\n` and appends
// a trailing newline — mirror that exactly so the test exercises the real bug.
const RAW_CAPTURE =
  ['S0', 'S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7',
    'STATUS count=05 thinking',
    'TIP send messages here',
    'INPUT',
    'HINT bypass on'].join('\n') + '\n';
const NORMALISED = normaliseCaptureLineEndings(RAW_CAPTURE);

// The pane cursor rests on the INPUT line (row 10, 0-based) at column 6 — i.e.
// NOT on the bottom row, exactly like Claude's input box.
const CURSOR = { x: 6, y: 10 };

// Live frames the CLI would emit next: from the rest position (INPUT, row 10)
// hop up 2 to STATUS (row 8), rewrite it, hop back down 2 to rest. Pure
// height-relative moves — no absolute positioning.
function liveFrames(from: number, to: number): string {
  let s = '';
  for (let k = from; k <= to; k++) {
    s += '\x1b[2A\r\x1b[2KSTATUS count=' + String(k).padStart(2, '0') +
      ' thinking\x1b[2B\r\x1b[6C';
  }
  return s;
}

function write(t: InstanceType<typeof Terminal>, data: string): Promise<void> {
  return new Promise((resolve) => t.write(data, resolve));
}

async function renderViewport(seed: string, live: string): Promise<string[]> {
  const t = new Terminal({ cols: COLS, rows: ROWS, allowProposedApi: true, scrollback: 1000 });
  await write(t, seed);
  await write(t, live);
  const buf = t.buffer.active;
  const lines: string[] = [];
  for (let y = buf.baseY; y < buf.baseY + ROWS; y++) {
    const ln = buf.getLine(y);
    lines.push((ln ? ln.translateToString(true) : '').replace(/\s+$/, ''));
  }
  t.dispose();
  return lines;
}

describe('pipe-mode web seed cursor restore', () => {
  it('updates the status line in place — no row drift, no ghost', async () => {
    const seed = composeSeedBody(NORMALISED, CURSOR);
    const viewport = await renderViewport(seed, liveFrames(6, 8));

    const statusLines = viewport.filter((l) => l.startsWith('STATUS count='));
    // Exactly one STATUS line (no stale ghost), showing the latest value.
    expect(statusLines).toEqual(['STATUS count=08 thinking']);
    // And it sits on the real STATUS row with the block intact below it.
    const idx = viewport.findIndex((l) => l.startsWith('STATUS count='));
    expect(viewport[idx + 1]).toBe('TIP send messages here');
    expect(viewport[idx + 2]).toBe('INPUT');
    expect(viewport[idx + 3]).toBe('HINT bypass on');
  });

  it('negative control: the raw capture (no cursor restore) DOES drift', async () => {
    // Seed with the un-fixed snapshot: trailing newline, no CUP.
    const viewport = await renderViewport(NORMALISED, liveFrames(6, 8));
    const statusLines = viewport.filter((l) => l.startsWith('STATUS count='));
    // Drift leaves a stale ghost line behind, so more than one STATUS row shows.
    expect(statusLines.length).toBeGreaterThan(1);
  });

  it('returns the body unchanged (minus trailing newline) when cursor is null', () => {
    expect(composeSeedBody('a\r\nb\r\n', null)).toBe('a\r\nb');
  });
});
