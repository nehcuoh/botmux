/**
 * Oncall bindings — persist chat_id → default workingDir into the bot config
 * JSON file, and keep the in-memory BotConfig in sync so events pick up
 * changes without a daemon restart.
 *
 * Permission model is intentionally simple: anyone in the bot's allowedUsers
 * can bind/unbind/edit (enforced at the call sites — daemon command handler
 * + dashboard token gate). No per-chat owner list.
 */
import { readFileSync, writeFileSync } from 'node:fs';
import { getBot, getLoadedConfigPath, type OncallChat } from '../bot-registry.js';
import { logger } from '../utils/logger.js';

function loadRawConfig(): { path: string; raw: any[] } {
  const path = getLoadedConfigPath();
  if (!path) throw new Error('Bot config path unknown — cannot persist oncall bindings');
  const raw = JSON.parse(readFileSync(path, 'utf-8'));
  if (!Array.isArray(raw)) throw new Error(`Config file is not a JSON array: ${path}`);
  return { path, raw };
}

function writeRawConfig(path: string, raw: any[]): void {
  writeFileSync(path, JSON.stringify(raw, null, 2) + '\n', 'utf-8');
}

function findEntryIndex(raw: any[], larkAppId: string): number {
  return raw.findIndex((e: any) => e?.larkAppId === larkAppId);
}

/**
 * Upsert an oncall binding. Returns whether it was newly created.
 */
export function bindOncall(
  larkAppId: string,
  chatId: string,
  workingDir: string,
): { ok: true; entry: OncallChat; created: boolean } | { ok: false; reason: string } {
  const bot = getBot(larkAppId);
  const existingList = bot.config.oncallChats ?? [];
  const existing = existingList.find(c => c.chatId === chatId);

  const next: OncallChat = { chatId, workingDir };

  const { path, raw } = loadRawConfig();
  const idx = findEntryIndex(raw, larkAppId);
  if (idx < 0) return { ok: false, reason: 'bot_not_in_config' };

  const cur: OncallChat[] = Array.isArray(raw[idx].oncallChats) ? raw[idx].oncallChats : [];
  const curIdx = cur.findIndex((c: OncallChat) => c.chatId === chatId);
  if (curIdx >= 0) {
    // Preserve any unknown keys the user might have added by hand.
    cur[curIdx] = { ...cur[curIdx], ...next };
  } else {
    cur.push(next);
  }
  raw[idx].oncallChats = cur;
  writeRawConfig(path, raw);

  // Keep in-memory config in sync
  const inMem = (bot.config.oncallChats ??= []);
  const memIdx = inMem.findIndex(c => c.chatId === chatId);
  if (memIdx >= 0) inMem[memIdx] = next; else inMem.push(next);

  logger.info(`[oncall:${larkAppId}] bind chat=${chatId} dir=${workingDir}`);
  return { ok: true, entry: next, created: !existing };
}

export function unbindOncall(
  larkAppId: string,
  chatId: string,
): { ok: true } | { ok: false; reason: string } {
  const bot = getBot(larkAppId);
  const existing = bot.config.oncallChats?.find(c => c.chatId === chatId);
  if (!existing) return { ok: false, reason: 'not_bound' };

  const { path, raw } = loadRawConfig();
  const idx = findEntryIndex(raw, larkAppId);
  if (idx < 0) return { ok: false, reason: 'bot_not_in_config' };
  const cur: OncallChat[] = Array.isArray(raw[idx].oncallChats) ? raw[idx].oncallChats : [];
  raw[idx].oncallChats = cur.filter((c: OncallChat) => c.chatId !== chatId);
  writeRawConfig(path, raw);

  if (bot.config.oncallChats) {
    bot.config.oncallChats = bot.config.oncallChats.filter(c => c.chatId !== chatId);
  }
  logger.info(`[oncall:${larkAppId}] unbind chat=${chatId}`);
  return { ok: true };
}

export function getOncallStatus(larkAppId: string, chatId: string): OncallChat | undefined {
  // Defensive: dashboard callers may probe with an app id whose bot isn't
  // registered yet (boot races, or tests exercising the IPC layer without
  // a full registry). Treat "no such bot" as "no oncall binding" — this
  // is best-effort enrichment, not a critical path.
  let bot;
  try { bot = getBot(larkAppId); } catch { return undefined; }
  return bot.config.oncallChats?.find(c => c.chatId === chatId);
}
