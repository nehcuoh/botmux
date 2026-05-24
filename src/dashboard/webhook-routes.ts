import type { IncomingMessage, ServerResponse } from 'node:http';
import { createHmac, timingSafeEqual } from 'node:crypto';
import { getConnector, type ConnectorDefinition } from '../services/connector-store.js';
import { getWebhookSecret } from '../services/webhook-key.js';
import type { TriggerRequest, TriggerResponse } from '../services/trigger-types.js';
import { appendTriggerLog } from '../services/trigger-log-store.js';
import { jsonRes } from './workflow-api.js';
import { dispatchTriggerRequest, newTriggerId, type TriggerApiDeps } from './trigger-api.js';

const replayNonces = new Map<string, number>();
const rateBuckets = new Map<string, { windowStart: number; count: number }>();

function headerValue(req: IncomingMessage, name: string): string | undefined {
  const v = req.headers[name.toLowerCase()];
  return Array.isArray(v) ? v[0] : v;
}

async function readRawBody(req: IncomingMessage, maxBytes: number): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let total = 0;
  for await (const c of req) {
    const b = c as Buffer;
    total += b.length;
    if (total > maxBytes) throw new Error('body_too_large');
    chunks.push(b);
  }
  return Buffer.concat(chunks);
}

function parseSignature(sig: string): Buffer | null {
  const raw = sig.trim().replace(/^sha256=/i, '');
  if (/^[0-9a-f]+$/i.test(raw) && raw.length % 2 === 0) {
    return Buffer.from(raw, 'hex');
  }
  try {
    const b = Buffer.from(raw, 'base64url');
    return b.length > 0 ? b : null;
  } catch {
    return null;
  }
}

export function verifyWebhookSignature(secret: string, ts: string, rawBody: Buffer, sig: string): boolean {
  const expected = createHmac('sha256', secret)
    .update(ts)
    .update('.')
    .update(rawBody)
    .digest();
  const got = parseSignature(sig);
  return !!got && got.length === expected.length && timingSafeEqual(got, expected);
}

function timestampOk(ts: string, toleranceSeconds: number): boolean {
  const n = Number(ts);
  if (!Number.isFinite(n)) return false;
  const tsMs = n > 10_000_000_000 ? n : n * 1000;
  return Math.abs(Date.now() - tsMs) <= toleranceSeconds * 1000;
}

function claimNonce(connectorId: string, nonce: string, ttlSeconds: number): boolean {
  const now = Date.now();
  for (const [key, exp] of replayNonces) {
    if (exp <= now) replayNonces.delete(key);
  }
  const key = `${connectorId}:${nonce}`;
  if (replayNonces.has(key)) return false;
  replayNonces.set(key, now + ttlSeconds * 1000);
  return true;
}

function rateAllowed(connector: ConnectorDefinition): boolean {
  const rl = connector.rateLimit;
  if (!rl || rl.windowSeconds <= 0 || rl.maxRequests <= 0) return true;
  const now = Date.now();
  const cur = rateBuckets.get(connector.id);
  if (!cur || now - cur.windowStart >= rl.windowSeconds * 1000) {
    rateBuckets.set(connector.id, { windowStart: now, count: 1 });
    return true;
  }
  if (cur.count >= rl.maxRequests) return false;
  cur.count += 1;
  return true;
}

function parsePayload(rawBody: Buffer): { payload: unknown; rawText: string } {
  const rawText = rawBody.toString('utf-8');
  try {
    return { payload: JSON.parse(rawText), rawText };
  } catch {
    return { payload: undefined, rawText };
  }
}

function pickAllowedHeaders(req: IncomingMessage, allowlist: string[]): Record<string, string> {
  const out: Record<string, string> = {};
  for (const h of allowlist) {
    const v = headerValue(req, h);
    if (typeof v === 'string') out[h.toLowerCase()] = v;
  }
  return out;
}

function dynamicChatId(req: IncomingMessage, url: URL, payload: unknown): string | undefined {
  const fromQuery = url.searchParams.get('chatId') ?? undefined;
  if (fromQuery) return fromQuery;
  const fromHeader = headerValue(req, 'x-botmux-chat-id');
  if (fromHeader) return fromHeader;
  if (payload && typeof payload === 'object' && !Array.isArray(payload)) {
    const p = payload as any;
    if (typeof p.chatId === 'string') return p.chatId;
    if (p.target && typeof p.target === 'object' && typeof p.target.chatId === 'string') return p.target.chatId;
  }
  return undefined;
}

function webhookError(
  res: ServerResponse,
  status: number,
  connectorId: string | undefined,
  errorCode: TriggerResponse['errorCode'],
  error: string,
): void {
  appendTriggerLog({
    triggerId: newTriggerId(),
    connectorId,
    action: 'failed',
    status: 'error',
    error,
    errorCode,
  });
  jsonRes(res, status, { ok: false, errorCode, error });
}

export async function handleWebhookRoute(
  req: IncomingMessage,
  res: ServerResponse,
  url: URL,
  deps: TriggerApiDeps,
): Promise<boolean> {
  const m = url.pathname.match(/^\/webhook\/([^/]+)$/);
  if (!m) return false;
  if (req.method !== 'POST') {
    jsonRes(res, 405, { ok: false, errorCode: 'bad_request', error: 'method not allowed' });
    return true;
  }

  const connectorId = decodeURIComponent(m[1]);
  const connector = getConnector(connectorId);
  if (!connector || !connector.enabled) {
    webhookError(res, 404, connectorId, 'bad_request', 'unknown or disabled connector');
    return true;
  }

  if (!rateAllowed(connector)) {
    webhookError(res, 429, connectorId, 'rate_limited', 'connector rate limit exceeded');
    return true;
  }

  let rawBody: Buffer;
  try {
    rawBody = await readRawBody(req, connector.promptEnvelope.maxBodyBytes);
  } catch {
    webhookError(res, 413, connectorId, 'bad_request', 'request body too large');
    return true;
  }

  const verify = connector.verify;
  const ts = headerValue(req, verify.timestampHeader);
  const nonce = headerValue(req, verify.nonceHeader);
  const sig = headerValue(req, verify.signatureHeader);
  if (!ts || !nonce || !sig) {
    webhookError(res, 401, connectorId, 'invalid_signature', 'missing signature, timestamp, or nonce header');
    return true;
  }
  if (!timestampOk(ts, verify.toleranceSeconds)) {
    webhookError(res, 401, connectorId, 'replay', 'timestamp outside tolerance window');
    return true;
  }
  if (!claimNonce(connector.id, nonce, verify.toleranceSeconds)) {
    webhookError(res, 409, connectorId, 'replay', 'nonce replay detected');
    return true;
  }
  const secret = getWebhookSecret(verify.secretRef);
  if (!secret || !verifyWebhookSignature(secret, ts, rawBody, sig)) {
    webhookError(res, 401, connectorId, 'invalid_signature', 'signature verification failed');
    return true;
  }

  if (connector.target.mode === 'new-group') {
    webhookError(res, 501, connectorId, 'workflow_trigger_not_implemented', 'new-group lifecycle is reserved for P2');
    return true;
  }

  const parsed = parsePayload(rawBody);
  const chatId = connector.target.mode === 'fixed'
    ? connector.target.chatId
    : dynamicChatId(req, url, parsed.payload);
  if (!chatId) {
    webhookError(res, 400, connectorId, 'target_required', 'target chatId is required');
    return true;
  }
  const allowChats = connector.target.allowChats ?? [];
  if (allowChats.length > 0 && !allowChats.includes(chatId)) {
    webhookError(res, 403, connectorId, 'chat_not_allowed', 'chatId is not allowed for this connector');
    return true;
  }

  const trigger: TriggerRequest = {
    source: {
      type: 'webhook',
      connectorId: connector.id,
      requestId: nonce,
      receivedAt: new Date().toISOString(),
    },
    target: {
      kind: connector.target.kind,
      botId: connector.target.botId,
      chatId,
      workflowId: connector.target.workflowId,
    },
    envelope: {
      format: 'botmux.webhook.v1',
      sourceName: connector.promptEnvelope.sourceName || connector.name,
      trusted: false,
      headers: pickAllowedHeaders(req, connector.promptEnvelope.headerAllowlist),
      payload: parsed.payload,
      ...(connector.promptEnvelope.includeRawText ? { rawText: parsed.rawText } : {}),
    },
    options: {},
  };

  const result = await dispatchTriggerRequest(trigger, deps);
  jsonRes(res, result.status, result.body);
  return true;
}
