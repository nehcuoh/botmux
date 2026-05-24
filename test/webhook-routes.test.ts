import { createHmac } from 'node:crypto';
import { describe, expect, it } from 'vitest';
import { verifyWebhookSignature } from '../src/dashboard/webhook-routes.js';

describe('webhook route verification helpers', () => {
  it('verifies HMAC over timestamp dot raw-body', () => {
    const ts = '1770000000';
    const raw = Buffer.from('{"ok":true}');
    const mac = createHmac('sha256', 'secret').update(ts).update('.').update(raw).digest();
    expect(verifyWebhookSignature('secret', ts, raw, `sha256=${mac.toString('hex')}`)).toBe(true);
    expect(verifyWebhookSignature('secret', ts, raw, mac.toString('base64url'))).toBe(true);
    expect(verifyWebhookSignature('wrong', ts, raw, mac.toString('base64url'))).toBe(false);
  });
});
