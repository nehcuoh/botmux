import type { Server } from 'node:http';

export interface ListenWithProbeOpts {
  server: Server;
  /** Preferred port to try first. */
  port: number;
  host: string;
  /** Max upward probes on EADDRINUSE before rejecting (default 20). */
  maxProbe?: number;
  log?: (msg: string) => void;
}

/**
 * Bind `server` to `port`, walking port+1, port+2 … up to `maxProbe` times when
 * the port is already in use, and resolve with the actually-bound port.
 *
 * Why this exists: several daemon/dashboard listeners (dashboard-ipc-server.ts,
 * dashboard.ts) historically did a single `server.listen(fixedPort)` with no
 * 'error' listener / no probe, so on a shared machine a second botmux instance
 * binding the same default port emitted an UNHANDLED 'error' that crashed the
 * whole process (the IPC bind even took the daemon down at startup). This
 * mirrors the already-proven probe in core/terminal-proxy.ts so those binds
 * self-heal to a free port; callers MUST advertise the returned (bound) port to
 * their consumers (the IPC port via the daemon descriptor, the dashboard port
 * via ~/.botmux/.dashboard-port) since it may differ from the requested one.
 */
export function listenWithProbe(opts: ListenWithProbeOpts): Promise<number> {
  const { server, host } = opts;
  const maxProbe = opts.maxProbe ?? 20;
  const log = opts.log ?? (() => { /* noop */ });

  return new Promise<number>((resolve, reject) => {
    let port = opts.port;
    let attempts = 0;
    let settled = false;

    // Single persistent handlers reused across every probe attempt. Passing a
    // callback to server.listen() would instead add a fresh one-time
    // 'listening' listener on each retry that is never removed on a failed
    // bind, leaking listeners (MaxListenersExceededWarning past 10 probes) and
    // firing every stale callback once a bind finally succeeds.
    const cleanup = () => {
      server.removeListener('listening', onListening);
      server.removeListener('error', onError);
    };
    const onListening = () => {
      if (settled) return;
      settled = true;
      cleanup();
      const addr = server.address();
      const bound = typeof addr === 'object' && addr ? addr.port : port;
      // Keep a permanent handler so a post-bind runtime error can't become an
      // unhandled 'error' event (which would crash the process).
      server.on('error', (e) => log(`server error: ${(e as Error).message}`));
      resolve(bound);
    };
    const onError = (err: NodeJS.ErrnoException) => {
      if (settled) return;
      if (err.code === 'EADDRINUSE' && attempts < maxProbe) {
        attempts++;
        log(`port ${port} in use, trying ${port + 1}`);
        port++;
        setImmediate(() => { if (!settled) server.listen(port, host); });
        return;
      }
      settled = true;
      cleanup();
      reject(err);
    };

    server.on('listening', onListening);
    server.on('error', onError);
    server.listen(port, host);
  });
}
