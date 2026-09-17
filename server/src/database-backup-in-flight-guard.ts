/**
 * Single-flight guard for the database backup.
 *
 * A boolean guard released in a `finally` is not enough. A `finally` runs when
 * a promise *settles*, and a backup can stop settling entirely: a COPY whose
 * consumer stopped draining leaves PostgreSQL blocked writing to the client
 * while Node waits for the query to finish before closing the socket. Observed
 * in production for six days — and terminating the database backend outright
 * cleared the server side without releasing the Node-side guard, so "make the
 * database fail fast" does not cover it either.
 *
 * So the guard carries the instant it was taken and is treated as abandoned
 * past a threshold. Leases are token-checked: a stale run that settles late
 * releases nothing, because by then it no longer holds the lease.
 */

export type DatabaseBackupLease = {
  readonly ok: true;
  /**
   * How long the abandoned predecessor had been holding the guard, or `null`
   * when this lease did not displace anyone.
   */
  readonly tookOverAfterMs: number | null;
  /** Idempotent, and a no-op once another run has taken the lease over. */
  release(): void;
};

export type DatabaseBackupLeaseRejection = {
  readonly ok: false;
  /** How long the current holder has held the guard. */
  readonly heldForMs: number;
};

export type DatabaseBackupGuard = {
  readonly staleAfterMs: number;
  acquire(): DatabaseBackupLease | DatabaseBackupLeaseRejection;
  /** Epoch ms at which the current holder acquired the guard, else `null`. */
  heldSince(): number | null;
};

export function createDatabaseBackupInFlightGuard(options: {
  staleAfterMs: number;
  now?: () => number;
}): DatabaseBackupGuard {
  const now = options.now ?? Date.now;
  const staleAfterMs = Math.max(1, Math.trunc(options.staleAfterMs));
  let holder: { token: symbol; acquiredAtMs: number } | null = null;

  return {
    staleAfterMs,
    heldSince: () => holder?.acquiredAtMs ?? null,
    acquire() {
      const acquiredAtMs = now();
      let tookOverAfterMs: number | null = null;

      if (holder !== null) {
        const heldForMs = acquiredAtMs - holder.acquiredAtMs;
        if (heldForMs < staleAfterMs) {
          return { ok: false, heldForMs };
        }
        tookOverAfterMs = heldForMs;
      }

      const token = Symbol("database-backup-lease");
      holder = { token, acquiredAtMs };

      return {
        ok: true,
        tookOverAfterMs,
        release() {
          // Only the current holder may release. Without this check a run that
          // was declared stale would, on finally settling, clear the lease of
          // the run that replaced it — and two backups could then overlap.
          if (holder?.token === token) holder = null;
        },
      };
    },
  };
}
