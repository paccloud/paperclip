import { describe, expect, it } from "vitest";
import { createDatabaseBackupInFlightGuard } from "../database-backup-in-flight-guard.js";

/**
 * The regression these cover is specifically *not* "a backup died without
 * releasing its guard" — release-on-failure already worked. It is a backup that
 * never settles at all, so no `finally` anywhere on the stack ever runs. Every
 * test here therefore models the stuck run with a promise that is never
 * resolved, and none of them may await it.
 */
function neverSettles(): Promise<never> {
  return new Promise<never>(() => {});
}

describe("createDatabaseBackupInFlightGuard", () => {
  it("rejects a second run while the first is genuinely still running", () => {
    let nowMs = 1_000;
    const guard = createDatabaseBackupInFlightGuard({ staleAfterMs: 60_000, now: () => nowMs });

    const first = guard.acquire();
    expect(first.ok).toBe(true);

    nowMs += 59_999;
    const second = guard.acquire();
    expect(second.ok).toBe(false);
    if (second.ok) throw new Error("unreachable");
    expect(second.heldForMs).toBe(59_999);
  });

  it("releases the guard for the next run without the stuck one ever settling", () => {
    let nowMs = 1_000;
    const guard = createDatabaseBackupInFlightGuard({ staleAfterMs: 60_000, now: () => nowMs });

    const stuck = guard.acquire();
    expect(stuck.ok).toBe(true);
    // The wedged backup. Nothing observes it again; it never settles.
    const wedged = neverSettles();
    expect(wedged).toBeInstanceOf(Promise);

    nowMs += 6 * 24 * 60 * 60 * 1000; // six days, as measured in production
    const next = guard.acquire();
    expect(next.ok).toBe(true);
    if (!next.ok) throw new Error("unreachable");
    expect(next.tookOverAfterMs).toBe(6 * 24 * 60 * 60 * 1000);
  });

  it("does not report a takeover when nothing was displaced", () => {
    let nowMs = 1_000;
    const guard = createDatabaseBackupInFlightGuard({ staleAfterMs: 60_000, now: () => nowMs });

    const first = guard.acquire();
    if (!first.ok) throw new Error("unreachable");
    expect(first.tookOverAfterMs).toBeNull();
    first.release();

    nowMs += 10;
    const second = guard.acquire();
    if (!second.ok) throw new Error("unreachable");
    expect(second.tookOverAfterMs).toBeNull();
    expect(guard.heldSince()).toBe(nowMs);
  });

  it("ignores a stale run that settles after it was taken over", () => {
    let nowMs = 1_000;
    const guard = createDatabaseBackupInFlightGuard({ staleAfterMs: 60_000, now: () => nowMs });

    const stale = guard.acquire();
    if (!stale.ok) throw new Error("unreachable");

    nowMs += 120_000;
    const current = guard.acquire();
    if (!current.ok) throw new Error("unreachable");

    // The abandoned run finally unwinds. It must not hand the guard away from
    // the run that replaced it, or two backups would overlap.
    stale.release();

    nowMs += 1;
    const third = guard.acquire();
    expect(third.ok).toBe(false);
    expect(guard.heldSince()).toBe(1_000 + 120_000);

    current.release();
    expect(guard.heldSince()).toBeNull();
  });

  it("treats release as idempotent", () => {
    const guard = createDatabaseBackupInFlightGuard({ staleAfterMs: 60_000, now: () => 0 });
    const lease = guard.acquire();
    if (!lease.ok) throw new Error("unreachable");

    lease.release();
    lease.release();
    expect(guard.heldSince()).toBeNull();

    const next = guard.acquire();
    expect(next.ok).toBe(true);
    // The double release above must not have freed *this* lease as well.
    expect(guard.heldSince()).toBe(0);
  });
});
