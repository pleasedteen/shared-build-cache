import fs from 'node:fs';
import path from 'node:path';
import { randomUUID, createHash } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import {
  casPathFor,
  ensureDir,
  isProcessAlive,
  normalizeRelativePath,
  pidStartTicks,
  resolveInside,
  tempName,
} from './util.ts';
import { verifiedCopyToTemp } from './hash-copy.ts';
import {
  CapacityError,
  ChecksumError,
  ConflictError,
  NotFoundError,
  SessionClosedError,
} from './errors.ts';
import type {
  ArtifactFile,
  ExportResult,
  RecoveryStats,
  UsageInfo,
  VersionInfo,
  VersionStatus,
} from './types.ts';

const DEFAULT_MAX_BYTES = 1024 * 1024 * 1024;

const SCHEMA = `
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=20000;

CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS owners (
  id TEXT PRIMARY KEY,
  pid INTEGER NOT NULL,
  pid_start INTEGER,
  created_at INTEGER NOT NULL,
  closed_at INTEGER,
  reaped_at INTEGER
);

CREATE TABLE IF NOT EXISTS build_keys (
  key TEXT PRIMARY KEY,
  current_version_id INTEGER
);

CREATE TABLE IF NOT EXISTS versions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  build_key TEXT NOT NULL,
  version_no INTEGER NOT NULL,
  status TEXT NOT NULL,
  owner_id TEXT,
  base_version_id INTEGER,
  upload_id TEXT UNIQUE,
  staging_dir TEXT,
  total_size INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  published_at INTEGER,
  committed_at INTEGER,
  aborted_at INTEGER,
  evicted_at INTEGER,
  last_successful_export_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_versions_key ON versions(build_key, id);
CREATE INDEX IF NOT EXISTS idx_versions_status_owner ON versions(status, owner_id);
CREATE INDEX IF NOT EXISTS idx_versions_eviction
  ON versions(status, last_successful_export_at, published_at, committed_at, created_at, id);

CREATE TABLE IF NOT EXISTS publish_files (
  version_id INTEGER NOT NULL REFERENCES versions(id),
  path TEXT NOT NULL,
  tmp_path TEXT NOT NULL,
  size INTEGER NOT NULL,
  mode INTEGER,
  bytes_written INTEGER NOT NULL DEFAULT 0,
  expected_sha256 TEXT,
  sha256 TEXT,
  complete INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY(version_id, path)
);

CREATE TABLE IF NOT EXISTS blobs (
  sha256 TEXT PRIMARY KEY,
  size INTEGER NOT NULL,
  created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS files (
  version_id INTEGER NOT NULL REFERENCES versions(id),
  path TEXT NOT NULL,
  sha256 TEXT NOT NULL REFERENCES blobs(sha256),
  size INTEGER NOT NULL,
  mode INTEGER,
  PRIMARY KEY(version_id, path)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_files_sha ON files(sha256);

CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY,
  owner_id TEXT NOT NULL REFERENCES owners(id),
  build_key TEXT NOT NULL,
  version_id INTEGER NOT NULL REFERENCES versions(id),
  created_at INTEGER NOT NULL,
  closed_at INTEGER,
  close_reason TEXT,
  last_export_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_sessions_owner_open ON sessions(owner_id, closed_at);
CREATE INDEX IF NOT EXISTS idx_sessions_version ON sessions(version_id);
CREATE INDEX IF NOT EXISTS idx_sessions_open ON sessions(closed_at, version_id);

CREATE TABLE IF NOT EXISTS cleanup_queue (
  sha256 TEXT PRIMARY KEY,
  reason TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
`;

export interface OpenCacheOptions {
  maxBytes?: number;
  now?: () => number;
}

export interface BeginPublishOptions {
  key: string;
  totalSize: number;
}

export interface OpenFileOptions {
  size: number;
  expectedSha256?: string;
  mode?: number;
}

export interface PublishBufferFile {
  path: string;
  content: Buffer | Uint8Array | string;
  sha256?: string;
  mode?: number;
}

function newRecoveryStats(now: number): RecoveryStats {
  return {
    startedAt: now,
    finishedAt: now,
    candidateOwnersChecked: 0,
    aliveOwnersSkipped: 0,
    deadOwnersReaped: 0,
    uploadsAborted: 0,
    sessionsReleased: 0,
    versionRecordsChecked: 0,
    artifactFilesRead: 0,
    artifactFilesUnlinked: 0,
    cleanupItemsProcessed: 0,
    stagingDirectoriesRemoved: 0,
    errors: [],
  };
}

function mapVersionRow(row: any): VersionInfo {
  return {
    id: Number(row.id),
    buildKey: row.build_key,
    versionNo: Number(row.version_no),
    status: row.status as VersionStatus,
    totalSize: Number(row.total_size),
    createdAt: Number(row.created_at),
    publishedAt: row.published_at == null ? null : Number(row.published_at),
    lastSuccessfulExportAt:
      row.last_successful_export_at == null ? null : Number(row.last_successful_export_at),
    files: [],
  };
}

export class BuildCache {
  readonly root: string;
  readonly db: DatabaseSync;
  readonly ownerId: string;
  readonly startupRecovery: RecoveryStats;
  private maxBytesValue: number;
  private readonly now: () => number;
  private closed = false;

  private constructor(
    root: string,
    db: DatabaseSync,
    ownerId: string,
    maxBytes: number,
    now: () => number,
    startupRecovery: RecoveryStats,
  ) {
    this.root = root;
    this.db = db;
    this.ownerId = ownerId;
    this.maxBytesValue = maxBytes;
    this.now = now;
    this.startupRecovery = startupRecovery;
  }

  static open(rootInput: string, options: OpenCacheOptions = {}): BuildCache {
    const root = path.resolve(rootInput);
    ensureDir(root);
    ensureDir(path.join(root, 'cas'));
    ensureDir(path.join(root, 'staging'));

    const db = new DatabaseSync(path.join(root, 'cache.sqlite'));
    db.exec(SCHEMA);

    const now = options.now ?? (() => Date.now());
    let maxBytes: number;
    const existing = db.prepare(`SELECT value FROM meta WHERE key='max_bytes'`).get() as
      | { value: string }
      | undefined;

    if (options.maxBytes !== undefined) {
      if (!Number.isSafeInteger(options.maxBytes) || options.maxBytes <= 0) {
        db.close();
        throw new Error('maxBytes must be a positive safe integer');
      }
      maxBytes = options.maxBytes;
      db.prepare(
        `INSERT INTO meta(key,value) VALUES('max_bytes',?)
         ON CONFLICT(key) DO UPDATE SET value=excluded.value`,
      ).run(String(maxBytes));
    } else if (existing) {
      maxBytes = Number(existing.value);
    } else {
      maxBytes = DEFAULT_MAX_BYTES;
      db.prepare(`INSERT INTO meta(key,value) VALUES('max_bytes',?)`).run(String(maxBytes));
    }

    const ownerId = randomUUID();
    db.prepare(
      `INSERT INTO owners(id,pid,pid_start,created_at) VALUES(?,?,?,?)`,
    ).run(ownerId, process.pid, pidStartTicks(process.pid), now());

    const cache = new BuildCache(root, db, ownerId, maxBytes, now, newRecoveryStats(now()));
    const recovery = cache.recoverDeadOwners();
    (cache as { startupRecovery: RecoveryStats }).startupRecovery = recovery;
    return cache;
  }

  get maxBytes(): number {
    return this.maxBytesValue;
  }

  timestamp(): number {
    return this.now();
  }

  setMaxBytes(maxBytes: number): void {
    if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0) {
      throw new Error('maxBytes must be a positive safe integer');
    }
    this.maxBytesValue = maxBytes;
    this.db
      .prepare(
        `INSERT INTO meta(key,value) VALUES('max_bytes',?)
         ON CONFLICT(key) DO UPDATE SET value=excluded.value`,
      )
      .run(String(maxBytes));
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    try {
      const now = this.now();
      const active = this.db
        .prepare(`SELECT id, staging_dir FROM versions WHERE owner_id=? AND status='staging'`)
        .all(this.ownerId) as Array<{ id: number; staging_dir: string | null }>;
      for (const row of active) {
        if (row.staging_dir) fs.rmSync(row.staging_dir, { recursive: true, force: true });
      }
      this.db
        .prepare(
          `UPDATE versions SET status='aborted', aborted_at=?
           WHERE owner_id=? AND status='staging'`,
        )
        .run(now, this.ownerId);
      this.db
        .prepare(
          `UPDATE sessions SET closed_at=?, close_reason='cache_close'
           WHERE owner_id=? AND closed_at IS NULL`,
        )
        .run(now, this.ownerId);
      this.db.prepare(`UPDATE owners SET closed_at=? WHERE id=?`).run(now, this.ownerId);
    } finally {
      this.db.close();
    }
  }

  writeTx<T>(fn: () => T): T {
    if (this.closed) throw new Error('cache is closed');
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const result = fn();
      this.db.exec('COMMIT');
      return result;
    } catch (error) {
      try {
        this.db.exec('ROLLBACK');
      } catch {
        /* transaction may already be gone */
      }
      throw error;
    }
  }

  isOwnerAlive(ownerId: string): boolean {
    const row = this.db
      .prepare(`SELECT pid,pid_start,closed_at,reaped_at FROM owners WHERE id=?`)
      .get(ownerId) as
      | { pid: number; pid_start: number | null; closed_at: number | null; reaped_at: number | null }
      | undefined;
    if (!row || row.closed_at != null || row.reaped_at != null) return false;
    return isProcessAlive(Number(row.pid), row.pid_start == null ? null : Number(row.pid_start));
  }

  recoverDeadOwners(): RecoveryStats {
    const stats = newRecoveryStats(this.now());
    const checkedVersions = new Set<number>();

    try {
      this.processCleanupQueue(stats);
    } catch (error) {
      stats.errors.push(`cleanup: ${(error as Error).message}`);
    }

    const candidates = this.db
      .prepare(
        `SELECT owner_id FROM versions
         WHERE status IN ('staging','committing') AND owner_id IS NOT NULL
         UNION
         SELECT owner_id FROM sessions
         WHERE closed_at IS NULL AND owner_id IS NOT NULL`,
      )
      .all() as Array<{ owner_id: string }>;

    for (const candidate of candidates) {
      stats.candidateOwnersChecked += 1;
      let alive = false;
      try {
        alive = this.isOwnerAlive(candidate.owner_id);
      } catch (error) {
        stats.errors.push(`owner ${candidate.owner_id}: ${(error as Error).message}`);
      }

      if (alive) {
        stats.aliveOwnersSkipped += 1;
        continue;
      }

      try {
        this.reapDeadOwner(candidate.owner_id, stats, checkedVersions);
      } catch (error) {
        stats.errors.push(`reap ${candidate.owner_id}: ${(error as Error).message}`);
      }
    }

    stats.versionRecordsChecked = checkedVersions.size;
    stats.finishedAt = this.now();
    return stats;
  }

  private reapDeadOwner(
    ownerId: string,
    stats: RecoveryStats,
    checkedVersions: Set<number>,
  ): void {
    const publishes = this.db
      .prepare(
        `SELECT id, staging_dir FROM versions
         WHERE owner_id=? AND status IN ('staging','committing')`,
      )
      .all(ownerId) as Array<{ id: number; staging_dir: string | null }>;

    for (const publish of publishes) {
      checkedVersions.add(Number(publish.id));
      if (publish.staging_dir) {
        try {
          fs.rmSync(publish.staging_dir, { recursive: true, force: true });
          stats.stagingDirectoriesRemoved += 1;
        } catch (error) {
          stats.errors.push(`rm staging ${publish.id}: ${(error as Error).message}`);
        }
      }

      const placed = this.db
        .prepare(
          `SELECT sha256 FROM publish_files
           WHERE version_id=? AND complete=1 AND sha256 IS NOT NULL`,
        )
        .all(publish.id) as Array<{ sha256: string }>;
      for (const file of placed) {
        this.removeOrphanCasAfterCrash(file.sha256, stats);
      }

      this.writeTx(() => {
        this.db
          .prepare(
            `UPDATE versions SET status='aborted', aborted_at=?
             WHERE id=? AND status IN ('staging','committing')`,
          )
          .run(this.now(), publish.id);
      });
      stats.uploadsAborted += 1;
    }

    const sessions = this.db
      .prepare(
        `SELECT id, version_id FROM sessions
         WHERE owner_id=? AND closed_at IS NULL`,
      )
      .all(ownerId) as Array<{ id: string; version_id: number }>;

    for (const session of sessions) {
      checkedVersions.add(Number(session.version_id));
      this.db
        .prepare(
          `UPDATE sessions SET closed_at=?, close_reason='owner_dead'
           WHERE id=? AND closed_at IS NULL`,
        )
        .run(this.now(), session.id);
      stats.sessionsReleased += 1;
    }

    this.db
      .prepare(`UPDATE owners SET reaped_at=? WHERE id=? AND reaped_at IS NULL`)
      .run(this.now(), ownerId);
    stats.deadOwnersReaped += 1;
  }

  private removeOrphanCasAfterCrash(sha256: string, stats: RecoveryStats): void {
    try {
      this.writeTx(() => {
        const blob = this.db.prepare(`SELECT 1 FROM blobs WHERE sha256=?`).get(sha256);
        if (blob) return; // A committed/concurrent record now owns this CAS path.
        const target = casPathFor(this.root, sha256);
        try {
          fs.unlinkSync(target);
          stats.artifactFilesUnlinked += 1;
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
        }
      });
    } catch (error) {
      stats.errors.push(`orphan cas ${sha256}: ${(error as Error).message}`);
    }
  }

  processCleanupQueue(stats?: RecoveryStats): number {
    let processed = 0;
    for (;;) {
      const item = this.db
        .prepare(`SELECT sha256 FROM cleanup_queue ORDER BY created_at, rowid LIMIT 1`)
        .get() as { sha256: string } | undefined;
      if (!item) break;

      this.writeTx(() => {
        const refRow = this.db
          .prepare(`SELECT COUNT(*) AS count FROM files WHERE sha256=?`)
          .get(item.sha256) as { count: number | bigint };
        if (Number(refRow.count) > 0) {
          this.db.prepare(`DELETE FROM cleanup_queue WHERE sha256=?`).run(item.sha256);
          return;
        }

        const target = casPathFor(this.root, item.sha256);
        try {
          fs.unlinkSync(target);
          if (stats) stats.artifactFilesUnlinked += 1;
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
        }

        this.db.prepare(`DELETE FROM cleanup_queue WHERE sha256=?`).run(item.sha256);
        this.db.prepare(`DELETE FROM blobs WHERE sha256=?`).run(item.sha256);
      });

      processed += 1;
      if (stats) stats.cleanupItemsProcessed += 1;
    }
    return processed;
  }

  getUsage(): UsageInfo {
    const blob = this.db
      .prepare(`SELECT COALESCE(SUM(size),0) AS bytes, COUNT(*) AS count FROM blobs`)
      .get() as { bytes: number | bigint; count: number | bigint };
    const queue = this.db
      .prepare(
        `SELECT COALESCE(SUM(b.size),0) AS bytes
         FROM cleanup_queue q JOIN blobs b ON b.sha256=q.sha256`,
      )
      .get() as { bytes: number | bigint };
    const staging = this.db
      .prepare(
        `SELECT COALESCE(SUM(total_size),0) AS bytes, COUNT(*) AS count
         FROM versions WHERE status='staging'`,
      )
      .get() as { bytes: number | bigint; count: number | bigint };
    const written = this.db
      .prepare(
        `SELECT COALESCE(SUM(pf.bytes_written),0) AS bytes
         FROM publish_files pf JOIN versions v ON v.id=pf.version_id
         WHERE v.status='staging'`,
      )
      .get() as { bytes: number | bigint };

    const casBytes = Number(blob.bytes);
    const stagingReservedBytes = Number(staging.bytes);
    const usedBytes = casBytes + stagingReservedBytes;
    return {
      maxBytes: this.maxBytesValue,
      usedBytes,
      casBytes,
      stagingReservedBytes,
      stagingWrittenBytes: Number(written.bytes),
      pendingCleanupBytes: Number(queue.bytes),
      blobCount: Number(blob.count),
      activePublishCount: Number(staging.count),
      availableBytes: this.maxBytesValue - usedBytes,
    };
  }

  private protectedVersionIds(): Set<number> {
    const ids = new Set<number>();
    const currents = this.db
      .prepare(`SELECT current_version_id AS id FROM build_keys WHERE current_version_id IS NOT NULL`)
      .all() as Array<{ id: number | bigint }>;
    currents.forEach((row) => ids.add(Number(row.id)));

    const sessions = this.db
      .prepare(`SELECT version_id AS id FROM sessions WHERE closed_at IS NULL`)
      .all() as Array<{ id: number | bigint }>;
    sessions.forEach((row) => ids.add(Number(row.id)));

    const replacing = this.db
      .prepare(
        `SELECT base_version_id AS id FROM versions
         WHERE status='staging' AND base_version_id IS NOT NULL`,
      )
      .all() as Array<{ id: number | bigint }>;
    replacing.forEach((row) => ids.add(Number(row.id)));

    return ids;
  }

  /**
   * Plans the oldest-first eviction prefix without mutating anything.
   * Refcount simulation correctly handles blobs shared by several versions.
   */
  private planEvictions(deficitBytes: number): { victims: number[]; reclaimableBytes: number } {
    const protectedIds = this.protectedVersionIds();
    const rows = this.db
      .prepare(
        `SELECT id FROM versions
         WHERE status='committed'
         ORDER BY COALESCE(last_successful_export_at, published_at, committed_at, created_at) ASC, id ASC`,
      )
      .all() as Array<{ id: number | bigint }>;

    const candidates = rows.map((r) => Number(r.id)).filter((id) => !protectedIds.has(id));
    const refCounts = new Map<string, number>();
    const blobSizes = new Map<string, number>();
    const victims: number[] = [];
    let reclaimableBytes = 0;

    const refCount = (sha: string): number => {
      const cached = refCounts.get(sha);
      if (cached !== undefined) return cached;
      const row = this.db
        .prepare(`SELECT COUNT(*) AS count FROM files WHERE sha256=?`)
        .get(sha) as { count: number | bigint };
      const value = Number(row.count);
      refCounts.set(sha, value);
      return value;
    };

    for (const versionId of candidates) {
      if (reclaimableBytes >= deficitBytes) break;
      const files = this.db
        .prepare(`SELECT sha256, size FROM files WHERE version_id=?`)
        .all(versionId) as Array<{ sha256: string; size: number | bigint }>;

      let freedByVersion = 0;
      for (const file of files) {
        const sha = file.sha256;
        blobSizes.set(sha, Number(file.size));
        const before = refCount(sha);
        const after = before - 1;
        refCounts.set(sha, after);
        if (before === 1 && after === 0) {
          freedByVersion += Number(file.size);
        }
      }

      reclaimableBytes += freedByVersion;
      victims.push(versionId);
    }

    return { victims, reclaimableBytes };
  }

  beginPublish(options: BeginPublishOptions): PublishHandle {
    const buildKey = String(options.key ?? '');
    if (!buildKey) throw new Error('publish key must be a non-empty string');
    const totalSize = options.totalSize;
    if (!Number.isSafeInteger(totalSize) || totalSize < 0) {
      throw new Error('totalSize must be a non-negative safe integer');
    }

    // Reap dead owners first so their reservations and pins do not leak quota.
    this.recoverDeadOwners();
    this.processCleanupQueue();

    const uploadId = randomUUID();
    const stagingDir = path.join(this.root, 'staging', uploadId);
    let versionId = -1;

    this.writeTx(() => {
      const blobRow = this.db
        .prepare(`SELECT COALESCE(SUM(size),0) AS bytes FROM blobs`)
        .get() as { bytes: number | bigint };
      const queueRow = this.db
        .prepare(
          `SELECT COALESCE(SUM(b.size),0) AS bytes
           FROM cleanup_queue q JOIN blobs b ON b.sha256=q.sha256`,
        )
        .get() as { bytes: number | bigint };
      const stagingRow = this.db
        .prepare(
          `SELECT COALESCE(SUM(total_size),0) AS bytes
           FROM versions WHERE status='staging'`,
        )
        .get() as { bytes: number | bigint };

      const blobBytes = Number(blobRow.bytes);
      const queueBytes = Number(queueRow.bytes);
      const stagingBytes = Number(stagingRow.bytes);
      const projected = blobBytes - queueBytes + stagingBytes + totalSize;
      let victims: number[] = [];

      if (projected > this.maxBytesValue) {
        const deficit = projected - this.maxBytesValue;
        const plan = this.planEvictions(deficit);
        if (plan.reclaimableBytes < deficit) {
          throw new CapacityError(
            `publish for '${buildKey}' needs ${totalSize} bytes but cache cannot free enough space`,
            {
              code: 'CAPACITY_EXCEEDED',
              maxBytes: this.maxBytesValue,
              totalSize,
              currentCasBytes: blobBytes,
              pendingCleanupBytes: queueBytes,
              activePublishReservedBytes: stagingBytes,
              requiredToReclaimBytes: deficit,
              maxReclaimableBytes: plan.reclaimableBytes,
            },
          );
        }
        victims = plan.victims;
      }

      const now = this.now();
      for (const victimId of victims) this.evictVersionInTx(victimId, now);

      const current = this.db
        .prepare(`SELECT current_version_id AS id FROM build_keys WHERE key=?`)
        .get(buildKey) as { id: number | bigint | null } | undefined;
      const baseVersionId = current?.id == null ? null : Number(current.id);

      const versionNoRow = this.db
        .prepare(`SELECT COALESCE(MAX(version_no),0)+1 AS next FROM versions WHERE build_key=?`)
        .get(buildKey) as { next: number | bigint };

      const result = this.db
        .prepare(
          `INSERT INTO versions(
             build_key, version_no, status, owner_id, base_version_id,
             upload_id, staging_dir, total_size, created_at
           ) VALUES(?,?, 'staging', ?,?,?,?,?,?)`,
        )
        .run(
          buildKey,
          Number(versionNoRow.next),
          this.ownerId,
          baseVersionId,
          uploadId,
          stagingDir,
          totalSize,
          now,
        );
      versionId = Number(result.lastInsertRowid);
    });

    ensureDir(stagingDir);
    this.processCleanupQueue();
    return new PublishHandle(this, versionId, uploadId, buildKey, totalSize, stagingDir);
  }

  private evictVersionInTx(versionId: number, now: number): void {
    const shas = this.db
      .prepare(`SELECT DISTINCT sha256 AS sha FROM files WHERE version_id=?`)
      .all(versionId) as Array<{ sha: string }>;

    this.db.prepare(`DELETE FROM files WHERE version_id=?`).run(versionId);
    const change = this.db
      .prepare(
        `UPDATE versions SET status='evicted', evicted_at=?
         WHERE id=? AND status='committed'`,
      )
      .run(now, versionId);
    if (Number(change.changes) === 0) {
      throw new ConflictError(`version ${versionId} is not a committed eviction candidate`);
    }

    for (const row of shas) {
      const ref = this.db
        .prepare(`SELECT COUNT(*) AS count FROM files WHERE sha256=?`)
        .get(row.sha) as { count: number | bigint };
      if (Number(ref.count) === 0) {
        this.db
          .prepare(`INSERT OR IGNORE INTO cleanup_queue(sha256,reason,created_at) VALUES(?,?,?)`)
          .run(row.sha, 'evicted-version', now);
      }
    }
  }

  getVersionInfo(versionId: number): VersionInfo {
    const row = this.db.prepare(`SELECT * FROM versions WHERE id=?`).get(versionId) as any;
    if (!row) throw new NotFoundError(`version ${versionId} not found`);
    const info = mapVersionRow(row);
    const files = this.db
      .prepare(`SELECT path,sha256,size,mode FROM files WHERE version_id=? ORDER BY path`)
      .all(versionId) as ArtifactFile[];
    info.files = files.map((f) => ({
      path: f.path,
      sha256: f.sha256,
      size: Number(f.size),
      mode: f.mode == null ? null : Number(f.mode),
    }));
    return info;
  }

  getCurrentVersion(buildKey: string): VersionInfo | null {
    const row = this.db
      .prepare(
        `SELECT v.id FROM build_keys k
         JOIN versions v ON v.id=k.current_version_id
         WHERE k.key=? AND v.status='committed'`,
      )
      .get(buildKey) as { id: number | bigint } | undefined;
    return row ? this.getVersionInfo(Number(row.id)) : null;
  }

  openSession(buildKey: string): FetchSession {
    const current = this.getCurrentVersion(buildKey);
    if (!current) throw new NotFoundError(`no committed version for key '${buildKey}'`);
    return this.createSession(buildKey, current.id);
  }

  openSessionForVersion(versionId: number): FetchSession {
    const info = this.getVersionInfo(versionId);
    if (info.status !== 'committed') {
      throw new NotFoundError(`version ${versionId} is ${info.status}, not committed`);
    }
    return this.createSession(info.buildKey, info.id);
  }

  private createSession(buildKey: string, versionId: number): FetchSession {
    const id = randomUUID();
    this.db
      .prepare(
        `INSERT INTO sessions(id,owner_id,build_key,version_id,created_at)
         VALUES(?,?,?,?,?)`,
      )
      .run(id, this.ownerId, buildKey, versionId, this.now());
    return new FetchSession(this, id, buildKey, versionId);
  }

  requireSessionActive(sessionId: string): { id: string; version_id: number; build_key: string } {
    const row = this.db
      .prepare(`SELECT id,version_id,build_key,closed_at FROM sessions WHERE id=?`)
      .get(sessionId) as
      | { id: string; version_id: number; build_key: string; closed_at: number | null }
      | undefined;
    if (!row) throw new NotFoundError(`session ${sessionId} not found`);
    if (row.closed_at != null) throw new SessionClosedError();
    return row;
  }

  closeSession(sessionId: string, reason = 'closed'): void {
    this.db
      .prepare(
        `UPDATE sessions SET closed_at=?, close_reason=?
         WHERE id=? AND closed_at IS NULL`,
      )
      .run(this.now(), reason, sessionId);
  }

  noteSuccessfulExport(sessionId: string, versionId: number): void {
    const now = this.now();
    this.writeTx(() => {
      this.db
        .prepare(`UPDATE versions SET last_successful_export_at=? WHERE id=?`)
        .run(now, versionId);
      this.db
        .prepare(`UPDATE sessions SET last_export_at=? WHERE id=?`)
        .run(now, sessionId);
    });
  }

  // Convenience wrapper used by CLI, tests, and embedders.
  async publishDirectory(
    buildKey: string,
    directory: string,
    options: { maxBytes?: never } = {},
  ): Promise<VersionInfo> {
    void options;
    const rootDir = path.resolve(directory);
    const files: Array<{ absolute: string; relative: string; size: number; mode: number }> = [];

    const walk = async (dir: string, prefix: string): Promise<void> => {
      const entries = await fs.promises.readdir(dir, { withFileTypes: true });
      for (const entry of entries) {
        const absolute = path.join(dir, entry.name);
        const relative = prefix ? `${prefix}/${entry.name}` : entry.name;
        if (entry.isDirectory()) {
          await walk(absolute, relative);
        } else if (entry.isFile()) {
          const stat = await fs.promises.stat(absolute);
          files.push({
            absolute,
            relative: normalizeRelativePath(relative),
            size: stat.size,
            mode: stat.mode & 0o777,
          });
        }
      }
    };
    await walk(rootDir, '');
    files.sort((a, b) => a.relative.localeCompare(b.relative));

    const handle = this.beginPublish({
      key: buildKey,
      totalSize: files.reduce((sum, f) => sum + f.size, 0),
    });
    try {
      for (const file of files) {
        await handle.addFile(file.absolute, file.relative, { mode: file.mode });
      }
      return handle.commit();
    } catch (error) {
      await handle.abort().catch(() => {});
      throw error;
    }
  }

  async publishBuffers(buildKey: string, files: PublishBufferFile[]): Promise<VersionInfo> {
    const normalized = files.map((file) => {
      const content = Buffer.isBuffer(file.content)
        ? file.content
        : Buffer.from(file.content as Uint8Array);
      return { ...file, path: normalizeRelativePath(file.path), content };
    });
    const handle = this.beginPublish({
      key: buildKey,
      totalSize: normalized.reduce((sum, f) => sum + f.content.length, 0),
    });
    try {
      for (const file of normalized) {
        const writer = handle.openFile(file.path, {
          size: file.content.length,
          expectedSha256: file.sha256,
          mode: file.mode,
        });
        await writer.end(file.content);
      }
      return handle.commit();
    } catch (error) {
      await handle.abort().catch(() => {});
      throw error;
    }
  }

  blobPath(sha256: string): string {
    return casPathFor(this.root, sha256);
  }
}

export interface PublishFileWriter {
  readonly relativePath: string;
  readonly size: number;
  write(chunk: Buffer | Uint8Array | string): Promise<void>;
  end(chunk?: Buffer | Uint8Array | string): Promise<ArtifactFile>;
  destroy(): Promise<void>;
}

class PublishFileWriterImpl implements PublishFileWriter {
  readonly relativePath: string;
  readonly size: number;
  private readonly cache: BuildCache;
  private readonly versionId: number;
  private readonly tmpPath: string;
  private readonly expectedSha256?: string;
  private readonly output: fs.WriteStream;
  private readonly hash = createHash('sha256');
  private written = 0;
  private ended = false;
  private destroyed = false;

  constructor(
    cache: BuildCache,
    versionId: number,
    relativePath: string,
    tmpPath: string,
    size: number,
    expectedSha256: string | undefined,
  ) {
    this.cache = cache;
    this.versionId = versionId;
    this.relativePath = relativePath;
    this.tmpPath = tmpPath;
    this.size = size;
    this.expectedSha256 = expectedSha256?.toLowerCase();
    this.output = fs.createWriteStream(tmpPath, { flags: 'wx', mode: 0o644 });
  }

  async write(chunk: Buffer | Uint8Array | string): Promise<void> {
    if (this.ended || this.destroyed) throw new Error('publish file writer is not writable');
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk as Uint8Array);
    if (this.written + buffer.length > this.size) {
      throw new Error(
        `write to ${this.relativePath} exceeds declared size ${this.size}: ` +
          `${this.written + buffer.length}`,
      );
    }
    this.hash.update(buffer);
    this.written += buffer.length;
    await new Promise<void>((resolve, reject) => {
      this.output.write(buffer, (error) => (error ? reject(error) : resolve()));
    });
    this.cache.db
      .prepare(`UPDATE publish_files SET bytes_written=? WHERE version_id=? AND path=?`)
      .run(this.written, this.versionId, this.relativePath);
  }

  async end(chunk?: Buffer | Uint8Array | string): Promise<ArtifactFile> {
    if (chunk !== undefined) await this.write(chunk);
    if (this.ended) throw new Error('writer already ended');
    this.ended = true;

    await new Promise<void>((resolve, reject) => {
      this.output.once('error', reject);
      this.output.end(() => resolve());
    });

    if (this.written !== this.size) {
      throw new Error(
        `file ${this.relativePath} ended with ${this.written} bytes; declared ${this.size}`,
      );
    }
    const sha256 = this.hash.digest('hex');
    if (this.expectedSha256 && this.expectedSha256 !== sha256) {
      throw new ChecksumError(`published file ${this.relativePath} does not match expected sha256`, {
        expected: this.expectedSha256,
        actual: sha256,
      });
    }

    this.cache.db
      .prepare(
        `UPDATE publish_files
         SET bytes_written=?, sha256=?, expected_sha256=?, complete=1
         WHERE version_id=? AND path=?`,
      )
      .run(this.written, sha256, this.expectedSha256 ?? null, this.versionId, this.relativePath);

    return { path: this.relativePath, sha256, size: this.size };
  }

  async destroy(): Promise<void> {
    if (this.destroyed) return;
    this.destroyed = true;
    this.output.destroy();
    await fs.promises.rm(this.tmpPath, { force: true }).catch(() => {});
  }
}

export class PublishHandle {
  readonly id: number;
  readonly uploadId: string;
  readonly key: string;
  readonly totalSize: number;
  readonly stagingDir: string;
  private readonly cache: BuildCache;
  private finalized = false;

  constructor(
    cache: BuildCache,
    id: number,
    uploadId: string,
    key: string,
    totalSize: number,
    stagingDir: string,
  ) {
    this.cache = cache;
    this.id = id;
    this.uploadId = uploadId;
    this.key = key;
    this.totalSize = totalSize;
    this.stagingDir = stagingDir;
  }

  openFile(relativePathInput: string, options: OpenFileOptions): PublishFileWriter {
    if (this.finalized) throw new Error('publish has already been committed or aborted');
    const relativePath = normalizeRelativePath(relativePathInput);
    const size = options.size;
    if (!Number.isSafeInteger(size) || size < 0) {
      throw new Error('file size must be a non-negative safe integer');
    }

    const row = this.cache.db
      .prepare(`SELECT status FROM versions WHERE id=?`)
      .get(this.id) as { status: VersionStatus } | undefined;
    if (!row || row.status !== 'staging') {
      throw new ConflictError(`publish ${this.id} is no longer staging`);
    }

    const declared = this.cache.db
      .prepare(`SELECT COALESCE(SUM(size),0) AS bytes FROM publish_files WHERE version_id=?`)
      .get(this.id) as { bytes: number | bigint };
    if (Number(declared.bytes) + size > this.totalSize) {
      throw new CapacityError('declared publish files exceed totalSize', {
        totalSize: this.totalSize,
        alreadyDeclared: Number(declared.bytes),
        fileSize: size,
      });
    }

    const tmpPath = path.join(this.stagingDir, tempName('part'));
    try {
      this.cache.db
        .prepare(
          `INSERT INTO publish_files(version_id,path,tmp_path,size,mode,expected_sha256)
           VALUES(?,?,?,?,?,?)`,
        )
        .run(
          this.id,
          relativePath,
          tmpPath,
          size,
          options.mode ?? null,
          options.expectedSha256?.toLowerCase() ?? null,
        );
    } catch (error) {
      if ((error as Error).message.includes('UNIQUE')) {
        throw new ConflictError(`duplicate artifact path in publish: ${relativePath}`);
      }
      throw error;
    }

    return new PublishFileWriterImpl(
      this.cache,
      this.id,
      relativePath,
      tmpPath,
      size,
      options.expectedSha256,
    );
  }

  async addFile(
    absolutePath: string,
    relativePathInput?: string,
    options: { expectedSha256?: string; mode?: number } = {},
  ): Promise<ArtifactFile> {
    const stat = await fs.promises.stat(absolutePath);
    if (!stat.isFile()) throw new Error(`not a regular file: ${absolutePath}`);
    const relativePath = normalizeRelativePath(relativePathInput ?? path.basename(absolutePath));
    const writer = this.openFile(relativePath, {
      size: stat.size,
      expectedSha256: options.expectedSha256,
      mode: options.mode ?? (stat.mode & 0o777),
    });
    try {
      for await (const chunk of fs.createReadStream(absolutePath)) {
        await writer.write(chunk as Buffer);
      }
      return await writer.end();
    } catch (error) {
      await writer.destroy().catch(() => {});
      throw error;
    }
  }

  commit(): VersionInfo {
    if (this.finalized) throw new Error('publish already finalized');
    const version = this.cache.getVersionInfo(this.id);
    if (version.status !== 'staging') throw new ConflictError(`publish ${this.id} is ${version.status}`);

    const staged = this.cache.db
      .prepare(`SELECT * FROM publish_files WHERE version_id=? ORDER BY path`)
      .all(this.id) as any[];

    const declaredTotal = staged.reduce((sum, f) => sum + Number(f.size), 0);
    if (declaredTotal !== this.totalSize) {
      throw new Error(
        `publish ${this.id} declares totalSize ${this.totalSize} but file sizes sum to ${declaredTotal}`,
      );
    }
    for (const file of staged) {
      if (!file.complete || Number(file.bytes_written) !== Number(file.size) || !file.sha256) {
        throw new ConflictError(`publish file ${file.path} is incomplete`);
      }
    }

    const now = this.cache.timestamp();
    this.cache.writeTx(() => {
      for (const file of staged) {
        const sha = String(file.sha256).toLowerCase();
        const target = casPathFor(this.cache.root, sha);
        fs.mkdirSync(path.dirname(target), { recursive: true });

        // Hold the SQLite write lock across name resolution/linking so the
        // cleanup queue cannot unlink a pending blob between check and ref.
        try {
          fs.linkSync(file.tmp_path, target);
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error;
        }

        this.cache.db
          .prepare(
            `INSERT INTO blobs(sha256,size,created_at) VALUES(?,?,?)
             ON CONFLICT(sha256) DO NOTHING`,
          )
          .run(sha, Number(file.size), now);
        this.cache.db
          .prepare(
            `INSERT INTO files(version_id,path,sha256,size,mode)
             VALUES(?,?,?,?,?)`,
          )
          .run(this.id, file.path, sha, Number(file.size), file.mode ?? null);
        this.cache.db.prepare(`DELETE FROM cleanup_queue WHERE sha256=?`).run(sha);
      }

      this.cache.db
        .prepare(
          `UPDATE versions
           SET status='committed', published_at=?, committed_at=?
           WHERE id=? AND status='staging'`,
        )
        .run(now, now, this.id);
      this.cache.db
        .prepare(
          `INSERT INTO build_keys(key,current_version_id) VALUES(?,?)
           ON CONFLICT(key) DO UPDATE SET current_version_id=excluded.current_version_id`,
        )
        .run(this.key, this.id);
      this.cache.db.prepare(`DELETE FROM publish_files WHERE version_id=?`).run(this.id);
    });

    fs.rmSync(this.stagingDir, { recursive: true, force: true });
    this.finalized = true;
    return this.cache.getVersionInfo(this.id);
  }

  async abort(): Promise<void> {
    if (this.finalized) return;
    this.finalized = true;
    try {
      this.cache.writeTx(() => {
        this.cache.db
          .prepare(
            `UPDATE versions SET status='aborted', aborted_at=?
             WHERE id=? AND status='staging'`,
          )
          .run(this.cache.timestamp(), this.id);
      });
    } finally {
      await fs.promises.rm(this.stagingDir, { recursive: true, force: true }).catch(() => {});
    }
  }
}

interface VerifiedJob {
  file: ArtifactFile;
  finalPath: string;
  tempPath: string;
}

export class FetchSession {
  readonly id: string;
  readonly buildKey: string;
  readonly versionId: number;
  private readonly cache: BuildCache;
  private closed = false;

  constructor(cache: BuildCache, id: string, buildKey: string, versionId: number) {
    this.cache = cache;
    this.id = id;
    this.buildKey = buildKey;
    this.versionId = versionId;
  }

  getManifest(): ArtifactFile[] {
    this.cache.requireSessionActive(this.id);
    return this.cache.getVersionInfo(this.versionId).files;
  }

  async exportAll(destination: string): Promise<ExportResult> {
    if (this.closed) throw new SessionClosedError();
    this.cache.requireSessionActive(this.id);

    const version = this.cache.getVersionInfo(this.versionId);
    if (version.status !== 'committed') {
      throw new NotFoundError(`pinned version ${this.versionId} is ${version.status}`);
    }

    const destRoot = path.resolve(destination);
    ensureDir(destRoot);
    const jobs: VerifiedJob[] = [];
    let bytes = 0;

    // Phase 1: verify every body into unique temp files.  No target file is
    // renamed/replaced until this entire phase succeeds.
    try {
      for (const file of version.files) {
        const finalPath = resolveInside(destRoot, file.path);
        await fs.promises.mkdir(path.dirname(finalPath), { recursive: true });
        const tempPath = path.join(
          path.dirname(finalPath),
          `.${path.basename(finalPath)}.${process.pid}.${randomUUID()}.verify.tmp`,
        );
        const source = casPathFor(this.cache.root, file.sha256);
        await verifiedCopyToTemp(source, tempPath, {
          expectedSha256: file.sha256,
          expectedSize: file.size,
        });
        jobs.push({ file, finalPath, tempPath });
        bytes += file.size;
      }
    } catch (error) {
      await Promise.all(
        jobs.map((job) => fs.promises.rm(job.tempPath, { force: true }).catch(() => {})),
      );
      throw error;
    }

    // Phase 2: swaps.  Checksum errors cannot occur here; backup/rollback
    // still protects prior files if a filesystem rename fails midway.
    const swapped: Array<{ finalPath: string; backupPath?: string }> = [];
    try {
      for (const job of jobs) {
        let backupPath: string | undefined;
        try {
          await fs.promises.access(job.finalPath);
          backupPath = path.join(
            path.dirname(job.finalPath),
            `.${path.basename(job.finalPath)}.${randomUUID()}.old`,
          );
          await fs.promises.rename(job.finalPath, backupPath);
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
        }

        try {
          await fs.promises.rename(job.tempPath, job.finalPath);
        } catch (error) {
          if (backupPath) await fs.promises.rename(backupPath, job.finalPath).catch(() => {});
          throw error;
        }

        if (job.file.mode != null) {
          await fs.promises.chmod(job.finalPath, job.file.mode).catch(() => {});
        }
        swapped.push({ finalPath: job.finalPath, backupPath });
      }
    } catch (error) {
      for (let i = swapped.length - 1; i >= 0; i -= 1) {
        const item = swapped[i];
        await fs.promises.rm(item.finalPath, { force: true }).catch(() => {});
        if (item.backupPath) {
          await fs.promises.rename(item.backupPath, item.finalPath).catch(() => {});
        }
      }
      await Promise.all(
        jobs.map((job) => fs.promises.rm(job.tempPath, { force: true }).catch(() => {})),
      );
      throw new ConflictError(`export swap failed; original files were restored where possible`, {
        cause: (error as Error).message,
      });
    }

    await Promise.all(
      swapped.map(async (item) => {
        if (item.backupPath) await fs.promises.rm(item.backupPath, { force: true });
      }),
    );

    this.cache.noteSuccessfulExport(this.id, this.versionId);
    return {
      sessionId: this.id,
      versionId: this.versionId,
      destination: destRoot,
      exported: version.files,
      bytes,
    };
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.cache.closeSession(this.id);
  }
}
