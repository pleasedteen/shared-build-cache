import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import readline from 'node:readline';
import { createHash } from 'node:crypto';
import { BuildCache } from '../src/cache.ts';
import { CapacityError } from '../src/errors.ts';
import type { ArtifactFile, RecoveryStats } from '../src/types.ts';

const startedAt = Date.now();
const tempRoots: string[] = [];
const workerPath = path.resolve(import.meta.dirname, 'workers', 'cache-worker.ts');
const fixtureDir = path.resolve(import.meta.dirname, '..', 'fixtures', 'sample-build');

function makeTempDir(prefix: string): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  tempRoots.push(dir);
  return dir;
}

function sha256Buffer(buffer: Buffer): string {
  return createHash('sha256').update(buffer).digest('hex');
}

async function verifyManifestOnDisk(destination: string, files: ArtifactFile[]): Promise<void> {
  for (const file of files) {
    const full = path.join(destination, file.path);
    const buffer = await fs.promises.readFile(full);
    assert.equal(buffer.length, file.size, `size for ${file.path}`);
    assert.equal(sha256Buffer(buffer), file.sha256, `checksum for ${file.path}`);
  }
}

async function exportCurrentOnce(
  cache: BuildCache,
  key: string,
  root: string,
  label: string,
): Promise<void> {
  const session = cache.openSession(key);
  const destination = path.join(root, `fetch-${label}-${session.id.slice(0, 8)}`);
  const result = await session.exportAll(destination);
  await verifyManifestOnDisk(destination, result.exported);
  session.close();
}

function makeBuildFiles(index: number) {
  return [
    { path: 'builder.txt', content: Buffer.from(`builder ${index}\n`) },
    {
      path: 'config/data.json',
      content: Buffer.from(JSON.stringify({ index, padding: 'x'.repeat(256) })),
    },
    { path: 'bin/blob.bin', content: Buffer.alloc(4096, (index * 13 + 1) & 0xff) },
    { path: 'debug/blob.dbg', content: Buffer.alloc(1024, (index + 2) & 0xff) },
  ];
}

async function testConcurrencyAndPinnedSessions(): Promise<void> {
  const root = makeTempDir('build-cache-concurrency-');
  const cache = BuildCache.open(path.join(root, 'cache'), { maxBytes: 16 * 1024 * 1024 });

  const v1 = await cache.publishDirectory('demo', fixtureDir);
  assert.ok(v1.files.length >= 4, 'fixture should contain at least four files');

  const pin = cache.openSessionForVersion(v1.id);
  const pinDest1 = path.join(root, 'pin-1');
  await pin.exportAll(pinDest1);
  await verifyManifestOnDisk(pinDest1, pin.getManifest());

  const beforeRepublish = cache.openSession('demo');
  assert.equal(beforeRepublish.versionId, v1.id);
  beforeRepublish.close();

  // Start a large publish and leave it half-written.
  const big = Buffer.alloc(32 * 1024, 0x5a);
  const paused = cache.beginPublish({ key: 'demo', totalSize: big.length });
  const pausedWriter = paused.openFile('paused/big.bin', { size: big.length });
  await pausedWriter.write(big.subarray(0, 16 * 1024));

  // Existing artifacts remain fetchable while input is paused.
  await exportCurrentOnce(cache, 'demo', root, 'during-pause');

  const publisherTasks = Array.from({ length: 10 }, async (_, index) => {
    const version = await cache.publishBuffers(`key-${index % 4}`, makeBuildFiles(index));
    const session = cache.openSessionForVersion(version.id);
    const destination = path.join(root, `publisher-${index}`);
    await session.exportAll(destination);
    await verifyManifestOnDisk(destination, session.getManifest());
    session.close();
    return version.id;
  });

  const initialFetchKeys = [0, 1, 2, 3].map(async (i) => {
    await cache.publishBuffers(`key-${i}`, makeBuildFiles(100 + i));
  });
  await Promise.all(initialFetchKeys);

  const fetcherTasks = Array.from({ length: 12 }, async (_, index) => {
    await exportCurrentOnce(cache, `key-${index % 4}`, root, `concurrent-${index}`);
  });

  const ids = await Promise.all(publisherTasks);
  assert.equal(new Set(ids).size, ids.length, 'each concurrent publish gets its own version');
  await Promise.all(fetcherTasks);

  // Finish the paused same-key replacement.
  await pausedWriter.end(big.subarray(16 * 1024));
  const v2 = paused.commit();

  const afterCommit = cache.openSession('demo');
  assert.equal(afterCommit.versionId, v2.id, 'new sessions see the replacement');
  const newDest = path.join(root, 'new-version');
  await afterCommit.exportAll(newDest);
  await verifyManifestOnDisk(newDest, afterCommit.getManifest());
  assert.ok(fs.existsSync(path.join(newDest, 'paused', 'big.bin')));
  afterCommit.close();

  assert.equal(pin.versionId, v1.id, 'old session remains pinned to opening version');
  const pinDest2 = path.join(root, 'pin-2');
  await pin.exportAll(pinDest2);
  await verifyManifestOnDisk(pinDest2, v1.files);
  await pin.exportAll(pinDest1); // repeated export to the same target
  await verifyManifestOnDisk(pinDest1, v1.files);
  assert.ok(!fs.existsSync(path.join(pinDest2, 'paused', 'big.bin')));
  pin.close();

  cache.close();
}

async function testExportChecksumPreservesOriginals(): Promise<void> {
  const root = makeTempDir('build-cache-checksum-');
  const cache = BuildCache.open(path.join(root, 'cache'), { maxBytes: 1024 * 1024 });
  const version = await cache.publishBuffers('corrupt-key', [
    { path: 'keep.txt', content: Buffer.from('original keep file\n') },
    { path: 'nested/target.bin', content: Buffer.alloc(256, 0x33) },
  ]);

  const session = cache.openSessionForVersion(version.id);
  const destination = path.join(root, 'destination');
  await session.exportAll(destination);

  const keepPath = path.join(destination, 'keep.txt');
  const targetPath = path.join(destination, 'nested', 'target.bin');
  const keepBefore = await fs.promises.readFile(keepPath);
  const targetBefore = await fs.promises.readFile(targetPath);

  const targetFile = version.files.find((file) => file.path === 'nested/target.bin');
  assert.ok(targetFile);
  const casPath = cache.blobPath(targetFile.sha256);
  const corrupted = await fs.promises.readFile(casPath);
  corrupted[0] = corrupted[0] ^ 0x01;
  await fs.promises.writeFile(casPath, corrupted);

  await assert.rejects(
    () => session.exportAll(destination),
    (error: unknown) => error instanceof Error && /checksum|size mismatch/i.test(error.message),
  );

  const keepAfter = await fs.promises.readFile(keepPath);
  const targetAfter = await fs.promises.readFile(targetPath);
  assert.deepEqual(keepAfter, keepBefore, 'unrelated original file is preserved');
  assert.deepEqual(targetAfter, targetBefore, 'failed target original is preserved');

  const allNames: string[] = [];
  const walk = async (dir: string) => {
    for (const entry of await fs.promises.readdir(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) await walk(full);
      else allNames.push(entry.name);
    }
  };
  await walk(destination);
  assert.ok(!allNames.some((name) => name.includes('.verify.tmp')), 'temp files are removed');
  assert.ok(!allNames.some((name) => name.includes('.old.')), 'backup files are removed/rolled back');

  session.close();
  cache.close();
}

async function testPausedPublishCapacityAndFetch(): Promise<void> {
  const root = makeTempDir('build-cache-pause-');
  const cache = BuildCache.open(path.join(root, 'cache'), { maxBytes: 200 });

  await cache.publishBuffers('baseline', [
    { path: 'baseline.bin', content: Buffer.alloc(20, 1) },
  ]);

  const first = cache.beginPublish({ key: 'paused-key', totalSize: 80 });
  const writer = first.openFile('large.bin', { size: 80 });
  await writer.write(Buffer.alloc(40, 2));
  const pausedUsage = cache.getUsage();
  assert.equal(pausedUsage.stagingReservedBytes, 80);
  assert.equal(pausedUsage.stagingWrittenBytes, 40);
  assert.equal(pausedUsage.usedBytes, 100, '20 byte baseline plus 80 byte publish reservation');

  // Artifacts already in cache stay fetchable while input is paused.
  await exportCurrentOnce(cache, 'baseline', root, 'pause-baseline');

  // 20 baseline + 80 first + 100 second = exactly the hard limit.
  const second = await cache.publishBuffers('other-key', [
    { path: 'other.bin', content: Buffer.alloc(100, 3) },
  ]);
  assert.equal(second.status, 'committed');

  await assert.rejects(
    () =>
      cache.publishBuffers('does-not-fit', [
        { path: 'x.bin', content: Buffer.alloc(1, 4) },
      ]),
    (error: unknown) => error instanceof CapacityError,
  );

  await writer.end(Buffer.alloc(40, 5));
  const firstVersion = first.commit();
  assert.equal(firstVersion.status, 'committed');

  const usage = cache.getUsage();
  assert.equal(usage.casBytes, 200);
  assert.equal(usage.stagingReservedBytes, 0);
  assert.equal(usage.usedBytes, 200);
  cache.close();
}

async function testCrossProcessPausedPublish(): Promise<void> {
  const root = makeTempDir('build-cache-cross-pause-');
  const cacheRoot = path.join(root, 'cache');
  const setup = BuildCache.open(cacheRoot, { maxBytes: 200 });
  await setup.publishBuffers('baseline', [
    { path: 'baseline.bin', content: Buffer.alloc(20, 1) },
  ]);
  setup.close();

  const pauser = new JsonWorker([
    '--role',
    'pause',
    '--root',
    cacheRoot,
    '--key',
    'paused-cross',
    '--total',
    '80',
    '--written',
    '40',
  ]);
  await pauser.next((message) => message.type === 'pause-ready');

  // A completely separate cache opener/process can fetch the old artifact.
  const reader = BuildCache.open(cacheRoot);
  await exportCurrentOnce(reader, 'baseline', root, 'cross-pause-reader');
  reader.close();

  const fittingPublisher = new JsonWorker([
    '--role',
    'publish-once',
    '--root',
    cacheRoot,
    '--key',
    'other-cross',
    '--size',
    '100',
    '--fill',
    '3',
  ]);
  const fittingResult = await fittingPublisher.next(
    (message) => message.type === 'published' || message.type === 'publish-error',
    30_000,
  );
  assert.equal(fittingResult.type, 'published', String(fittingResult.error ?? 'publish failed'));
  await fittingPublisher.waitExit();

  const oversizedPublisher = new JsonWorker([
    '--role',
    'publish-once',
    '--root',
    cacheRoot,
    '--key',
    'third-cross',
    '--size',
    '1',
    '--fill',
    '4',
  ]);
  const oversizedResult = await oversizedPublisher.next(
    (message) => message.type === 'published' || message.type === 'publish-error',
    30_000,
  );
  assert.equal(oversizedResult.type, 'publish-error');
  assert.equal(oversizedResult.code, 'CAPACITY_EXCEEDED');
  await oversizedPublisher.waitExit();
  assert.equal(oversizedPublisher.exitCode, 1);

  pauser.send({ cmd: 'finish' });
  const done = await pauser.next((message) => message.type === 'pause-done', 10_000);
  assert.equal(done.type, 'pause-done');
  await pauser.waitExit();

  const verifier = BuildCache.open(cacheRoot);
  assert.equal(verifier.getUsage().casBytes, 200);
  assert.equal(verifier.getUsage().activePublishCount, 0);
  const pausedCurrent = verifier.openSession('paused-cross');
  const destination = path.join(root, 'cross-paused-final');
  await pausedCurrent.exportAll(destination);
  await verifyManifestOnDisk(destination, pausedCurrent.getManifest());
  pausedCurrent.close();
  verifier.close();
}

async function exportVersionOnce(
  cache: BuildCache,
  versionId: number,
  root: string,
  label: string,
): Promise<void> {
  const session = cache.openSessionForVersion(versionId);
  const destination = path.join(root, `quota-export-${label}-${versionId}`);
  await session.exportAll(destination);
  session.close();
}

async function testQuotaEvictionOrder(): Promise<void> {
  const root = makeTempDir('build-cache-quota-');
  let clock = 1_000_000;
  const cache = BuildCache.open(path.join(root, 'cache'), {
    maxBytes: 10_000,
    now: () => clock,
  });

  const unique = async (key: string, size: number, fill: number) =>
    cache.publishBuffers(key, [{ path: 'blob.bin', content: Buffer.alloc(size, fill) }]);

  clock = 1_000;
  const neverExported = await unique('key-n', 30, 10);
  clock = 1_010;
  await unique('key-n', 10, 11); // current, protects itself

  clock = 1_400;
  const pinnedOld = await unique('key-c', 60, 12);
  clock = 1_410;
  await unique('key-c', 10, 13);
  const pin = cache.openSessionForVersion(pinnedOld.id);
  clock = 1_500;
  await pin.exportAll(path.join(root, 'pinned-c-export'));

  clock = 2_000;
  const bOld = await unique('key-b', 40, 14);
  clock = 2_010;
  await unique('key-b', 10, 15);
  clock = 2_500;
  await exportVersionOnce(cache, bOld.id, root, 'b');

  clock = 3_000;
  const aOld = await unique('key-a', 50, 16);
  clock = 3_010;
  await unique('key-a', 10, 17);
  clock = 4_000;
  await exportVersionOnce(cache, aOld.id, root, 'a');

  cache.setMaxBytes(220);
  assert.equal(cache.getUsage().casBytes, 220);

  clock = 5_000;
  const pressureOne = await unique('pressure-1', 10, 18);
  assert.equal(pressureOne.status, 'committed');
  assert.equal(cache.getVersionInfo(neverExported.id).status, 'evicted');
  assert.equal(cache.getVersionInfo(bOld.id).status, 'committed');
  assert.equal(cache.getVersionInfo(aOld.id).status, 'committed');
  assert.equal(cache.getVersionInfo(pinnedOld.id).status, 'committed');

  clock = 6_000;
  await unique('pressure-2', 50, 19);
  assert.equal(cache.getVersionInfo(bOld.id).status, 'evicted');
  assert.equal(cache.getVersionInfo(aOld.id).status, 'committed');

  // Needs 70; only A (50) is eligible because C is session-pinned. Planning
  // must fail before evicting A or inserting a partial publish.
  clock = 7_000;
  await assert.rejects(
    () => unique('pressure-3-too-big', 70, 20),
    (error: unknown) => error instanceof CapacityError,
  );
  assert.equal(cache.getVersionInfo(aOld.id).status, 'committed');
  assert.equal(cache.getVersionInfo(pinnedOld.id).status, 'committed');
  await exportVersionOnce(cache, aOld.id, root, 'a-after-failed-publish');

  // Closing the session releases C; its older export timestamp makes it next.
  pin.close();
  clock = 8_000;
  await unique('pressure-3-too-big', 70, 21);
  assert.equal(cache.getVersionInfo(pinnedOld.id).status, 'evicted');
  assert.equal(cache.getVersionInfo(aOld.id).status, 'committed');

  cache.close();
}

async function testDeduplicationAndPendingCleanupAccounting(): Promise<void> {
  const root = makeTempDir('build-cache-dedup-');
  const cache = BuildCache.open(path.join(root, 'cache'), { maxBytes: 4096 });
  const shared = [
    { path: 'a.bin', content: Buffer.alloc(40, 7) },
    { path: 'b.bin', content: Buffer.alloc(10, 9) },
  ];

  await cache.publishBuffers('dedup-1', shared);
  const afterFirst = cache.getUsage();
  assert.equal(afterFirst.blobCount, 2);
  assert.equal(afterFirst.casBytes, 50);

  await cache.publishBuffers('dedup-2', shared);
  const afterSecond = cache.getUsage();
  assert.equal(afterSecond.blobCount, 2, 'identical committed content is not stored twice');
  assert.equal(afterSecond.casBytes, 50);

  await cache.publishBuffers('dedup-3', [
    { path: 'x.bin', content: Buffer.alloc(40, 7) },
    { path: 'y.bin', content: Buffer.alloc(40, 7) },
  ]);
  assert.equal(cache.getUsage().blobCount, 2, 'same blob at two paths is still one body');

  const temporary = await cache.publishBuffers('pending-cleanup', [
    { path: 'tmp.bin', content: Buffer.alloc(25, 11) },
  ]);
  const temporaryFile = temporary.files[0];
  assert.ok(temporaryFile);

  // Simulate a process kill after the eviction transaction committed its
  // cleanup_queue row but before the pending CAS body was unlinked.
  cache.writeTx(() => {
    cache.db.prepare(`DELETE FROM files WHERE version_id=?`).run(temporary.id);
    cache.db.prepare(`UPDATE versions SET status='evicted' WHERE id=?`).run(temporary.id);
    cache.db
      .prepare(`INSERT INTO cleanup_queue(sha256,reason,created_at) VALUES(?,?,?)`)
      .run(temporaryFile.sha256, 'test-pending', Date.now());
  });

  const pendingUsage = cache.getUsage();
  assert.equal(pendingUsage.pendingCleanupBytes, 25);
  assert.equal(pendingUsage.casBytes, 75);
  assert.equal(pendingUsage.usedBytes, 75, 'pending cleanup copies count against usage');

  cache.processCleanupQueue();
  const cleanedUsage = cache.getUsage();
  assert.equal(cleanedUsage.pendingCleanupBytes, 0);
  assert.equal(cleanedUsage.casBytes, 50);
  assert.equal(fs.existsSync(cache.blobPath(temporaryFile.sha256)), false);
  cache.close();
}

interface WorkerMessage {
  type: string;
  [key: string]: unknown;
}

class JsonWorker {
  readonly child: ChildProcessWithoutNullStreams;
  private readonly lines: WorkerMessage[] = [];
  private readonly waiters: Array<(message: WorkerMessage) => void> = [];
  private stderr = '';
  exitCode: number | null = null;
  exitSignal: NodeJS.Signals | null = null;

  constructor(args: string[]) {
    this.child = spawn(process.execPath, [
      '--experimental-strip-types',
      '--experimental-sqlite',
      '--no-warnings',
      workerPath,
      ...args,
    ], {
      stdio: ['pipe', 'pipe', 'pipe'],
      env: { ...process.env, NODE_NO_WARNINGS: '1' },
    });
    const rl = readline.createInterface({ input: this.child.stdout });
    rl.on('line', (line) => {
      let message: WorkerMessage;
      try {
        message = JSON.parse(line) as WorkerMessage;
      } catch {
        return;
      }
      const waiter = this.waiters.shift();
      if (waiter) waiter(message);
      else this.lines.push(message);
    });
    this.child.stderr.on('data', (chunk) => {
      this.stderr += String(chunk);
    });
    this.child.on('exit', (code, signal) => {
      this.exitCode = code;
      this.exitSignal = signal;
      while (this.waiters.length) {
        const waiter = this.waiters.shift();
        waiter?.({ type: '__exit__', code, signal, stderr: this.stderr });
      }
    });
  }

  send(value: unknown): void {
    this.child.stdin.write(`${JSON.stringify(value)}\n`);
  }

  async next(predicate: (message: WorkerMessage) => boolean, timeoutMs = 20_000): Promise<WorkerMessage> {
    const existingIndex = this.lines.findIndex(predicate);
    if (existingIndex >= 0) {
      const [message] = this.lines.splice(existingIndex, 1);
      return message;
    }

    return new Promise<WorkerMessage>((resolve, reject) => {
      const timer = setTimeout(() => {
        const index = this.waiters.indexOf(onMessage);
        if (index >= 0) this.waiters.splice(index, 1);
        reject(new Error(`worker timed out; stderr:\n${this.stderr}`));
      }, timeoutMs);

      const onMessage = (message: WorkerMessage) => {
        if (message.type === '__exit__') {
          clearTimeout(timer);
          reject(new Error(`worker exited early: ${JSON.stringify(message)} stderr:\n${this.stderr}`));
          return;
        }
        if (predicate(message)) {
          clearTimeout(timer);
          resolve(message);
        } else {
          this.lines.push(message);
          this.waiters.push(onMessage);
        }
      };
      this.waiters.push(onMessage);
    });
  }

  async kill(signal: NodeJS.Signals): Promise<void> {
    if (this.exitCode !== null || this.exitSignal !== null) return;
    this.child.kill(signal);
    await new Promise<void>((resolve) => this.child.once('exit', () => resolve()));
  }

  async stop(): Promise<void> {
    if (this.exitCode !== null || this.exitSignal !== null) return;
    this.send({ cmd: 'exit' });
    await new Promise<void>((resolve) => {
      const timer = setTimeout(() => {
        this.child.kill('SIGKILL');
        resolve();
      }, 3_000);
      this.child.once('exit', () => {
        clearTimeout(timer);
        resolve();
      });
    });
  }

  async waitExit(timeoutMs = 10_000): Promise<void> {
    if (this.exitCode !== null || this.exitSignal !== null) return;
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('worker did not exit')), timeoutMs);
      this.child.once('exit', () => {
        clearTimeout(timer);
        resolve();
      });
    });
  }
}

async function testMultipleBuildProcesses(): Promise<void> {
  const root = makeTempDir('build-cache-multiprocess-');
  const cacheRoot = path.join(root, 'cache');
  const workDir = path.join(root, 'exports');
  fs.mkdirSync(workDir, { recursive: true });

  // Initialize persistent quota/metadata, then close before concurrent writers.
  BuildCache.open(cacheRoot, { maxBytes: 64 * 1024 * 1024 }).close();

  const processCount = 4;
  const iterations = 8;
  const workers = Array.from({ length: processCount }, (_, index) => {
    const worker = new JsonWorker([
      '--role',
      'stress',
      '--root',
      cacheRoot,
      '--work-dir',
      workDir,
      '--process-index',
      String(index),
      '--iterations',
      String(iterations),
    ]);
    return worker;
  });

  const done = await Promise.all(
    workers.map((worker) => worker.next((message) => message.type === 'stress-done', 45_000)),
  );
  assert.equal(done.length, processCount);
  for (const worker of workers) await worker.waitExit(10_000);

  const verifier = BuildCache.open(cacheRoot);
  for (let keyIndex = 0; keyIndex < 5; keyIndex += 1) {
    const key = `mp-key-${keyIndex}`;
    const current = verifier.getCurrentVersion(key);
    assert.ok(current, `concurrent key ${key} has a current version`);
    const session = verifier.openSession(key);
    const destination = path.join(root, `verify-${key}`);
    const result = await session.exportAll(destination);
    await verifyManifestOnDisk(destination, result.exported);
    session.close();
  }
  assert.equal(verifier.getUsage().activePublishCount, 0);
  verifier.close();
}

interface RecoveryGroupResult {
  name: string;
  unrelated: number;
  stats: RecoveryStats;
}

async function seedRecoveryGroup(root: string, unrelatedCount: number) {
  const cache = BuildCache.open(root, { maxBytes: 512 * 1024 * 1024 });
  const killA = await cache.publishBuffers('kill-key-a', [
    { path: 'old-a.txt', content: Buffer.from('committed-old-a') },
  ]);
  const killB = await cache.publishBuffers('kill-key-b', [
    { path: 'old-b.txt', content: Buffer.from('committed-old-b') },
  ]);
  await cache.publishBuffers('session-key-1', [
    { path: 'session-1.txt', content: Buffer.from('session-one') },
  ]);
  await cache.publishBuffers('session-key-2', [
    { path: 'session-2.txt', content: Buffer.from('session-two') },
  ]);
  const survivor = await cache.publishBuffers('survivor-key', [
    { path: 'survivor.txt', content: Buffer.from('survivor-body') },
  ]);

  // Bulk seeding is deliberately synchronous and metadata-realistic: it creates
  // `unrelatedCount` committed version rows and CAS bodies without spending the
  // test budget on 1000 separate streaming publish transactions.
  const now = Date.now();
  cache.writeTx(() => {
    const insertBlob = cache.db.prepare(
      `INSERT OR IGNORE INTO blobs(sha256,size,created_at) VALUES(?,?,?)`,
    );
    const insertVersion = cache.db.prepare(
      `INSERT INTO versions(
         build_key,version_no,status,owner_id,total_size,
         created_at,published_at,committed_at
       ) VALUES(?,1,'committed',NULL,?,?,?,?)`,
    );
    const insertFile = cache.db.prepare(
      `INSERT INTO files(version_id,path,sha256,size,mode) VALUES(?, 'u.txt', ?, ?, NULL)`,
    );
    const upsertKey = cache.db.prepare(
      `INSERT INTO build_keys(key,current_version_id) VALUES(?,?)
       ON CONFLICT(key) DO UPDATE SET current_version_id=excluded.current_version_id`,
    );

    for (let i = 0; i < unrelatedCount; i += 1) {
      const content = Buffer.from(`unrelated-${i}-${'x'.repeat(32)}`);
      const sha = sha256Buffer(content);
      const target = cache.blobPath(sha);
      fs.mkdirSync(path.dirname(target), { recursive: true });
      fs.writeFileSync(target, content);

      insertBlob.run(sha, content.length, now);
      const versionResult = insertVersion.run(`unrelated-${i}`, content.length, now, now, now);
      const versionId = Number(versionResult.lastInsertRowid);
      insertFile.run(versionId, sha, content.length);
      upsertKey.run(`unrelated-${i}`, versionId);
    }
  });
  cache.close();
  return { killAId: killA.id, killBId: killB.id, survivorId: survivor.id };
}

async function runOneRecoveryGroup(
  name: string,
  unrelated: number,
): Promise<RecoveryGroupResult> {
  const groupDir = makeTempDir(`build-cache-recovery-${name}-`);
  const root = path.join(groupDir, 'cache');
  const seeded = await seedRecoveryGroup(root, unrelated);

  const doomed = new JsonWorker(['--role', 'doomed', '--root', root]);
  const doomedReady = await doomed.next((m) => m.type === 'ready');
  assert.equal(doomedReady.type, 'ready');

  const survivor = new JsonWorker([
    '--role',
    'survivor',
    '--root',
    root,
    '--initial-dest',
    path.join(groupDir, 'survivor-initial'),
  ]);
  const survivorReady = await survivor.next((m) => m.type === 'ready');
  assert.equal(survivorReady.type, 'ready');
  const survivorSessionId = String(survivorReady.sessionId);

  await doomed.kill('SIGKILL');

  const recovery = new JsonWorker(['--role', 'recover', '--root', root]);
  const recoveryMessage = await recovery.next((m) => m.type === 'recovery', 30_000);
  const stats = recoveryMessage.stats as RecoveryStats;
  assert.equal(stats.uploadsAborted, 2);
  assert.equal(stats.sessionsReleased, 2);
  assert.equal(stats.versionRecordsChecked, 4);
  assert.equal(stats.artifactFilesRead, 0);
  await recovery.stop();

  // The live process's original session must still export after recovery.
  survivor.send({ cmd: 'export', id: 1, dest: path.join(groupDir, 'survivor-after-1') });
  const export1 = await survivor.next((m) => m.type === 'export-result' && m.id === 1);
  assert.equal(export1.ok, true, String(export1.error ?? 'survivor export failed'));

  survivor.send({ cmd: 'export', id: 2, dest: path.join(groupDir, 'survivor-after-2') });
  const export2 = await survivor.next((m) => m.type === 'export-result' && m.id === 2);
  assert.equal(export2.ok, true, String(export2.error ?? 'second survivor export failed'));

  // Reopen in the test process: unfinished keys still resolve to old current.
  const reopened = BuildCache.open(root);
  const currentA = reopened.getCurrentVersion('kill-key-a');
  const currentB = reopened.getCurrentVersion('kill-key-b');
  assert.ok(currentA && currentB);
  assert.equal(currentA.id, seeded.killAId);
  assert.equal(currentB.id, seeded.killBId);

  const fetchA = reopened.openSession('kill-key-a');
  const destA = path.join(groupDir, 'reopened-a');
  await fetchA.exportAll(destA);
  await verifyManifestOnDisk(destA, fetchA.getManifest());
  assert.equal(await fs.promises.readFile(path.join(destA, 'old-a.txt'), 'utf8'), 'committed-old-a');
  fetchA.close();

  const survivorRow = reopened.db
    .prepare(`SELECT closed_at FROM sessions WHERE id=?`)
    .get(survivorSessionId) as { closed_at: number | null };
  assert.equal(survivorRow.closed_at, null, 'live owner session is retained');

  const deadOpenSessions = reopened.db
    .prepare(
      `SELECT COUNT(*) AS count FROM sessions s
       JOIN owners o ON o.id=s.owner_id
       WHERE o.pid=? AND s.closed_at IS NULL`,
    )
    .get(Number(doomedReady.pid)) as { count: number | bigint };
  assert.equal(Number(deadOpenSessions.count), 0);
  reopened.close();

  await survivor.stop();
  return { name, unrelated, stats };
}

async function testKillRecoveryScaling(): Promise<RecoveryGroupResult[]> {
  const small = await runOneRecoveryGroup('small', 10);
  const hundredX = await runOneRecoveryGroup('hundred-x', 1000);

  assert.equal(
    small.stats.versionRecordsChecked,
    hundredX.stats.versionRecordsChecked,
    'checked version records must not grow with unrelated versions',
  );
  assert.equal(
    small.stats.artifactFilesRead,
    hundredX.stats.artifactFilesRead,
    'artifact reads during recovery must not grow with unrelated versions',
  );
  return [small, hundredX];
}

async function main(): Promise<void> {
  const checks: Array<{ name: string; run: () => Promise<unknown> }> = [
    { name: 'concurrent publish, pinned fetch, and repeated export', run: testConcurrencyAndPinnedSessions },
    { name: 'four real Node processes publishing and exporting concurrently', run: testMultipleBuildProcesses },
    { name: 'every export verifies and checksum failure preserves originals', run: testExportChecksumPreservesOriginals },
    { name: 'paused publish keeps fetch and leaves room for two full publishes', run: testPausedPublishCapacityAndFetch },
    { name: 'separate process publishes while another process input is paused', run: testCrossProcessPausedPublish },
    { name: 'hard quota evicts oldest export/publish and protects pins/current', run: testQuotaEvictionOrder },
    { name: 'deduplication and pending-cleanup quota accounting', run: testDeduplicationAndPendingCleanupAccounting },
  ];

  for (const check of checks) {
    const start = Date.now();
    process.stdout.write(`\n▶ ${check.name} ...\n`);
    await check.run();
    process.stdout.write(`  ✓ passed in ${Date.now() - start} ms\n`);
  }

  process.stdout.write(`\n▶ SIGKILL recovery with two scale groups ...\n`);
  const recoveryStart = Date.now();
  const recoveryGroups = await testKillRecoveryScaling();
  process.stdout.write(`  ✓ recovery groups passed in ${Date.now() - recoveryStart} ms\n`);

  process.stdout.write('\nRecovery storage-access counts (actual run):\n');
  console.table(
    recoveryGroups.map((group) => ({
      group: group.name,
      unrelatedCommittedVersions: group.unrelated,
      versionRecordsChecked: group.stats.versionRecordsChecked,
      artifactFilesRead: group.stats.artifactFilesRead,
      candidateOwnersChecked: group.stats.candidateOwnersChecked,
      sessionsReleased: group.stats.sessionsReleased,
      uploadsAborted: group.stats.uploadsAborted,
      artifactFilesUnlinked: group.stats.artifactFilesUnlinked,
    })),
  );
  for (const group of recoveryGroups) {
    console.log(
      `recovery[${group.name}] unrelated=${group.unrelated} ` +
        `checkedVersionRecords=${group.stats.versionRecordsChecked} ` +
        `readArtifactFiles=${group.stats.artifactFilesRead} ` +
        `releasedSessions=${group.stats.sessionsReleased} ` +
        `abortedUploads=${group.stats.uploadsAborted}`,
    );
  }

  const elapsed = ((Date.now() - startedAt) / 1000).toFixed(2);
  process.stdout.write(`\nAll self-tests passed in ${elapsed}s (limit: 80s).\n`);

  if (!process.env.KEEP_TEST_TMP) {
    await Promise.all(
      tempRoots.map((dir) => fs.promises.rm(dir, { recursive: true, force: true }).catch(() => {})),
    );
  } else {
    process.stdout.write(`Temporary directories retained under ${os.tmpdir()} with prefix build-cache-*\n`);
  }

  // Workers detach intervals but should be stopped; this is a final guard.
  setTimeout(() => process.exit(0), 50).unref();
}

main().catch(async (error) => {
  console.error('\n✖ self-test failed');
  console.error(error);
  if (process.env.KEEP_TEST_TMP) {
    console.error('Temporary directories:', tempRoots);
  }
  process.exitCode = 1;
  setTimeout(() => process.exit(1), 50).unref();
});

setTimeout(() => {
  console.error('\n✖ global 75s watchdog reached');
  process.exit(2);
}, 75_000).unref?.();
