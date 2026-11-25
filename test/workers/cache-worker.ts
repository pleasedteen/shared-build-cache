import readline from 'node:readline';
import path from 'node:path';
import { BuildCache } from '../../src/cache.ts';

function arg(name: string, fallback?: string): string | undefined {
  const index = process.argv.indexOf(name);
  if (index < 0) return fallback;
  return process.argv[index + 1];
}

function requiredArg(name: string): string {
  const value = arg(name);
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function send(message: unknown): void {
  process.stdout.write(`${JSON.stringify(message)}\n`);
}

function keepAlive(): NodeJS.Timeout {
  return setInterval(() => {}, 1 << 30);
}

async function doomed(root: string): Promise<void> {
  const cache = BuildCache.open(root);

  const publishA = cache.beginPublish({ key: 'kill-key-a', totalSize: 8 });
  const writerA = publishA.openFile('half-written.bin', { size: 8 });
  await writerA.write(Buffer.from('1234'));

  const publishB = cache.beginPublish({ key: 'kill-key-b', totalSize: 5 });
  const writerB = publishB.openFile('complete-but-uncommitted.txt', { size: 5 });
  await writerB.end(Buffer.from('hello'));

  const session1 = cache.openSession('session-key-1');
  const session2 = cache.openSession('session-key-2');

  send({
    type: 'ready',
    pid: process.pid,
    ownerId: cache.ownerId,
    publishIds: [publishA.id, publishB.id],
    sessionIds: [session1.id, session2.id],
  });

  keepAlive();
}

async function survivor(root: string, initialDest?: string): Promise<void> {
  const cache = BuildCache.open(root);
  const session = cache.openSession('survivor-key');
  if (initialDest) await session.exportAll(initialDest);

  send({
    type: 'ready',
    pid: process.pid,
    ownerId: cache.ownerId,
    sessionId: session.id,
    versionId: session.versionId,
  });

  const rl = readline.createInterface({ input: process.stdin });
  rl.on('line', async (line) => {
    let command: any;
    try {
      command = JSON.parse(line);
    } catch (error) {
      send({ type: 'result', ok: false, error: `bad JSON: ${(error as Error).message}` });
      return;
    }

    if (command.cmd === 'export') {
      try {
        const result = await session.exportAll(command.dest);
        send({
          type: 'export-result',
          id: command.id,
          ok: true,
          result: {
            destination: result.destination,
            bytes: result.bytes,
            files: result.exported.map((file) => ({ path: file.path, sha256: file.sha256 })),
          },
        });
      } catch (error) {
        send({
          type: 'export-result',
          id: command.id,
          ok: false,
          error: error instanceof Error ? error.message : String(error),
          code: (error as { code?: string }).code,
        });
      }
      return;
    }

    if (command.cmd === 'exit') {
      session.close();
      cache.close();
      process.exit(0);
    }
  });

  rl.on('close', () => {
    session.close();
    cache.close();
    process.exit(0);
  });

  keepAlive();
}

function recover(root: string): void {
  const cache = BuildCache.open(root);
  const stats = cache.startupRecovery;
  const usage = cache.getUsage();
  send({ type: 'recovery', stats, usage });
  cache.close();
}

async function stress(
  root: string,
  workDir: string,
  processIndex: number,
  iterations: number,
): Promise<void> {
  const cache = BuildCache.open(root);
  let bytes = 0;

  for (let i = 0; i < iterations; i += 1) {
    const seed = processIndex * 1000 + i;
    const key = `mp-key-${i % 5}`;
    const version = await cache.publishBuffers(key, [
      { path: 'object/main.o', content: Buffer.alloc(2048, seed & 0xff) },
      { path: 'symbols/main.dbg', content: Buffer.alloc(768, (seed >> 3) & 0xff) },
      {
        path: 'meta/build.json',
        content: Buffer.from(JSON.stringify({ processIndex, i, seed })),
      },
    ]);
    bytes += version.totalSize;

    const session = cache.openSessionForVersion(version.id);
    const destination = path.join(workDir, `p${processIndex}`, `export-${i}`);
    await session.exportAll(destination);
    session.close();
  }

  send({ type: 'stress-done', processIndex, iterations, bytes, ownerId: cache.ownerId });
  cache.close();
  setTimeout(() => process.exit(0), 100);
}

async function publishOnce(
  root: string,
  key: string,
  size: number,
  fill: number,
): Promise<void> {
  const cache = BuildCache.open(root);
  try {
    const version = await cache.publishBuffers(key, [
      { path: 'one.bin', content: Buffer.alloc(size, fill & 0xff) },
    ]);
    send({ type: 'published', versionId: version.id });
    cache.close();
    setTimeout(() => process.exit(0), 50);
  } catch (error) {
    send({
      type: 'publish-error',
      code: (error as { code?: string }).code,
      error: error instanceof Error ? error.message : String(error),
    });
    cache.close();
    setTimeout(() => process.exit(1), 50);
  }
}

async function pausePublisher(
  root: string,
  key: string,
  totalSize: number,
  writtenSize: number,
): Promise<void> {
  const cache = BuildCache.open(root);
  const publish = cache.beginPublish({ key, totalSize });
  const writer = publish.openFile('large.bin', { size: totalSize });
  await writer.write(Buffer.alloc(writtenSize, 2));

  send({ type: 'pause-ready', pid: process.pid, ownerId: cache.ownerId, publishId: publish.id });

  const rl = readline.createInterface({ input: process.stdin });
  rl.on('line', async (line) => {
    let command: any;
    try {
      command = JSON.parse(line);
    } catch {
      return;
    }
    if (command.cmd === 'finish') {
      await writer.end(Buffer.alloc(totalSize - writtenSize, 5));
      const version = publish.commit();
      send({ type: 'pause-done', versionId: version.id });
      cache.close();
      setTimeout(() => process.exit(0), 50);
    } else if (command.cmd === 'abort') {
      await publish.abort();
      cache.close();
      setTimeout(() => process.exit(0), 50);
    }
  });
  keepAlive();
}

async function main(): Promise<void> {
  const role = arg('--role');
  const root = arg('--root');
  if (!root) throw new Error('--root is required');

  if (role === 'doomed') await doomed(root);
  else if (role === 'survivor') await survivor(root, arg('--initial-dest'));
  else if (role === 'recover') recover(root);
  else if (role === 'stress') {
    const workDir = arg('--work-dir');
    if (!workDir) throw new Error('--work-dir is required for stress');
    await stress(
      root,
      workDir,
      Number(arg('--process-index', '0')),
      Number(arg('--iterations', '12')),
    );
  } else if (role === 'publish-once') {
    await publishOnce(
      root,
      requiredArg('--key'),
      Number(arg('--size', '0')),
      Number(arg('--fill', '1')),
    );
  } else if (role === 'pause') {
    await pausePublisher(
      root,
      requiredArg('--key'),
      Number(arg('--total', '0')),
      Number(arg('--written', '0')),
    );
  } else throw new Error(`unknown --role ${role ?? ''}`);
}

main().catch((error) => {
  send({ type: 'worker-error', error: error instanceof Error ? error.stack ?? error.message : String(error) });
  process.exit(1);
});
