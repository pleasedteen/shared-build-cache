#!/usr/bin/env node
import path from 'node:path';
import { BuildCache } from './cache.ts';
import { parseByteSize } from './bytes.ts';

function option(name: string, fallback?: string): string | undefined {
  const index = process.argv.indexOf(name);
  if (index < 0) return fallback;
  return process.argv[index + 1];
}

function required(name: string): string {
  const value = option(name);
  if (!value) {
    console.error(`missing required option ${name}`);
    process.exit(2);
  }
  return value;
}

function print(value: unknown): void {
  process.stdout.write(`${JSON.stringify(value, null, 2)}\n`);
}

async function main(): Promise<void> {
  const command = process.argv[2];

  if (!command || command === '--help' || command === '-h' || command === 'help') {
    console.log(`shared-build-cache CLI

Usage:
  node --no-warnings src/cli.ts init    --root DIR [--max-bytes 1GiB]
  node --no-warnings src/cli.ts publish --root DIR --key KEY --dir ARTIFACT_DIR [--max-bytes 64MiB]
  node --no-warnings src/cli.ts fetch   --root DIR --key KEY --dest TARGET_DIR [--repeat 2]
  node --no-warnings src/cli.ts stats   --root DIR
  node --no-warnings src/cli.ts serve   --root DIR [--port 8787] [--host 127.0.0.1]

Environment:
  npm run cli -- <command> [options]
`);
    return;
  }

  if (command === 'serve') {
    process.argv = [process.argv[0]!, path.resolve(import.meta.dirname, 'server.ts'), ...process.argv.slice(3)];
    await import('./server.ts');
    return;
  }

  const root = path.resolve(required('--root'));
  const maxBytes = parseByteSize(option('--max-bytes'));

  if (command === 'init') {
    const cache = BuildCache.open(root, maxBytes === undefined ? {} : { maxBytes });
    const recovery = cache.startupRecovery;
    const usage = cache.getUsage();
    cache.close();
    print({ ok: true, root, usage, recovery });
    return;
  }

  if (command === 'publish') {
    const key = required('--key');
    const dir = path.resolve(required('--dir'));
    const cache = BuildCache.open(root, maxBytes === undefined ? {} : { maxBytes });
    try {
      const version = await cache.publishDirectory(key, dir);
      print({
        ok: true,
        versionId: version.id,
        buildKey: version.buildKey,
        versionNo: version.versionNo,
        totalSize: version.totalSize,
        files: version.files,
      });
    } finally {
      cache.close();
    }
    return;
  }

  if (command === 'fetch') {
    const key = required('--key');
    const destination = path.resolve(required('--dest'));
    const repeat = Number(option('--repeat', '1'));
    if (!Number.isInteger(repeat) || repeat < 1) throw new Error('--repeat must be a positive integer');

    const cache = BuildCache.open(root);
    try {
      const session = cache.openSession(key);
      const exports = [];
      for (let i = 0; i < repeat; i += 1) {
        const result = await session.exportAll(i === 0 ? destination : destination);
        exports.push({
          iteration: i + 1,
          versionId: result.versionId,
          bytes: result.bytes,
          fileCount: result.exported.length,
        });
      }
      session.close();
      print({ ok: true, sessionId: session.id, key, exports });
    } finally {
      cache.close();
    }
    return;
  }

  if (command === 'stats') {
    const cache = BuildCache.open(root);
    try {
      print({ ok: true, root, usage: cache.getUsage(), startupRecovery: cache.startupRecovery });
    } finally {
      cache.close();
    }
    return;
  }

  throw new Error(`unknown command: ${command}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
