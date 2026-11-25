#!/usr/bin/env node
import http from 'node:http';
import path from 'node:path';
import { BuildCache, FetchSession } from './cache.ts';
import { CacheError } from './errors.ts';
import { parseByteSize } from './bytes.ts';

function option(name: string, fallback?: string): string | undefined {
  const index = process.argv.indexOf(name);
  if (index < 0) return fallback;
  return process.argv[index + 1];
}

const root = path.resolve(option('--root', process.env.CACHE_ROOT ?? './.build-cache')!);
const port = Number(option('--port', process.env.PORT ?? '8787'));
const host = option('--host', process.env.HOST ?? '127.0.0.1')!;
const maxBytesInput = option('--max-bytes', process.env.CACHE_MAX_BYTES);
const maxBytes = parseByteSize(maxBytesInput);

const cache = BuildCache.open(root, maxBytes === undefined ? {} : { maxBytes });

function sendJson(response: http.ServerResponse, status: number, body: unknown): void {
  const payload = JSON.stringify(body);
  response.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(payload),
  });
  response.end(payload);
}

async function readJson(request: http.IncomingMessage): Promise<any> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += buffer.length;
    if (size > 1024 * 1024 * 1024) throw new CacheError('BODY_TOO_LARGE', 'JSON body exceeds 1 GiB');
    chunks.push(buffer);
  }
  if (chunks.length === 0) return {};
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}

function statusForError(error: unknown): number {
  const code = (error as { code?: string }).code;
  if (code === 'NOT_FOUND') return 404;
  if (code === 'CAPACITY_EXCEEDED') return 507;
  if (code === 'CHECKSUM_MISMATCH') return 422;
  if (code === 'SESSION_CLOSED' || code === 'CONFLICT') return 409;
  return 400;
}

const server = http.createServer(async (request, response) => {
  try {
    const url = new URL(request.url ?? '/', `http://${request.headers.host ?? 'localhost'}`);
    const pathname = url.pathname;

    if (request.method === 'GET' && pathname === '/health') {
      sendJson(response, 200, { ok: true, root, ownerId: cache.ownerId });
      return;
    }

    if (request.method === 'GET' && pathname === '/v1/stats') {
      sendJson(response, 200, { ok: true, usage: cache.getUsage() });
      return;
    }

    if (request.method === 'GET' && pathname === '/v1/versions/current') {
      const key = url.searchParams.get('key');
      if (!key) throw new CacheError('BAD_REQUEST', 'query parameter key is required');
      const version = cache.getCurrentVersion(key);
      if (!version) throw new CacheError('NOT_FOUND', `no committed version for ${key}`);
      sendJson(response, 200, { ok: true, version });
      return;
    }

    if (request.method === 'POST' && pathname === '/v1/publish-directory') {
      const body = await readJson(request);
      if (!body.key || !body.directory) {
        throw new CacheError('BAD_REQUEST', 'fields key and directory are required');
      }
      const version = await cache.publishDirectory(String(body.key), path.resolve(String(body.directory)));
      sendJson(response, 201, { ok: true, version });
      return;
    }

    if (request.method === 'POST' && pathname === '/v1/publish') {
      const body = await readJson(request);
      if (!body.key || !Array.isArray(body.files)) {
        throw new CacheError('BAD_REQUEST', 'fields key and files[] are required');
      }
      const files = body.files.map((file: any) => ({
        path: String(file.path),
        content:
          file.contentBase64 != null
            ? Buffer.from(String(file.contentBase64), 'base64')
            : Buffer.from(String(file.content ?? ''), 'utf8'),
        sha256: file.sha256 ? String(file.sha256) : undefined,
      }));
      const version = await cache.publishBuffers(String(body.key), files);
      sendJson(response, 201, { ok: true, version });
      return;
    }

    if (request.method === 'POST' && pathname === '/v1/sessions') {
      const body = await readJson(request);
      if (!body.key) throw new CacheError('BAD_REQUEST', 'field key is required');
      const session = cache.openSession(String(body.key));
      sendJson(response, 201, {
        ok: true,
        sessionId: session.id,
        buildKey: session.buildKey,
        versionId: session.versionId,
      });
      return;
    }

    const exportMatch = /^\/v1\/sessions\/([^/]+)\/export$/.exec(pathname);
    if (request.method === 'POST' && exportMatch) {
      const sessionId = decodeURIComponent(exportMatch[1]!);
      const active = cache.requireSessionActive(sessionId);
      const body = await readJson(request);
      if (!body.destination) throw new CacheError('BAD_REQUEST', 'field destination is required');

      // Reconstruct a handle for this server-owned session. requireSessionActive
      // proves it exists; the export implementation only needs cache/id/version.
      const session = new FetchSession(cache, active.id, active.build_key, Number(active.version_id));
      const result = await session.exportAll(path.resolve(String(body.destination)));
      sendJson(response, 200, { ok: true, result });
      return;
    }

    const sessionMatch = /^\/v1\/sessions\/([^/]+)$/.exec(pathname);
    if (request.method === 'DELETE' && sessionMatch) {
      const sessionId = decodeURIComponent(sessionMatch[1]!);
      cache.closeSession(sessionId, 'http-delete');
      sendJson(response, 200, { ok: true });
      return;
    }

    sendJson(response, 404, { ok: false, error: 'not found' });
  } catch (error) {
    const status = statusForError(error);
    sendJson(response, status, {
      ok: false,
      error: error instanceof Error ? error.message : String(error),
      code: (error as { code?: string }).code,
      details: (error as CacheError).details,
    });
  }
});

server.listen(port, host, () => {
  // eslint-disable-next-line no-console
  console.log(`shared-build-cache listening on http://${host}:${port}, root=${root}`);
});

function shutdown(): void {
  server.close(() => {
    cache.close();
    process.exit(0);
  });
  setTimeout(() => process.exit(0), 1000).unref();
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
