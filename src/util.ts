import path from 'node:path';
import fs from 'node:fs';
import { randomUUID } from 'node:crypto';

export function ensureDir(dir: string): void {
  fs.mkdirSync(dir, { recursive: true });
}

export function normalizeRelativePath(input: string): string {
  if (typeof input !== 'string' || input.length === 0) {
    throw new Error('artifact path must be a non-empty string');
  }
  if (input.includes('\0')) {
    throw new Error(`invalid artifact path (NUL byte): ${input}`);
  }

  const unified = input.replaceAll('\\', '/');
  if (path.posix.isAbsolute(unified) || /^[A-Za-z]:/.test(unified)) {
    throw new Error(`artifact path must be relative: ${input}`);
  }

  const parts: string[] = [];
  for (const part of unified.split('/')) {
    if (part === '' || part === '.') continue;
    if (part === '..') {
      throw new Error(`artifact path must not contain '..': ${input}`);
    }
    parts.push(part);
  }

  if (parts.length === 0) {
    throw new Error(`artifact path does not name a file: ${input}`);
  }
  return parts.join('/');
}

export function resolveInside(root: string, relativePath: string): string {
  const rootResolved = path.resolve(root);
  const finalPath = path.resolve(rootResolved, relativePath);
  if (finalPath !== rootResolved && !finalPath.startsWith(rootResolved + path.sep)) {
    throw new Error(`path escapes destination: ${relativePath}`);
  }
  return finalPath;
}

export function casPathFor(root: string, sha256: string): string {
  if (!/^[0-9a-f]{64}$/i.test(sha256)) {
    throw new Error(`invalid sha256: ${sha256}`);
  }
  const sha = sha256.toLowerCase();
  return path.join(root, 'cas', sha.slice(0, 2), sha.slice(2, 4), sha);
}

export function tempName(prefix: string): string {
  return `${prefix}.${process.pid}.${randomUUID()}.tmp`;
}

/**
 * Linux-specific process incarnation token.  A PID alone can be reused;
 * combining it with starttime from /proc/<pid>/stat identifies the exact
 * process. State Z means a killed process that has not yet been reaped.
 */
export function readProcStat(pid: number): { startTicks: number | null; state: string | null } {
  if (process.platform !== 'linux') return { startTicks: null, state: null };
  try {
    const raw = fs.readFileSync(`/proc/${pid}/stat`, 'utf8');
    const commEnd = raw.lastIndexOf(')');
    if (commEnd < 0) return { startTicks: null, state: null };
    const rest = raw.slice(commEnd + 2).split(' ');
    // After comm: field 3 state is index 0; field 22 starttime is index 19.
    const start = Number(rest[19]);
    return {
      startTicks: Number.isFinite(start) ? start : null,
      state: rest[0] ?? null,
    };
  } catch {
    return { startTicks: null, state: null };
  }
}

export function pidStartTicks(pid: number): number | null {
  return readProcStat(pid).startTicks;
}

export function isProcessAlive(pid: number, expectedStart: number | null): boolean {
  let exists = false;
  try {
    process.kill(pid, 0);
    exists = true;
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === 'EPERM') exists = true;
    else if (code === 'ESRCH') exists = false;
    else throw error;
  }

  if (!exists) return false;
  if (process.platform !== 'linux') return true;

  const stat = readProcStat(pid);
  if (stat.state === 'Z' || stat.state === 'X') return false;
  if (expectedStart == null || stat.startTicks == null) return true;
  return stat.startTicks === expectedStart;
}

export async function removePathQuiet(target: string): Promise<void> {
  await fs.promises.rm(target, { recursive: true, force: true });
}
