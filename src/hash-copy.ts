import fs from 'node:fs';
import { createHash } from 'node:crypto';
import { pipeline } from 'node:stream/promises';
import { ChecksumError } from './errors.ts';

export async function sha256File(filePath: string): Promise<string> {
  const hash = createHash('sha256');
  await pipeline(fs.createReadStream(filePath), hash);
  return hash.digest('hex');
}

export interface VerifiedCopyOptions {
  expectedSha256: string;
  expectedSize: number;
  onReadByteCount?: (bytes: number) => void;
}

/**
 * Streams src through SHA-256 into a temporary destination.  The caller is
 * responsible for renaming the verified temp file into its final position,
 * so a checksum failure never modifies an existing target file.
 */
export async function verifiedCopyToTemp(
  src: string,
  tempDest: string,
  options: VerifiedCopyOptions,
): Promise<{ sha256: string; size: number }> {
  const hash = createHash('sha256');
  let size = 0;

  try {
    await pipeline(
      fs.createReadStream(src),
      async function* inspect(source) {
        for await (const chunk of source as AsyncIterable<Buffer>) {
          size += chunk.length;
          hash.update(chunk);
          options.onReadByteCount?.(chunk.length);
          yield chunk;
        }
      },
      fs.createWriteStream(tempDest, { flags: 'wx', mode: 0o644 }),
    );
  } catch (error) {
    await fs.promises.rm(tempDest, { force: true }).catch(() => {});
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      throw new ChecksumError(`cache artifact is missing: ${options.expectedSha256}`, {
        src,
        sha256: options.expectedSha256,
      });
    }
    throw error;
  }

  const actualSha256 = hash.digest('hex');
  if (size !== options.expectedSize) {
    await fs.promises.rm(tempDest, { force: true }).catch(() => {});
    throw new ChecksumError(
      `artifact size mismatch for ${options.expectedSha256}: expected ${options.expectedSize}, read ${size}`,
      { expectedSize: options.expectedSize, actualSize: size },
    );
  }
  if (actualSha256 !== options.expectedSha256.toLowerCase()) {
    await fs.promises.rm(tempDest, { force: true }).catch(() => {});
    throw new ChecksumError(
      `artifact checksum mismatch: expected ${options.expectedSha256}, computed ${actualSha256}`,
      { expectedSha256: options.expectedSha256, actualSha256 },
    );
  }
  return { sha256: actualSha256, size };
}
