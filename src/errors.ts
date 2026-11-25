export class CacheError extends Error {
  code: string;
  details?: unknown;

  constructor(code: string, message: string, details?: unknown) {
    super(message);
    this.name = 'CacheError';
    this.code = code;
    this.details = details;
  }
}

export class NotFoundError extends CacheError {
  constructor(message: string, details?: unknown) {
    super('NOT_FOUND', message, details);
    this.name = 'NotFoundError';
  }
}

export class CapacityError extends CacheError {
  constructor(message: string, details?: unknown) {
    super('CAPACITY_EXCEEDED', message, details);
    this.name = 'CapacityError';
  }
}

export class ChecksumError extends CacheError {
  constructor(message: string, details?: unknown) {
    super('CHECKSUM_MISMATCH', message, details);
    this.name = 'ChecksumError';
  }
}

export class ConflictError extends CacheError {
  constructor(message: string, details?: unknown) {
    super('CONFLICT', message, details);
    this.name = 'ConflictError';
  }
}

export class SessionClosedError extends CacheError {
  constructor(message = 'fetch session is closed', details?: unknown) {
    super('SESSION_CLOSED', message, details);
    this.name = 'SessionClosedError';
  }
}
