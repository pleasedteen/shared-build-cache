export type VersionStatus = 'staging' | 'committed' | 'evicted' | 'aborted';

export interface ArtifactFile {
  path: string;
  sha256: string;
  size: number;
  mode?: number | null;
}

export interface VersionInfo {
  id: number;
  buildKey: string;
  versionNo: number;
  status: VersionStatus;
  totalSize: number;
  createdAt: number;
  publishedAt: number | null;
  lastSuccessfulExportAt: number | null;
  files: ArtifactFile[];
}

export interface RecoveryStats {
  startedAt: number;
  finishedAt: number;
  candidateOwnersChecked: number;
  aliveOwnersSkipped: number;
  deadOwnersReaped: number;
  uploadsAborted: number;
  sessionsReleased: number;
  /** Distinct versions rows inspected while recovering dead owners. */
  versionRecordsChecked: number;
  /** Number of product/artifact file bodies opened and read by recovery. */
  artifactFilesRead: number;
  /** CAS files unlinked by recovery/cleanup; not reads. */
  artifactFilesUnlinked: number;
  cleanupItemsProcessed: number;
  stagingDirectoriesRemoved: number;
  errors: string[];
}

export interface UsageInfo {
  maxBytes: number;
  /** Unique CAS bodies plus all active publish reservations. */
  usedBytes: number;
  casBytes: number;
  stagingReservedBytes: number;
  stagingWrittenBytes: number;
  pendingCleanupBytes: number;
  blobCount: number;
  activePublishCount: number;
  availableBytes: number;
}

export interface ExportResult {
  sessionId: string;
  versionId: number;
  destination: string;
  exported: ArtifactFile[];
  bytes: number;
}
