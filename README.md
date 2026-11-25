# Shared Build Cache

一个零运行时依赖的 TypeScript/Node.js 共享本地构建产物缓存。多个构建进程可以打开同一个缓存目录，按构建键原子发布、打开固定版本的取件会话、整套导出、按容量回收，并在 `SIGKILL` 后做有界恢复。

- 元数据：Node 22 内置的 `node:sqlite`（WAL + `BEGIN IMMEDIATE`）
- 产物正文：SHA-256 内容寻址文件存储，相同内容只保存一份
- 并发：多进程共享同一目录；SQLite 负责元数据事务，内容寻址和临时目录避免写串
- 会话：打开时固定版本，导出时逐文件流式校验 SHA-256
- 崩溃恢复：SQLite 事务回滚 + 属主进程存活性检测（PID + `/proc/<pid>/stat` 启动时间）
- 运行时依赖：无；Node `>= 22.6` 可直接运行 TypeScript

## 一条测试命令

```bash
npm test
```

该命令会运行自带夹具和全部场景，通常约 **8–20 秒**完成（限制目标为 80 秒；冷启动或高负载 CI 上可能更久，但百倍版本夹具已用批量元数据播种以避免把时间花在测试数据上）。它验证：

1. 同键并发发布、提交前读取旧版、提交后读取完整新版；
2. **4 个真实 Node 子进程**并发发布和导出；
3. 取件会话多次导出且版本固定；
4. 每次导出重新计算校验和，损坏时报错且不覆盖目标位置原文件；
5. 暂停大文件输入时仍可取件，容量足够时另一个真实发布进程/构建键可完成发布；
6. 硬容量、暂存/待清理占用、CAS 去重、当前版本和会话固定保护；
7. 按“最近成功导出时间，否则发布时间”从旧到新回收；
8. `SIGKILL` 后的暂存/会话释放、存活进程继续导出、旧当前版本可读；
9. 两组恢复规模（10 个无关版本 vs 1000 个，正好百倍）的实际存储访问计数。

恢复报告会直接打印类似下表：

```text
Recovery storage-access counts (actual run):
┌─────────┬─────────────┬────────────────────────────┬───────────────────────┬───────────────────┬────────────────┐
│ group   │ unrelated…  │ versionRecordsChecked      │ artifactFilesRead     │ sessionsReleased  │ uploadsAborted │
├─────────┼─────────────┼────────────────────────────┼───────────────────────┼───────────────────┼────────────────┤
│ small   │ 10          │ 4                          │ 0                     │ 2                 │ 2              │
│ hundred-x │ 1000      │ 4                          │ 0                     │ 2                 │ 2              │
└─────────┴─────────────┴────────────────────────────┴───────────────────────┴───────────────────┴────────────────┘
```

两组都安排了相同的 **2 个未完成发布 + 2 个遗留会话**，因此实际检查的不同版本记录数都是 `4`；恢复只做索引查询、元数据提交和定向删除，不读取产物正文，所以产物文件读取数都是 `0`。无关已提交版本扩大一百倍时，这两个计数不增长。

## 自带夹具

[`fixtures/sample-build`](fixtures/sample-build) 模拟一次构建输出，包含：

- `README.txt`
- `config/build.json`
- `obj/main.o`、`obj/render.o`（确定性二进制夹具）
- `symbols/app.dbg`

## 启动方式

### 1. 直接使用 TypeScript API

```ts
import { BuildCache } from './src/cache.ts';

const cache = BuildCache.open('.cache/demo', {
  maxBytes: 1024 * 1024 * 1024, // 产物正文硬上限，可后续 cache.setMaxBytes()
});

// 发布整套目录
const version = await cache.publishDirectory('linux-x64-release', './fixtures/sample-build');

// 打开取件会话：版本固定在此刻的 current
const session = cache.openSession('linux-x64-release');
await session.exportAll('./out/1');
await session.exportAll('./out/2'); // 可多次导出；每次都重新校验

session.close();
cache.close();
```

暂停大文件输入：

```ts
const publish = cache.beginPublish({
  key: 'linux-x64-release',
  totalSize: bigBuffer.length, // 整笔发布所需正文，开始时即纳入容量计划
});

const writer = publish.openFile('obj/large.o', { size: bigBuffer.length });
await writer.write(bigBuffer.subarray(0, half));
// 在这里暂停：旧 current 仍可被其他会话取件，其他键也可使用剩余容量发布

await writer.end(bigBuffer.subarray(half));
publish.commit();
```

### 2. CLI

```bash
# 初始化并设置容量（支持 64MiB、1GiB 等写法）
npm run cli -- init --root .cache/demo --max-bytes 1GiB

# 发布夹具目录
npm run cli -- publish \
  --root .cache/demo \
  --key sample \
  --dir fixtures/sample-build

# 同一会话内重复导出两次到同一目标
npm run cli -- fetch \
  --root .cache/demo \
  --key sample \
  --dest /tmp/sample-out \
  --repeat 2

# 查看容量和 blob 数
npm run cli -- stats --root .cache/demo
```

### 3. HTTP 服务

```bash
npm start -- --root .cache/http --port 8787 --host 127.0.0.1 --max-bytes 1GiB
```

发布内联文件：

```bash
curl -sS -X POST http://127.0.0.1:8787/v1/publish \
  -H 'content-type: application/json' \
  -d '{
    "key":"demo",
    "files":[{"path":"hello.txt","content":"hello shared cache"}]
  }'
```

打开会话：

```bash
curl -sS -X POST http://127.0.0.1:8787/v1/sessions \
  -H 'content-type: application/json' \
  -d '{"key":"demo"}'
```

用上一步返回的 `sessionId` 导出到运行服务的本机构建目录：

```bash
curl -sS -X POST http://127.0.0.1:8787/v1/sessions/<sessionId>/export \
  -H 'content-type: application/json' \
  -d '{"destination":"/tmp/http-cache-out"}'
```

其他接口：

- `GET /health`
- `GET /v1/stats`
- `GET /v1/versions/current?key=demo`
- `POST /v1/publish-directory`，body：`{"key":"demo","directory":"/abs/path"}`
- `DELETE /v1/sessions/<sessionId>`

HTTP 会话由服务进程持有；嵌入式 TypeScript API/CLI 更适合演示多构建进程和 `SIGKILL` 属主恢复。

## 磁盘布局

```text
<root>/
  cache.sqlite        # 版本、键 current、会话、属主、引用、清理队列
  cache.sqlite-wal
  cache.sqlite-shm
  cas/aa/bb/<sha256>  # 不可变内容寻址正文
  staging/<uploadId>/ # 未提交发布的临时正文
```

关键表：

- `versions`：`staging | committed | evicted | aborted`
- `build_keys`：每个构建键的原子当前版本指针
- `files`：版本的完整清单和 SHA-256
- `blobs`：去重后的正文及大小
- `sessions`：属主、固定版本、关闭时间
- `owners`：进程 PID 和 PID 启动时间标记
- `cleanup_queue`：已从元数据摘除但尚未完成物理删除的待清理副本

## 容量与回收规则

`getUsage().usedBytes` 包含：

- `blobs` 中所有唯一 CAS 正文，包括 `cleanup_queue` 里的待清理副本；
- 所有活动 `staging` 发布的预留正文（已写入部分和声明但尚未输入的部分都计入容量计划）。

因此：

- 会话固定的旧版本仍被 `sessions` 引用，其 CAS 正文计入容量且不会被回收；
- 相同 SHA-256 的正文在 `blobs`/CAS 中只有一份；
- 开始发布时先做不修改数据的回收计划；
- 只有“当前版本、活动会话固定版本、未完成发布的 base 版本”受保护；
- 候选版本按 `COALESCE(last_successful_export_at, published_at, committed_at, created_at)` 升序和 `id` 排序；
- 模拟引用计数时会正确处理跨版本共享 blob；
- 即使回收全部可回收版本仍放不下新发布，也返回 `CAPACITY_EXCEEDED`，且不会提前写入回收结果或新条目。

## 导出完整性

`FetchSession#exportAll()` 分两阶段：

1. 对清单中每个 CAS 文件流式读取、更新 SHA-256、写入目标目录中的唯一临时文件；
2. 所有文件校验通过后，再逐个 rename 到正式位置；rename 阶段带备份和回滚。

因此校验和/大小失败发生在任何正式文件被替换之前，目标位置原有文件会保留。

## 崩溃恢复和有界访问

每个 `BuildCache.open()` 都生成一个 `ownerId`，记录进程 PID 和 Linux `/proc/<pid>/stat` 的启动时间。恢复时只查询：

- 未完成的 `staging/committing` 版本的属主；
- 未关闭会话的属主。

然后逐个检查这些少量属主是否仍是同一个存活进程实例：

- 已死亡：中止其未完成发布、定向清理其暂存/孤儿 CAS、关闭其遗留会话；
- 仍存活：完全跳过，原进程中的 `FetchSession` 可继续导出；
- 无关的已提交版本不会被扫描。

恢复不读取产物正文；正文完整性留给每次导出时校验。测试中的 `versionRecordsChecked` 和 `artifactFilesRead` 是恢复循环中的实际计数，不是从表行数推算。

## 项目脚本

```bash
npm test          # 80 秒目标内的完整可读自检
npm run typecheck # 可选：TypeScript 严格类型检查（需要 devDependencies）
npm start         # 启动 HTTP 服务
npm run cli -- --help
```

运行时不依赖 `npm install`；Node 22 可直接剥离并执行这些 `.ts` 文件。开发期类型检查使用 TypeScript 和 Node 类型声明。
