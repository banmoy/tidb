# TiDB 查询与 TiKV 交互机制分析

本文基于源码分析 TiDB 查询如何与 TiKV 进行交互，覆盖从 SQL 执行器到底层 KV 存储的完整链路。

## 目录

1. [总体架构](#总体架构)
2. [核心接口层 (pkg/kv)](#核心接口层)
3. [存储驱动层 (pkg/store/driver)](#存储驱动层)
4. [两条主要交互路径](#两条主要交互路径)
5. [路径一：点查 (PointGet) — KV Get/BatchGet](#路径一点查-pointget)
6. [路径二：范围扫描 — Coprocessor/DistSQL](#路径二范围扫描--coprocessordistsql)
7. [事务与写入路径](#事务与写入路径)
8. [MPP 路径 (TiFlash)](#mpp-路径-tiflash)
9. [完整调用链总结](#完整调用链总结)

---

## 总体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        TiDB Server                              │
│                                                                 │
│  SQL Parser ──▶ Planner ──▶ Executor                            │
│                                │                                │
│                    ┌───────────┼───────────────┐                │
│                    │           │               │                │
│                    ▼           ▼               ▼                │
│             PointGet    TableReader      MPP Executor            │
│             (KV Get)    IndexReader     (TiFlash)               │
│                    │    IndexLookup          │                  │
│                    │           │              │                  │
│                    ▼           ▼              ▼                  │
│              kv.Snapshot   distsql.Select  kv.MPPClient          │
│              kv.Transaction  kv.Client                          │
│                    │           │              │                  │
│  ──────────────────┼───────────┼──────────────┼──── KV 接口层 ──│
│                    │           │              │                  │
│               tikvSnapshot CopClient    MPPClient               │
│                    │           │              │                  │
│  ──────────────────┼───────────┼──────────────┼── store/driver ─│
│                    ▼           ▼              ▼                  │
│                    tikv.KVStore (client-go)                      │
│                    │                                            │
│                    ▼                                            │
│              gRPC (tikvrpc)                                     │
└────────────────────┬────────────────────────────────────────────┘
                     │
              ┌──────┴──────┐
              │    TiKV     │
              │  (RawKV /   │
              │ Coprocessor)│
              └─────────────┘
```

TiDB 与 TiKV 的交互主要通过 `client-go`（`github.com/tikv/client-go/v2`）库实现，分为以下几层：

| 层级 | 包路径 | 职责 |
|------|--------|------|
| **KV 接口层** | `pkg/kv/` | 定义 `Storage`, `Transaction`, `Snapshot`, `Client` 等核心接口 |
| **存储驱动层** | `pkg/store/driver/` | 将 `tikv.KVStore` 适配为 TiDB 的 `kv.Storage` 接口 |
| **Coprocessor 层** | `pkg/store/copr/` | 构建 Coprocessor 请求，管理 region 级别的并发任务 |
| **DistSQL 层** | `pkg/distsql/` | 在 Executor 和 Coprocessor 之间搭建桥梁，构建 `kv.Request` 并处理结果 |
| **执行器层** | `pkg/executor/` | TableReader、IndexReader、PointGet 等具体执行器 |

---

## 核心接口层

`pkg/kv/kv.go` 定义了 TiDB 与任何 KV 存储交互的核心抽象：

### kv.Storage

```go
// Storage defines the interface for storage.
type Storage interface {
    Begin(opts ...tikv.TxnOption) (Transaction, error)
    GetSnapshot(ver Version) Snapshot
    GetClient() Client           // 返回 Coprocessor 客户端
    GetMPPClient() MPPClient     // 返回 MPP 客户端
    Close() error
    // ...
}
```

`Storage` 是所有存储引擎的统一入口。对于 TiKV，具体实现在 `pkg/store/driver/tikv_driver.go` 中的 `tikvStore`。

### kv.Transaction

```go
type Transaction interface {
    RetrieverMutator              // Get/Set/Delete/Iter
    Commit(context.Context) error
    Rollback() error
    LockKeys(ctx context.Context, lockCtx *LockCtx, keys ...Key) error
    BatchGet(ctx context.Context, keys []Key, options ...BatchGetOption) (map[string]ValueEntry, error)
    GetSnapshot() Snapshot
    GetMemBuffer() MemBuffer
    // ...
}
```

事务封装了 MVCC 语义下的读写操作。实际实现通过 `tikvTxn`（`pkg/store/driver/txn/txn_driver.go`）代理到 `tikv.KVTxn`。

### kv.Snapshot

```go
type Snapshot interface {
    Retriever                     // Get/Iter/IterReverse
    BatchGet(ctx context.Context, keys []Key, options ...BatchGetOption) (map[string]ValueEntry, error)
    SetOption(opt int, val any)
}
```

快照代表某个时间戳下的一致性读视图。实现为 `tikvSnapshot`（`pkg/store/driver/txn/snapshot.go`），它将调用代理到 `txnsnapshot.KVSnapshot`，后者通过 gRPC 向 TiKV 发送 `KvGet` / `KvBatchGet` / `KvScan` 请求。

### kv.Client

```go
type Client interface {
    Send(ctx context.Context, req *Request, vars any, option *ClientSendOption) Response
    IsRequestTypeSupported(reqType, subType int64) bool
}
```

`Client` 用于发送 Coprocessor 请求（DAG 请求）。实现为 `CopClient`（`pkg/store/copr/coprocessor.go`）。

---

## 存储驱动层

`pkg/store/driver/tikv_driver.go` 中的 `TiKVDriver` 是 TiKV 存储引擎的入口。初始化流程：

```
TiKVDriver.Open(path)
  ├── 解析 PD 地址
  ├── 创建 pd.Client (与 PD 通信获取集群拓扑、TSO 等)
  ├── 创建 tikv.CodecPDClient (处理 keyspace 编码)
  ├── 创建 tikv.RPCClient (底层 gRPC 连接池)
  ├── 创建 tikv.KVStore (封装所有 TiKV 交互逻辑)
  └── 创建 copr.Store (封装 Coprocessor 交互)
```

`tikvStore` 结构体组合了多个关键组件：

```go
type tikvStore struct {
    *tikv.KVStore   // client-go 核心存储，管理 region cache、RPC client
    coprStore *copr.Store  // Coprocessor 子系统
    codec     tikv.Codec   // Key 编码器 (API V1 / V2)
    // ...
}
```

关键方法映射：
- `Begin()` → 创建 `tikv.KVTxn` → 包装为 `tikvTxn`（实现 `kv.Transaction`）
- `GetSnapshot()` → 创建 `txnsnapshot.KVSnapshot` → 包装为 `tikvSnapshot`（实现 `kv.Snapshot`）
- `GetClient()` → 返回 `CopClient`（实现 `kv.Client`）

---

## 两条主要交互路径

TiDB 查询与 TiKV 交互有两条主要路径，选择哪条取决于优化器生成的物理执行计划：

| 路径 | 适用场景 | 协议 | 计算位置 |
|------|----------|------|----------|
| **KV Get/BatchGet** | 点查（PointGet）、BatchPointGet | `tikvrpc.CmdGet` / `CmdBatchGet` | TiDB 侧 |
| **Coprocessor** | 范围扫描（TableScan、IndexScan）及下推计算 | `tikvrpc.CmdCop` | TiKV 侧 |

---

## 路径一：点查 (PointGet)

PointGet 是最简单的读路径，直接通过 KV 接口读取单行数据。

### 调用链

```
PointGetExecutor.Next()
  │
  ├── e.get(ctx, key)                          // pkg/executor/point_get.go
  │     └── txn.GetSnapshot().Get(ctx, key)    // 使用事务快照
  │           └── tikvSnapshot.Get(ctx, key)    // pkg/store/driver/txn/snapshot.go
  │                 └── KVSnapshot.Get(ctx, key)  // client-go
  │                       ├── 查询 MemBuffer（本事务未提交数据）
  │                       ├── 查询 Snapshot Cache
  │                       └── RPC: KvGet 请求 → TiKV
  │
  └── 解码行数据 → 填入 chunk
```

### 关键细节

1. **Key 编码**：TiDB 使用 `tablecodec` 将表 ID + 行 handle 编码为 KV 的 Key。例如 `tablecodec.EncodeRowKeyWithHandle(tableID, handle)` 生成类似 `t{tableID}_r{handle}` 的 Key。

2. **索引回表**：如果是通过唯一索引查找，先通过索引 Key 拿到 handle，再用 handle 构造行 Key 进行第二次 Get。

3. **Region 路由**：`client-go` 内部通过 Region Cache 定位 Key 所在的 Region 和 Leader 节点，自动处理 Region 分裂、迁移等情况。

4. **MVCC 读**：TiKV 使用 MVCC，Get 请求携带 `start_ts`，TiKV 返回该时间戳可见的最新版本。

---

## 路径二：范围扫描 — Coprocessor/DistSQL

范围扫描是更常见的路径，用于 `SELECT * FROM t WHERE ...` 等需要扫描多行的查询。TiDB 将计算任务（过滤、聚合等）下推到 TiKV 的 Coprocessor 执行。

### 总体流程

```
TableReaderExecutor.Open()
  │
  ├── buildKVReq()                                // 构建 kv.Request
  │     └── distsql.RequestBuilder
  │           ├── SetDAGRequest(dagPB)            // Protobuf 编码的执行计划
  │           ├── SetKeyRanges(ranges)            // 扫描的 Key 范围
  │           ├── SetStartTS(startTS)
  │           └── Build() → *kv.Request
  │
  ├── distsql.SelectWithRuntimeStats()
  │     └── distsql.Select()                       // pkg/distsql/distsql.go
  │           └── client.Send(ctx, kvReq, ...)     // kv.Client.Send
  │                 └── CopClient.Send()            // pkg/store/copr/coprocessor.go
  │                       ├── BuildCopIterator()
  │                       │     ├── buildCopTasks()  // 按 Region 拆分任务
  │                       │     └── 创建 copIterator
  │                       └── copIterator.open()    // 启动并发 worker
  │
  └── 返回 selectResult → resultHandler

TableReaderExecutor.Next()
  │
  └── resultHandler.nextChunk()
        └── selectResult.Next()
              └── copIterator.Next()              // 从 worker 获取下一批结果
                    └── copResponse → 解码为 chunk
```

### 详细分解

#### 1. DAG 请求构建

优化器生成物理计划后，执行器将其序列化为 `tipb.DAGRequest`（Protobuf），包含下推到 TiKV 的算子链：

```
DAGRequest {
    Executors: [TableScan, Selection, Aggregation, TopN, Limit, ...]
    // 或使用树状结构:
    RootExecutor: { children: [...] }
}
```

#### 2. Key Range 计算

`distsql.RequestBuilder` 根据表 ID 和 ranger 生成的范围，计算出需要扫描的 KV Key 范围：

```go
// 表扫描范围: [t{tableID}_r{startHandle}, t{tableID}_r{endHandle})
// 索引扫描范围: [t{tableID}_i{indexID}{startKey}, t{tableID}_i{indexID}{endKey})
```

#### 3. 按 Region 拆分任务 (buildCopTasks)

`buildCopTasks()`（`pkg/store/copr/coprocessor.go`）将全局 Key 范围按 Region 边界拆分为多个 `copTask`：

```go
type copTask struct {
    region    tikv.RegionVerID   // Region 标识
    ranges    *KeyRanges         // 该 Region 内的 Key 范围
    storeAddr string             // TiKV 节点地址
    cmdType   tikvrpc.CmdType    // CmdCop
    // ...
}
```

拆分过程依赖 Region Cache（从 PD 获取并缓存）来确定每个 Key 范围落在哪个 Region。

#### 4. 并发执行 (copIterator)

`copIterator` 启动多个 worker goroutine 并发处理 copTask：

```
copIterator.open()
  ├── 启动 N 个 copIteratorWorker (N = req.Concurrency)
  └── 每个 worker:
        ├── 从 taskCh 获取 copTask
        ├── 构造 tikvrpc.Request (CmdCop)
        │     ├── 设置 KeyRanges
        │     ├── 设置 DAGRequest (序列化后的执行计划)
        │     └── 设置 StartTS, IsolationLevel 等
        ├── 发送 RPC 到 TiKV
        │     └── tikv.Client.SendRequest(addr, req, timeout)
        │           └── gRPC: Coprocessor RPC
        ├── 处理响应
        │     ├── 处理 Region 错误 (重试)
        │     ├── 处理锁冲突 (resolve lock)
        │     └── 返回 copResponse
        └── 将结果发送到 respCh
```

#### 5. 结果收集 (selectResult)

`selectResult`（`pkg/distsql/select_result.go`）从 `copIterator` 逐批获取结果：

```go
type selectResult struct {
    resp       kv.Response          // copIterator
    fieldTypes []*types.FieldType   // 列类型
    // ...
}

func (r *selectResult) Next(ctx context.Context, chk *chunk.Chunk) error {
    // 1. 从 resp 获取下一个 ResultSubset
    // 2. 解析 Protobuf 响应 (tipb.SelectResponse / tipb.Chunk)
    // 3. 解码数据到 chunk.Chunk
}
```

### Coprocessor 在 TiKV 侧的处理

TiKV 收到 Coprocessor 请求后：

1. 解析 DAGRequest 中的执行计划
2. 按照算子链执行：TableScan → Selection → Aggregation → ...
3. 在本 Region 的数据范围内执行
4. 将结果序列化为 `tipb.SelectResponse` 返回

这样，过滤和聚合等计算被下推到存储层，减少了网络传输量。

---

## 事务与写入路径

### 写入路径

```
INSERT/UPDATE/DELETE
  │
  └── 执行器写入 MemBuffer (内存缓冲区)
        └── txn.Set(key, value) / txn.Delete(key)
              └── UnionStore.SetInMemBuffer()
```

写入操作首先缓存在事务的 MemBuffer 中，不直接发送到 TiKV。

### 提交路径 (2PC)

```
txn.Commit()
  │
  └── tikv.KVTxn.Commit()
        └── 两阶段提交 (2PC)
              ├── Prewrite: 向各 Region 发送 KvPrewrite 请求
              │     └── TiKV 写入锁 + 数据到 RocksDB
              ├── 从 PD 获取 commit_ts
              └── Commit: 向各 Region 发送 KvCommit 请求
                    └── TiKV 清除锁，写入 commit 记录
```

### 悲观锁

```
LockKeys()
  └── tikv.KVTxn.LockKeys()
        └── 向 TiKV 发送 KvPessimisticLock 请求
              └── TiKV 写入悲观锁
```

---

## MPP 路径 (TiFlash)

对于 TiFlash 的 MPP（Massively Parallel Processing）查询，使用独立的交互路径：

```
MPPGather Executor
  │
  └── MppCoordinator.Execute()
        ├── MPPClient.ConstructMPPTasks()   // 确定 TiFlash 节点分布
        ├── MPPClient.DispatchMPPTask()     // 分发执行计划到各 TiFlash 节点
        └── MPPClient.EstablishMPPConns()   // 建立流式连接接收结果
              └── gRPC streaming
```

MPP 与 Coprocessor 的区别：
- Coprocessor 是"推模型"，TiDB 向每个 Region 发送独立请求
- MPP 是"拉模型"，TiFlash 节点间可以直接交换数据（shuffle/broadcast），TiDB 只接收最终结果

---

## 完整调用链总结

### 读路径对比

| 阶段 | PointGet (KV Get) | TableReader (Coprocessor) |
|------|-------------------|---------------------------|
| 1. 计划生成 | `PointGetPlan` | `PhysicalTableScan` + 下推算子 |
| 2. 执行器 | `PointGetExecutor` | `TableReaderExecutor` |
| 3. 请求构建 | 直接构造 Key | `RequestBuilder` 构建 DAG + KeyRanges |
| 4. 中间层 | `kv.Snapshot.Get()` | `distsql.Select()` → `CopClient.Send()` |
| 5. 任务拆分 | 无（单 Key） | `buildCopTasks()` 按 Region 拆分 |
| 6. RPC | `KvGet` | `Coprocessor` |
| 7. TiKV 处理 | 直接 MVCC 读 | 执行 DAG 算子链 |
| 8. 结果处理 | 解码单行 | `selectResult` 逐批解码 |

### 关键组件交互图

```
┌─────────────────────────────────────────────────────────────┐
│                     Executor Layer                           │
│  PointGetExecutor  TableReaderExecutor  IndexReaderExecutor  │
│  BatchPointGet     IndexLookUpExecutor  MPPGatherExecutor    │
└──────────┬───────────────┬─────────────────────┬────────────┘
           │               │                     │
     kv.Snapshot      distsql.Select        kv.MPPClient
     kv.Transaction   kv.Client.Send
           │               │                     │
┌──────────┴───────────────┴─────────────────────┴────────────┐
│                 Store Driver Layer                            │
│  tikvSnapshot         CopClient / copIterator   MPPClient    │
│  tikvTxn              copr.Store                             │
└──────────┬───────────────┬─────────────────────┬────────────┘
           │               │                     │
┌──────────┴───────────────┴─────────────────────┴────────────┐
│                  client-go (tikv/client-go/v2)               │
│                                                              │
│  tikv.KVStore ─── RegionCache ─── RPCClient                 │
│       │                │              │                      │
│       │           PD Client      gRPC Connections            │
│       │           (TSO, Region    (to TiKV/TiFlash)          │
│       │            routing)                                  │
└───────┴────────────────┴──────────────┬─────────────────────┘
                                        │
                                   ┌────┴────┐
                                   │  TiKV   │
                                   │ Cluster │
                                   └─────────┘
```

---

## 按 Region 切分 Key 范围的详细机制

这是 Coprocessor 路径中最关键的步骤之一。TiKV 的数据按 Region 分布在不同节点上，每个 Region 负责一段连续的 Key 范围。TiDB 必须将全局的查询 Key 范围拆分到各个 Region，才能向正确的 TiKV 节点发送请求。

### 总览

```
查询的全局 Key 范围 (如 [t1_r0, t1_r100))
          │
          ▼
  ┌──────────────────────────────────────────────┐
  │ 1. BatchLocateKeyRanges (client-go)          │
  │    查 Region Cache → 缓存未命中 → 查 PD      │
  │    返回 []*KeyLocation                        │
  └──────────────────┬───────────────────────────┘
                     │
                     ▼
  ┌──────────────────────────────────────────────┐
  │ 2. SplitKeyRangesByLocations (copr)          │
  │    用每个 Location 的边界切分 Key Ranges       │
  │    返回 []*LocationKeyRanges                  │
  └──────────────────┬───────────────────────────┘
                     │
                     ▼
  ┌──────────────────────────────────────────────┐
  │ 3. splitKeyRangesByBuckets (copr, 可选)      │
  │    在 Region 内部按 Bucket 边界进一步切分       │
  │    返回更细粒度的 []*LocationKeyRanges         │
  └──────────────────┬───────────────────────────┘
                     │
                     ▼
  ┌──────────────────────────────────────────────┐
  │ 4. buildCopTasks                              │
  │    每个 LocationKeyRanges → 一个 copTask      │
  │    (大范围可能按 rangesPerTask 再拆)            │
  └──────────────────────────────────────────────┘
```

### 第 1 步：通过 Region Cache 定位 Region（BatchLocateKeyRanges）

**源码位置**: `client-go/v2/internal/locate/region_cache.go` — `BatchLocateKeyRanges()`

Region Cache 是 TiDB 端维护的一份 Region 信息缓存，记录了每个 Region 的 Key 范围 `[StartKey, EndKey)`、版本号、Leader 节点地址等。数据来源是 PD（Placement Driver）。

`BatchLocateKeyRanges` 的工作流程分两阶段：

**阶段一：查 Region Cache（内存操作，无 RPC）**

```go
// 对每个输入的 KeyRange:
for _, keyRange := range keyRanges {
    // 尝试从缓存中查找包含 keyRange.StartKey 的 Region
    r := c.tryFindRegionByKey(keyRange.StartKey, false)
    if r == nil {
        // 缓存未命中，记录为 uncachedRanges，后续从 PD 加载
        uncachedRanges = append(uncachedRanges, keyRange)
        continue
    }
    cachedRegions = append(cachedRegions, r)

    // 如果这个 Region 不能覆盖整个 keyRange，继续扫描后续 Region
    if !r.ContainsByEnd(keyRange.EndKey) {
        // scanRegionsFromCache 从缓存中批量扫描后续 Region
        for _, r := range c.scanRegionsFromCache(r.EndKey(), keyRange.EndKey, batchSize) {
            cachedRegions = append(cachedRegions, r)
            // ...直到覆盖整个 keyRange 或缓存缺失
        }
    }
}
```

Region Cache 内部使用有序结构（B-tree / sorted map）存储 Region 信息，`tryFindRegionByKey` 通过二分查找定位 Region。

**阶段二：从 PD 加载缺失的 Region（gRPC 调用）**

```go
for len(uncachedRanges) > 0 {
    regions, err := c.BatchLoadRegionsWithKeyRanges(bo, uncachedRanges, batchSize)
    // BatchLoadRegionsWithKeyRanges 内部调用 PD 的 BatchScanRegions RPC
    // 加载到的 Region 会写入缓存: c.insertRegionToCache(region)
    // ...
}
```

**阶段三：合并结果**

`batchLocateRangesMerger` 将缓存命中的 Region 和从 PD 新加载的 Region 按 Key 顺序合并，生成有序的 `[]*KeyLocation` 列表。

`KeyLocation` 结构：

```go
type KeyLocation struct {
    Region   RegionVerID    // Region 的 ID + 版本号
    StartKey []byte         // Region 的起始 Key（包含）
    EndKey   []byte         // Region 的结束 Key（不包含，空表示 +∞）
    Buckets  *metapb.Buckets // Region 内的 Bucket 信息（可选）
}
```

### 第 2 步：按 Location 边界切分 Key Ranges（splitKeyRangesByLocation）

**源码位置**: `pkg/store/copr/region_cache.go` — `SplitKeyRangesByLocations()` 和 `splitKeyRangesByLocation()`

拿到所有 Location 后，需要将输入的 Key Ranges 按 Location 的 `[StartKey, EndKey)` 边界精确切分。

核心算法在 `splitKeyRangesByLocation` 中：

```go
func (c *RegionCache) splitKeyRangesByLocation(ctx context.Context, loc *KeyLocation, ranges *KeyRanges,
    res []*LocationKeyRanges) ([]*LocationKeyRanges, *KeyRanges, bool) {

    // 从头遍历 ranges，找到第一个不完全在 loc 中的 range
    var i int
    for ; i < ranges.Len(); i++ {
        r = ranges.At(i)
        if !(loc.Contains(r.EndKey) || bytes.Equal(loc.EndKey, r.EndKey)) {
            break  // 这个 range 的 EndKey 超出了 loc 的边界
        }
    }

    // 情况1: 所有 ranges 都在 loc 内 → 直接返回
    if i == ranges.Len() {
        res = append(res, &LocationKeyRanges{Location: loc, Ranges: ranges})
        return res, ranges, true  // isBreak=true
    }

    // 情况2: range r 横跨 loc 边界 → 需要切分
    if loc.Contains(r.StartKey) {
        // r 的前半部分在 loc 内:  [r.StartKey, loc.EndKey)
        taskRanges := ranges.Slice(0, i)
        taskRanges.last = &kv.KeyRange{StartKey: r.StartKey, EndKey: loc.EndKey}
        res = append(res, &LocationKeyRanges{Location: loc, Ranges: taskRanges})

        // r 的后半部分留给下一个 loc: [loc.EndKey, r.EndKey)
        ranges = ranges.Slice(i+1, ranges.Len())
        ranges.first = &kv.KeyRange{StartKey: loc.EndKey, EndKey: r.EndKey}
    }
    return res, ranges, false  // isBreak=false, 继续处理下一个 loc
}
```

**图示例子**：

假设有 3 个 Region 和 2 个查询 Range：

```
Region 1: [a, f)     Region 2: [f, k)     Region 3: [k, p)

Range A: [b, h)              ← 横跨 Region 1 和 Region 2
Range B: [m, o)              ← 完全在 Region 3 内

切分结果:
  copTask 1: Region 1, ranges = [b, f)    ← Range A 被截断到 Region 1 边界
  copTask 2: Region 2, ranges = [f, h)    ← Range A 的剩余部分
  copTask 3: Region 3, ranges = [m, o)    ← Range B 不用切分
```

**`KeyRanges` 的零拷贝优化**：

`KeyRanges` 使用 `first`/`mid`/`last` 三段式设计，避免切分时的大量内存分配：

```go
type KeyRanges struct {
    first *kv.KeyRange   // 可选的头部（切分产生的前缀）
    mid   []kv.KeyRange  // 中间部分（引用原始 slice 的子切片）
    last  *kv.KeyRange   // 可选的尾部（切分产生的后缀）
}
```

切分操作只修改 `first`/`last` 指针和 `mid` 的 slice 头，不复制底层数据。

### 第 3 步：按 Bucket 进一步切分（可选）

**源码位置**: `pkg/store/copr/region_cache.go` — `SplitKeyRangesByBuckets()` 和 `splitKeyRangesByBuckets()`

TiKV 支持在 Region 内部划分 Bucket（子范围），用于更细粒度的负载均衡和热点打散。如果 Region 有 Bucket 信息，会在 Region 边界切分后进一步按 Bucket 边界切分：

```
Region 2: [f, k), Buckets: [f, h), [h, k)

Range: [f, j)
  → Bucket [f, h): ranges = [f, h)
  → Bucket [h, k): ranges = [h, j)
```

如果 Bucket 元数据过期或不一致，会回退到仅按 Region 切分（`SplitKeyRangesByLocations`），保证正确性。

### 第 4 步：构建 copTask（buildCopTasks）

**源码位置**: `pkg/store/copr/coprocessor.go` — `buildCopTasks()`

每个 `LocationKeyRanges` 被转化为一个或多个 `copTask`。如果一个 Location 内的 ranges 数量超过 `rangesPerTask`（默认 25000），会进一步拆分：

```go
for _, loc := range locs {
    rLen := loc.Ranges.Len()
    for i := 0; i < rLen; {
        nextI := min(i + rangesPerTaskLimit, rLen)
        task := &copTask{
            region: loc.Location.Region,  // Region ID + 版本
            ranges: loc.Ranges.Slice(i, nextI),
            cmdType: tikvrpc.CmdCop,      // Coprocessor 命令类型
            // ...
        }
        builder.handle(task)
        i = nextI
    }
}
```

如果启用了 **Store Batching**（`StoreBatchSize > 0`），同一个 TiKV 节点上的多个 copTask 会被合并到一次 RPC 中发送，减少网络开销。

### 第 5 步：发送 RPC 请求到 TiKV

每个 `copTask` 最终被序列化为一个 `coprocessor.Request` 并通过 gRPC 发送：

```go
copReq := coprocessor.Request{
    Tp:        worker.req.Tp,       // 请求类型 (DAG)
    StartTs:   worker.req.StartTs,  // 事务起始时间戳
    Data:      worker.req.Data,     // 序列化的 DAGRequest (执行计划)
    Ranges:    task.ranges.ToPBRanges(), // 该 Task 负责的 Key 范围
}

// 通过 RegionRequestSender 发送到正确的 TiKV 节点
resp, rpcCtx, storeAddr, err := worker.kvclient.SendReqCtx(
    bo, req, task.region, timeout, endpointType, task.storeAddr)
```

`SendReqCtx` 内部通过 Region Cache 查找 Region 的 Leader 地址，建立 gRPC 连接并发送请求。如果遇到 Region 错误（如 Region 已分裂、Leader 已迁移），会自动重试。

### Region 错误重试机制

当 TiKV 返回 Region 错误时，copIterator 会自动处理：

```
TiKV 返回 RegionError
  │
  ├── EpochNotMatch (Region 版本不匹配)
  │     → 用新的 Region 信息重建 copTask
  │
  ├── NotLeader (发给了非 Leader 节点)
  │     → 更新 Leader 缓存，重发到新 Leader
  │
  ├── ServerIsBusy (TiKV 过载)
  │     → 指数退避后重试
  │
  └── StaleCommand / RegionNotFound
        → 从 PD 刷新 Region 信息后重试
```

### 完整的数据结构关系

```
kv.Request.KeyRanges (全局范围)
    │
    │  ForEachPartitionWithErr(buildTaskFunc)
    │  ← 先按 partition 分组
    ▼
KeyRanges (每个 partition 的范围)
    │
    │  SplitKeyRangesByBuckets / SplitKeyRangesByLocations
    │  ← Region Cache + PD
    ▼
[]*LocationKeyRanges
    │  每个元素 = 一个 Region(+Bucket) 内的 Key 范围
    │
    │  按 rangesPerTask 拆分
    ▼
[]*copTask
    │  每个 copTask 包含:
    │   - region: RegionVerID (发往哪个 Region)
    │   - ranges: *KeyRanges (本 task 负责的范围)
    │   - cmdType: CmdCop
    │
    │  copIterator 并发执行
    ▼
coprocessor.Request → gRPC → TiKV
```

---

### 核心源码文件索引

| 文件 | 用途 |
|------|------|
| `pkg/kv/kv.go` | 核心 KV 接口定义：Storage、Transaction、Snapshot、Client |
| `pkg/store/driver/tikv_driver.go` | TiKV 存储驱动，初始化 KVStore 和 CoprStore |
| `pkg/store/driver/txn/txn_driver.go` | 事务实现（tikvTxn），代理到 tikv.KVTxn |
| `pkg/store/driver/txn/snapshot.go` | 快照实现（tikvSnapshot），代理到 KVSnapshot |
| `pkg/store/copr/store.go` | Coprocessor Store，提供 CopClient 和 MPPClient |
| `pkg/store/copr/coprocessor.go` | Coprocessor 客户端核心：任务拆分、并发执行、结果收集 |
| `pkg/store/copr/region_cache.go` | Region Cache 封装：SplitKeyRangesByLocations/Buckets |
| `pkg/store/copr/key_ranges.go` | KeyRanges 零拷贝数据结构 |
| `pkg/distsql/distsql.go` | DistSQL 入口，Select/Analyze/Checksum 函数 |
| `pkg/distsql/request_builder.go` | 构建 kv.Request（DAG + KeyRanges + 选项） |
| `pkg/distsql/select_result.go` | 结果迭代器，解码 Coprocessor 响应 |
| `pkg/executor/table_reader.go` | TableReaderExecutor，范围扫描主逻辑 |
| `pkg/executor/point_get.go` | PointGetExecutor，单行精确查找 |
| `pkg/kv/mpp.go` | MPP 相关接口定义 |
| `pkg/store/copr/mpp.go` | MPP 客户端实现 |
| `client-go/.../region_cache.go` | Region Cache 核心：BatchLocateKeyRanges、LocateKey |
| `client-go/.../region_request.go` | Region 请求发送与 Region 错误重试 |

---

## Region 元信息存储在哪里

Region 元信息在三个层面存储和流转：**PD（权威源）**、**TiKV（本地持久化）**、**TiDB/client-go（内存缓存）**。

### Region 元信息的数据结构

```protobuf
// kvproto/pkg/metapb — Region 元信息的 Protobuf 定义
message Region {
    uint64       id          = 1;   // Region 全局唯一 ID（PD 分配）
    bytes        start_key   = 2;   // Region 负责的起始 Key（包含）
    bytes        end_key     = 3;   // Region 负责的结束 Key（不包含，空=+∞）
    RegionEpoch  region_epoch = 4;  // 版本号 {ConfVer, Version}
    repeated Peer peers      = 5;   // 副本所在的 Store 列表
}

message RegionEpoch {
    uint64 conf_ver = 1;  // 成员变更时递增
    uint64 version  = 2;  // Split/Merge 时递增
}

message Peer {
    uint64    id       = 1;  // Peer ID（PD 分配）
    uint64    store_id = 2;  // 所在 TiKV Store 的 ID
    PeerRole  role     = 3;  // Voter / Learner / Witness
}
```

### 三层存储架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                          PD (权威源)                                │
│                                                                     │
│  存储位置: etcd (内嵌)                                              │
│  数据结构: metapb.Region + Leader + 统计信息                        │
│  更新方式: TiKV Leader 通过 RegionHeartbeat 上报                    │
│  接口:     GetRegion / ScanRegions / BatchScanRegions               │
│                                                                     │
│  PD 内部按 Key 范围维护一棵 Region Tree，支持：                      │
│   - 按 Key 查找所在 Region                                          │
│   - 按范围扫描 Region 列表                                          │
│   - 检测 Region overlap 和一致性                                    │
└────────────────┬────────────────────────────────────────────────────┘
                 │  RegionHeartbeat (上行)
                 │  GetRegion / ScanRegions (下行)
                 │
┌────────────────┴────────────────────────────────────────────────────┐
│                    TiKV (每个节点本地持久化)                         │
│                                                                     │
│  存储位置: RocksDB (Raft Engine) — REGION_STATE_KEY                 │
│  数据结构: RegionLocalState {                                       │
│      State:  PeerState (Normal / Applying / Tombstone / Merging)    │
│      Region: metapb.Region (和 PD 中的相同)                         │
│      MergeState: 合并进行中的状态                                   │
│  }                                                                  │
│                                                                     │
│  更新时机:                                                          │
│   - Raft Apply Split/Merge/ConfChange 日志后写入                    │
│   - TiKV 重启时从本地加载，恢复 Region 信息                         │
│                                                                     │
│  Leader 职责:                                                       │
│   - 定期向 PD 发送 RegionHeartbeat（携带 Region 元信息 +            │
│     Leader 信息 + 统计数据）                                        │
│   - 响应请求时校验 RegionEpoch，不匹配则返回 EpochNotMatch          │
└────────────────┬────────────────────────────────────────────────────┘
                 │  BatchScanRegions / GetRegion (gRPC)
                 │  EpochNotMatch 错误返回 currentRegions
                 │
┌────────────────┴────────────────────────────────────────────────────┐
│               TiDB / client-go (内存缓存)                           │
│                                                                     │
│  存储位置: RegionCache (内存)                                       │
│  数据结构:                                                          │
│    regionIndexMu {                                                  │
│      regions:        map[RegionVerID]*Region  // ID+版本→Region     │
│      latestVersions: map[uint64]RegionVerID   // ID→最新版本映射    │
│      sorted:         *SortedRegions (B-tree)  // Key→Region 有序   │
│    }                                                                │
│                                                                     │
│  Region 结构:                                                       │
│    Region {                                                         │
│      meta:  *metapb.Region      // 从 PD 获取的原始元信息           │
│      store: *regionStore {      // 副本的 Store 信息               │
│        stores:      []*Store    // 各 Peer 所在 Store 的连接信息    │
│        accessIndex: [][]int     // TiKV/TiFlash 访问索引           │
│        workTiKVIdx: AccessIndex // 当前 Leader 在 stores 中的位置   │
│        buckets:     *Buckets    // Region 内部的 Bucket 信息       │
│      }                                                              │
│      ttl: int64                 // 缓存 TTL                        │
│    }                                                                │
│                                                                     │
│  刷新方式:                                                          │
│   1. 按需加载: 首次访问某 Key 范围时从 PD 获取                      │
│   2. 错误驱动: 收到 EpochNotMatch 等错误后更新或失效                │
│   3. 后台刷新: TTL 过期后异步从 PD 刷新                             │
└─────────────────────────────────────────────────────────────────────┘
```

### PD 中的存储

PD 是 Region 元信息的**权威源（Source of Truth）**。

**底层存储**：PD 使用内嵌的 **etcd** 持久化集群元数据。Region 信息同时维护在内存中的 Region Tree（按 Key 排序的树结构）中，供快速查询。

**数据来源**：TiKV 的每个 Region Leader 定期（默认约 60 秒，变化时立即）向 PD 发送 `RegionHeartbeat`：

```go
// kvproto/pkg/pdpb — TiKV → PD 的心跳请求
type RegionHeartbeatRequest struct {
    Region          *metapb.Region  // Region 元信息（ID, Key范围, Epoch, Peers）
    Leader          *metapb.Peer    // 当前 Leader 是哪个 Peer
    DownPeers       []*PeerStats    // Leader 认为 down 的 Peer
    PendingPeers    []*metapb.Peer  // 还在同步中的 Peer
    ApproximateSize uint64          // Region 大小（近似，用于调度）
    ApproximateKeys uint64          // Region Key 数量（近似）
    BytesWritten    uint64          // 写入流量
    BytesRead       uint64          // 读取流量
    Term            uint64          // Raft Term
}
```

**PD 提供的查询接口**（client-go 通过 `pd.Client` 调用）：

| 接口 | 用途 |
|------|------|
| `GetRegion(key)` | 根据 Key 查找所在 Region |
| `GetRegionByID(id)` | 根据 Region ID 查找 |
| `ScanRegions(startKey, endKey, limit)` | 扫描一段 Key 范围内的所有 Region |
| `BatchScanRegions(keyRanges, limit)` | 批量扫描多个 Key 范围的 Region |

### TiKV 中的存储

每个 TiKV 节点在本地持久化它所负责的 Region 的元信息。

**存储位置**：写入 RocksDB（Raft Engine 的一部分），使用特定的 Key 前缀 `REGION_STATE_KEY`。

**数据结构** — `RegionLocalState`：

```go
// kvproto/pkg/raft_serverpb
type RegionLocalState struct {
    State      PeerState       // Normal / Applying / Tombstone / Merging
    Region     *metapb.Region  // Region 元信息（和 PD 中的相同结构）
    MergeState *MergeState     // 合并进行中的状态
}

// PeerState 枚举
const (
    PeerState_Normal    = 0  // 正常服务
    PeerState_Applying  = 1  // 正在 Apply Snapshot
    PeerState_Tombstone = 2  // 已被销毁（Split/Merge 后的旧 Region）
    PeerState_Merging   = 3  // 正在执行 Merge
)
```

**更新时机**：
- Raft Apply 产生 Region 变化的日志时（Split、Merge、ConfChange）
- 收到 Snapshot 并 Apply 时
- TiKV 重启时从本地读取，恢复所有 Region 的 Raft 状态机

**TiKV 重启恢复流程**：
```
TiKV 启动
  ├── 扫描 RocksDB 中所有 REGION_STATE_KEY
  ├── 过滤掉 Tombstone 状态的 Region
  ├── 对每个 Normal/Applying 状态的 Region:
  │     ├── 恢复 Raft 状态机
  │     ├── 恢复 Apply State
  │     └── 开始 Raft tick
  └── Leader 选举后向 PD 发送心跳
```

### TiDB 的 Region Cache

TiDB（通过 client-go）在内存中维护 Region 元信息的缓存。

**核心数据结构**：

```go
// client-go/internal/locate/region_cache.go
type RegionCache struct {
    pdClient pd.Client           // PD 客户端，用于查询 Region
    mu       regionIndexMu       // Region 索引（加锁保护）
    stores   storeCache          // Store 信息缓存
}

type regionIndexMu struct {
    sync.RWMutex
    regions        map[RegionVerID]*Region  // {ID, ConfVer, Ver} → Region 映射
    latestVersions map[uint64]RegionVerID   // RegionID → 最新版本号
    sorted         *SortedRegions           // B-tree，按 StartKey 排序
}
```

**B-tree 索引**：`SortedRegions` 使用 B-tree（`btree.BTreeG`）按 Region 的 `StartKey` 排序存储，支持：
- `O(log N)` 按 Key 查找所在 Region
- `O(log N + M)` 范围扫描（M 为结果数）
- 插入时自动检测和清理交叉 Region

**缓存刷新策略**：

| 触发条件 | 刷新方式 |
|---------|---------|
| 首次访问某 Key 范围 | `BatchLocateKeyRanges` → 先查缓存，缺失部分查 PD |
| `EpochNotMatch` 错误 | TiKV 在错误中附带新 Region 信息，直接更新缓存 |
| `RegionNotFound` / `KeyNotInRegion` | `InvalidateCachedRegion` 失效缓存，下次访问时重新从 PD 加载 |
| `NotLeader` 错误 | 更新 Leader 信息（`UpdateLeader`），不失效整个 Region |
| TTL 过期 | 后台 goroutine 异步标记需刷新，下次访问时触发 |
| `StoreNotMatch` | 失效缓存，重新解析 Store 地址 |

**插入时的一致性检查**：

```go
func (mu *regionIndexMu) insertRegionToCache(cachedRegion *Region, ...) bool {
    newVer := cachedRegion.VerID()
    oldVer, ok := mu.latestVersions[newVer.id]

    // 拒绝过期的 Region（epoch 更旧的不覆盖更新的）
    if ok && (oldVer.GetVer() > newVer.GetVer() ||
              oldVer.GetConfVer() > newVer.GetConfVer()) {
        return false  // 丢弃过期数据
    }

    // 删除 B-tree 中与新 Region Key 范围重叠的旧条目
    mu.sorted.removeIntersecting(cachedRegion, newVer)

    // 插入新 Region
    mu.sorted.ReplaceOrInsert(cachedRegion)
}
```

### 元信息流转全景

```
                    ┌─────────────┐
                    │     PD      │
                    │  (etcd 持久化)│
                    │             │
                    │ Region Tree │ ◄─── 权威源
                    │ (按Key排序)  │
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
      RegionHeartbeat  ScanRegions  GetRegion
        (上报)          (查询)       (查询)
              │            │            │
    ┌─────────┴──┐    ┌────┴────┐  ┌───┴────┐
    │   TiKV-1   │    │ TiKV-2  │  │ TiKV-3 │
    │            │    │         │  │        │
    │ RocksDB:   │    │ ...     │  │ ...    │
    │ RegionLocal│    │         │  │        │
    │ State      │    │         │  │        │
    │ (持久化)    │    │         │  │        │
    └────────────┘    └─────────┘  └────────┘
              │
     EpochNotMatch 错误
     (附带最新 Region)
              │
    ┌─────────┴──────────┐
    │    TiDB (client-go) │
    │                     │
    │  RegionCache:       │
    │   B-tree (按Key)    │
    │   Map (按ID+版本)   │
    │   (内存缓存)        │ ◄─── 按需加载 + 错误驱动刷新
    └─────────────────────┘
```

| 存储层 | 存储介质 | 持久性 | 数据新鲜度 | 用途 |
|--------|---------|--------|-----------|------|
| **PD** | etcd（磁盘） | 持久 | 最新（心跳驱动） | 权威源，调度决策 |
| **TiKV** | RocksDB（磁盘） | 持久 | 实时（Raft Apply 时更新） | 重启恢复，请求校验 |
| **TiDB** | 内存 B-tree | 非持久 | 近实时（按需刷新） | 请求路由，Key→Region 映射 |

---

## Region 分裂合并过程中如何确保数据一致性

Region 分裂（Split）和合并（Merge）是 TiKV 自动维护数据分布的核心机制。在这些操作进行期间和完成之后，TiDB 客户端可能持有过期的 Region 信息，因此需要一套完整的机制来保证查询的正确性和一致性。

### 核心保障机制概览

```
                      TiKV 侧                        TiDB 侧
              ┌─────────────────────┐        ┌──────────────────────┐
              │  1. Raft 协议保证    │        │  3. Region Epoch     │
              │     原子性           │        │     版本检测          │
              ├─────────────────────┤        ├──────────────────────┤
              │  2. Region Epoch    │───────▶│  4. Region Error     │
              │     版本递增         │        │     自动重试          │
              ├─────────────────────┤        ├──────────────────────┤
              │  5. 分裂/合并期间    │        │  6. Region Cache     │
              │     读写阻塞         │        │     失效与刷新        │
              └─────────────────────┘        └──────────────────────┘
```

### 1. Region Epoch：分裂合并的版本号机制

每个 Region 都有一个 **RegionEpoch**，它是保证一致性的关键：

```go
// kvproto/pkg/metapb/metapb.pb.go
type RegionEpoch struct {
    // ConfVer: 当 Region 的 Peer 成员变更时自增（Add/Remove Peer）
    ConfVer uint64
    // Version: 当 Region 分裂或合并时自增
    Version uint64
}
```

**版本变化规则**：

| 操作 | ConfVer | Version |
|------|---------|---------|
| Region Split | 不变 | +1（原 Region 和新 Region 都递增） |
| Region Merge | 不变 | +1（合并后的目标 Region 递增） |
| Add Peer | +1 | 不变 |
| Remove Peer | +1 | 不变 |

TiDB 在发送每个 RPC 请求时都会携带它所知道的 `RegionEpoch`（包含在 `RegionVerID` 中）。TiKV 收到请求后会将携带的 epoch 与自身当前的 epoch 进行比较。

### 2. TiKV 侧：Raft 保证分裂/合并的原子性

**Region Split 过程**（以分裂为两个 Region 为例）：

```
原 Region [a, z), Epoch{ConfVer=1, Version=1}
   │
   │  1. PD 调度或 TiKV 自主触发 Split
   │
   ▼
Leader 提出 Split Raft 日志
   │
   │  2. Raft 共识：所有 Peer 都 Apply 这条 Split 日志
   │     这是原子的——要么全部 Peer 都分裂，要么都不分裂
   │
   ▼
Apply Split:
   ├── Region 1 [a, m), Epoch{ConfVer=1, Version=2}  ← Version 递增
   └── Region 2 [m, z), Epoch{ConfVer=1, Version=2}  ← 新 Region
```

关键点：
- Split 是通过 **Raft 日志** 传播的，所有副本原子地执行分裂操作
- 分裂前所有进行中的 Raft 日志必须先 Apply 完成
- 分裂后，原 Region 的 Key 范围缩小，新 Region 获得后半段数据（数据已在本地，只是修改 Region 元数据）

**Region Merge 过程**：

```
Region A [a, m), Epoch{ConfVer=1, Version=2}  ← source
Region B [m, z), Epoch{ConfVer=1, Version=2}  ← target
   │
   │  1. PD 调度 Merge
   │
   ▼
PrepareMerge (在 source Region A 上):
   │  - 通过 Raft 提案，阻止 source 接受新的写入
   │  - 等待所有进行中的 Raft 日志 Apply 完成
   │
   ▼
CommitMerge (在 target Region B 上):
   │  - 通过 Raft 提案，原子地将 source 的数据并入 target
   │  - Target Region B 的 Version 递增
   │
   ▼
结果: Region B [a, z), Epoch{ConfVer=1, Version=3}
       Region A 被销毁
```

关键点：
- Merge 分为 `PrepareMerge` 和 `CommitMerge` 两个阶段
- `PrepareMerge` 期间 source Region 拒绝新的写入（返回 `ProposalInMergingMode` 错误）
- 读请求可能收到 `ReadIndexNotReady` 错误
- 整个过程通过 Raft 保证原子性

### 3. TiDB 侧：Region Error 检测与重试

当 Region 发生分裂或合并后，TiDB 持有的 Region 信息可能过期。TiKV 通过返回 **Region Error** 来通知客户端。client-go 的 `RegionRequestSender.onRegionError()` 处理所有类型的 Region 错误：

#### EpochNotMatch（最常见的分裂/合并错误）

当 TiDB 携带的 RegionEpoch 与 TiKV 当前的不匹配时触发：

```go
// client-go/internal/locate/region_request.go
if epochNotMatch := regionErr.GetEpochNotMatch(); epochNotMatch != nil {
    // TiKV 在 EpochNotMatch 错误中附带了当前的 Region 信息
    // （可能是分裂后的多个 Region）
    retry, err := s.regionCache.OnRegionEpochNotMatch(
        bo, ctx, epochNotMatch.CurrentRegions)
    return retry, err
}
```

`OnRegionEpochNotMatch` 的处理逻辑：

```go
// client-go/internal/locate/region_cache.go
func (c *RegionCache) OnRegionEpochNotMatch(bo, ctx, currentRegions) {
    // 1. 检查是否是 TiDB 的 epoch 超前于 TiKV（异常情况）
    //    如果是，说明缓存可能不一致，需要 backoff 重试
    for _, meta := range currentRegions {
        if meta.GetId() == ctx.Region.id &&
           meta.Epoch.Version < ctx.Region.ver {
            return true, bo.Backoff(...)  // 退避重试
        }
    }

    // 2. 用 TiKV 返回的最新 Region 信息更新本地缓存
    for _, meta := range currentRegions {
        region := newRegion(meta)
        region.switchWorkLeaderToPeer(...)  // 设置 Leader
        newRegions = append(newRegions, region)
    }

    // 3. 使旧的缓存条目失效
    if needInvalidateOld {
        cachedRegion.invalidate(EpochNotMatch)
    }

    // 4. 将新 Region 信息插入缓存
    for _, region := range newRegions {
        c.insertRegionToCache(region, true, true)
    }

    return false, nil  // 不直接重试，让上层用新缓存重建请求
}
```

#### 分裂场景下 EpochNotMatch 的具体表现

```
TiDB 缓存: Region 1 [a, z), Epoch{Version=1}
   │
   │  TiDB 发送 Coprocessor 请求，扫描 [a, z)
   │
   ▼
TiKV 发现 Region 已分裂:
   Region 1 [a, m), Epoch{Version=2}
   Region 2 [m, z), Epoch{Version=2}
   │
   │  返回 EpochNotMatch{
   │      CurrentRegions: [Region1{[a,m)}, Region2{[m,z)}]
   │  }
   │
   ▼
TiDB client-go 处理:
   1. 用 Region1, Region2 更新 Region Cache
   2. 返回到 Coprocessor 层（handleCopResponse）
   │
   ▼
Coprocessor 层 (handleCopResponse):
   1. 收到 RegionError
   2. Backoff（BoRegionMiss）
   3. 用 task.ranges 重新调用 buildCopTasks
   4. buildCopTasks 用更新后的 Region Cache 重新切分范围
      → copTask 1: Region 1 [a, m), ranges = [a, m)
      → copTask 2: Region 2 [m, z), ranges = [m, z)
   5. 返回新 tasks 作为 remains，继续执行
```

#### 合并场景下的错误处理

```
TiDB 缓存: Region 1 [a, m) 和 Region 2 [m, z)
   │
   │  Region 已合并为 Region 2 [a, z)
   │
   ▼
发送请求到旧的 Region 1 → 可能收到:
   ├── RegionNotFound    ← Region 1 已不存在
   │     → InvalidateCachedRegion(Region1)
   │     → 上层重新 buildCopTasks，从 PD 刷新 Region 信息
   │
   ├── EpochNotMatch     ← 如果目标节点知道合并后的 Region
   │     → 用 CurrentRegions 中的合并后 Region 更新缓存
   │
   └── KeyNotInRegion    ← Key 不再属于该 Region
         → InvalidateCachedRegion
         → 上层重建请求
```

### 4. 两层重试机制

Region 分裂/合并的一致性保证通过**两层重试**实现：

**第一层：client-go `SendReqCtx` 内部重试**

```go
// SendReqCtx 内部是一个 for 循环
for !state.next() {
    // next() 内部:
    // 1. 从 Region Cache 获取 Region 地址
    // 2. 发送 RPC
    // 3. 如果收到 Region Error:
    //    - NotLeader → 更新 Leader，重试
    //    - StaleCommand → Backoff，重试
    //    - ReadIndexNotReady → Backoff，重试（正在分裂/合并中）
    //    - ProposalInMergingMode → Backoff，重试（正在合并中）
    //    - EpochNotMatch → 更新缓存，返回给上层
    //    - RegionNotFound → 失效缓存，返回给上层
}
```

`SendReqCtx` 能处理的是：请求发到了正确的 Region 但 Leader 变化或暂时不可用的情况。它**不会**改变请求的 Key 范围。

**第二层：Coprocessor `handleCopResponse` 重建任务**

当第一层返回 `RegionError`（如 `EpochNotMatch`、`RegionNotFound`），Coprocessor 层需要**重新切分范围**：

```go
// pkg/store/copr/coprocessor.go - handleCopResponse
if regionErr := getRegionError(resp); regionErr != nil {
    // 1. Backoff
    bo.Backoff(tikv.BoRegionMiss(), ...)

    // 2. 用该 task 原始的 ranges 重新构建 copTasks
    //    此时 Region Cache 已被第一层更新
    remains, err := buildCopTasks(bo, task.ranges, &buildCopTaskOpt{
        req:   worker.req,
        cache: worker.store.GetRegionCache(),
    })

    // 3. 将新 tasks 返回为 remains，由 copIterator 继续调度
    return &copTaskResult{remains: remains}, nil
}
```

这层重试的关键是：使用**原始 Key 范围**重新调用 `buildCopTasks`，让 Region Cache 中的最新 Region 信息重新切分范围。这保证了：
- 不会遗漏任何 Key 范围
- 不会重复扫描已完成的范围（因为用的是失败 task 的 ranges，不是全局 ranges）

### 5. 分裂/合并期间的读写阻塞

TiKV 在分裂/合并的关键阶段会暂时阻塞读写请求：

| 错误类型 | 触发条件 | TiDB 处理 |
|---------|----------|----------|
| `ReadIndexNotReady` | Region 正在分裂或合并，ReadIndex 无法处理 | `BoRegionScheduling` 退避重试 |
| `ProposalInMergingMode` | Region 正在执行 Merge，拒绝写入 | `BoRegionScheduling` 退避重试 |
| `RegionNotInitialized` | 新 Region 的 Peer 尚未初始化完成 | `BoMaxRegionNotInitialized` 退避重试 |
| `DataIsNotReady` | Follower 的数据还没追上（Stale Read 场景） | 切换到 Leader 重试 |

这些阻塞是**短暂的**（通常毫秒级），分裂/合并完成后自动恢复。

### 6. MVCC 快照读的一致性保障

即使 Region 发生分裂/合并，MVCC 快照读的一致性仍然得到保障：

1. **start_ts 不变**：一个事务/查询的 `start_ts` 在整个执行期间不变，无论请求被重试多少次
2. **Raft 保证线性一致性**：所有的分裂/合并操作都通过 Raft 日志序列化，有明确的时间戳顺序
3. **MVCC 版本不受影响**：分裂/合并只是改变 Region 的 Key 范围和元数据，不会修改数据的 MVCC 版本

```
时间线:
  t1: 事务开始, start_ts = 100
  t2: Region Split 发生 (所有数据版本不变，只是 Region 边界变了)
  t3: 事务读取 → 使用 start_ts=100 读取
      即使请求因 EpochNotMatch 被重试，
      重试后仍然使用 start_ts=100，
      读到的 MVCC 数据是一致的
```

### 7. 完整的一致性保证链

```
┌──────────────────────────────────────────────────────────────────┐
│                     一致性保证的完整链路                           │
│                                                                  │
│  TiKV 侧:                                                       │
│  ┌─────────────────────────────────────────────────────────┐     │
│  │ Raft 共识 → Split/Merge 日志原子 Apply → Epoch 递增     │     │
│  │                                                         │     │
│  │ Split/Merge 期间:                                       │     │
│  │  - 写请求被 ProposalInMergingMode 阻塞                  │     │
│  │  - 读请求被 ReadIndexNotReady 阻塞                      │     │
│  │  - 完成后返回 EpochNotMatch 给过期请求                   │     │
│  └─────────────────────────────────────────────────────────┘     │
│                              │                                   │
│                              ▼                                   │
│  client-go 侧:                                                  │
│  ┌─────────────────────────────────────────────────────────┐     │
│  │ SendReqCtx:                                              │     │
│  │  - EpochNotMatch → 用 TiKV 返回的新 Region 更新缓存     │     │
│  │  - RegionNotFound → 失效缓存                            │     │
│  │  - ReadIndexNotReady/Merging → 退避重试                  │     │
│  └─────────────────────────────────────────────────────────┘     │
│                              │                                   │
│                              ▼                                   │
│  Coprocessor 侧:                                                │
│  ┌─────────────────────────────────────────────────────────┐     │
│  │ handleCopResponse:                                       │     │
│  │  - 收到 RegionError → 用原始 ranges 重新 buildCopTasks  │     │
│  │  - 新 tasks 使用更新后的 Region Cache                    │     │
│  │  - 保证所有 Key 范围被完整覆盖                           │     │
│  └─────────────────────────────────────────────────────────┘     │
│                              │                                   │
│                              ▼                                   │
│  MVCC 侧:                                                       │
│  ┌─────────────────────────────────────────────────────────┐     │
│  │ start_ts 贯穿整个请求生命周期（包括重试）                │     │
│  │ → 保证快照读的一致性不受 Region 变化影响                 │     │
│  └─────────────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────────┘
```

### 核心源码索引（Region 分裂合并相关）

| 文件 | 关键函数/类型 | 用途 |
|------|-------------|------|
| `kvproto/pkg/metapb/metapb.pb.go` | `RegionEpoch{ConfVer, Version}` | Region 版本号定义 |
| `kvproto/pkg/errorpb/errorpb.pb.go` | `EpochNotMatch{CurrentRegions}` | 分裂/合并错误定义 |
| `kvproto/pkg/errorpb/errorpb.pb.go` | `ProposalInMergingMode`, `ReadIndexNotReady` | 合并/分裂期间阻塞错误 |
| `client-go/.../region_request.go` | `onRegionError()` | 所有 Region 错误的分发处理 |
| `client-go/.../region_cache.go` | `OnRegionEpochNotMatch()` | EpochNotMatch 后更新缓存 |
| `client-go/.../region_cache.go` | `InvalidateCachedRegion()` | 失效过期 Region 缓存 |
| `pkg/store/copr/coprocessor.go` | `handleCopResponse()` | Coprocessor 层 Region 错误重试 |
| `pkg/store/copr/coprocessor.go` | `buildCopTasks()` | 用最新 Region 信息重新切分范围 |
