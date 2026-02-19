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
