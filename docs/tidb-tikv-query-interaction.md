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

### 核心源码文件索引

| 文件 | 用途 |
|------|------|
| `pkg/kv/kv.go` | 核心 KV 接口定义：Storage、Transaction、Snapshot、Client |
| `pkg/store/driver/tikv_driver.go` | TiKV 存储驱动，初始化 KVStore 和 CoprStore |
| `pkg/store/driver/txn/txn_driver.go` | 事务实现（tikvTxn），代理到 tikv.KVTxn |
| `pkg/store/driver/txn/snapshot.go` | 快照实现（tikvSnapshot），代理到 KVSnapshot |
| `pkg/store/copr/store.go` | Coprocessor Store，提供 CopClient 和 MPPClient |
| `pkg/store/copr/coprocessor.go` | Coprocessor 客户端核心：任务拆分、并发执行、结果收集 |
| `pkg/distsql/distsql.go` | DistSQL 入口，Select/Analyze/Checksum 函数 |
| `pkg/distsql/request_builder.go` | 构建 kv.Request（DAG + KeyRanges + 选项） |
| `pkg/distsql/select_result.go` | 结果迭代器，解码 Coprocessor 响应 |
| `pkg/executor/table_reader.go` | TableReaderExecutor，范围扫描主逻辑 |
| `pkg/executor/point_get.go` | PointGetExecutor，单行精确查找 |
| `pkg/kv/mpp.go` | MPP 相关接口定义 |
| `pkg/store/copr/mpp.go` | MPP 客户端实现 |
