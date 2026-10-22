# 真实性能基准与持续负载工具

`ProxyBenchmark.exe` 是独立的 Windows x64 命令行工具，无需安装 Go/Python。随完整便携包移动即可，默认按自身位置寻找上一层的 `AntigravitySystemProxy.exe`。不读取业务账号，不修改系统代理或 Antigravity 配置，不停止已安装后台；创建独立临时配置、动态端口和测试进程，结束后只清理自己创建的进程与已知临时文件。

在解压包根目录运行：

```powershell
.\benchmark\ProxyBenchmark.exe -out .\benchmark-results-10min -samples 300 -bulk-samples 12 -resource-window 10s -soak-duration 10m
```

追加公网小流量对照，地址应填写同一上游代理的实际地址；工具不会自动读取或切换系统设置：

```powershell
.\benchmark\ProxyBenchmark.exe -out .\benchmark-results-wan -samples 300 -bulk-samples 12 -soak-duration 10m -wan-upstream http://127.0.0.1:7890 -wan-samples 16
```

24 小时持续负载（保持终端运行，可按 Ctrl+C 中止并生成部分报告）：

```powershell
.\benchmark\ProxyBenchmark.exe -out .\benchmark-results-24h -samples 300 -bulk-samples 12 -resource-window 15s -soak-duration 24h -qps 4 -concurrency 4
```

`-out` 必须是新的目录，避免覆盖历史结果；含空格路径加引号。更换候选版本可用 `-bridge-exe "D:\Tools\Candidate\AntigravitySystemProxy.exe"`，不需要安装候选。本工具的 Go 源码在 `benchmark-source`；编译使用 `go build -buildvcs=false -trimpath -o ../benchmark/ProxyBenchmark.exe ./`。

普通基准可增加停止负载后的观察窗口，例如 `-soak-duration 10m -cooldown-duration 5m`。cooldown默认12秒，允许12秒至1小时；期间停止投递业务负载，继续资源采样和已有后台健康/历史工作，随后检查活动连接及队列归零。它不会主动调用GC、释放OS内存或重启工作进程。metadata新增cooldown_requested_seconds / cooldown_actual_seconds，普通套件标记4-idle-recovery；Ctrl+C可能缩短实际窗口并标记interrupted。逐事件stream套件保留原来的归零等待规则，不使用这个普通套件参数。

判断内存应分清Go堆对象、Windows私有提交和工作集：私有提交不等于常驻物理内存。当前Go 1.27.2标准密码学模块还带有32 MiB静态缓冲区，Go堆数值不会涵盖这部分；它是固定成本，不能直接用来解释后续斜率。应观察相同负载下的后段趋势、停止负载后的回落，以及线程/句柄/活动连接，避免把一次峰值或缓存保留直接当作泄漏。

默认 `-bridge-mode entry` 测完整入口架构（v5.15 及以上）：私有临时目录中复制候选，启动常驻入口和工作进程，流量实际经过两者；Windows CPU、工作集、私有提交和句柄为两个进程之和。Go heap/goroutine、请求和队列统计仍来自工作进程。共享内存页可能在工作集求和中重复计数；进程各自生命周期峰值之和不表示同一时刻的峰值，应看采样合计峰值。

隔离固定入口的开销、仅测独立工作进程时，明确追加 `-bridge-mode worker`。两种模式不能直接混比资源成本；报告记录 `bridge_mode` 和 `resource_scope`。为兼容旧分析脚本，JSON 中 `Worker` / `worker_cpu_percent_one_core` 字段名保留，其范围以元数据为准。旧工具全部属于 worker 模式，未计入常驻入口开销。

入口模式结束时用测试私有配置停止其后台，并核实进程创建时间、映像位置和控制接口身份；不操作安装实例。启动中的取消会等待有界启动命令结束，再清理，避免取消后又启动孤立后台。继承环境中的内部工作进程身份会被移除。测量期间进程切换会报身份错误，不能把重启前后 CPU 累计量拼接成有效趋势。

入口模式的启动器在挂起状态加入 Windows Job 后才恢复执行，所有后代均归测试 Job 管理；Job 最后一个句柄关闭时终止其余测试后代，覆盖尚未来得及发布控制记录的启动失败，以及测试工具被硬终止。若系统限制导致无法加入 Job，明确启动失败，不降级为不受管理的后台。Windows Job 属于测试隔离机制，生产安装后台不受它管理；硬终止工具仍可能留下临时文件。

每个阶段结束后，工具关闭该阶段自己创建的客户端底层连接，避免 HTTP/2 取消流在清理空闲池后才释放、留下测试隧道干扰下一阶段。报告新增 `final_quiescent`：结束冷却后仍有活动请求、隧道、WS、队列或入口连接会返回非零。`http_connections` 包括正常缓存的普通 HTTP 上游空闲连接，允许按90秒策略回收，不把缓存非零误判为活动业务残留。该强制关闭仅针对测试客户端，生产流不受影响。

## 实际测量什么

v5.17上传优化验证新增 `-soak-scheme http`，使持续负载中的短请求、上传、下载与SSE走普通HTTP；默认仍为 `https`，WebSocket均继续通过WSS/CONNECT。元数据记录所选协议，避免用纯HTTPS持续负载冒充普通HTTP新路径的压力验证。旧报告没有该字段时为原默认HTTPS；不同协议的资源数据不可直接当作版本差异。

- 本地两条路径：客户端 → 同一个受控上游；客户端 → 隔离转发器 → 同一个受控上游。目标为专用 `bench.invalid`，由测试上游处理，避免生产转发器对回环目标的直连规则使对照失真。
- HTTP/HTTPS，各有冷连接和预热连接；每对 A/B 随机交错顺序，固定种子，预热不计入统计。冷连接指客户端不复用，转发器自身的普通 HTTP 上游连接池仍可能复用，符合真实使用行为。
- 16 MiB 上传/下载使用流式生成与 SHA256 校验。HTTPS 通过 CONNECT，源站与客户端可协商 HTTP/2；普通 HTTP 为 HTTP/1.1。
- 4 个固定速率窗口，以随机 AB/BA 次序分别测两轮直接/转发路径。默认每窗 10 秒，目标每秒 20 次 1 MiB 下载；请求串行，若无法跟上，实际完成量会下降，不宣称已经测出最大并发容量。
- 持续负载在两条路径间分配，共默认 4 请求/秒、4 个有界工作线程，混合小响应、256 KiB 上传、1 MiB 下载与 8 个逐步刷新的 SSE 事件。两条持久 WebSocket 每 2 秒校验一次 ping/pong。内容校验可以发现模拟流的截断/改变，不能证明真实模型业务确认可以恢复。
- Windows 高精度性能计数器计时；首字节 P50/P95/P99、总完成时间尾延迟、连接获取/TCP/TLS 阶段、复用、错误与吞吐统计。连接获取含连接池和建连，不等于纯代理排队。失败请求单独计数，不混入成功延迟分位数。
- 每 200 ms 采样 Windows 进程 CPU/工作集/私有提交/句柄，每秒取一次独立状态接口的 Go heap/goroutine/连接计数。压测进程与转发进程分开记录；CPU 100% 表示占满一个逻辑核心。公网只测低流量的公开 HTTPS 404 页面延迟，不测公网大文件吞吐；资源监控针对本地受控测试的转发进程。

## 输出与判读

输出 `report.md`、`summary.json`、`samples.jsonl`、`resources.jsonl`。报告保留候选 EXE 的 SHA256、版本、CPU 数、参数、采样周期与错误计数。原始逐条日志各上限 64 MiB，满后停止写原始日志，聚合仍持续；每组最多保留 8192 个蓄水池样本估算分位数。内存趋势仅分析启动后 60 秒以后的数据；首尾窗口各最多 60 秒。内存、句柄和 goroutine 趋势是观测线索，不自动宣布泄漏。长时间测试数据量受限，避免压测工具自身无限增长。

请求失败、身份/采样错误、负载投递丢弃或磁盘写入失败会返回非零退出码，仍尽量保留报告。计划结束时取消的在途请求不算传输故障，因此统计会略少于“速率 × 时长”；WebSocket 正常按截止时间关闭。持续负载延迟从实际开始发送请求起算，不包含压测工具排队等待；不能把它当作所有投递请求的端到端延迟。中止后的报告明确标记 `interrupted`，不是完整通过。

本地吞吐的瓶颈可能在测试源站、TLS、哈希和本机调度，不能直接当作公网速度。吞吐列是“成功有效载荷 / 成功请求时长之和”，固定窗口另报墙钟时间吞吐。少于 100 个样本的 P99 只作描述；公网少量交错请求不能排除线路波动、会话/节点变化等影响。建议在同一机器/上游/电源模式，避免其他下载或编译任务，重复多轮再比较。

10 分钟结果只能说明该次测试；24 小时命令是可执行入口，并不表示已经完成 24 小时验证。没有模型 API、账号登录、执行命令或业务重放测试，也不测试代理开关、后台恢复或升级故障注入；这些仍由现有专项回归覆盖。

指标依据：[Windows 进程内存](https://learn.microsoft.com/en-us/windows/win32/api/psapi/ns-psapi-process_memory_counters_ex)、[Windows CPU 计时](https://learn.microsoft.com/en-us/windows/win32/api/processthreadsapi/nf-processthreadsapi-getprocesstimes)、[Go HTTP trace](https://pkg.go.dev/net/http/httptrace#ClientTrace)。

兼容 v5.13 后台结构化历史：隔离测试结束时仅清理测试私有状态目录中的已知历史片段和锁文件，不读取或删除用户已安装实例的诊断历史。

## 逐事件流式延迟与混合负载

在包根目录运行：

```powershell
.\benchmark\ProxyBenchmark.exe -out .\stream-results -stream-events 200 -stream-rounds 5 -stream-interval 5ms -stream-load 4
```

stream-events大于0时只运行逐事件套件，跳过普通基准/公网阶段。HTTP与HTTPS分别测试空闲/混合负载，每轮随机交换direct/bridge次序，同一路径配独立的流式连接及背景传输连接。正式比较保留默认entry模式，worker仅用于隔离入口开销。每条流先做不计入事件统计的小请求预热，首事件时间仍含该次请求处理；冷建连请使用普通基准。

参数：事件数1–65536，轮数1–100，间隔1ms–1s，单流计划持续时间不超过20秒；混合背景工作数2–16。默认事件数0关闭该套件，其余默认4轮、10ms间隔、4个背景工作。每个背景工作反复传输16MiB，上传每次读取最多32KiB后间隔1ms，下载源每写32KiB后间隔1ms；这是受控带宽竞争测试，不等于压满真实网卡。已完成背景请求验证长度与SHA256；流结束时主动取消的背景请求排除错误计数，完成数单独报告。

源站在格式化每个合成SSE事件前记录QPC时间，客户端完整扫描事件空行分隔符后记录到达。两端在同一进程使用同一时钟，避免机器时钟偏移；结果仍含测试源站调度、TLS、扫描器、观测与本机网络开销。实际源站间隔也记录，用于识别源站本身停顿。不会追赶错过的定时点而人为生成补发突发。它不测真实模型token，不识别Antigravity加密正文。

stream-summary.json包含首事件P95、逐事件交付P50/P95/P99、到达间隔P99、相对源站的正向额外间隔P99及状态/资源；stream-events.jsonl保存逐事件序号、轮次、路径、源/客户端间隔和延迟，可据此计算最长停顿。每组超过8192事件时百分位采用有界随机水塘；原始文件上限64MiB，达到上限标记raw_truncated。first_event只有轮数个样本，小轮数P95描述性有限。重复/丢失/截断事件、时钟异常或过长行会使测试失败。

长流之外的普通HTTP空闲缓存无需清零；结束必须活动请求、CONNECT、WS、队列及入口归属连接归零。Ctrl+C会取消测试、输出可用的部分汇总并返回失败；独立测试后台通过原有所有权校验与Job清理。

追加 `-observe-writes` 会在测试自己的临时配置中启用 socket_write_metrics，要求候选支持并明确返回enabled=true；不修改已安装配置。正常对照保持关闭，观测开销实验应另列结果。汇总metadata记录开关。若要比较开启与关闭，请使用同一候选、同一套件参数交错运行。

v5.21 起 `-stream-log-mode deferred` 为默认值：每条流预分配最多65536条事件，接收期间只解析、计时和存入有界内存，结束并取消背景负载后才聚合及写盘。`inline` 保留旧的同步日志路径，仅用于观察者效应对照。旧版逐事件数据不能与新版直接比较来宣称生产提速。持续资源采样仍有独立线程和开销，deferred 不能完全消除测量扰动。

汇总新增 max_delivery_ms、max_arrival_gap_ms、max_source_gap_ms、max_excess_gap_ms，最大值使用所有事件，不受8192条分位数水塘限制。事件日志新增 source_qpc_ns / arrival_qpc_ns；资源日志新增 qpc_ns 和每次状态调用的 status_start_qpc_ns / status_end_qpc_ns。QPC 可在本次同机测试内关联，不应直接减去 Go trace 的 Time 字段（时基不同）。源端与客户端共用测试进程，本进程停顿可能影响双方；到达间隔大也可能只是源站迟迟未生成事件。

需要 Go 开发环境时，可在 benchmark-source 下对**合成流**显式采集轨迹：

```powershell
$env:AG_STREAM_TRACE_EXE=(Resolve-Path ..\AntigravitySystemProxy.exe).Path
$env:AG_STREAM_TRACE_OUT=Join-Path $env:TEMP ('ag-stream-trace-'+[guid]::NewGuid().ToString('N'))
go test -count=1 -run '^TestStreamTailTraceProbe$' -trace runner.trace -v ./
go tool trace runner.trace
Remove-Item Env:\AG_STREAM_TRACE_EXE,Env:\AG_STREAM_TRACE_OUT
```

轨迹记录源端写入前、刷新返回与客户端完整事件位置，以及测试进程的调度/GC/系统调用状态；不涵盖独立入口和工作进程的全部执行轨迹，也不能看到真实加密模型正文。轨迹会扰动调度，单独用于归因，正式性能对照不启用。仅在显式设置环境变量时运行该探针，普通单元测试跳过。Go 1.27 的文本解析使用 `go tool trace -d=parsed`，输出可能远大于二进制，应按需过滤。文档依据：[Go 执行轨迹](https://go.dev/blog/execution-traces-2024)、[runtime/metrics](https://pkg.go.dev/runtime/metrics)。

后续修复（benchmark_revision=6-stream-recovery）：在已启动的流式套件中取消测试，会停止发起下一路径的预热，保留已收集事件与汇总并标记 interrupted=true，退出码仍为失败。预热请求失败也会生成汇总，warmup_errors 单列计数，同时包含在 stream_errors 中；该路径不进入正式流式采样。套件初始化、创建输出文件或启动后台失败仍可能无法生成报告。取消后 final_quiescent=false 表示未完成归零验证，不能当作已确认连接泄漏；独立后台仍通过既有所有权清理退出。
