# Design: 流式卡顿超时替代总时长超时（issue #9）

## 架构与边界

三层改动，职责分明：

1. **Config 层**（`internal/config/config.go`）：新增三个流式专用旋钮，env 解析沿用现有范式。
2. **Provider 层**（`internal/provider/`）：帧间卡顿看门狗，openai/anthropic 两个 SSE 读取循环接入；卡顿判定为 `ClassTimeout`。
3. **Gateway 层**（`internal/gateway/service.go`）：`Stream` 换用流式专用总上限与重试预算；重试纪律（outputStarted 后不重试）不变。

非流式（Complete/Embeddings）与异步路径（不调用 `Stream`，见 `internal/async/worker.go` 仅用 `Complete`）零改动。

## 配置契约

| env | 默认 | 校验 | 语义 |
|---|---|---|---|
| `GATEWAY_STREAM_STALL_TIMEOUT` | `30s` | `0` = 关闭卡顿检测（回滚开关）；否则正数 duration | 相邻两帧（含首帧 TTFT）最大等待 |
| `GATEWAY_STREAM_TOTAL_TIMEOUT` | `10m` | `0` = 不限制；否则正数 duration | 流式整请求兜底上限（Q2 决策） |
| `GATEWAY_STREAM_MAX_RETRIES` | `10` | 整数 0..10（与 `GATEWAY_MAX_RETRIES` 同界） | 流式路径首选候选的额外尝试次数 |

解析实现放进现有 duration env 表（`config.go:290` 一带）与整数校验范式（`config.go:163-168`）。

## Provider 看门狗

- 新增共享小工具（如 `internal/provider/stall.go`）：基于 `time.AfterFunc(stall, cancel)` 的看门狗，对派生 context 取消以中断阻塞中的 `scanner.Scan()`（取消请求 context 会关闭 `resp.Body`，Scan 立即返回 false）。
- 每读到一行（=一帧）`Reset`；结束后 `Stop`。注意 Reset 与已触发回调的竞态：看门狗持有 atomic 标记，读循环在 Scan 返回 false 后区分三种情况——① 看门狗触发且父 ctx 未过期 → `&Error{Class: ClassTimeout, Msg: "upstream stalled"}`；② 父 ctx 过期 → 现有 timeout 错误不变；③ 其余按现有 EOF/网络错误路径。
- 接入点：`internal/provider/openai.go:393-398` 与 `internal/provider/anthropic.go:386-390`。首帧前不 Reset 也生效（覆盖 TTFT 卡顿，即 Q1 决策中「可重试」的部分）。
- Fake provider 不接看门狗（进程内无阻塞读）；卡顿行为通过 contract_test 的 httptest 慢服务器覆盖（`contract_test.go:600+` 已有 Sleep 型慢流测试基建）。

## Gateway 层

- `Service` 增加字段：`StreamTotalTimeout`（0=不限制）、`StreamStallTimeout`（透传给 provider：通过 request context value 或 provider 构造参数注入，倾向 provider 构造参数，见下）、`StreamMaxRetries`。
- `Stream`（`service.go:336`）改动：
  - `withDeadline` → 流式专用：`StreamTotalTimeout <= 0` 时仅 `WithCancel`；否则 `WithTimeout(StreamTotalTimeout)`。`Complete`/`Embeddings` 仍用 `s.Timeout`。
  - 主候选重试预算：`maxTries = 1 + s.StreamMaxRetries`（原 `s.MaxRetries` 仅非流式沿用）。
  - `outputStarted` 门禁原样保留：出帧后任何错误（含卡顿）原样返回，不重试不切换（Q1 决策 A；spec「never retry after SSE starts」继续成立）。
- 看门狗阈值注入方式二选一（实现时定）：a) provider 构造参数（`NewOpenAI(..., stallTimeout)`），main.go 装配处传入；b) context value。倾向 a——stall 是适配器级传输参数，与 timeout 语义同族，context value 易被遗忘且难测试。

## 数据流（一次首帧卡顿重试）

```
client → chat handler → Service.Stream
  attempt 1: provider.Stream(watchdog=30s) → TTFT>30s → ClassTimeout("stalled")
             RetryEligible=true, outputStarted=false → backoff → attempt 2
  ...
  attempt N(N≤1+10): 仍卡 → 返回 timeout 错误 → handler 按 pre-output 映射
             （chat.go:331 SSE error 事件 / responses.go 同族）
```

出帧后卡顿：watchdog 触发 → 同样 ClassTimeout → `outputStarted=true` → 原样返回 → handler 走现有截断/失败收尾（不发 done/completed，审计 timeout 类）。

## 兼容与迁移

- 默认值下行为变化仅两处：① 健康长流不再被 60s 砍（修复点）；② 流式 pre-output 重试预算 2→10、新增卡顿检测。
- 回滚：`GATEWAY_STREAM_STALL_TIMEOUT=0` + `GATEWAY_STREAM_TOTAL_TIMEOUT=60s` + `GATEWAY_STREAM_MAX_RETRIES=2` 可精确还原旧行为（近似——旧路径 60s 来自 RequestTimeout）。
- 不改 HTTP/SSE 线上协议、不改管理面路由表、不改异步任务。

## 权衡记录

- **流式重试预算统一为 10**（而非仅卡顿类 10、其余 2）：单预算语义简单、测试面小；代价是非卡顿类 pre-output 错误（429/5xx/网络）在流式路径也最多试 11 次，受 StreamTotalTimeout 与 backoff 上限约束，风险可接受。issue 字面只约束卡顿，此为设计取舍。
- **重试语义沿用「额外次数」约定**：`StreamMaxRetries=10` = 首选候选最多 11 次尝试，与 `MaxRetries` 注释（`service.go:64`）一致。
- **`MaxRetries` 默认值与 `GATEWAY_MAX_RETRIES` 上界 0..10 不动**：非流式零影响。
- 看门狗 per-line Reset 粒度：SSE 一行即一帧粒度足够；不分帧内字节。

## 运维与观测

- 卡顿触发打点：沿用 providerSpanObserve 的 error class（timeout），attempt span 已有 `gw.stream`；无需新指标。可选后续：stall 专属审计字段，本期不做。
- `cmd/gateway/main.go:173-181` 装配处接线；`docker-compose.yml.example`、README 配置表补充三个新 env。

## Spec 更新（随实现提交）

`.trellis/spec/backend/error-handling.md:19` 的重试措辞需补一句：流式路径 pre-output 重试含卡顿（timeout 类）错误、总上限由流式专用旋钮约束；「never retry after SSE starts」保留。走 trellis-update-spec 流程。
