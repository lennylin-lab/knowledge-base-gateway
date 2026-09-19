# 流式卡顿超时替代总时长超时

## Goal

修复 issue #9：网关当前的 LLM 流式超时是「一次响应的总时长」（默认 60s），健康但耗时长（>60s）的流会被误杀，而上游卡顿要等总 deadline 才被感知。目标：流式路径改为「帧间卡顿检测」——每收到一帧重置计时器，超过阈值无新帧即判定卡顿；首帧前的卡顿可重试，重试满 10 次终止；流式总时长改为独立的大额兜底上限。非流式行为不变。

## Background（代码证据）

- 总时长超时：`internal/gateway/service.go:181`（`withDeadline`）将 `cfg.RequestTimeout`（默认 60s，`internal/config/config.go:140`）盖在 Stream/Complete 整个请求上；异步任务用 `AsyncJobTimeout`=10min（`cmd/gateway/main.go:180`）。
- 帧间无检测：SSE 读取循环 `internal/provider/openai.go:393-398`、`internal/provider/anthropic.go:386-390` 用 `bufio.Scanner` 逐行读，仅在每行检查 `ctx.Err()`；上游停发帧只能等总 deadline。
- 重试纪律：`provider.RetryEligible`（`internal/provider/provider.go:66`）放行 pre-output 的 timeout/network/429/5xx；`internal/gateway/service.go:381` 规定 `outputStarted` 后绝不重试；`cfg.MaxRetries` 默认 2（`internal/config/config.go:144`），仅作用首选候选。
- 下行协议无「重启」事件（`internal/model/model.go:193-199`）；出帧后重发必然造成客户端内容重复。
- 成文规约：`.trellis/spec/backend/error-handling.md:19`「Retry only pre-output …; never retry after SSE starts.」
- 异步路径不调用 `Service.Stream`（仅 `internal/httpapi/chat.go:298`、`internal/httpapi/responses.go:453` 两个同步 handler 调用），故本任务不影响异步任务。

## Requirements

- R1 帧间卡顿超时：流式路径每收到一帧重置计时器；超过 `GATEWAY_STREAM_STALL_TIMEOUT`（默认 30s，0=关闭）无新帧即返回 timeout 类错误；首帧等待（TTFT）同用该阈值。
- R2 卡顿重试仅限首帧之前（已决策，方案 A）：首帧卡顿重试最多 `GATEWAY_STREAM_MAX_RETRIES`（默认 10）次，重试满后终止并按现有 pre-output 错误映射返回；已出帧后的卡顿立即失败终止，不重发、不产生重复内容，审计 timeout 类。
- R3 流式总时长改为独立兜底上限 `GATEWAY_STREAM_TOTAL_TIMEOUT`（默认 10m，0=不限制），健康长流不再被 `RequestTimeout` 误杀；`Complete`/`Embeddings` 仍用 `RequestTimeout`（R5）。
- R4 流式路径重试预算独立：`StreamMaxRetries` 仅作用于 `Stream` 的首选候选（沿用「额外次数」约定）；`MaxRetries` 及其默认值 2 继续用于非流式。
- R5 非流式与异步路径行为零变化。
- R6 三个新 env 均有默认值与启动期校验，非法值拒绝启动；`GATEWAY_STREAM_STALL_TIMEOUT=0 GATEWAY_STREAM_TOTAL_TIMEOUT=60s GATEWAY_STREAM_MAX_RETRIES=2` 可近似还原旧行为（回滚开关）。
- R7 spec 同步：`.trellis/spec/backend/error-handling.md:19` 措辞补充流式卡顿重试与独立总上限（「never retry after SSE starts」保留）。

## Technical Notes

- 技术设计见 `design.md`：provider 层 AfterFunc 看门狗（派生 context 取消中断阻塞的 `scanner.Scan()`，Reset 竞态用触发标记处理）、`Service.Stream` 换用流式专用总上限与重试预算、fake provider 不接看门狗（卡顿用 contract_test 的 httptest 慢服务器覆盖）。
- 执行清单与验证命令见 `implement.md`。

## Acceptance Criteria

- [ ] AC1 长健康流：上游持续出帧、总时长超过 `RequestTimeout`(60s) 但低于 `GATEWAY_STREAM_TOTAL_TIMEOUT` → 流正常完成，不被误杀（openai 与 anthropic 合同测试）。
- [ ] AC2 首帧卡顿：上游建连后超过 stall 阈值不发首帧 → 每次尝试判 ClassTimeout 并重试；重试满 `1+StreamMaxRetries` 次仍卡 → 终止，返回现有 pre-output 错误映射（SSE error 事件）。
- [ ] AC3 首帧卡顿后恢复：第 N 次（N ≤ 1+StreamMaxRetries）尝试正常出帧 → 响应成功，客户端无重复内容。
- [ ] AC4 出帧后卡顿：已发若干帧后上游停发超过 stall 阈值 → 流立即失败终止，不重试；已 emit 的帧保留，不发 done/completed，审计 timeout 类。
- [ ] AC5 非流式回归：现有 `Complete`/`Embeddings` 超时与重试测试全绿，行为不变。
- [ ] AC6 配置：三个新 env 默认值正确、0 值特例生效（关检测/不限制）、非法值启动报错；异步路径测试不受影响。
- [ ] AC7 `gofmt -l .` 无输出、`go vet ./...` 与 `go test ./...` 全绿。

## Out of Scope

- 非 HTTP/SSE 线上协议改动（无「重启」事件，方案 A 不需要）。
- 管理面/路由表级的 stall 配置（仅 env 级）。
- 异步任务超时语义。
- stall 专属审计字段或指标（沿用现有 error class 打点）。
