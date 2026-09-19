# Implement: 流式卡顿超时替代总时长超时（issue #9）

## 执行顺序

1. **Config 层**
   - [x] `internal/config/config.go`：新增 `StreamStallTimeout`（默认 30s，允许显式 0=关闭）、`StreamTotalTimeout`（默认 10m，允许显式 0=不限制）、`StreamMaxRetries`（默认 10，0..10）三个字段与 env 解析（沿用 duration 表与 0..10 整数范式；`0` 特例需绕过「正数」校验，单独分支处理）。
   - [x] `internal/config/config_test.go`：默认值、非法值报错、0 值特例。
2. **Provider 层**
   - [x] `internal/provider/stall.go`：AfterFunc 看门狗（含 Reset 竞态处理与触发标记）。
   - [x] `internal/provider/openai.go` Stream 读取循环、`internal/provider/anthropic.go` 流读取循环接入看门狗；卡顿 → `&Error{Class: ClassTimeout, Msg: "upstream stalled..."}`；父 ctx 过期路径行为不变。
   - [x] `OpenAI`/`Anthropic` 结构体新增导出字段 `StallTimeout`（零值=关闭，现有测试不受影响），`newProviderFromRegistry` 装配处注入 `cfg.StreamStallTimeout`；fake 不受影响。
   - [x] `internal/provider/contract_test.go`：三组用例——健康慢流（帧间隔 > RequestTimeout 总和场景不适用，这里验证看门狗不误触发）、首帧卡顿（返回 ClassTimeout）、出帧后卡顿（ClassTimeout 且已 emit 的帧保留）。
3. **Gateway 层**
   - [x] `internal/gateway/service.go`：`Stream` 专用总上限（`StreamTotalTimeout`，0=不限制）与 `StreamMaxRetries` 预算；`outputStarted` 门禁不动；`Complete`/`Embeddings` 不动。
   - [x] `internal/gateway/service_test.go` / `failover_test.go`：长健康流不被 60s 砍；首帧卡顿重试至成功；重试满 1+StreamMaxRetries 次终止；出帧后卡顿不重试；backoff/waitBackoff 与新预算交互。
4. **装配与文档**
   - [x] `cmd/gateway/main.go:173-181` 接线（svc 与 asyncSvc——async 不用 Stream，只需 svc；但字段赋值保持两者一致以免误用）。
   - [x] README 配置表与「Routing and reliability」段落已更新；docker-compose.yml.example 保持最小 env 集（同 GATEWAY_MAX_RETRIES 待遇，不逐项罗列），默认值由二进制携带。
5. **Spec**
   - [x] trellis-update-spec：`.trellis/spec/backend/error-handling.md:19` 措辞补充（流式 pre-output 重试含卡顿类；流式总上限独立旋钮）。

## 验证命令

```
gofmt -l .
go vet ./...
go test ./...
```

重点跑：`go test ./internal/config/ ./internal/provider/ ./internal/gateway/ ./cmd/gateway/`。

## 风险文件与回滚点

- 高风险：`internal/gateway/service.go`（重试纪律核心，现存大量测试）、`internal/provider/openai.go` / `anthropic.go`（流循环）。
- 回滚：三 env 设为 `GATEWAY_STREAM_STALL_TIMEOUT=0 GATEWAY_STREAM_TOTAL_TIMEOUT=60s GATEWAY_STREAM_MAX_RETRIES=2` ≈ 旧行为；代码级回滚 revert 整个任务提交即可，无迁移/无状态。

## task.py start 前检查

- [x] prd.md 已做 convergence pass，验收标准可观测。
- [x] design.md / implement.md 就位（复杂任务三件套齐全）。
- [x] implement.jsonl / check.jsonl 已填真实条目（若走子代理派发；内联工作流可跳过，Phase 2 用 trellis-before-dev 注入上下文）。
- [x] 用户对最终规划摘要的明确批准（新消息中给出）。
