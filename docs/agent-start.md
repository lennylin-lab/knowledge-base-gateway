# Knowledge Base Gateway: Agent 开工说明

## 1. 项目定位

这是一个独立的 Go 大模型网关，位于客户端与模型供应商之间。第一阶段的目标是提供稳定、可审计、可扩展的模型访问层，而不是重写 `knowledge-base-server` 的知识库、RAG、Agent 或 MCP 逻辑。

当前后端项目位于 `../knowledge-base-server`，技术栈为 Python/FastAPI。建议初期由 Python 服务作为网关客户端：

```text
客户端 -> knowledge-base-server -> knowledge-base-gateway -> LLM provider
```

网关以后可以独立对外开放，但必须先稳定内部协议和权限模型，避免两个项目各自实现一套认证、模型配置和计费规则。

## 2. 第一阶段目标（MVP）

必须实现：

1. API Key 认证。
2. 模型/provider 注册与白名单校验。
3. OpenAI-compatible 的聊天接口，至少支持非流式和 SSE 流式响应。
4. 上游请求超时、有限重试、错误映射和请求取消传播。
5. 按 API Key 或主体限流。
6. 请求审计记录：主体、模型、状态、延迟、token 用量（上游提供时）、trace id。
7. 健康检查、就绪检查、结构化日志和基础 Prometheus 指标。

明确不在 MVP：知识库文档权限、向量检索、Agent 编排、工具执行、复杂工作流、管理后台、完整计费结算、WebSocket。

## 3. 边界与原则

- 网关权限只决定“谁可以调用哪个模型、额度和速率是多少”；文档访问权限仍由 knowledge-base-server 负责。
- provider 密钥只能从环境变量或 Secret Manager 读取，数据库中不得保存明文。
- 外部响应不能泄漏 provider 原始密钥、内部 URL、SQL 错误或完整上游请求头。
- 所有请求必须有可关联的 `trace_id`；日志不得默认记录完整 prompt/completion。
- 统一错误格式，客户端不应依赖某个 provider 的私有错误结构。
- 先采用 HTTP + SSE；接口设计保留后续增加 Responses API 的空间。

## 4. 建议技术方案

- Go 1.24+（以仓库实际 go.mod 为准）。
- HTTP 使用标准库 `net/http`，路由可选 chi；不要为 MVP 引入重量级框架。
- PostgreSQL 保存 API key 哈希、主体、策略、模型目录和审计元数据。
- Redis 用于分布式限流和短期额度计数；本地内存限流只适合开发环境。
- OpenTelemetry 负责 trace/metrics/log correlation，Prometheus 暴露 `/metrics`。
- provider 适配器采用接口隔离，不把某个 SDK 类型泄漏到 HTTP 层。

推荐分层：

```text
internal/http        请求解析、认证中间件、错误映射、SSE
internal/auth        API key 校验与主体解析
internal/policy      模型白名单、额度、并发和速率策略
internal/gateway     provider 路由、超时、重试、熔断
internal/provider    OpenAI/Anthropic/本地模型适配器
internal/audit       审计事件与 token 用量
internal/store       PostgreSQL/Redis 接口及实现
internal/config      配置加载与校验
```

## 5. HTTP 契约

### 5.1 聊天接口

`POST /v1/chat/completions`

请求兼容 OpenAI Chat Completions 的最小子集：`model`、`messages`、`temperature`、`max_tokens`、`stream`、`metadata`。MVP 应拒绝未知或明显危险的超大字段，并设置 body、消息数、单消息长度和总超时上限。

请求头：

- `Authorization: Bearer <api-key>`
- `X-Request-ID` 可选；缺失时服务端生成。
- `Idempotency-Key` 可选，仅对非流式请求预留支持。

非流式响应应保持 OpenAI 兼容的 `id`、`object`、`created`、`model`、`choices`、`usage` 字段。流式响应使用 `Content-Type: text/event-stream`，逐个转发标准 `data: ...` 事件，并以 `data: [DONE]` 结束。

### 5.2 运维接口

- `GET /healthz`：进程存活，不检查依赖。
- `GET /readyz`：数据库、Redis（若启用）和必要配置可用。
- `GET /metrics`：Prometheus 指标。

管理接口不应在 MVP 暴露公网；模型目录和策略先通过配置/迁移初始化。

### 5.3 错误格式

```json
{
  "error": {
    "type": "authentication_error",
    "code": "invalid_api_key",
    "message": "invalid API key",
    "request_id": "req_..."
  }
}
```

建议映射：认证失败 401、权限不足 403、限流 429、客户端参数错误 400/422、上游超时 504、上游暂时不可用 503、未知内部错误 500。不要把 provider 的 HTTP 状态码未经判断直接透传。

## 6. 认证、权限和额度

API key 只在创建时明文返回，数据库保存带 salt 的不可逆哈希、前缀、主体 ID、状态、过期时间和最后使用时间。请求认证成功后生成 `Principal`：`subject_id`、`tenant_id`、`key_id`、角色和策略版本。

策略至少包含：

- 允许的模型集合。
- 每分钟请求数和并发数。
- 每日/月 token 上限（计量不可得时记录未知，不要伪造）。
- 单请求最大 input/output token。

权限判断顺序固定为：认证 -> 请求校验 -> 模型存在性 -> 主体模型权限 -> 限流/额度 -> provider 调用。任何拒绝都写入审计事件，但不记录 prompt 内容。

## 7. 模型路由与 provider 抽象

定义类似以下能力边界（名称可按项目风格调整）：

```go
type Provider interface {
    Complete(ctx context.Context, req ChatRequest) (ChatResponse, error)
    Stream(ctx context.Context, req ChatRequest, send func([]byte) error) error
    Name() string
}
```

模型目录将公开模型名映射到 provider、上游模型名、能力（stream/tools/vision 等）、默认超时和是否启用。客户端只传公开模型名，不能自行指定上游 URL 或 provider。

重试只允许发生在请求尚未向客户端产生不可逆输出时，并且只针对明确的网络错误、429 或 5xx；流式响应开始后禁止切换 provider。每次重试都必须受总 deadline 约束。

## 8. 数据模型（初版）

建议表：

- `tenants` / `subjects`
- `api_keys`：哈希、前缀、主体、状态、过期时间、时间戳
- `model_catalog`：公开模型名、provider、上游名、能力、状态、配置版本
- `access_policies`：主体与模型权限、限流和 token 限额
- `llm_requests`：request id、主体、模型、provider、状态、耗时、token、错误分类、创建时间

审计表只存必要元数据。prompt/completion 默认不落库；如未来需要采样，必须增加显式配置、脱敏和保留期限。

## 9. 与 knowledge-base-server 的集成

Python 服务应通过一个独立的 LLM client 调用网关，并从配置读取网关 base URL 和内部 API key。不要在 Python 业务代码中拼接 provider URL 或读取 provider 密钥。

内部调用至少传递：`Authorization`、`X-Request-ID`，必要时传递租户/用户上下文。网关返回的错误应能被 Python 服务转换成自己的标准错误 envelope，同时保留 request id 便于排查。

## 10. 实施顺序

1. 建立 Go 模块、配置加载、HTTP server、`/healthz` 和优雅停机。
2. 定义请求/响应/error 契约与 provider 接口，先写单元测试。
3. 实现 API key 哈希校验和 Principal 中间件。
4. 实现静态模型目录与单 provider 适配器。
5. 实现非流式转发，再实现 SSE 流式转发和取消传播。
6. 加入模型权限、限流、超时、重试和错误映射。
7. 接入 PostgreSQL/Redis、审计、metrics 和 tracing。
8. 用 knowledge-base-server 做端到端联调，再补 provider 故障和权限矩阵测试。

每一步都应保持服务可启动、可测试；不要先搭建管理后台或多 provider 全量适配。

## 11. 验收标准

- 无效、过期、吊销 API key 均返回 401，且不调用上游。
- 无权模型返回 403；不存在模型与无权模型的错误信息不能泄漏策略细节。
- 非流式和流式请求均能正确透传 usage、结束事件、客户端取消和上游错误。
- 超时、429、5xx 的行为有测试覆盖，重试次数和总 deadline 可配置。
- 限流在单实例和多实例（Redis）模式下行为一致或差异有明确文档。
- 日志和审计不包含明文 API key 或默认 prompt/completion。
- `go test ./...`、静态检查、迁移测试和容器化启动检查通过。
- knowledge-base-server 可以只配置一个网关 URL 和内部 key 完成聊天调用。

## 12. Agent 开始工作前的检查清单

- 先阅读本文件和仓库现有 `README`、`go.mod`、配置及测试约定。
- 先确认 Go 版本、数据库/Redis 是否已有运行方式，再决定依赖版本。
- 先写协议和边界测试，再实现 provider 细节。
- 所有外部输入都做长度、枚举、超时和取消校验。
- 修改接口契约时同步更新本文档和集成测试。
- 不要修改 `../knowledge-base-server`，除非明确是在做网关联调。
