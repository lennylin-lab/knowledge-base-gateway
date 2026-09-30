# 开发模式零配置启动与默认值可信度（issue #13）

## Goal

让「启动门槛类」配置降到接近零、默认值可信可验证（约定优于配置）：
fake provider 零配置裸跑、启动时打印生效配置摘要、README 配置表与
.env.example 三层分层，并修复 compose 对 OTLP 开关的死配置透传。

来源：GitHub issue #13（feature issue: 开发模式零配置启动与默认值可信度）。

## Requirements

### R1 fake provider 零配置启动

- `GATEWAY_PROVIDER=fake`（默认值）、无 `GATEWAY_DATABASE_URL`、且
  `GATEWAY_API_KEYS` 与 `GATEWAY_MODELS` **均未设置**时，注入开发默认值：
  一个 subject（`dev`）+ 一个固定明文开发 key + 一个 fake 模型
  （public 名沿用 `gpt-4o-mini`，与现有文档示例一致）。
- `go run ./cmd/gateway` 在零环境变量下启动成功，`/healthz` 返回 200。
- 注入的 key 是固定、文档化的常量；README 写明该值，开发者可直接 curl。
- 仅 fake 受益：`openai` / `anthropic` 的凭据强制校验保持不变。
- 显式设置了 `GATEWAY_API_KEYS` 或 `GATEWAY_MODELS` **其中之一**时，
  不注入任何默认值，沿用现有「至少一个 key / 一个 model」校验报错
  （显式配置必须完整，避免半配置静默注入带来的意外密钥/模型）。
- 数据库模式（`GATEWAY_DATABASE_URL` 已设置）行为不变，不注入。

### R2 启动配置摘要

- 配置加载成功后输出**一行**结构化日志，呈现运行模式与各开关状态：
  - 数据库模式示例：
    `mode=database limits=redis responses=on embeddings=on async=off budgets=off lifecycle=on otlp=off admin=:8081`
  - 开发模式示例：
    `mode=dev provider=fake keys=1 models=1`
- `admin` 字段：`GATEWAY_ADMIN_TOKEN` 未设置时显示 `admin=off`。
- 摘要不得包含任何密钥、DSN 等敏感值（只允许模式名、计数、地址、开关）。

### R3 README 配置表三层分层

- 将 README 的平铺配置表拆为三层：
  1. **必填**（0–2 个：真实 provider 凭据等）；
  2. **模式切换**（`GATEWAY_DATABASE_URL` / `GATEWAY_LIMITS_MODE` /
     `GATEWAY_ADMIN_TOKEN` / `GATEWAY_ADMIN_ADDR` 等）；
  3. **运维旋钮**（stream 超时、async、OTLP、lifecycle、限流等，
     默认即可、按需覆盖）。
- 保留全部现有变量的文档行，不删信息；标注 dev-only 变量在零配置下的行为。
- 新增「零配置启动」小节：裸跑命令、注入的默认 key/model、curl 示例。

### R4 .env.example 分层与补全

- 与 README 三层对应分节。
- 补上 compose 实际从 `.env` 插值的变量（注释形式，带默认值）：
  `GATEWAY_HOST_PORT`、`GATEWAY_ADMIN_HOST_PORT`、`POSTGRES_HOST_PORT`、
  `REDIS_HOST_PORT`、`POSTGRES_USER/PASSWORD/DB`、`GATEWAY_IMAGE`、
  `GATEWAY_STREAM_STALL_TIMEOUT`。
- 不包含任何真实密钥。

### R5 compose OTLP 死配置修复

- `docker-compose.yml` 的 `gateway.environment` 透传
  `GATEWAY_OTLP_ENABLED` / `GATEWAY_OTLP_ENDPOINT` /
  `GATEWAY_OTLP_INSECURE`（`${VAR:-代码默认值}` 形式），使 `.env` 中的
  OTLP 设置在 compose 部署路径真实生效。
- 注释说明：容器内 `localhost:4318` 指向容器自身；采集器在宿主时应使用
  `host.docker.internal:4318`（Linux 需 extra_hosts 或宿主 IP）。
- 默认值与代码默认一致，现有栈行为不变。

## Constraints（issue 非目标，同样约束本任务）

- 不删除/合并任何运维旋钮：回滚开关的价值大于「看起来少」。
- 不引入 `GATEWAY_PROFILE` 之类的预设层。
- 真实 provider 密钥、生产 DSN 不提供「方便的」默认值。
- 日志与文档不得输出真实密钥。

## Acceptance Criteria

- [ ] 零环境变量下 `go run ./cmd/gateway` 启动成功；`/healthz` 返回 200；
      用 README 文档中的开发 key 调 `/v1/chat/completions`（model 为注入
      的默认模型）得到 fake echo 响应。
- [ ] 仅设置 `GATEWAY_API_KEYS`（或仅 `GATEWAY_MODELS`）时启动失败并报
      现有的「at least one key/model is required」错误（未注入默认值）。
- [ ] `GATEWAY_PROVIDER=openai` 且无 `OPENAI_API_KEY`、无 DSN 时仍启动失败。
- [ ] 两种模式各验证一次：启动日志各出现一行配置摘要，格式符合 R2，
      且不含密钥明文。
- [ ] README 配置表呈现三层结构、原有变量行全部保留、含零配置小节；
      `.env.example` 分节并包含 compose 插值变量块。
- [ ] compose 下设置 `GATEWAY_OTLP_ENABLED=true` 后容器内生效（启动摘要
      显示 `otlp=on`）；不设置时行为与现状一致。
- [ ] `go build ./...`、`go vet ./...`、`go test ./...` 全部通过；
      `docker compose config` 渲染无错误。
