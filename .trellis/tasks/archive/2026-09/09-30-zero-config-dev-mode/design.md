# 技术设计：开发模式零配置启动与默认值可信度

对应 `prd.md` R1–R5。改动面：`internal/config`、`cmd/gateway/main.go`、
`docker-compose.yml`、`.env.example`、`README.md`。无数据库迁移、无 API 变更。

## D1 零配置注入（R1）

**位置**：`internal/config/config.go` 的 `FromEnv()`，在 keys/models 解析
完成之后、既有校验（`at least one key/model is required`）之前插入一个
显式分支。

```go
// Zero-config dev convenience: with the default fake provider, no database,
// and neither dev list supplied, inject documented development defaults so
// `go run ./cmd/gateway` boots with zero configuration. Explicit partial
// configuration is deliberately NOT completed: setting either list opts out.
if c.DatabaseURL == "" && c.Provider == "fake" && len(c.Keys) == 0 && len(c.Models) == 0 {
    c.Keys = []KeyEntry{{ID: DevSubject, Subject: DevSubject, PlaintextKey: DevAPIKey}}
    c.Models = []ModelEntry{{PublicName: DevModel, Provider: "fake", UpstreamModel: DevModel, Enabled: true}}
}
```

**导出常量**（同文件，供 main/测试/文档引用）：

```go
const (
    DevSubject = "dev"
    DevAPIKey  = "sk-dev-local" // documented throwaway; fake provider only
    DevModel   = "gpt-4o-mini"  // matches existing README/.env.example examples
)
```

**语义与边界**：

- 触发条件要求**两者皆空**：`GATEWAY_API_KEYS` / `GATEWAY_MODELS` 的解析
  分支已经把「显式设置但为空」变成报错，所以到达注入点时 `len==0` 等价于
  「未设置」。
- 只设置其一时不注入 → 既有校验继续报错（PRD R1 的半配置约束）。
- `openai` / `anthropic` 不在分支内 → 凭据校验保持不变。
- `DatabaseURL != ""` 不在分支内 → 数据库模式完全不触碰。
- fake provider 的 `Capabilities(string)` 忽略模型名（`internal/provider/fake.go`），
  任意 public 名都可用；选 `gpt-4o-mini` 纯粹为了与现有文档示例一致。
- 注入后 `Keys`/`Models` 非空，下游所有路径（路由、鉴权、管理视图）照常工作，
  无需其他改动。

**权衡**：也曾考虑「补齐缺失的一半」（如只设 keys 时补模型），否决——
显式配置静默混入注入密钥是安全味道，且报错信息已经足够指向缺失项。

## D2 启动配置摘要（R2）

**位置**：`internal/config` 新增纯函数方法，便于单测；`cmd/gateway/main.go`
在配置加载成功后、监听启动前调用一次：

```go
func (c Config) Summary() string
// main.go: logger.Info("config effective", "summary", cfg.Summary())
```

**格式**（严格按 issue 示例）：

- 数据库模式：
  `mode=database limits=<local|redis> responses=<on|off> embeddings=<on|off> async=<on|off> budgets=<on|off> lifecycle=<on|off> otlp=<on|off> admin=<addr|off>`
- 开发模式：
  `mode=dev provider=<name> keys=<n> models=<n>`

`admin=` 字段：`AdminToken == ""` 时为 `admin=off`，否则 `admin=<AdminAddr>`。
开关渲染统一 `on/off`。**白名单式拼接**：只输出上述键，任何密钥/DSN 无进入
路径。放在 `config` 包而不是 `main` 是为了让 `config_test.go` 直接对两种
模式断言完整字符串，不需要起进程。

## D3 docker-compose OTLP 透传（R5）

`gateway.environment` 追加（默认值 = 代码默认，现有栈行为不变）：

```yaml
      # OTLP trace export passthrough: .env values reach the container.
      # NOTE: inside the container "localhost:4318" is the gateway itself —
      # point GATEWAY_OTLP_ENDPOINT at the collector's in-network address
      # (or host.docker.internal:4318 for a host-side collector).
      GATEWAY_OTLP_ENABLED: ${GATEWAY_OTLP_ENABLED:-false}
      GATEWAY_OTLP_ENDPOINT: ${GATEWAY_OTLP_ENDPOINT:-localhost:4318}
      GATEWAY_OTLP_INSECURE: ${GATEWAY_OTLP_INSECURE:-false}
```

只透传三个布尔/地址开关（issue 点名的「等」以最小集为准）；sampling ratio
与 export timeout 属于调优项，代码默认已可用，避免 compose 无限膨胀。
`GATEWAY_OTLP_ENABLED=true` 时网关会尝试向 endpoint 导出，但导出失败
永不阻塞 serving（现有行为），所以透传本身无新增故障面。

## D4 .env.example 与 README 分层（R3/R4）

**.env.example 分节**（自上而下）：

1. 零配置说明（fake 默认即裸跑；显式配置示例保留为注释）；
2. 模式切换（DSN / LIMITS_MODE / ADMIN_TOKEN / ADMIN_ADDR，注释形式）；
3. compose 插值块（`*_HOST_PORT`、`POSTGRES_*`、`GATEWAY_IMAGE`、
   `GATEWAY_STREAM_STALL_TIMEOUT`，全部注释形式带默认值）；
4. 运维旋钮（stream/async/OTLP/lifecycle，注释形式）；
5. per-provider 凭据模式说明（现有内容整理，含 anthropic 对称示例）。

**README**：`## Configuration` 节内拆三个子表（Required / Mode switches /
Operational knobs）+「零配置启动」小节（裸跑命令、`sk-dev-local`、curl
示例）；database-mode 表与 compose overrides 表保持原位，仅修内部引用。
所有既有变量行保留，只移动与归组。

## D5 兼容性与回滚

- 代码默认值零变化；注入只在「完全未配置 + fake + 无 DSN」触发，
  现有任何显式部署（含 compose 生产模式）路径不经过该分支。
- 新增的 compose 变量默认值 = 代码默认 → `docker compose config` 渲染
  前后等价。
- 回滚：D1 是单一 `if`，revert 即恢复原校验报错；D2 是独立日志行，
  删除无副作用；D3/D4/D5 是纯配置/文档。

## D6 测试策略

- `internal/config/config_test.go` 新增：
  - 零环境变量（fake 默认）→ 注入成功，值等于三个导出常量；
  - 仅 keys / 仅 models / 两者显式为空 → 原错误保留；
  - `openai` 无 key 无 DSN → 原错误保留；
  - DSN 设置 + 空 lists → 不注入（`len(Keys)==0`）且不报错；
  - `Summary()` 两种模式完整字符串断言（database: token 设置/未设置两例；
    dev: keys/models 计数）。
- `cmd/gateway/main_test.go`：现有测试回归；摘要行断言由 config 单测覆盖，
  不起进程。
- 手动验收（进 implement.md 验证命令）：裸跑 + curl echo、半配置报错、
  `docker compose config`、compose 下 `GATEWAY_OTLP_ENABLED=true` 的摘要行。
