# 执行计划：开发模式零配置启动与默认值可信度

前置：`prd.md`（需求与验收）、`design.md`（技术设计）。按序执行，每步独立可回滚。

## 步骤

### 1. internal/config：常量 + 零配置注入 + Summary()

- [ ] `config.go`：新增导出常量 `DevSubject` / `DevAPIKey` / `DevModel`。
- [ ] `FromEnv()`：在 models 解析后、`DatabaseURL==""` 校验前插入零配置
      分支（条件与位置见 design.md D1）。
- [ ] `config.go`：新增 `func (c Config) Summary() string`（格式见 D2）。
- **验证**：`go build ./... && go vet ./...`
- 回滚点：删除 if 分支与常量即恢复现状。

### 2. cmd/gateway：启动摘要日志

- [ ] `main.go`：配置加载成功后输出
      `logger.Info("config effective", "summary", cfg.Summary())`
      （位置：`run()` 内配置就绪之后、监听启动之前）。
- **验证**：`go build ./...`；`go run ./cmd/gateway` 零环境变量裸跑，
  确认摘要行出现且无密钥值。

### 3. internal/config 单测

- [ ] 零配置注入成功例 + 三个导出常量断言。
- [ ] 仅 keys / 仅 models / 两者显式为空 → 原错误。
- [ ] `openai` 无 key 无 DSN → 原错误；DSN 设置时空 lists 不注入。
- [ ] `Summary()`：database（token 有/无）与 dev 两模式字符串断言。
- **验证**：`go test ./internal/config/...`
- 评审门：注入语义（半配置不注入）需对照 PRD R1 确认。

### 4. docker-compose.yml：OTLP 透传

- [ ] `gateway.environment` 追加三个 OTLP 变量（design.md D3 的 YAML 与
      注释，含 localhost-in-container 提示）。
- **验证**：`docker compose config` 渲染无错误、默认值与现状等价。
- 回滚点：删除三行即恢复。

### 5. .env.example 分层

- [ ] 按 design.md D4 分五节重排；补 compose 插值变量块（注释形式带默认值）。
- [ ] 复查无真实密钥（与本地 `.env` diff 确认）。
- **验证**：人工浏览；`git diff .env.example` 确认仅新增/移动。

### 6. README 三层分层 + 零配置小节

- [ ] `## Configuration` 拆三子表：Required / Mode switches / Operational
      knobs；全部既有行保留。
- [ ] 新增「零配置启动」小节：裸跑命令、`sk-dev-local`、curl 示例、
      「显式设置其一即退出零配置」说明。
- [ ] 核对 compose overrides 表与正文引用一致。
- 评审门：分层归组是否符合直觉，交用户过目。

### 7. 全量验收（对应 PRD Acceptance Criteria）

```bash
go build ./... && go vet ./... && go test ./...

# 零配置裸跑
env -i PATH="$PATH" HOME="$HOME" go run ./cmd/gateway &
curl -s localhost:8080/healthz
curl -s localhost:8080/v1/chat/completions -H 'Authorization: Bearer sk-dev-local' \
  -H 'Content-Type: application/json' \
  -d '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}]}'
kill %1

# 半配置报错（期望 startup failed: ... at least one ...）
env -i PATH="$PATH" HOME="$HOME" GATEWAY_API_KEYS="k1:dev:sk-x" go run ./cmd/gateway || true

# compose 渲染 + OTLP 透传生效检查
docker compose config >/dev/null
GATEWAY_OTLP_ENABLED=true docker compose config | grep -A2 GATEWAY_OTLP_ENABLED
```

- [ ] PRD 验收清单逐项勾选。
- [ ] 最后一轮按 trellis-check 全量检查（含 spec 合规）。

## 顺序依据

先代码后文档：README 要引用注入的常量值与摘要格式，等代码定稿再写文档避免返工；
compose 透传（步骤 4）不依赖代码步骤，但摘要行是 otlp=on 的验证手段，故排在步骤 2 之后。
