# 深空辐照标定 · 批次封存系统

多座地面站复测同一批深空辐照标定时，审查员通过本系统录入批次配置、收集各站稳定投票，
在赞成站数达到阈值时获得**唯一且不可变**的封存证书。纯 Python 标准库实现（无第三方运行时依赖），
Docker Compose 一键交付页面、健康响应与自动验收。

## 领域规则（系统保证）

| 规则 | 实现 |
| --- | --- |
| 首次有效投票**原子冻结**站点名单、阈值、标定摘要 | 名单/阈值在批次创建时固定；第一票在同一把锁 + 同一次落盘内写入摘要冻结时间 |
| 同站 + 同摘要 + 同稳定投票标识重传：**只回放首次结果，不增票** | 三元组去重，封存后重传仍返回首次投票记录（含原内部 vote_id），HTTP 200 `replayed=true` |
| 投票标识内容改变 / 不同摘要：**记录冲突、隔离、不参与封存** | `VOTE_ID_CHANGED` / `DIGEST_MISMATCH`，冲突票单列展示且重传去重回放 |
| 站点不在冻结名单：**配置不符拒绝** | `STATION_NOT_LISTED`，响应附带冻结名单 |
| 多站并发达到阈值：**只生成一份不可变证书** | 判定→计数→封签→fsync 落盘全部在全局锁同一临界区；证书含 SHA-256 指纹，`immutable=true` |
| 已封存批次不可回收集中；迟到票不改写证书 | 未见新载荷 → `409 LATE_VOTE_REJECTED`；已接受载荷重传 → 回放 |
| 写入/封签途中崩溃可恢复 | 临时文件 + `fsync` + 原子 `rename`；启动加载持久记录并清理半截 `.tmp` 文件 |
| 刷新/轮询/过期响应不得用较旧状态覆盖封存 | 前端按批次比较 `revision` 丢弃过期响应；封存状态单向不可逆，不降级渲染 |
| 无效参数可操作反馈 | 全部错误带机器可读 `code`、中文说明与修正提示 |

## 一键启动与验收

```bash
# 默认宿主端口 8080；可用 WEB_PORT 覆盖
WEB_PORT=9090 docker compose up --build --abort-on-container-exit --exit-code-from verify
```

- `web`：交付页面 `http://localhost:${WEB_PORT:-8080}/` 与健康检查 `/health`，数据写入命名卷 `seal-data`。
- `verify`：通过 `depends_on: service_healthy` **等待页面与健康响应可用后**执行
  `verify/verify.py`，覆盖：
  1. 代码测试（unittest）：幂等重放回放、冲突票隔离、配置/参数拒绝；
  2. 页面构建可用（`/` 返回含控制台与脚本的真实页面）；
  3. API/HTTP 冒烟：健康响应、20 路并发投票封签唯一性、封存后 30 路并发冲击；
  4. 重启恢复：复制线上持久记录冷启动全新进程（含两次重启 + 半截临时文件），
     校验收集状态、赞成票、冲突票、证书逐字节一致与迟到票拒绝。
  
  **完成即退出，退出码 0/非 0 报告验收结果。**

页面在验收期间与验收后持续可访问；单独复跑：`docker compose run --rm verify`。

> 无 Docker 的本地环境可直接运行：
> `DATA_DIR=/tmp/seal python3 web/app/server.py`，
> 然后 `BASE_URL=http://127.0.0.1:8080 APP_PATH=$PWD/web/app/server.py \
> SOURCE_DATA_DIR=/tmp/seal python3 verify/verify.py`。

## HTTP 接口

### `POST /api/batches` —— 录入批次配置
```json
{ "id": "IRR-2026-M42-07", "stations": ["站A", "站B", "站C"], "threshold": 2 }
```
`201` 返回批次快照；批次标识限 `[A-Za-z0-9_.@://-]{1,64}`；阈值为 1..站点数的整数；
站点名单非空、≤100、不可重复。重复创建返回 `409 BATCH_EXISTS`（封存后为 `BATCH_SEALED`）。

### `POST /api/batches/{id}/votes` —— 站点提交稳定投票
```json
{ "station": "站A", "digest": "sha256:9f2c…", "vote_id": "sv-20261004-A-0001" }
```
- `200`：`{accepted, replayed, froze_config, vote_id, batch}`——
  重传时 `replayed=true` 且 `vote_id` 为首次记录；首票 `froze_config=true`；
  达到阈值时 `batch.certificate` 一次性出现。
- `422 VOTE_ID_CHANGED` / `422 DIGEST_MISMATCH`：冲突隔离（详情含 `replayed`、当前批次快照）。
- `422 STATION_NOT_LISTED`、`422 INVALID_*`：配置不符 / 参数无效，附可操作字段。
- `409 LATE_VOTE_REJECTED`：批次已封存且为未见过的新载荷。

### 查询
- `GET /api/batches` / `GET /api/batches/{id}`：快照含 `status`、冻结配置、
  `yes_count`/`yes_stations`、`conflicts[]`、`certificate`、单调 `revision`。
- `GET /health`：`{"status":"ok","revision":N}`，同时用于容器健康检查。
- `GET /`：封存控制台页面（每 2 秒轮询，revision 防旧、封存不降级）。

## 持久化与证书

- 数据文件：`$DATA_DIR/seal.db.json`（compose 中为命名卷）。
  每次状态变更：写 `seal.db.json.tmp.<pid>.<tid>` → `fsync` → `os.replace` → 目录 `fsync`。
- 证书字段：`batch_id`、冻结 `digest`、`threshold`、赞成站有序名单、
  `fingerprint`（对冻结配置 + 赞成票规范序列化后的 SHA-256）、`sealed_at`、`immutable: true`。

## 目录结构

```
docker-compose.yml      # web + verify 编排，WEB_PORT 可配置宿主端口
web/
  Dockerfile            # python:3.11-slim，零 pip 安装
  app/server.py         # 服务：API + 原子冻结/封签 + 崩溃安全持久化
  app/index.html        # 控制台页面（轮询防旧、封存不降级）
verify/
  Dockerfile            # 验收镜像（同时携带 app 代码用于冷启动恢复测试）
  verify.py             # 代码测试 + 页面/健康冒烟 + 并发唯一性 + 重启恢复，退出码报告
```
