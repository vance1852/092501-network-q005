# 城市地下管网安全监测与应急调度服务

本项目为城市供水、排水和燃气管网提供离线后台服务，保存管段、传感器读数、泄漏告警、巡检工单、维修审批和应急资源分配。系统使用确定性的风险评分帮助值班人员优先处理高风险管段，账号按角色授予读取、处置和审批权限，状态变化写入 SQLite 审计表。

## 目录

- `src/urban_network/`：管网领域服务、风险计算、权限、SQLite 存储和 JSON API；
- `src/power_dispatch/`：应急泵站资源分配使用的计划与容量计算组件；
- `src/plant_science/`：传感器校准与统计分析组件；
- `tests/`：领域规则、存储事务和 API 测试。

## 环境

- Linux
- Python 3.11 或更高版本
- 运行时仅使用 Python 标准库和 SQLite

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -q
```

## 构建检查

```bash
python3 -m compileall -q src tests
```

## 离线验收

```bash
PYTHONPATH=src python3 -m urban_network.acceptance --workspace .
```

验收命令会创建演示管段、导入传感器读数、计算泄漏风险、生成巡检工单并输出 JSON。它不访问外部网络，也不要求常驻的数据库、队列或其他服务。

## HTTP API

```bash
PYTHONPATH=src python3 -m urban_network.api --database network.sqlite3 --host 127.0.0.1 --port 8080
```

`GET /health` 返回服务状态，其余接口使用 JSON 和 `Authorization: Bearer <token>` 会话，支持管段登记、读数上报、风险查询、工单创建和应急资源分配。

### 工单流转的版本条件

`POST /work-orders/{id}/transitions` 接收 `{target, reason, expected_version}`，用乐观并发控制裁决工单状态变化：

- 每张工单携带单调递增的 `version`（创建时为 1），流转请求必须带上读取到的 `expected_version`；同一前置版本最多被成功消费一次，先到者生效，其余请求得到 `409`。
- 已完成或已取消的工单是终态，任何携带旧版本的迟到请求都无法重新打开它们。
- 内容完全相同的重试（同一提交人、同一版本、同一目标和理由）按请求摘要识别，幂等地返回首次裁决结果，不重复改状态、不重复写审计。
- 真正冲突的请求不会丢失：服务把双方提交（目标、理由、提交人、期望版本）和胜出版本一并记录在 `work_order_decisions` 表，调度员可通过 `GET /work-orders/{id}/decisions` 核对。
- 状态变更、裁决记录和审计事件在同一个 `BEGIN IMMEDIATE` 事务内提交，审计按 `event_id` 单调排列，重启后仍可完整核对。

响应状态码：`200` 流转生效或幂等重放成功，`409` 版本冲突或终态拒绝，`404` 工单不存在，`422` 缺少或非法的 `expected_version`。
