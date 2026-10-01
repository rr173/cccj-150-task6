# 地籍界址测量与宗地版本管理平台

离线外业采集 → 回网提交候选边界 → 几何校验 → 并发共享边争议 → 仲裁裁定 →
分割/合并/历史更正 → 不可变版本与哈希链 → 历史证明持续可验 → 任意时间点查询。

## 快速开始

```bash
pip install -r requirements.txt
python3 seed.py                                   # 初始化并种入甲/乙两宗相邻地块
python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8000
# 浏览器打开 http://localhost:8000
```

运行测试：

```bash
python3 -m pytest tests/ -q          # 130 项：业务规则 + HTTP 集成（含二阶段更正、生效计划、快照交换包、接收窗口与移交、重开拒绝终局去重全场景）
python3 demo_live.py                 # 完整 HTTP 端到端演示（需先启动服务并 seed）
```

## 需求 → 实现 对照

| 需求 | 实现位置 |
| --- | --- |
| 离线采集界址点/精度/界标照片/相邻权利人确认 | `services/sync.py`（bundle/job/media）、`schemas.py`（SurveyPointIn/OwnerConfirmationIn） |
| 回网提交候选边界 | `POST /api/sync/submit`（幂等键 idem_key，断网重放安全） |
| 多边形闭合校验 | `validation.validate_ring` → `NOT_CLOSED`（回点与首点偏差 > 5cm） |
| 面积容差 | `AREA_TOL_RATIO=2%` → `AREA_MISMATCH`（声明面积 vs 鞋带公式实算） |
| 共享边一致 | `geo.shared_segments` + 逐重合段偏差 → `SHARED_INCONSISTENT` / `SHARED_NEIGHBOR_UNCONFIRMED` |
| 不能自交 | 线段对精确求交 → `SELF_INTERSECT`（另含零长边、面积非正、非法重叠） |
| 两组测量同改共享边不得后提交覆盖 | 提交时对比**库内当前版本**与作业基线，按「宗地对+折点锚点」检测分歧，双方方案几何不一致即生成 `dispute`（A/B 差异几何并存），整批挂起不落版本 |
| 相邻权利人确认 | 单方改边必须携带确认；确认后共享链以"只替换该片段"方式传播到邻宗候选并成对落库 |
| 裁定只能选/修订争议片段 | `POST /api/disputes/adjudicate`：decision=A/B/REVISED；修订链端点必须吸附锚点；服务端用当前环替换争议段重建 |
| 裁定不能重写未争议边界 | 服务端强制补链逐点不动（Hausdorff ≤ 5cm），越界即 400；并校验两宗共享边一致、面积变化等量反向 |
| 分割保持面积关系 | `POST /api/ops/split`：子宗并集与父宗**全等**且面积和守恒（0.5%） |
| 合并保持面积关系 | `POST /api/ops/merge`：结果与父宗并集全等、面积守恒 |
| 历史更正（即时） | `POST /api/ops/correction`：仅可更正当前版本；旧版标 `ANNULLED` 但**行不删**；`parcel_op` 记录 SPLIT/MERGE/CORRECTION/ADJUDICATION 的父子版本 |
| **历史更正（二阶段可预演）** | `POST /api/correction-drafts`（建草案+预演）→ `POST /api/correction-drafts/{id}/publish`（原子发布分叉链）；`rehearse` 重新预演、`cancel` 撤销；草案不改在版宗地，基线漂移（后代/争议/证明变化）禁止套用旧预演；旧链与旧证明保留 |
| **生效计划（未来执行/互斥占用/崩溃补跑）** | `POST /api/effect-plans` 把 releasable 草案登记到未来时刻，对全部目标地块建互斥占用；相同 `request_key` 活动期唯一，占用交集返回冲突清单；后台执行器靠**持久租约 + 完成令牌**重启补跑且不可二次落账；执行前快照/争议/证书变化 → `PAUSED`（数据库原样），可 `resume`（刷新基线）/ `reschedule` / `cancel`（释放占用与请求键）|
| 已签发历史证明继续可验证 | `certificate` HMAC-SHA256 绑定版本哈希；`GET /api/certificates/{id}/verify` 重算签名+几何+哈希链；宗地后续变化不影响旧证明 |
| 地图编辑界面 | `static/index.html` + Canvas（平移缩放、加点/拖点、精度/界标/照片、A/B 差异叠加、锚点间修订、时间轴、血缘图），零外部依赖 |
| 离线同步接口 | `/api/sync/bundle`（底图+基线版本+容差+邻宗）、`/jobs/{id}/media`、`/validate`、`/submit`、`/jobs/{id}` |
| 按时间点查询 | `GET /api/parcels/{id}?at=ISO` 与 `GET /api/at/{ts}`，半开区间 `[valid_from, valid_to)` |
| **地籍快照交换包** | `POST /api/snapshots/export`（时间点+最小闭包导出）、`/precheck`（无网络离线预检）、`/imports`（分块乱序/重复/断点续传，全匹配才一次性导入；见下节） |
| **接收窗口与移交** | 建会话指定负责人/有效期；分块与终检仅当前负责人；到期/人工冻结保留分块缺块冲突但拒绝写入；移交/续期/冻结/重开携带 `request_key` 幂等键 + `version` 会话版本号防并发覆盖；重开重验清单与现存分块（失配保持冻结、可定位）；已导入不可重开/清理；保留期清理；操作审计（见下节） |

## 版本模型

* `parcel_version` 不可变：几何、面积、生效时间、上一版本哈希、跨操作父哈希共同进入 SHA-256；
  新版本另起一行，旧版本置 `SUPERSEDED/ANNULLED`（状态流转不计入哈希）。
* 争议期不落新版本，后提交方不会覆盖先落库方；`candidate` 与 `dispute` 保留双方原始几何。
* 分割子版本与合并结果版本的哈希包含全部父版本哈希，构成跨宗地 DAG 血缘。

## 二阶段历史更正（草案 / 预演 / 原子发布）

历史更正扩展为"先建可预演草案、再原子发布"，避免预演即生效、也避免在过期基线上套用旧结论。

**阶段一 DRAFT（只预演，不改世界）**

`POST /api/correction-drafts` 校验更正环后生成一条 `correction_draft`（`status=OPEN`），
**不产生任何 `parcel_version`、不改变在版宗地**。服务端沿 `parcel_op` 血缘从目标版本
向**下游**闭包遍历所有受影响后代（SPLIT 子宗、MERGE 结果、CORRECTION/ADJUDICATION 链），
用弧长参数传导（`geo.transfer_fragments`）逐节点推演，预览里给出：

* `new_ring`：每个后代传导后的新几何（非整环替换；内部分割线顶点保留，落在被更正
  片段上的切口随片段移动；切口落在片段中间也正确——支持任意方向/跨环接缝的更正）；
* `old_area / new_area / area_delta`：旧面积、新面积与面积差；
* `lineage`：血缘变化（由哪个 SPLIT/MERGE/… 操作产生、父版本是谁、发布后的新 id 映射）；
* 跨节点守恒校验（分割子宗面积和=更正后父宗、合并结果=父宗并集）、自交/重叠校验；
  任一不过则 `releasable=false`（草案保留、错误可见、不可发布）。
* 同时保存一份世界状态**指纹**（受影响宗地全部版本哈希/状态/效力区间、相关争议、相关证明）。

**阶段二 PUBLISH（原子 + 分叉 + 幂等）**

`POST /api/correction-drafts/{id}/publish` 在单事务内：

1. **实时漂移检测**：重算指纹与建案时比对。受影响后代出现新增/修改/消失、当前版本
   变化、相关争议被裁定/新增、相关证明新签发/吊销 → 返回 `status=DRIFTED`，
   **不套用旧预演、不写任何版本**，在 `drift_nodes` 标出漂移节点，草案置 `DRIFTED` 保留；
2. 同时**实时重算预演**，世界变化导致传导结果不再 releasable 同样拒绝；
3. 无漂移才原子生成整条更正链：每个受影响版本一个新版本行，`chain_parent_id` 指向
   同宗地新链锚点，旧行/旧哈希原样保留（**旧链与新链平行分叉、各自哈希可验**）；
   血缘写 `CORRECTION_REPLAY`（逐原操作）+ `CORRECTION_PUBLISH`（总结）。
   只有血缘**叶子**新版本在版（已分割父宗不与其子宗同时在版，面积不重复）；
4. 已签发证明绑定旧版本哈希，新链不影响其验真；历史时间点查询仍返回旧几何。
5. 相同发布请求（同一草案/同一 `idem_key`）重放返回首次结果。

`rehearse` 在漂移后按当前世界重新预演（草案回 `OPEN`）；`cancel` 撤销 OPEN/DRIFTED
草案——草案从未产生版本，所以撤销没有任何半成品残留。草案状态机：
`OPEN → PUBLISHED` / `OPEN|DRIFTED → CANCELLED` / `OPEN ⇄ DRIFTED`。

前端「分割/合并/更正」页建草案，「更正草案」页查看逐节点影响/漂移、重新预演、
批准、撤销；地图上以**琥珀虚线**叠加草案目标环与后代预演几何，明确区别于在版版本（蓝/绿实线）。

## 生效计划（登记未来执行 / 互斥占用 / 崩溃补跑）

完成影响计算（`releasable`）的更正草案可登记一条**生效计划**，在未来时刻自动原子落账。

**登记（`POST /api/effect-plans`）**

* 请求含 `draft_id / execute_at（必须未来）/ request_key`；
* 对预演闭包里的**全部目标地块**建立互斥占用（`plan_occupancy`）；
* **相同 `request_key`** 在 `WAITING/PAUSED` 期间全局唯一（DB 部分唯一索引兜底）：
  完全相同的重复请求幂等返回首次计划，同键不同内容 400；
* 与任何活动计划存在**占用交集**时不建立任何占用，返回 `status=CONFLICT` 与
  冲突清单（对方计划 id/状态/执行时间/交集地块）。

**执行器（后台轮询，`PLAN_POLL_SECONDS` 可调）**

* 每轮先回收失效租约（`PLAN_LEASE_TTL_SECONDS`），再为到期（`execute_at<=now`）
  的 WAITING 计划**在同一写事务内先持久化租约**（`lease_owner/lease_at`）后执行；
* 执行前重算基线指纹（**地块版本快照 / 纠纷状态 / 证书集合**）并实时重算预演：
  任一不同或不再 releasable → `PAUSED`，**宗地版本/血缘/证明数据库保持原样**，
  占用继续保留、草案同步标 `DRIFTED`；
* 无漂移才原子发布落账 → `COMPLETED`。**完成令牌**（登记时生成并持久化的
  `completion_token`）同时作为发布幂等键，重复补跑只返回首次结果，**不可二次落账**；
* 服务重启（startup bootstrap）立即清空上个进程遗留在 WAITING 计划上的租约，
  到期计划由新进程补跑；硬崩溃在落账前后都安全（租约持久、令牌幂等）。

**暂停后处置 / 状态机**

`WAITING → COMPLETED / PAUSED`；`PAUSED → WAITING（resume）/ CANCELLED`；
`WAITING|PAUSED → CANCELLED`。

* `POST /{id}/reschedule`：改期（PAUSED 改期后仍暂停，需先恢复）；
* `POST /{id}/resume`：按当前世界**重新预演、刷新指纹基线**（闭包扩大则同步占用，
  仍冲突/不可发布则保持 PAUSED 并给原因），干净则回 WAITING；
* `POST /{id}/cancel`：取消并删除全部占用、释放 `request_key`，**之后可用同键重新登记**。

前端「生效计划」页：左侧栏按 **等待 / 暂停 / 完成 / 取消** 过滤列计划与占用对象，
右侧为横向时间轴（含"现在"刻度）与详情/操作面板（改期、恢复基线、取消释放）；
地图上以**紫色虚线**描出被占用地块，选中计划高亮。

## 地籍快照交换包（离线导出 / 分块导入 / 一次性核验）

跨节点（如外业站 → 县中心）按**时间点**交付一宗地籍快照的离线交换模块。

**导出（`POST /api/snapshots/export`）**

* 入参 `parcel_ids + at + chunk_size`：种子取各宗在 `at` 时点的有效版本；
* 服务端计算**最小闭包**（不动点扩张）：
  * **版本链**——同宗 `chain_parent_id`/`base_version_id` 前驱 + 跨宗地
    `parcel_op`（SPLIT/MERGE/CORRECTION/ADJUDICATION）的全部父版本，含界址点/挂接；
  * **共享边**——闭包各宗在该时点版本两两之间的重合界址边；
  * **争议**——涉及闭包宗地的 dispute（连同双方作业单/候选/裁定结果版本）；
  * **证明**——闭包宗地在该时点前签发的 certificate（HMAC 签章随包）；
  * **生效计划**——占用与闭包宗地相交的 effect_plan（连同 correction_draft、
    占用对象宗地存根）；
* 载荷规范化后整体 SHA-256 并切块；**清单**逐项登记内容哈希、计数、分块哈希，
  再用 HMAC-SHA256 签发。接收方仅凭包内信息即可独立验真（无需联网/无需库）。

**接收（预检 → 分块 → 终检一次性导入）**

* `POST /api/snapshots/precheck`：可在**完全无网络**环境核验——清单签章 →
  分块哈希/缺块 → 载荷整体哈希 → 条目内容哈希与计数 → **引用关系最小闭包**
  （证明/候选/占用等引用缺失）→ **重算版本哈希链**（链断裂）→ 证明签章/绑定 →
  共享边重算；带 `local=true` 时再**只读**比对本地库（同 id 不同版本、
  在版分叉、计划 request_key/占用互斥），不做任何写入；
* `POST /api/snapshots/imports` 凭清单建会话：**相同包标识 + 相同清单**恢复原会话，
  同标识不同内容直接 `409 PACKAGE_ID_CONFLICT`；
* `PUT /imports/{id}/chunks/{seq}`：分块可**乱序、重复**上传（multipart 文件或
  base64 表单字段）；同序号相同内容幂等；**同序号不同内容**记为
  `CHUNK_CONTENT_MISMATCH` 且不覆盖已收正确块，重传正确内容即清除；
  上传进程中断后凭 package_id **续传**（分块实体持久化）；
* `POST /imports/{id}/finalize`：只有全块齐备且**清单/链/引用/本地**全部匹配时，
  才在**单事务**内一次性导入；缺块返回 `MISSING_CHUNKS`，任何内容/本地冲突返回
  可定位的冲突明细（`entity/id/seq/detail`）并**保持数据库原样**；
* **相同包标识再次导入返回首次结果**（结果持久化在会话上，不可二次落账）；
* 前端「快照交换」页三栏：导出（含 `.pkg.json` 下载）/ 接收（预检、建会话、
  单块或乱序自动上传、终检）/ 会话结果——分块点阵（绿已收/灰缺失/红异内容）、
  可定位冲突、**来源时间点**、**新旧版本对应关系**（NEW/IDENTICAL、导入前后在版 id）。

```
POST /api/snapshots/export                  按时间点导出最小闭包（清单 + base64 分块）
POST /api/snapshots/precheck                无网络离线预检（可带分块做内容级核验）
POST /api/snapshots/imports                 凭清单建/恢复上传会话（同 id 异内容 409；可带 owner/valid_until/ttl_minutes）
GET  /api/snapshots/imports                 会话清单（负责人/剩余时间/冻结原因，惰性到期）
GET  /api/snapshots/imports/{id}            会话详情（缺块/冲突/窗口/冻结/结果）
PUT  /api/snapshots/imports/{id}/chunks/{seq}  分块上传（乱序/重复/续传；文件或 base64；仅当前负责人）
POST /api/snapshots/imports/{id}/finalize   终检并一次性导入（全匹配才落库；重放幂等；仅当前负责人）
POST /api/snapshots/imports/{id}/handover   负责人移交（request_key 幂等 + version 乐观锁）
POST /api/snapshots/imports/{id}/renew      续期（窗口开启期间；request_key + version）
POST /api/snapshots/imports/{id}/freeze     人工冻结（保留分块/缺块/冲突，拒绝写入）
POST /api/snapshots/imports/{id}/reopen     重新开启（重验清单与现存分块；失配保持冻结并给可定位原因）
GET  /api/snapshots/imports/{id}/events     会话操作审计（创建/移交/续期/冻结/重开/分块/终检/拒绝）
POST /api/snapshots/imports-cleanup         保留期清理（仅冻结超期且从未导入；已导入永不清理）
```

### 接收窗口与移交（负责人 / 有效期 / 冻结 / 重开 / 保留期）

* **创建窗口**：`POST /imports` 可带 `owner`（负责人）与 `valid_until`（绝对截止）
  或 `ttl_minutes`（时长；均缺省为不限期）。此后**上传分块与终检只允许当前负责人**
  （`actor` 表单/JSON 字段或 `X-Cadastre-Actor` 头；未指定负责人的会话不限制，向后兼容）。
* **到期冻结**：窗口截止后写入被**惰性冻结**（`FROZEN / EXPIRED`）——
  已经验真的分块、缺块点阵、同序号异内容与终检冲突**全部保留**，但拒绝任何继续写入
  （分块 PUT / 终检 → 409 `SESSION_FROZEN`，响应附完整可定位会话视图与审计事件 `WRITE_DENIED`）。
* **人工冻结**：负责人可随时 `freeze`（`FROZEN / MANUAL`），语义同到期冻结。
* **移交 / 续期 / 冻结 / 重开必须携带 `request_key`（幂等请求键）与 `version`（会话版本号）**：
  版本号不匹配 → 409 `VERSION_CONFLICT`（并发覆盖被乐观锁挡下，客户端须刷新后重试；
  版本冲突失败**不消费**请求键）；成功动作的同键同内容重放 → 返回首次结果
  （`replayed=true`，不二次变更、不重复记审计）；同键不同内容 → 400。每次成功
  动作 `version_no += 1`。
* **重开校验拒绝是终局判定（请求去重）**：一次 `reopen` 若因**磁盘分块损坏**等
  失配被拒（409 `REOPEN_MISMATCH`，保持冻结），该操作标识（`request_key`）即被
  **永久绑定到这次拒绝响应**——之后同键重放（连续点击、断网重试、甚至携带新的
  版本号）一律 409 原样回放首次拒绝（响应带 `replayed=true` 与首次失配明细），
  即使后台文件后来已修正、会话已被其他操作标识推进（解冻甚至已完成导入），原标识
  也**不再重新校验、不产生任何新状态变更**（不重复冻结、不重复记审计；`FREEZE` 与
  `REOPEN_REJECTED` 审计各只记一次）。操作者必须提交**新的 `request_key` 与最新
  `version`** 才能再次校验并恢复接收；前端在重开失败时给出终局提示并提供
  「新操作标识重验（文件已修复）」按钮（自动取最新版本号），操作进行中连续点击
  与网络重试复用同一待决标识，不会绕过首次判定。
* **移交**在冻结状态下也允许（不解除冻结）：负责人交接后由新负责人发起重开；
  **续期**只在窗口开启、未冻结期间允许（过期后须先重开）。
* **重新开启（reopen）前重新核验清单与现存分块**：清单签章 → 现存分块哈希与缺块；
  全块齐备时按**完整终检口径**（载荷/闭包/链/证明/共享边/本地只读）复验。
  失配 → **保持冻结**（`REOPEN_MISMATCH`），返回 `entity/id/seq` 级可定位原因，
  会话版本号不递增，但请求键被**终局消费**（同键永远回放这次拒绝，见上条）；
  通过 → 解冻、现存分块决定 UPLOADING/READY、版本号递增，并须给出新窗口
  （原窗口已过期时必填）。
* **已导入保护**：`IMPORTED` 会话拒绝移交/续期/冻结/重开（409 `SESSION_IMPORTED`），
  终检仅返回首次结果；保留期清理**永不删除**已导入会话及其导入结果。
* **保留期清理**：`POST /imports-cleanup` 只删除冻结超过 `retention_days`
  （默认 `CADASTRE_SNAPSHOT_RETENTION_DAYS=7`）且从未导入的会话，连同其分块、
  审计与幂等请求记录；响应返回清单与受保护的已导入会话计数。
* 前端「快照交换」页会话清单/详情展示**负责人、剩余时间（倒计时/过期）、冻结原因**、
  会话版本与**操作审计时间线**，并支持**续期、移交、冻结、重开**与保留期清理。

## 坐标与容差（`app/config.py`，单位：米/m²）

| 参数 | 值 | 含义 |
| --- | --- | --- |
| CLOSURE_TOL_M | 0.05 | 环闭合容差 |
| POINT_SNAP_TOL_M | 0.15 | 界址点归并/锚点吸附 |
| SHARED_CONSIST_TOL_M | 0.05 | 共享边一致性 |
| SHARED_SEARCH_TOL_M | 0.50 | 共边/基线片段寻找 |
| UNCHANGED_EDGE_TOL_M | 0.05 | 裁定时未争议边界保持容差 |
| AREA_TOL_RATIO | 2% | 外业面积容差 |
| SPLIT_MERGE_AREA_RATIO | 0.5% | 分割/合并面积守恒 |

> 内部使用平面投影坐标（米）。真实部署时设备端把地方 CRS（如 CGCS2000 高斯带）投影后提交。

## 主要接口

```
POST /api/sync/bundle            下载离线包（基线版本/邻宗/容差）
POST /api/sync/jobs/{id}/media   界标照片上传（multipart）
POST /api/sync/validate          只校验不入库（弱网预检）
POST /api/sync/submit            幂等提交候选边界 → COMMITTED/DISPUTED/REJECTED
GET  /api/disputes               未决争议（含 base/A/B 差异几何）
POST /api/disputes/adjudicate    裁定：A / B / REVISED（端点固定）
POST /api/ops/split | merge | correction
POST /api/correction-drafts            建更正草案并预演（不产生版本）
GET  /api/correction-drafts[?status=]  草案列表
GET  /api/correction-drafts/{id}       草案详情（预演影响 + 实时漂移）
POST /api/correction-drafts/{id}/rehearse  漂移后重新预演
POST /api/correction-drafts/{id}/publish  原子发布（漂移→DRIFTED，不写版本；重放幂等）
POST /api/correction-drafts/{id}/cancel   撤销草案（无半成品残留）
POST /api/effect-plans                    登记生效计划（未来执行+互斥占用；冲突清单/幂等）
GET  /api/effect-plans[?status=]          计划列表（等待/暂停/完成/取消）
GET  /api/effect-plans/{id}               计划详情（占用对象/租约/令牌/暂停原因/实时漂移）
POST /api/effect-plans/{id}/reschedule    改期
POST /api/effect-plans/{id}/resume        暂停后刷新基线并恢复
POST /api/effect-plans/{id}/cancel        取消并释放占用（request_key 可重新登记）
POST /api/snapshots/export                按时间点导出最小闭包离线交换包（清单+分块）
POST /api/snapshots/precheck              接收方离线预检（签章/分块/引用/链/证明/共享边/本地）
POST /api/snapshots/imports               建/恢复分块上传会话（同包标识异内容 409；可带 owner/valid_until/ttl_minutes）
GET  /api/snapshots/imports[/{id}]        会话清单/详情（负责人/剩余时间/冻结原因/缺块/冲突/导入结果）
PUT  /api/snapshots/imports/{id}/chunks/{seq}   分块上传（乱序/重复/断点续传；仅当前负责人）
POST /api/snapshots/imports/{id}/finalize 终检：全匹配一次性导入；冲突可定位且库原样；重放幂等；仅当前负责人
POST /api/snapshots/imports/{id}/handover|renew|freeze|reopen  接收窗口管理（request_key 幂等 + version 乐观锁）
GET  /api/snapshots/imports/{id}/events   会话操作审计
POST /api/snapshots/imports-cleanup       保留期清理（已导入永不清理）
POST /api/certificates           签发证明
GET  /api/certificates/{id}/verify
GET  /api/parcels/{id}?at=ISO    单宗时点状态（含血缘操作）
GET  /api/at/{ts}                全区时点快照
GET  /api/map                    编辑器一次拉取（宗地/界址点/争议/证明）
GET  /api/ops                    生命周期操作血缘
```

## 目录

```
app/
  config.py          容差与路径
  db.py              SQLite schema（版本/候选/争议/操作/证明/幂等）
  geo.py             几何内核：自交求交、片段差异、定向选路、片段替换、面积
  schemas.py         API 模型
  services/
    sync.py          离线包、提交、并发分歧检测、确认传播
    adjudication.py  裁定（锚点约束/补链不动/面积守恒）
    lifecycle.py     分割/合并/更正
    plans.py         生效计划：登记/互斥占用/租约执行器/暂停恢复/取消
    snapshots.py     快照交换包：时间点最小闭包导出/清单签章/离线预检/分块会话/一次性导入
    versions.py      不可变版本、时间点查询、哈希链审计
    certificates.py  证明签发/验真
    crypto.py        哈希链与 HMAC
  routers/           HTTP 层（含 effect-plans 生效计划、snapshots 快照交换包）
static/              Canvas 编辑器（index.html / app.js / styles.css）
tests/               业务规则 + HTTP 集成测试（test_snapshots.py 覆盖交换包导出闭包/离线预检/分块/冲突/幂等全场景；test_snapshot_window.py 覆盖接收窗口/负责人/到期冻结/移交续期版本幂等/重开重验/已导入保护/保留期清理）
seed.py              演示种子（甲/乙两宗，共享 x=100 边）
demo_live.py         实时端到端断言脚本
```

## 安全与生产化说明（演示边界）

* 证明使用对称 HMAC（`CADASTRE_CERT_KEY`），生产应替换为非对称 CA/KMS 密钥与吊销列表；
* 未做鉴权/RBAC、操作人电子签章与设备证书绑定；
* 照片为本地文件存储，生产应转对象存储并做病毒扫描；
* SQLite + 进程内写锁适合单节点演示，多节点应换 PostGIS/PostgreSQL 并加行级乐观锁；
* 生效计划执行器是单节点后台线程（持久租约 + 完成令牌已保证崩溃补跑/不二次落账），
  多节点部署应改为带行锁/选主的分布式调度（如 `SELECT ... FOR UPDATE SKIP LOCKED`），
  租约回收间隔由 `CADASTRE_PLAN_POLL_SECONDS` / `CADASTRE_PLAN_LEASE_TTL_SECONDS` 控制。
