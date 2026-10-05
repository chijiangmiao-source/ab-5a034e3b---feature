#!/usr/bin/env python3
"""深空辐照标定封存服务（标准库实现，无第三方依赖）。

核心规则：
  * 首次有效投票原子冻结：站点名单、阈值、摘要摘要(digest)。
  * 同站 + 同摘要 + 同稳定投票标识的重传只回放首次结果，不增票。
  * 绑定内容改变（不同投票标识）或不同摘要 -> 记录冲突票，隔离且不计入赞成票。
  * 赞成站数达到阈值的瞬间在同一把锁、同一次落盘内生成唯一不可变证书；
    此后一切投票迟到拒绝，证书永不变更。
  * 每次变更以 临时文件 + fsync + rename 原子落盘；重启后从持久记录恢复，
    已封存批次恢复后仍为 sealed。
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import sys
import threading
import traceback
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

DATA_DIR = os.environ.get("DATA_DIR", "/data")
DB_PATH = os.environ.get("DB_PATH") or os.path.join(DATA_DIR, "seal.db.json")
BIND_HOST = os.environ.get("SEAL_HOST", "0.0.0.0")
BIND_PORT = int(os.environ.get("SEAL_PORT", "8080"))

MAX_BODY_BYTES = 64 * 1024
MAX_STATIONS = 100
STATION_MAX_LEN = 64
DIGEST_MAX_LEN = 200
VOTE_ID_MAX_LEN = 200
BATCH_ID_RE = re.compile(r"^[A-Za-z0-9_.@:/\-]{1,64}$")
EVENTS_DEFAULT_LIMIT = 20
EVENTS_MAX_LIMIT = 100

VOTE_KIND_YES = "yes"
VOTE_KIND_CONFLICT = "conflict"

# 封存流水事件类型（result_code 取值，写入后永不改变）。
EVENT_VOTE_ACCEPTED = "VOTE_ACCEPTED"
EVENT_VOTE_CONFLICT = "VOTE_CONFLICT"
EVENT_SEALED = "SEALED"
EVENT_LATE_REJECTED = "LATE_VOTE_REJECTED"


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class ApiError(Exception):
    """带机器可读 code 与 HTTP 状态码的业务异常。"""

    def __init__(self, status: int, code: str, message: str, details=None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.details = details or {}

    def body(self) -> dict:
        return {"error": {"code": self.code, "message": self.message, "details": self.details}}


def _require_str(value, field: str, max_len: int):
    if not isinstance(value, str):
        raise ApiError(422, "INVALID_PARAMETER", f"参数 {field} 必须是字符串", {"field": field})
    value = value.strip()
    if not value:
        raise ApiError(422, "INVALID_PARAMETER", f"参数 {field} 不能为空", {"field": field})
    if len(value) > max_len:
        raise ApiError(422, "INVALID_PARAMETER", f"参数 {field} 长度超过 {max_len}", {"field": field, "max": max_len})
    return value


class Store:
    """线程安全、崩溃安全的批次存储。"""

    def __init__(self, path: str):
        self.path = path
        self.lock = threading.RLock()
        self.state = {"version": 1, "revision": 0, "batches": {}}
        self._load()

    # ---------- 持久化 ----------

    def _load(self):
        d = os.path.dirname(self.path) or "."
        os.makedirs(d, exist_ok=True)
        # 清理上次崩溃可能残留的临时文件（正式文件从不以 .tmp 结尾）。
        for name in os.listdir(d):
            if name.startswith("seal.db.json.tmp."):
                try:
                    os.unlink(os.path.join(d, name))
                except OSError:
                    pass
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if not isinstance(loaded, dict) or "batches" not in loaded:
                raise RuntimeError(f"持久记录格式损坏: {self.path}")
            self.state = loaded
            # 旧版本记录可能没有封存流水：按已持久的投票/证书确定性回填，
            # 回填结果在多次重启间保持一致。
            for b in self.state["batches"].values():
                b.setdefault("events", [])
                b.setdefault("late_rejections", [])
                if not b["events"] and b.get("votes"):
                    self._backfill_events_locked(b)
            sealed = sum(1 for b in self.state["batches"].values() if b.get("certificate"))
            print(
                f"[recover] 从 {self.path} 恢复：批次 {len(self.state['batches'])} 个，"
                f"已封存 {sealed} 个，revision={self.state.get('revision')}",
                file=sys.stderr,
            )
        else:
            self._persist_locked()

    def _persist_locked(self):
        """调用方必须持有 self.lock。临时文件 + fsync + 原子 rename。"""
        d = os.path.dirname(self.path) or "."
        os.makedirs(d, exist_ok=True)
        tmp = f"{self.path}.tmp.{os.getpid()}.{threading.get_ident()}"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.state, f, ensure_ascii=False, indent=2, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)
        dfd = os.open(d, os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)

    # ---------- 封存流水 ----------

    def _append_event_locked(self, b, event_type, station, vote_id, result_code,
                             detail=None, digest=None, record_id=None, ts=None):
        """追加一条封存流水。调用方必须已在同一次提交内完成快照变更
        （本方法不自建持久化边界，保证流水与批次状态原子提交）。

        seq 为批次内严格递增的稳定序号（1 起），重启后不变；
        revision 记录事件落盘后的全局 revision。
        vote_id 为站点提交的稳定投票标识；record_id 为库内投票记录 id
        （封签/拒绝等非投票事件为 None）。
        """
        seq = len(b["events"]) + 1
        event = {
            "seq": seq,
            "type": event_type,
            "station": station,
            "vote_id": vote_id,
            "record_id": record_id,
            "result_code": result_code,
            "digest": digest,
            "detail": detail or {},
            "ts": ts or utc_now(),
            "revision": self.state["revision"],
        }
        b["events"].append(event)
        return event

    @staticmethod
    def _event_public(e: dict) -> dict:
        return {
            "seq": e["seq"],
            "type": e["type"],
            "station": e["station"],
            "vote_id": e["vote_id"],
            "record_id": e.get("record_id"),
            "result_code": e["result_code"],
            "digest": e.get("digest"),
            "detail": e.get("detail") or {},
            "ts": e["ts"],
            "revision": e["revision"],
        }

    def _backfill_events_locked(self, b):
        """旧记录（无流水）冷启动时，依据持久投票与证书确定性重建流水。

        按持久化顺序（即发生顺序）映射 接受/冲突 事件；封签发生在阈值第
        threshold 张赞成票落盘的同一临界区，故在其后插入唯一封签事件。
        迟到拒绝发生在封签之后且不入库，无从回填，也不允许伪造。
        """
        events = []
        yes_seen = 0
        seal_after = None
        cert = b.get("certificate")
        revision = self.state.get("revision", 0)
        for v in b["votes"]:
            if v["kind"] == VOTE_KIND_YES:
                yes_seen += 1
                events.append({
                    "seq": len(events) + 1,
                    "type": "vote_accepted",
                    "station": v["station"],
                    "vote_id": v["vote_id"],
                    "record_id": v["id"],
                    "result_code": EVENT_VOTE_ACCEPTED,
                    "digest": v["digest"],
                    "detail": {},
                    "ts": v["ts"],
                    "revision": revision,
                })
                if cert and seal_after is None and yes_seen == b["threshold"]:
                    seal_after = v["id"]
            else:
                events.append({
                    "seq": len(events) + 1,
                    "type": "vote_conflict",
                    "station": v["station"],
                    "vote_id": v["vote_id"],
                    "record_id": v["id"],
                    "result_code": v.get("reason") or EVENT_VOTE_CONFLICT,
                    "digest": v["digest"],
                    "detail": {"reason": v.get("reason")},
                    "ts": v["ts"],
                    "revision": revision,
                })
            if cert and v["kind"] == VOTE_KIND_YES and v["id"] == seal_after:
                events.append({
                    "seq": len(events) + 1,
                    "type": "sealed",
                    "station": None,
                    "vote_id": None,
                    "record_id": None,
                    "result_code": EVENT_SEALED,
                    "digest": b["digest"],
                    "detail": {"certificate_serial": cert.get("serial", 1)},
                    "ts": cert["sealed_at"],
                    "revision": revision,
                })
        b["events"] = events

    # ---------- 快照 ----------

    @staticmethod
    def _snapshot(b: dict, revision: int) -> dict:
        yes_votes = [v for v in b["votes"] if v["kind"] == VOTE_KIND_YES]
        yes_stations = sorted({v["station"] for v in yes_votes})
        conflicts = [
            {
                "station": v["station"],
                "digest": v["digest"],
                "vote_id": v["vote_id"],
                "reason": v["reason"],
                "ts": v["ts"],
            }
            for v in b["votes"]
            if v["kind"] == VOTE_KIND_CONFLICT
        ]
        return {
            "id": b["id"],
            "status": "sealed" if b["certificate"] else "collecting",
            "stations": list(b["stations"]),
            "threshold": b["threshold"],
            "digest": b["digest"],
            "frozen_at": b["frozen_at"],
            "yes_count": len(yes_stations),
            "yes_stations": yes_stations,
            "conflicts": conflicts,
            "certificate": b["certificate"],
            "latest_event_seq": len(b.get("events", [])),
            "revision": revision,
        }

    def get_batch_snapshot(self, batch_id: str) -> dict:
        with self.lock:
            b = self.state["batches"].get(batch_id)
            if b is None:
                raise ApiError(404, "BATCH_NOT_FOUND", f"批次 {batch_id} 不存在", {"batch_id": batch_id})
            return self._snapshot(b, self.state["revision"])

    def list_snapshots(self) -> list:
        with self.lock:
            return [self._snapshot(b, self.state["revision"]) for b in
                    sorted(self.state["batches"].values(), key=lambda x: x["id"])]

    # ---------- 封存流水读取（按批次、稳定序号游标分页） ----------

    @staticmethod
    def _encode_cursor(batch_id: str, before_seq: int) -> str:
        raw = json.dumps({"b": batch_id, "s": before_seq}, ensure_ascii=False,
                         separators=(",", ":")).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

    @staticmethod
    def _decode_cursor(cursor: str):
        try:
            padded = cursor + "=" * (-len(cursor) % 4)
            raw = base64.urlsafe_b64decode(padded.encode("ascii"))
            data = json.loads(raw.decode("utf-8"))
        except Exception:
            data = None
        if not isinstance(data, dict) or not isinstance(data.get("b"), str) \
                or isinstance(data.get("s"), bool) or not isinstance(data.get("s"), int):
            raise ApiError(
                422, "INVALID_CURSOR", "游标无法解析或已损坏",
                {"cursor": cursor,
                 "hint": "请原样使用上次响应中的 next_cursor；或省略 cursor 从最新事件重新加载"},
            )
        return data["b"], data["s"]

    def get_events(self, batch_id: str, cursor=None, limit_raw=None):
        """按批次读取封存流水：无游标时返回最新一页，此后沿 next_cursor 向更早翻页。"""
        if limit_raw is None or limit_raw == "":
            limit = EVENTS_DEFAULT_LIMIT
        else:
            try:
                limit = int(limit_raw)
            except (TypeError, ValueError):
                raise ApiError(422, "INVALID_LIMIT", "limit 必须是正整数",
                               {"limit": limit_raw, "max": EVENTS_MAX_LIMIT,
                                "hint": f"请传 1..{EVENTS_MAX_LIMIT} 之间的整数，或省略使用默认值"})
            if not 1 <= limit <= EVENTS_MAX_LIMIT:
                raise ApiError(
                    422, "INVALID_LIMIT",
                    f"limit 必须在 1 与 {EVENTS_MAX_LIMIT} 之间",
                    {"limit": limit, "max": EVENTS_MAX_LIMIT,
                     "hint": f"请传 1..{EVENTS_MAX_LIMIT} 之间的整数，或省略使用默认值"},
                )
        with self.lock:
            b = self.state["batches"].get(batch_id)
            if b is None:
                raise ApiError(404, "BATCH_NOT_FOUND", f"批次 {batch_id} 不存在",
                               {"batch_id": batch_id,
                                "hint": "请先创建该批次，或从批次列表中选择正确的标识"})
            total = len(b["events"])
            upper = total + 1  # 合法游标区间 [1, total+1]
            if cursor:
                cursor_batch, before_seq = self._decode_cursor(cursor)
                if cursor_batch != batch_id:
                    raise ApiError(
                        422, "CURSOR_BATCH_MISMATCH",
                        "游标不属于该批次，不能用于读取另一批次的流水",
                        {"cursor_batch_id": cursor_batch, "batch_id": batch_id,
                         "hint": "请省略 cursor 从该批次最新事件开始加载，再跟随返回的 next_cursor 翻页"},
                    )
                if before_seq < 1 or before_seq > upper:
                    raise ApiError(
                        422, "INVALID_CURSOR",
                        f"游标指向的序号 {before_seq} 在该批次中不存在（当前有效序号 1..{total}）",
                        {"batch_id": batch_id, "event_seq": before_seq,
                         "latest_seq": total,
                         "hint": "批次可能已按更短历史重新查询；请省略 cursor 从最新事件重新加载"},
                    )
            else:
                before_seq = upper

            # 严格按稳定 seq 由新到旧取窗口；序号即持久顺序，重启后不变。
            window = [e for e in b["events"] if e["seq"] < before_seq]
            window.sort(key=lambda e: e["seq"], reverse=True)
            page = window[:limit]
            has_more = bool(page) and page[-1]["seq"] > 1
            next_cursor = self._encode_cursor(batch_id, page[-1]["seq"]) if has_more else None
            return {
                "batch_id": batch_id,
                "events": [self._event_public(e) for e in page],
                "next_cursor": next_cursor,
                "has_more": has_more,
                "latest_seq": total,
                "revision": self.state["revision"],
            }

    # ---------- 批次创建（配置录入） ----------

    def create_batch(self, batch_id, stations, threshold):
        batch_id = _require_str(batch_id, "id", 64)
        if not BATCH_ID_RE.match(batch_id):
            raise ApiError(
                422, "INVALID_BATCH_ID",
                "批次标识仅限字母数字与 _ . @ : / -，长度 1-64",
                {"batch_id": batch_id},
            )
        if not isinstance(stations, list) or not stations:
            raise ApiError(422, "INVALID_STATIONS", "站点名单必须是非空数组", {})
        if len(stations) > MAX_STATIONS:
            raise ApiError(422, "INVALID_STATIONS", f"站点数量超过上限 {MAX_STATIONS}", {"max": MAX_STATIONS})
        cleaned = []
        seen = set()
        for raw in stations:
            s = _require_str(raw, "stations", STATION_MAX_LEN)
            if s in seen:
                raise ApiError(422, "DUPLICATE_STATION", f"站点名单存在重复站点: {s}", {"station": s})
            seen.add(s)
            cleaned.append(s)
        if isinstance(threshold, bool) or not isinstance(threshold, int):
            raise ApiError(422, "INVALID_THRESHOLD", "阈值必须是整数", {})
        if not 1 <= threshold <= len(cleaned):
            raise ApiError(
                422, "INVALID_THRESHOLD",
                f"阈值必须在 1 与站点数({len(cleaned)})之间",
                {"threshold": threshold, "station_count": len(cleaned)},
            )
        with self.lock:
            if batch_id in self.state["batches"]:
                existing = self.state["batches"][batch_id]
                if existing["certificate"]:
                    raise ApiError(409, "BATCH_SEALED", f"批次 {batch_id} 已封存，不可重建",
                                   {"batch_id": batch_id})
                raise ApiError(409, "BATCH_EXISTS", f"批次 {batch_id} 已存在且在收集中",
                               {"batch_id": batch_id})
            b = {
                "id": batch_id,
                "stations": cleaned,
                "threshold": threshold,
                "digest": None,        # 由首次有效投票冻结
                "frozen_at": None,
                "votes": [],
                "events": [],          # 封存流水（稳定 seq，仅追加）
                "late_rejections": [],  # 封签后迟到载荷去重表（回放拒绝用）
                "certificate": None,
            }
            self.state["batches"][batch_id] = b
            self.state["revision"] += 1
            self._persist_locked()
            return self._snapshot(b, self.state["revision"])

    # ---------- 投票 ----------

    def _find_vote(self, b, station, digest, vote_id, kind=None):
        for v in b["votes"]:
            if v["station"] == station and v["digest"] == digest and v["vote_id"] == vote_id:
                if kind is None or v["kind"] == kind:
                    return v
        return None

    def _find_yes(self, b, station):
        for v in b["votes"]:
            if v["station"] == station and v["kind"] == VOTE_KIND_YES:
                return v
        return None

    @staticmethod
    def _find_late_rejection_locked(b, station, digest, vote_id):
        for r in b.get("late_rejections", []):
            if r["station"] == station and r["digest"] == digest and r["vote_id"] == vote_id:
                return r
        return None

    def _record_conflict(self, b, station, digest, vote_id, reason, detail):
        """冲突票隔离并在同一持久化提交内追加冲突流水；重传同一冲突载荷只回放。"""
        existing = self._find_vote(b, station, digest, vote_id, VOTE_KIND_CONFLICT)
        replayed = existing is not None
        if existing is None:
            record = {
                "id": f"v_{len(b['votes']) + 1}_{hashlib.sha1(os.urandom(16)).hexdigest()[:10]}",
                "station": station,
                "digest": digest,
                "vote_id": vote_id,
                "kind": VOTE_KIND_CONFLICT,
                "reason": reason,
                "ts": utc_now(),
            }
            b["votes"].append(record)
            self.state["revision"] += 1
            # 流水与冲突记录在同一次提交内落盘（此时尚未 persist）。
            self._append_event_locked(
                b, "vote_conflict", station, vote_id, reason,
                detail={"reason": reason, "digest_frozen": b["digest"],
                        "digest_submitted": digest},
                digest=digest, record_id=record["id"],
            )
            self._persist_locked()
        snap = self._snapshot(b, self.state["revision"])
        err = ApiError(
            422, reason,
            f"来自站点 {station} 的投票被隔离：{detail}，不计入赞成票、不参与封存",
            {"replayed": replayed, "batch": snap},
        )
        err.replayed = replayed
        raise err

    @staticmethod
    def _fingerprint(b) -> str:
        yeses = sorted(
            ((v["station"], v["vote_id"], v["id"]) for v in b["votes"] if v["kind"] == VOTE_KIND_YES),
            key=lambda x: x[0],
        )
        material = {
            "batch_id": b["id"],
            "stations": sorted(b["stations"]),
            "threshold": b["threshold"],
            "digest": b["digest"],
            "yes_votes": [{"station": s, "vote_id": vid, "id": i} for s, vid, i in yeses],
        }
        canonical = json.dumps(material, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def cast_vote(self, batch_id, station, digest, vote_id):
        """返回 (http_status, payload)。整个判定+封存+落盘在同一把锁内完成。"""
        station = _require_str(station, "station", STATION_MAX_LEN)
        digest = _require_str(digest, "digest", DIGEST_MAX_LEN)
        vote_id = _require_str(vote_id, "vote_id", VOTE_ID_MAX_LEN)
        with self.lock:
            b = self.state["batches"].get(batch_id)
            if b is None:
                raise ApiError(404, "BATCH_NOT_FOUND", f"批次 {batch_id} 不存在", {"batch_id": batch_id})

            # 站点不在冻结名单：配置不符，拒绝（不污染冲突记录）。
            if station not in b["stations"]:
                raise ApiError(
                    422, "STATION_NOT_LISTED",
                    f"站点 {station} 不在批次冻结名单中",
                    {"station": station, "frozen_stations": list(b["stations"])},
                )

            # 幂等回放优先于一切状态判定：同站同摘要同投票标识的赞成票，
            # 即使批次已封存也只回放首次结果（“迟到票”仅指未见过的新载荷）。
            existing_yes = self._find_vote(b, station, digest, vote_id, VOTE_KIND_YES)
            if existing_yes is not None:
                return 200, {
                    "accepted": True,
                    "replayed": True,
                    "vote_id": existing_yes["id"],
                    "batch": self._snapshot(b, self.state["revision"]),
                }

            # 已记录过的冲突载荷重传：回放首次冲突结果，不重复堆积记录。
            existing_conflict = self._find_vote(b, station, digest, vote_id, VOTE_KIND_CONFLICT)
            if existing_conflict is not None:
                raise ApiError(
                    422, existing_conflict["reason"],
                    f"来自站点 {station} 的冲突票为重传：回放首次隔离结果，不计入赞成票",
                    {"replayed": True, "batch": self._snapshot(b, self.state["revision"])},
                )

            # 已封存且载荷未见过：迟到票一律拒绝并留痕，绝不可能改写证书。
            # 同一迟到载荷重传只回放原拒绝（同一 seq），不产生第二条流水。
            if b["certificate"] is not None:
                prior = self._find_late_rejection_locked(b, station, digest, vote_id)
                if prior is None:
                    self.state["revision"] += 1
                    self._append_event_locked(
                        b, "late_rejected", station, vote_id, EVENT_LATE_REJECTED,
                        detail={"reason": EVENT_LATE_REJECTED,
                                "certificate_serial": b["certificate"].get("serial", 1),
                                "certificate_fingerprint": b["certificate"]["fingerprint"]},
                        digest=digest,
                    )
                    b.setdefault("late_rejections", []).append({
                        "station": station, "digest": digest, "vote_id": vote_id,
                        "seq": b["events"][-1]["seq"],
                    })
                    self._persist_locked()
                    replayed_late = False
                    reject_seq = b["events"][-1]["seq"]
                else:
                    replayed_late = True
                    reject_seq = prior["seq"]
                raise ApiError(
                    409, "LATE_VOTE_REJECTED",
                    f"批次 {batch_id} 已封存，证书不可变，迟到票被拒绝（流水序号 #{reject_seq}）",
                    {"replayed": replayed_late, "event_seq": reject_seq,
                     "batch": self._snapshot(b, self.state["revision"])},
                )

            # 摘要已冻结且不同：冲突隔离。
            if b["digest"] is not None and digest != b["digest"]:
                self._record_conflict(
                    b, station, digest, vote_id, "DIGEST_MISMATCH",
                    f"摘要 {digest[:16]}… 与冻结摘要 {b['digest'][:16]}… 不一致",
                )

            # 同站对同摘要使用了不同的稳定投票标识：绑定内容改变，冲突隔离。
            prior_yes = self._find_yes(b, station)
            if prior_yes is not None:
                self._record_conflict(
                    b, station, digest, vote_id, "VOTE_ID_CHANGED",
                    f"站点 {station} 已用投票标识 {prior_yes['vote_id']} 投过赞成票，"
                    "同一绑定不得更换投票标识",
                )

            # 首次有效投票：原子冻结摘要（名单与阈值在创建时已固定）。
            frozen_now = False
            if b["digest"] is None:
                b["digest"] = digest
                b["frozen_at"] = utc_now()
                frozen_now = True

            vote = {
                "id": f"v_{len(b['votes']) + 1}_{hashlib.sha1(os.urandom(16)).hexdigest()[:10]}",
                "station": station,
                "digest": digest,
                "vote_id": vote_id,
                "kind": VOTE_KIND_YES,
                "reason": None,
                "ts": utc_now(),
            }
            b["votes"].append(vote)

            self.state["revision"] += 1
            # 接受事件：与赞成票、（可能的）冻结与封签在同一次持久化提交内追加。
            self._append_event_locked(
                b, "vote_accepted", station, vote_id, EVENT_VOTE_ACCEPTED,
                detail={"froze_config": frozen_now},
                digest=digest, record_id=vote["id"], ts=vote["ts"],
            )

            snapshot = self._snapshot(b, self.state["revision"])
            # 达到阈值的瞬间在同一事务内封签；并发临界区只会产生一个封签事件。
            if snapshot["yes_count"] >= b["threshold"]:
                sealed_at = utc_now()
                certificate = {
                    "serial": 1,
                    "batch_id": b["id"],
                    "digest": b["digest"],
                    "threshold": b["threshold"],
                    "stations": snapshot["yes_stations"],
                    "fingerprint": self._fingerprint({**b, "votes": b["votes"]}),
                    "sealed_at": sealed_at,
                    "immutable": True,
                }
                b["certificate"] = certificate
                self._append_event_locked(
                    b, "sealed", None, None, EVENT_SEALED,
                    detail={"certificate_serial": certificate["serial"],
                            "fingerprint": certificate["fingerprint"],
                            "yes_stations": snapshot["yes_stations"]},
                    digest=b["digest"], ts=sealed_at,
                )

            # 赞成票、接受流水、封签证书与封签流水原子落盘（单次 fsync 提交）。
            self._persist_locked()
            return 200, {
                "accepted": True,
                "replayed": False,
                "froze_config": frozen_now,
                "vote_id": vote["id"],
                "batch": self._snapshot(b, self.state["revision"]),
            }


STORE = Store(DB_PATH)


class Handler(BaseHTTPRequestHandler):
    server_version = "SealServer/1.0"

    def log_message(self, fmt, *args):
        print(f"[http] {self.address_string()} {fmt % args}", file=sys.stderr)

    # ---------- 工具 ----------

    def _send_json(self, status: int, payload: dict):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            raise ApiError(400, "EMPTY_BODY", "请求体为空且需要 JSON")
        if length > MAX_BODY_BYTES:
            raise ApiError(413, "BODY_TOO_LARGE", f"请求体超过 {MAX_BODY_BYTES} 字节")
        raw = self.rfile.read(length)
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ApiError(400, "INVALID_JSON", f"JSON 解析失败: {exc}")
        if not isinstance(data, dict):
            raise ApiError(422, "INVALID_BODY", "请求体必须是 JSON 对象")
        return data

    def _api_error(self, err: ApiError):
        self._send_json(err.status, err.body())

    def _serve_index(self):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
        try:
            with open(path, "rb") as f:
                body = f.read()
        except OSError:
            self._send_json(500, {"error": {"code": "INDEX_MISSING", "message": "页面文件缺失"}})
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ---------- 路由 ----------

    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            if path == "/health":
                self._send_json(200, {"status": "ok", "revision": STORE.state["revision"]})
            elif path == "/":
                self._serve_index()
            elif path == "/api/batches":
                self._send_json(200, {"batches": STORE.list_snapshots()})
            elif path.startswith("/api/batches/") and path.endswith("/events"):
                batch_id = unquote(path[len("/api/batches/"):-len("/events")])
                qs = parse_qs(parsed.query)
                cursor = (qs.get("cursor") or [None])[0]
                limit_raw = (qs.get("limit") or [None])[0]
                self._send_json(200, STORE.get_events(batch_id, cursor=cursor, limit_raw=limit_raw))
            elif path.startswith("/api/batches/"):
                batch_id = unquote(path[len("/api/batches/"):])
                self._send_json(200, {"batch": STORE.get_batch_snapshot(batch_id)})
            else:
                self._send_json(404, {"error": {"code": "NOT_FOUND", "message": path}})
        except ApiError as err:
            self._api_error(err)
        except Exception:  # noqa: BLE001
            traceback.print_exc()
            self._send_json(500, {"error": {"code": "INTERNAL", "message": "服务内部错误"}})

    def do_POST(self):
        try:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/")
            if path == "/api/batches":
                data = self._read_json()
                snap = STORE.create_batch(data.get("id"), data.get("stations"), data.get("threshold"))
                self._send_json(201, {"batch": snap})
            elif path.startswith("/api/batches/") and path.endswith("/votes"):
                batch_id = unquote(path[len("/api/batches/"):-len("/votes")])
                data = self._read_json()
                status, payload = STORE.cast_vote(
                    batch_id, data.get("station"), data.get("digest"), data.get("vote_id")
                )
                self._send_json(status, payload)
            else:
                self._send_json(404, {"error": {"code": "NOT_FOUND", "message": path}})
        except ApiError as err:
            self._api_error(err)
        except Exception:  # noqa: BLE001
            traceback.print_exc()
            self._send_json(500, {"error": {"code": "INTERNAL", "message": "服务内部错误"}})


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    httpd = ThreadingHTTPServer((BIND_HOST, BIND_PORT), Handler)
    httpd.daemon_threads = True
    print(f"[boot] 封存服务监听 {BIND_HOST}:{BIND_PORT}，数据文件 {DB_PATH}", file=sys.stderr)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
