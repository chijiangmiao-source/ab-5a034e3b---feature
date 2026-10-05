#!/usr/bin/env python3
"""验收脚本：在页面与健康响应可用后运行。

覆盖内容（全部走真实 HTTP 接口）：
  A. 代码测试（unittest）
     - 同站同摘要同稳定投票标识的幂等重传：回放首次结果、不增票；
     - 投票标识内容改变 / 摘要不同：冲突票隔离、不计入赞成票、不参与封存；
     - 配置不符与参数无效的可操作拒绝（错误码可读）。
  B. 页面构建可用：/ 返回包含控制台与脚本的真实页面。
  C. API/HTTP 冒烟
     - /health 健康响应；
     - 多站并发投票下封签唯一（证书唯一、指纹一致、迟到票拒绝）；
     - 封存后并发冲击不可改写证书。
  D. 重启恢复
     - 复制线上持久记录，拉起全新服务进程（模拟重启），
       验证收集中批次的票与冻结配置恢复、已封存批次仍为 sealed 且证书逐字节一致；
     - 模拟写入途中崩溃残留的临时文件不影响恢复；
     - 二次重启结果不变，迟到票仍被拒绝。

成功退出码 0，任一失败退出码 1。
"""

import base64
import json
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
import uuid

BASE_URL = os.environ.get("BASE_URL", "http://web:8080").rstrip("/")
APP_PATH = os.environ.get("APP_PATH", "/app/server.py")
SOURCE_DATA_DIR = os.environ.get("SOURCE_DATA_DIR", "/data")
RESTART_ROOT = os.environ.get("RESTART_ROOT", "/tmp/restart")

FAILURES = []


def check(cond, msg):
    if cond:
        print(f"    ✅ {msg}")
    else:
        print(f"    ❌ {msg}")
        FAILURES.append(msg)


# ---------------------------------------------------------------- HTTP 客户端

class Http:
    def __init__(self, base):
        self.base = base.rstrip("/")

    def request(self, method, path, body=None, timeout=15):
        url = self.base + path
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8")
                if not raw:
                    return resp.status, None
                try:
                    return resp.status, json.loads(raw)
                except json.JSONDecodeError:
                    return resp.status, raw
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8")
            try:
                return e.code, json.loads(raw)
            except json.JSONDecodeError:
                return e.code, {"raw": raw}

    def get(self, path, **kw):
        return self.request("GET", path, **kw)

    def post(self, path, body, **kw):
        return self.request("POST", path, body=body, **kw)

    def get_batch(self, bid):
        st, body = self.get(f"/api/batches/{bid}")
        return st, body["batch"]

    def create(self, stations, threshold, bid=None):
        bid = bid or f"vt-{uuid.uuid4().hex[:12]}"
        st, body = self.post("/api/batches", {"id": bid, "stations": stations, "threshold": threshold})
        return st, body, bid

    def vote(self, bid, station, digest, vote_id):
        return self.post(f"/api/batches/{bid}/votes",
                         {"station": station, "digest": digest, "vote_id": vote_id})

    def events(self, bid, cursor=None, limit=None):
        qs = []
        if cursor is not None:
            qs.append("cursor=" + urllib.parse.quote(cursor, safe=""))
        if limit is not None:
            qs.append("limit=" + str(limit))
        suffix = ("?" + "&".join(qs)) if qs else ""
        return self.get(f"/api/batches/{bid}/events{suffix}")

    def all_events(self, bid, page=20):
        """沿 next_cursor 翻到起点，返回按 seq 升序的完整流水。"""
        merged, cursor, seen = {}, None, set()
        while True:
            st, body = self.events(bid, cursor=cursor, limit=page)
            if st != 200:
                raise AssertionError(f"events 接口失败 {st}: {body}")
            for e in body["events"]:
                if e["seq"] in seen:
                    raise AssertionError(f"分页返回重复 seq {e['seq']}")
                seen.add(e["seq"])
                merged[e["seq"]] = e
            if not body["has_more"]:
                return [merged[s] for s in sorted(merged)]
            cursor = body["next_cursor"]
            if not cursor:
                raise AssertionError("has_more=true 却没有 next_cursor")


def encode_cursor(bid, seq):
    raw = json.dumps({"b": bid, "s": seq}, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


API = Http(BASE_URL)


def wait_available(max_wait=90):
    print("\n== 等待页面与健康响应可用 ==")
    deadline = time.time() + max_wait
    health_ok = page_ok = False
    while time.time() < deadline:
        try:
            st, body = API.get("/health", timeout=3)
            health_ok = st == 200 and isinstance(body, dict) and body.get("status") == "ok"
        except Exception:
            health_ok = False
        try:
            st, body = API.get("/", timeout=3)
            if st != 200 or not isinstance(body, str):
                page_ok = False
            else:
                page_ok = ("封存控制台" in body and "castVote" in body
                           and "封存流水" in body and "/events" in body)
        except Exception:
            page_ok = False
        if health_ok and page_ok:
            break
        time.sleep(0.5)
    check(health_ok, f"健康响应可用：GET {BASE_URL}/health → 200 status=ok")
    check(page_ok, "页面可用：GET / 返回 200 且包含控制台界面与投票脚本")
    return health_ok and page_ok


# ---------------------------------------------------------- A. 代码测试 unittest

class CodeTests(unittest.TestCase):
    """覆盖幂等重传与冲突票隔离的代码测试。"""

    def test_01_idempotent_replay_replays_first_result_without_new_vote(self):
        st, body, bid = API.create(["站A", "站B", "站C"], 3)
        self.assertEqual(st, 201, body)

        st, r1 = API.vote(bid, "站A", "digest-D1", "sv-A-1")
        self.assertEqual(st, 200, r1)
        self.assertFalse(r1["replayed"])
        self.assertTrue(r1["froze_config"])
        self.assertEqual(r1["batch"]["yes_count"], 1)
        first_vote_id = r1["vote_id"]

        # 同站 + 同摘要 + 同稳定投票标识重传 ×2：只回放，不增票。
        for _ in range(2):
            st, r = API.vote(bid, "站A", "digest-D1", "sv-A-1")
            self.assertEqual(st, 200, r)
            self.assertTrue(r["replayed"], "重传必须被识别为回放")
            self.assertEqual(r["vote_id"], first_vote_id, "回放的必须是首次投票记录")
            self.assertEqual(r["batch"]["yes_count"], 1, "重传不得增票")

        st, b = API.get_batch(bid)
        self.assertEqual(b["yes_count"], 1)
        self.assertEqual(b["digest"], "digest-D1", "首次有效投票冻结摘要")
        self.assertEqual(b["conflicts"], [])

    def test_02_changed_vote_id_is_recorded_conflict_and_isolated(self):
        st, _, bid = API.create(["站A", "站B", "站C"], 3)
        st, r = API.vote(bid, "站A", "digest-D1", "sv-A-1")
        self.assertEqual((st, r["batch"]["yes_count"]), (200, 1))

        # 同站同摘要但更换稳定投票标识 = 绑定内容改变 → 冲突隔离。
        st, r = API.vote(bid, "站A", "digest-D1", "sv-A-TAMPERED")
        self.assertEqual(st, 422, r)
        self.assertEqual(r["error"]["code"], "VOTE_ID_CHANGED")
        st, b = API.get_batch(bid)
        self.assertEqual(b["yes_count"], 1, "冲突票不计入赞成票")
        self.assertEqual(len(b["conflicts"]), 1)
        self.assertEqual(b["conflicts"][0]["station"], "站A")
        self.assertEqual(b["conflicts"][0]["reason"], "VOTE_ID_CHANGED")

        # 同一冲突载荷重传：回放冲突结果，不重复堆积冲突记录。
        st2, r2 = API.vote(bid, "站A", "digest-D1", "sv-A-TAMPERED")
        self.assertEqual(st2, 422)
        self.assertTrue(r2["error"]["details"].get("replayed"), "冲突票重传应为回放")
        st, b = API.get_batch(bid)
        self.assertEqual(len(b["conflicts"]), 1, "冲突重传不新增记录")
        self.assertEqual(b["yes_count"], 1)

        # 原始标识重传仍正常回放，不增票。
        st, r = API.vote(bid, "站A", "digest-D1", "sv-A-1")
        self.assertEqual(st, 200)
        self.assertTrue(r["replayed"])

        # 其余两站赞成到达阈值：冲突站点不参与封存，证书只含有效赞成站。
        st, r = API.vote(bid, "站B", "digest-D1", "sv-B-1")
        self.assertEqual(st, 200, r)
        st, r = API.vote(bid, "站C", "digest-D1", "sv-C-1")
        self.assertEqual(st, 200, r)
        cert = r["batch"]["certificate"]
        self.assertIsNotNone(cert, "达到阈值必须封签")
        self.assertEqual(cert["stations"], ["站A", "站B", "站C"])
        self.assertEqual(len(cert["stations"]), 3)

        # 封存后冲突票/迟到票不得改写证书。
        fp = cert["fingerprint"]
        st, r = API.vote(bid, "站B", "digest-D1", "sv-B-TAMPERED")
        self.assertEqual(st, 409)
        self.assertEqual(r["error"]["code"], "LATE_VOTE_REJECTED")
        st, b = API.get_batch(bid)
        self.assertEqual(b["status"], "sealed")
        self.assertEqual(b["certificate"]["fingerprint"], fp, "证书指纹不可变")

    def test_03_different_digest_is_conflict_and_cannot_seal(self):
        st, _, bid = API.create(["站A", "站B"], 2)
        st, r = API.vote(bid, "站A", "digest-D1", "sv-A-1")
        self.assertEqual(st, 200)

        st, r = API.vote(bid, "站B", "digest-D2", "sv-B-1")
        self.assertEqual(st, 422)
        self.assertEqual(r["error"]["code"], "DIGEST_MISMATCH")
        st, b = API.get_batch(bid)
        self.assertEqual(b["yes_count"], 1, "异摘要票隔离，不增赞成票")
        self.assertEqual(len(b["conflicts"]), 1)
        self.assertIsNone(b["certificate"], "冲突票不得触发封存")

        # 站B 改投冻结摘要 → 达到阈值才封签，证书锚定冻结摘要。
        st, r = API.vote(bid, "站B", "digest-D1", "sv-B-1")
        self.assertEqual(st, 200, r)
        self.assertEqual(r["batch"]["certificate"]["digest"], "digest-D1")

    def test_04_config_mismatch_and_invalid_params_get_actionable_rejections(self):
        st, _, bid = API.create(["站A", "站B"], 2)

        st, r = API.vote(bid, "站X-未授权", "digest-D1", "sv-X-1")
        self.assertEqual(st, 422)
        self.assertEqual(r["error"]["code"], "STATION_NOT_LISTED", r)
        self.assertIn("站A", r["error"]["details"].get("frozen_stations", []),
                      "拒绝反馈须带回冻结名单以便修正")

        st, r, _ = API.create(["站A"], 1, bid=bid)
        self.assertEqual(st, 409)
        self.assertEqual(r["error"]["code"], "BATCH_EXISTS")

        bad_cases = [
            ([{"id": "bad id!", "stations": ["站A"], "threshold": 1}], "INVALID_BATCH_ID"),
            ([{"id": f"x-{uuid.uuid4().hex[:8]}", "stations": [], "threshold": 1}], "INVALID_STATIONS"),
            ([{"id": f"x-{uuid.uuid4().hex[:8]}", "stations": ["站A", "站A"], "threshold": 1}],
             "DUPLICATE_STATION"),
            ([{"id": f"x-{uuid.uuid4().hex[:8]}", "stations": ["站A"], "threshold": 0}], "INVALID_THRESHOLD"),
            ([{"id": f"x-{uuid.uuid4().hex[:8]}", "stations": ["站A"], "threshold": 5}], "INVALID_THRESHOLD"),
            ([{"id": f"x-{uuid.uuid4().hex[:8]}", "stations": ["站A"], "threshold": "2"}],
             "INVALID_THRESHOLD"),
        ]
        for payload, code in bad_cases:
            st, r = API.post("/api/batches", payload[0])
            self.assertEqual(st, 422, payload)
            self.assertEqual(r["error"]["code"], code, payload)

        st, r = API.vote(f"missing-{uuid.uuid4().hex[:6]}", "站A", "d", "v")
        self.assertEqual(st, 404)
        self.assertEqual(r["error"]["code"], "BATCH_NOT_FOUND")

        st, r = API.post(f"/api/batches/{bid}/votes", {"station": "站A", "digest": "d"})
        self.assertEqual(st, 422)
        self.assertEqual(r["error"]["code"], "INVALID_PARAMETER")


# ---------------------------------------------------------- A2. 封存流水测试

EVENT_FIELDS = ("seq", "type", "station", "vote_id", "record_id", "result_code",
                "digest", "ts", "revision")


def event_sig(e):
    return tuple(e[k] for k in EVENT_FIELDS)


class EventTimelineTests(unittest.TestCase):
    """封存流水：稳定序号、原子提交、回放不增流水、游标分页与可操作错误。"""

    def test_01_timeline_order_fields_and_atomic_seal_commit(self):
        st, _, bid = API.create(["站A", "站B", "站C"], 2)
        self.assertEqual(st, 201)

        st, r = API.vote(bid, "站A", "digest-D1", "sv-A-1")
        self.assertEqual(st, 200)
        rev_after_a = r["batch"]["revision"]
        record_a = r["vote_id"]

        st, r = API.vote(bid, "站A", "digest-D1", "sv-A-TAMPERED")
        self.assertEqual(st, 422)
        self.assertEqual(r["error"]["code"], "VOTE_ID_CHANGED")
        rev_after_conflict = r["error"]["details"]["batch"]["revision"]

        st, r = API.vote(bid, "站B", "digest-D1", "sv-B-1")
        self.assertEqual(st, 200)
        cert = r["batch"]["certificate"]
        self.assertIsNotNone(cert)
        rev_seal = r["batch"]["revision"]

        st, r = API.vote(bid, "站C", "digest-D1", "sv-C-LATE")
        self.assertEqual(st, 409)
        self.assertEqual(r["error"]["code"], "LATE_VOTE_REJECTED")
        late_seq = r["error"]["details"]["event_seq"]
        self.assertFalse(r["error"]["details"]["replayed"])
        fp = cert["fingerprint"]

        # 同一迟到载荷重传：回放原拒绝，不产生第二条流水。
        st, r = API.vote(bid, "站C", "digest-D1", "sv-C-LATE")
        self.assertEqual(st, 409)
        self.assertTrue(r["error"]["details"]["replayed"], "迟到票重传必须标记回放")
        self.assertEqual(r["error"]["details"]["event_seq"], late_seq, "回放须指向同一流水序号")
        st, b = API.get_batch(bid)
        self.assertEqual(b["certificate"]["fingerprint"], fp, "迟到拒绝不得改写证书")
        self.assertEqual(b["latest_event_seq"], 5)

        events = API.all_events(bid)
        self.assertEqual([e["seq"] for e in events], [1, 2, 3, 4, 5])
        self.assertEqual([e["result_code"] for e in events],
                         ["VOTE_ACCEPTED", "VOTE_ID_CHANGED", "VOTE_ACCEPTED",
                          "SEALED", "LATE_VOTE_REJECTED"])
        self.assertEqual([e["type"] for e in events],
                         ["vote_accepted", "vote_conflict", "vote_accepted",
                          "sealed", "late_rejected"])

        e1, e2, e3, e4, e5 = events
        # 每项带站点、投票标识、结果码、发生时刻和变更后的 revision。
        for e in events:
            self.assertTrue(e["ts"], "事件必须有发生时刻")
            self.assertIsInstance(e["revision"], int)
        self.assertEqual((e1["station"], e1["vote_id"], e1["record_id"]),
                         ("站A", "sv-A-1", record_a))
        self.assertEqual(e1["revision"], rev_after_a)
        self.assertEqual(e2["station"], "站A")
        self.assertEqual(e2["vote_id"], "sv-A-TAMPERED")
        self.assertEqual(e2["revision"], rev_after_conflict)
        # 接受与封签同一持久化提交：revision 相同且相邻；并发临界只一个封签事件。
        self.assertEqual(e3["revision"], rev_seal)
        self.assertEqual(e4["revision"], rev_seal)
        self.assertIsNone(e4["station"])
        self.assertEqual(e4["detail"]["fingerprint"], fp)
        self.assertEqual(e5["station"], "站C")
        self.assertEqual(e5["detail"]["certificate_fingerprint"], fp,
                         "迟到拒绝事件须锚定未被改写的证书指纹")
        # revision 单调不减。
        revs = [e["revision"] for e in events]
        self.assertEqual(revs, sorted(revs))

    def test_02_replays_create_no_second_event(self):
        st, _, bid = API.create(["站A", "站B"], 2)
        API.vote(bid, "站A", "digest-D1", "sv-A-1")
        API.vote(bid, "站A", "digest-D1", "sv-A-X")  # 冲突
        for _ in range(2):
            st, r = API.vote(bid, "站A", "digest-D1", "sv-A-1")
            self.assertEqual((st, r["replayed"]), (200, True))
        for _ in range(2):
            st, r = API.vote(bid, "站A", "digest-D1", "sv-A-X")
            self.assertEqual(st, 422)
            self.assertTrue(r["error"]["details"]["replayed"])
        events = API.all_events(bid)
        self.assertEqual([e["result_code"] for e in events],
                         ["VOTE_ACCEPTED", "VOTE_ID_CHANGED"],
                         "等内容重传只能回放原结果，不能生成第二条流水")

    def test_03_default_read_is_newest_first_and_pages_backward(self):
        st, _, bid = API.create([f"s{i}" for i in range(6)], 6)
        for i in range(4):
            API.vote(bid, f"s{i}", f"digest-{bid}", f"vid-{i}")
        st, body = API.events(bid, limit=2)
        self.assertEqual(st, 200)
        self.assertEqual([e["seq"] for e in body["events"]], [4, 3], "默认最新在前")
        self.assertTrue(body["has_more"] and body["next_cursor"])
        self.assertEqual(body["latest_seq"], 4)
        st, page2 = API.events(bid, cursor=body["next_cursor"], limit=2)
        self.assertEqual([e["seq"] for e in page2["events"]], [2, 1])
        self.assertFalse(page2["has_more"])
        self.assertIsNone(page2["next_cursor"], "到起点后不再给游标")

    def test_04_bad_or_foreign_cursor_gets_actionable_error(self):
        st, _, b1 = API.create(["站A"], 1)
        API.vote(b1, "站A", "d", "v")
        st, _, b2 = API.create(["站A"], 1)

        st, body = API.events(b1, cursor="%%%not-base64%%%")
        self.assertEqual((st, body["error"]["code"]), (422, "INVALID_CURSOR"))
        self.assertIn("hint", body["error"]["details"], "错误须给出可操作提示")

        st, body = API.events(b1, cursor=encode_cursor(b1, 999))
        self.assertEqual((st, body["error"]["code"]), (422, "INVALID_CURSOR"),
                        "不存在的序号应被拒绝")
        self.assertEqual(body["error"]["details"]["latest_seq"], 2)

        st, body = API.events(b1, cursor=encode_cursor(b1, 0))
        self.assertEqual((st, body["error"]["code"]), (422, "INVALID_CURSOR"))

        st, body = API.events(b1, cursor=encode_cursor(b2, 1))
        self.assertEqual((st, body["error"]["code"]), (422, "CURSOR_BATCH_MISMATCH"),
                        "不属于该批次的游标须明确拒绝")
        self.assertEqual(body["error"]["details"]["cursor_batch_id"], b2)

        st, body = API.events(f"missing-{uuid.uuid4().hex[:6]}")
        self.assertEqual((st, body["error"]["code"]), (404, "BATCH_NOT_FOUND"))

        for bad_limit in ("0", "101", "abc"):
            st, body = API.events(b1, limit=bad_limit)
            self.assertEqual((st, body["error"]["code"]), (422, "INVALID_LIMIT"), bad_limit)

    def test_05_concurrent_threshold_leaves_exactly_one_seal_event(self):
        stations = [f"e{i}" for i in range(4)]
        st, _, bid = API.create(stations, 4)
        barrier = threading.Barrier(len(stations))
        out = []

        def worker(s):
            barrier.wait()
            out.append(API.vote(bid, s, f"digest-{bid}", f"vid-{s}"))

        ts = [threading.Thread(target=worker, args=(s,)) for s in stations]
        for t in ts: t.start()
        for t in ts: t.join()
        events = API.all_events(bid)
        seals = [e for e in events if e["result_code"] == "SEALED"]
        accepts = [e for e in events if e["result_code"] == "VOTE_ACCEPTED"]
        self.assertEqual(len(seals), 1, "并发临界封签只留下一个封签事件")
        self.assertEqual(len(accepts), 4)
        self.assertEqual([e["seq"] for e in events], list(range(1, 6)), "序号连续")
        # 封签事件与第 4 张赞成票同一 revision（同一次提交）。
        self.assertEqual(seals[0]["revision"], accepts[-1]["revision"])
        self.assertTrue(seals[0]["seq"] == accepts[-1]["seq"] + 1)


# ------------------------------------------------------------- C. 并发封签唯一性

def test_concurrent_seal_uniqueness():
    print("\n== 并发封签唯一性 ==")
    stations = [f"c{i}" for i in range(5)]
    st, _, bid = API.create(stations, 3)
    check(st == 201, f"批次 {bid} 创建：5 站、阈值 3")
    digest = f"D-{bid}"

    results = []
    barrier = threading.Barrier(len(stations) * 4)

    def worker(station, seq):
        barrier.wait()
        results.append((station, seq, API.vote(bid, station, digest, f"vid-{station}")))

    threads = []
    for s in stations:
        for seq in range(4):  # 每站 1 票 + 3 次完全相同的并发重传
            t = threading.Thread(target=worker, args=(s, seq))
            threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    accepted = [r for r in results if r[2][0] == 200 and not r[2][1].get("replayed")]
    replayed = [r for r in results if r[2][0] == 200 and r[2][1].get("replayed")]
    late = [r for r in results if r[2][0] == 409]
    check(len(accepted) == 3, f"20 个并发请求中恰好 3 票首次生效（实际 {len(accepted)}）")
    check(len(replayed) >= 1, f"存在并发幂等回放（{len(replayed)} 个）")
    check(len(late) >= 1, f"阈值后到达的票被迟到拒绝（{len(late)} 个 409）")

    fps = set()
    for _, _, (st_, body) in results:
        cert = (body.get("batch") or {}).get("certificate") if isinstance(body, dict) else None
        if cert:
            fps.add(cert["fingerprint"])
    check(len(fps) == 1, f"所有封签响应中的证书指纹唯一（{fps or '无证书！'}）")

    st, b = API.get_batch(bid)
    check(b["status"] == "sealed", "批次最终状态为 sealed")
    check(b["yes_count"] == 3, f"赞成站数锁定在阈值 3（实际 {b['yes_count']}）")
    check(len(b["certificate"]["stations"]) == 3, "证书恰好列出 3 个赞成站")
    fp_final = b["certificate"]["fingerprint"]

    # 并发冲突票与篡改票在封存后冲击：证书绝不改写。
    hammer = []

    def hammer_worker(i):
        if i % 3 == 0:
            hammer.append(API.vote(bid, stations[i % 5], digest, f"vid-changed-{i}"))
        elif i % 3 == 1:
            hammer.append(API.vote(bid, stations[i % 5], f"digest-EVIL-{i}", f"vid-{stations[i % 5]}"))
        else:
            hammer.append(API.vote(bid, "站X-未授权", digest, "vid-x"))

    ths = [threading.Thread(target=hammer_worker, args=(i,)) for i in range(30)]
    for t in ths: t.start()
    for t in ths: t.join()
    check(all(s in (409, 422) for s, _ in hammer), "封存后全部冲击票被 409/422 拒绝")
    st, b = API.get_batch(bid)
    check(b["certificate"]["fingerprint"] == fp_final, "30 个并发冲击后证书指纹不变")

    # 流水视角：封签事件恰好一个；每个新迟到载荷都可追溯且重放回放不增流水。
    evs = API.all_events(bid)
    seal_events = [e for e in evs if e["result_code"] == "SEALED"]
    check(len(seal_events) == 1, f"并发全过程只留下一个封签事件（实际 {len(seal_events)}）")
    late_events = [e for e in evs if e["result_code"] == "LATE_VOTE_REJECTED"]
    check(len(late_events) >= 1, "封存后的迟到票在流水中留痕，可追溯")
    check(all(e["seq"] > seal_events[0]["seq"] for e in late_events),
          "迟到拒绝严格位于封签事件之后，且没有插入第二张证书")
    seqs = [e["seq"] for e in evs]
    check(seqs == list(range(1, len(seqs) + 1)), "流水稳定序号从 1 连续递增、无空洞")
    fp_events = {e["detail"].get("fingerprint") for e in seal_events}
    check(fp_events == {fp_final}, "封签流水锚定的指纹与证书一致")

    # 并发异摘要票在封签前隔离：预置一票冻结正确摘要，再并发投票。
    st2, _, bid2 = API.create([f"x{i}" for i in range(6)], 5)
    digest2 = f"D-{bid2}"
    st, _ = API.vote(bid2, "x0", digest2, "vid-x0")
    check(st == 200, "预置 x0 赞成票冻结正确摘要")
    res2 = []
    jobs = [(f"x{i}", False) for i in range(1, 5)] + [("x5", True)]
    barrier2 = threading.Barrier(len(jobs))

    def worker2(station, bad_digest=False):
        barrier2.wait()
        d = "digest-WRONG" if bad_digest else digest2
        res2.append(API.vote(bid2, station, d, f"vid-{station}"))

    ths = [threading.Thread(target=worker2, args=(*j,)) for j in jobs]
    for t in ths: t.start()
    for t in ths: t.join()
    st, b2 = API.get_batch(bid2)
    check(b2["status"] == "sealed", "5 个有效赞成站并发到达后封签")
    check(b2["certificate"]["stations"] == [f"x{i}" for i in range(5)],
          "证书只含 5 个有效站，异摘要站 x5 被排除")
    check(b2["yes_count"] == 5, "冲突票不计入赞成票")


# ---------------------------------------------------------------- D. 重启恢复

def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _start_server(data_dir, port):
    env = dict(os.environ)
    env.update({
        "DATA_DIR": data_dir,
        "SEAL_PORT": str(port),
        "SEAL_HOST": "127.0.0.1",
        "PYTHONUNBUFFERED": "1",
    })
    proc = subprocess.Popen(
        [sys.executable, APP_PATH],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    api = Http(f"http://127.0.0.1:{port}")
    deadline = time.time() + 30
    while time.time() < deadline:
        if proc.poll() is not None:
            out = proc.stdout.read() if proc.stdout else ""
            raise RuntimeError(f"恢复服务进程提前退出\n{out}")
        try:
            st, body = api.get("/health", timeout=2)
            if st == 200 and body.get("status") == "ok":
                return proc, api
        except Exception:
            time.sleep(0.3)
    proc.kill()
    raise RuntimeError("恢复服务 30s 内未就绪")


def test_restart_recovery():
    print("\n== 重启恢复（以持久记录冷启动新进程）==")
    # 线上服务准备：一个收集中批次（1 票），一个已封存批次（阈值 1）。
    _, _, collecting = API.create(["rA", "rB"], 2)
    st, r = API.vote(collecting, "rA", f"D-{collecting}", "vid-rA")
    check(st == 200 and r["batch"]["yes_count"] == 1, "收集中批次已含 1 张赞成票")

    _, _, sealed = API.create(["sA"], 1)
    st, r = API.vote(sealed, "sA", f"D-{sealed}", "vid-sA")
    check(st == 200 and r["batch"]["certificate"] is not None, "已封存批次持有证书")
    st, live_sealed = API.get_batch(sealed)
    live_cert = live_sealed["certificate"]

    # 收集中批次 + 一张冲突票：验证冲突记录同样持久化、重启后继续隔离。
    _, _, conflicting = API.create(["cA", "cB"], 2)
    st, _ = API.vote(conflicting, "cA", f"D-{conflicting}", "vid-cA")
    st, r = API.vote(conflicting, "cB", "digest-WRONG", "vid-cB-wrong")
    check(st == 422 and r["error"]["code"] == "DIGEST_MISMATCH", "收集中批次含 1 张冲突票")

    src_db = os.path.join(SOURCE_DATA_DIR, "seal.db.json")
    for attempt in range(30):
        if os.path.exists(src_db):
            break
        time.sleep(0.5)
    check(os.path.exists(src_db), f"读到线上持久记录 {src_db}")

    # 留存重启前的流水基线（含一条封存后的迟到拒绝），重启后须逐字段一致。
    st, _ = API.vote(sealed, "sA", f"D-{sealed}", "vid-sA-late-before-restart")
    check(st == 409, "封存批次再投一张新载荷：迟到拒绝留痕")
    live_events = API.all_events(sealed)
    live_event_sigs = [event_sig(e) for e in live_events]
    check(any(e["result_code"] == "LATE_VOTE_REJECTED" for e in live_events),
          "重启前流水含迟到拒绝事件")

    data_dir = os.path.join(RESTART_ROOT, "boot1")
    shutil.rmtree(data_dir, ignore_errors=True)
    os.makedirs(data_dir, exist_ok=True)
    shutil.copy2(src_db, os.path.join(data_dir, "seal.db.json"))
    # 模拟“写入投票途中异常中断”：正式文件之外残留半截临时文件。
    with open(os.path.join(data_dir, "seal.db.json.tmp.crash-9999"), "w") as f:
        f.write('{"version":1, "batches": {"半截内容')

    port = _free_port()
    proc, rapi = _start_server(data_dir, port)
    try:
        leftovers = [n for n in os.listdir(data_dir) if n.startswith("seal.db.json.tmp.")]
        check(not leftovers, "冷启动清理崩溃残留的临时文件，正式记录完好")

        st, b = rapi.get_batch(collecting)
        check(st == 200 and b["status"] == "collecting", "收集中批次恢复为 collecting")
        check(b["yes_count"] == 1 and b["digest"] == f"D-{collecting}",
              "冻结摘要与赞成票从持久记录恢复")

        # 冲突票持久化恢复：重传回放首次冲突，正确票仍可让批次封签。
        st, b = rapi.get_batch(conflicting)
        check(b["yes_count"] == 1 and len(b["conflicts"]) == 1,
              "冲突记录从持久记录恢复（1 赞成 + 1 冲突）")
        st, r = rapi.vote(conflicting, "cB", "digest-WRONG", "vid-cB-wrong")
        check(st == 422 and r["error"]["details"].get("replayed") is True,
              "恢复后冲突票重传仍为回放，不重复堆积")
        st, r = rapi.vote(conflicting, "cB", f"D-{conflicting}", "vid-cB")
        check(st == 200 and r["batch"]["status"] == "sealed"
              and r["batch"]["conflicts"], "冲突不影响正确票到达阈值封签，证书与冲突记录并存")
        conf_fp = r["batch"]["certificate"]["fingerprint"]

        st, b = rapi.get_batch(sealed)
        check(st == 200 and b["status"] == "sealed", "已封存批次恢复后仍为 sealed，不回收集中")
        check(b["certificate"] == live_cert, "恢复出的证书与线上逐字节一致（含指纹与封存时间）")

        # 流水从持久记录恢复：顺序、序号、内容与重启前逐字段一致。
        evs = rapi.all_events(sealed)
        check([event_sig(e) for e in evs] == live_event_sigs,
              "重启后封存流水的顺序、序号、站点、结果码、时刻、revision 逐字段一致")
        st, body = rapi.events(sealed)
        check(body["latest_seq"] == len(live_events) and body["events"][0]["seq"] == body["latest_seq"],
              "流水接口默认返回最新一页且 latest_seq 与重启前一致")

        # 迟到票在恢复后的实例上同样被拒绝。
        st, r = rapi.vote(sealed, "sA", f"D-{sealed}", "vid-late")
        check(st == 409 and r["error"]["code"] == "LATE_VOTE_REJECTED",
              "恢复后迟到票仍被拒绝，不改写证书")
        # 重启前已记录的迟到载荷重传：回放同一 seq，不新增流水。
        st, r = rapi.vote(sealed, "sA", f"D-{sealed}", "vid-sA-late-before-restart")
        check(st == 409 and r["error"]["details"].get("replayed") is True,
              "恢复后历史迟到载荷重传仍回放原拒绝")

        # 收集中批次可继续推进并在恢复实例上封签。
        st, r = rapi.vote(collecting, "rB", f"D-{collecting}", "vid-rB")
        check(st == 200 and r["batch"]["certificate"] is not None, "恢复后收集中批次可继续并封签")
        rc_fp = r["batch"]["certificate"]["fingerprint"]

        # 记录首次恢复实例上的最终流水，供第二次冷启动逐字段比对。
        sealed_sigs_boot1 = [event_sig(e) for e in rapi.all_events(sealed)]
        collecting_sigs_boot1 = [event_sig(e) for e in rapi.all_events(collecting)]
    finally:
        proc.kill()
        proc.wait(timeout=10)

    # 再次冷启动（第二次重启），结果不变。
    with open(os.path.join(data_dir, "seal.db.json.tmp.crash-again"), "w") as f:
        f.write("garbage")
    port = _free_port()
    proc2, rapi2 = _start_server(data_dir, port)
    try:
        leftovers = [n for n in os.listdir(data_dir) if n.startswith("seal.db.json.tmp.")]
        check(not leftovers, "第二次冷启动同样清理残留临时文件")
        st, b = rapi2.get_batch(collecting)
        check(b["status"] == "sealed" and b["certificate"]["fingerprint"] == rc_fp,
              "第二次重启：新封存批次仍 sealed 且指纹不变")
        st, b = rapi2.get_batch(sealed)
        check(b["certificate"] == live_cert, "第二次重启：历史证书依然逐字节一致")
        check([event_sig(e) for e in rapi2.all_events(sealed)] == sealed_sigs_boot1,
              "第二次重启：已封存批次流水与首次恢复后逐字段一致（含迟到拒绝）")
        check([event_sig(e) for e in rapi2.all_events(collecting)] == collecting_sigs_boot1,
              "第二次重启：新封存批次流水顺序与内容保持一致")
        st, r = rapi2.vote(collecting, "rA", f"D-{collecting}", "vid-rA")
        check(st == 200 and r.get("replayed") is True,
              "已统计过的投票重启后重传仍为幂等回放")
        check([event_sig(e) for e in rapi2.all_events(collecting)] == collecting_sigs_boot1,
              "回放投票不产生新流水，序号与内容不变")
    finally:
        proc2.kill()
        proc2.wait(timeout=10)


# ---------------------------------------------------------------------- 主流程

def run_unittest_suite():
    print("\n== 代码测试：幂等重传 / 冲突票隔离 / 参数校验 / 封存流水 ==")
    loader = unittest.TestLoader()
    suite = unittest.TestSuite([
        loader.loadTestsFromTestCase(CodeTests),
        loader.loadTestsFromTestCase(EventTimelineTests),
    ])
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():
        FAILURES.append(f"unittest 失败 {len(result.failures) + len(result.errors)} 项")
    return result.wasSuccessful()


def main():
    t0 = time.time()
    print(f"目标服务：{BASE_URL}")
    if not wait_available():
        print("页面或健康响应不可用，终止验收。")
        return 1

    # 列表接口冒烟。
    st, body = API.get("/api/batches")
    check(st == 200 and isinstance(body.get("batches"), list), "GET /api/batches 返回批次列表")

    run_unittest_suite()
    try:
        test_concurrent_seal_uniqueness()
    except Exception as e:
        FAILURES.append(f"并发封签测试异常: {e!r}")
        print(f"    ❌ 并发测试异常: {e!r}")
    try:
        test_restart_recovery()
    except Exception as e:
        FAILURES.append(f"重启恢复测试异常: {e!r}")
        import traceback
        traceback.print_exc()

    print("\n" + "=" * 64)
    if FAILURES:
        print(f"验收失败：{len(FAILURES)} 项未通过，用时 {time.time() - t0:.1f}s")
        for f in FAILURES:
            print(" -", f)
        return 1
    print(f"✅ 验收全部通过：页面、健康、幂等、冲突隔离、并发唯一封签、重启恢复，"
          f"用时 {time.time() - t0:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
