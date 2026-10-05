"""候补申请端到端测试（真实 PostgreSQL + TestClient）。

覆盖：
- 受理三分支：全部可承接直接成交 / 仅额度不足入队 / 缺少匹配授权逐项拒绝；
- 队列处理：撤销、预留取消/到期、改期、改挂释放额度后按受理顺序重算队首，
  额度不足停止本轮，授权不再匹配标记失败并继续后项；
- 新直接申请先处理可成交候补、不抢其额度；
- 查询（状态/失败原因/生成的申请编号）、待处理候补可取消、
  已转申请沿用现有撤销规则；
- 并发释放与受理：不重复生成申请、不超额、账实相符。

每个场景使用独立 channel，隔离各通道候补队列互不影响。

运行：
    DATABASE_URL=postgresql+psycopg://licensing@/licensing?host=/tmp&port=5439 \
        python3 -m tests.test_waitlist
"""
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select
from sqlalchemy import update as sa_update

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+psycopg://licensing@/licensing?host=/tmp&port=5439",
)

from fastapi.testclient import TestClient  # noqa: E402

from app.database import Base, SessionLocal, engine, init_db  # noqa: E402
from app.main import app  # noqa: E402
from app import models  # noqa: E402

CST = timezone(timedelta(hours=8))
NOW = datetime(2026, 10, 5, 12, 0, tzinfo=CST)

passed = 0
failed = 0


def check(name, cond, detail=""):
    global passed, failed
    if cond:
        passed += 1
        print(f"  PASS  {name}")
    else:
        failed += 1
        print(f"  FAIL  {name}  {detail}")


def make_material(c: TestClient, code: str) -> int:
    r = c.post("/api/v1/materials", json={"code": code, "name": code})
    assert r.status_code == 201, r.text
    return r.json()["id"]


def make_auth(
    c: TestClient,
    mid: int,
    max_count: int,
    *,
    channel: str = "web",
    region: str = "CN",
    starts: datetime | None = None,
    ends: datetime | None = None,
) -> int:
    r = c.post(
        "/api/v1/authorizations",
        json={
            "material_id": mid,
            "region": region,
            "channel": channel,
            "starts_at": (starts or NOW - timedelta(days=1)).isoformat(),
            "ends_at": (ends or NOW + timedelta(days=1)).isoformat(),
            "max_count": max_count,
        },
    )
    assert r.status_code == 201, r.text
    return r.json()["id"]


def auth_view(c: TestClient, aid: int) -> dict:
    return c.get(f"/api/v1/authorizations/{aid}").json()


def submit_waitlist(c: TestClient, mids, *, channel: str, occur_at=None):
    r = c.post(
        "/api/v1/waitlists",
        json={
            "material_ids": mids,
            "region": "CN",
            "channel": channel,
            "occur_at": (occur_at or NOW).isoformat(),
        },
    )
    return r.status_code, r.json()


def submit_distribution(c: TestClient, mids, *, channel: str, occur_at=None):
    r = c.post(
        "/api/v1/distributions",
        json={
            "material_ids": mids,
            "region": "CN",
            "channel": channel,
            "occur_at": (occur_at or NOW).isoformat(),
        },
    )
    return r


def waitlist_view(c: TestClient, wid: int) -> dict:
    return c.get(f"/api/v1/waitlists/{wid}").json()


def force_expire_reservation(reservation_id: int) -> None:
    db = SessionLocal()
    try:
        db.execute(
            sa_update(models.Reservation)
            .where(models.Reservation.id == reservation_id)
            .values(expires_at=func.now())
        )
        db.commit()
    finally:
        db.close()


def main() -> int:
    global failed
    Base.metadata.drop_all(bind=engine)
    init_db()
    c = TestClient(app)

    # ---------------------------------------------------------- 受理分支
    print("== 受理：全部可承接 → 直接生成已通过申请 ==")
    ch = "ch-intake"
    m1 = make_material(c, "w-m1")
    a1 = make_auth(c, m1, 2, channel=ch)
    code, body = submit_waitlist(c, [m1], channel=ch)
    check("直接成交 201", code == 201, body)
    check("状态 fulfilled", body["status"] == "fulfilled", body)
    check("生成申请编号", isinstance(body["distribution_id"], int), body)
    check("内嵌申请 approved", body["distribution"]["status"] == "approved", body)
    check(
        "申请命中授权",
        body["distribution"]["items"][0]["authorization"]["id"] == a1,
        body,
    )
    check("fulfilled_at 已记录", body["fulfilled_at"] is not None, body)
    check("失败原因为空", body["failure_reason"] is None, body)
    check("素材清单完整", body["items"][0]["material_id"] == m1, body)
    dist1 = body["distribution_id"]
    check("授权占用 1", auth_view(c, a1)["used_count"] == 1)
    r = c.get(f"/api/v1/distributions/{dist1}")
    check("生成的申请可经既有接口查询", r.status_code == 200, r.text)

    print("== 受理：每项均有匹配授权但额度不足 → 入候补队列 ==")
    m2 = make_material(c, "w-m2")
    a2 = make_auth(c, m2, 1, channel=ch)
    r = submit_distribution(c, [m2], channel=ch)
    assert r.status_code == 201, r.text
    code, body = submit_waitlist(c, [m2], channel=ch)
    check("入队 201", code == 201, body)
    check("状态 pending", body["status"] == "pending", body)
    check("未生成申请", body["distribution_id"] is None, body)
    check("内嵌申请为空", body["distribution"] is None, body)
    w2 = body["id"]
    check("入队不占额度", auth_view(c, a2)["used_count"] == 1)

    print("== 受理：缺少匹配授权 → 逐项拒绝、不入队 ==")
    m3 = make_material(c, "w-m3")  # 无授权
    before = len(c.get("/api/v1/waitlists", params={"status": "pending"}).json())
    code, body = submit_waitlist(c, [m3], channel=ch)
    check("无匹配 422", code == 422, body)
    check(
        "错误码 waitlist_rejected",
        body["error"]["code"] == "waitlist_rejected",
        body,
    )
    check(
        "逐项原因 no_matching_authorization",
        body["error"]["details"][0]["reason"] == "no_matching_authorization",
        body,
    )
    after = len(c.get("/api/v1/waitlists", params={"status": "pending"}).json())
    check("拒绝不入队", before == after, f"{before}->{after}")

    m4 = make_material(c, "w-m4")
    a4 = make_auth(c, m4, 1, channel=ch)
    c.patch(f"/api/v1/authorizations/{a4}", json={"status": "inactive"})
    code, body = submit_waitlist(c, [m4], channel=ch)
    check("匹配授权均停用 422", code == 422, body)
    check(
        "逐项原因 authorization_inactive",
        body["error"]["details"][0]["reason"] == "authorization_inactive",
        body,
    )

    print("== 受理：混合（一项缺匹配 + 一项额度不足）→ 整体拒绝 ==")
    code, body = submit_waitlist(c, [m3, m2], channel=ch)
    check("混合 422", code == 422, body)
    reasons = {d["item_index"]: d["reason"] for d in body["error"]["details"]}
    check(
        "逐项列明两种原因",
        reasons == {0: "no_matching_authorization", 1: "quota_exhausted"},
        body,
    )

    print("== 受理参数校验 ==")
    code, body = submit_waitlist(c, [m2, m2], channel=ch)
    check(
        "重复素材 422",
        code == 422 and body["error"]["code"] == "duplicate_material_items",
        body,
    )
    code, body = submit_waitlist(c, [999999], channel=ch)
    check(
        "未知素材 422",
        code == 422 and body["error"]["code"] == "unknown_material",
        body,
    )
    r = c.post(
        "/api/v1/waitlists",
        json={
            "material_ids": [m2],
            "region": "CN",
            "channel": ch,
            "occur_at": "2026-10-05T12:00:00",
        },
    )
    check("发行时刻缺时区 422", r.status_code == 422, r.text)
    r = c.post(
        "/api/v1/waitlists",
        json={
            "material_ids": [],
            "region": "CN",
            "channel": ch,
            "occur_at": NOW.isoformat(),
        },
    )
    check("空素材清单 422", r.status_code == 422, r.text)

    print("== 成交沿用重叠授权选择顺序（到期早优先、其次编号小） ==")
    ch = "ch-order"
    m5 = make_material(c, "w-m5")
    a5_late = make_auth(c, m5, 1, channel=ch, ends=NOW + timedelta(days=3))
    a5_early = make_auth(c, m5, 1, channel=ch)  # 到期更早
    code, body = submit_waitlist(c, [m5], channel=ch)
    check(
        "命中到期更早的授权",
        code == 201
        and body["distribution"]["items"][0]["authorization"]["id"] == a5_early,
        body,
    )
    code, body = submit_waitlist(c, [m5], channel=ch)
    check(
        "次条命中剩余授权",
        code == 201
        and body["distribution"]["items"][0]["authorization"]["id"] == a5_late,
        body,
    )
    code, body = submit_waitlist(c, [m5], channel=ch)
    check("额度耗尽后入队", code == 201 and body["status"] == "pending", body)

    # -------------------------------------------------------- 队列处理
    print("== 撤销释放额度 → 队首成交 ==")
    ch = "ch-revoke"
    m6 = make_material(c, "w-m6")
    a6 = make_auth(c, m6, 1, channel=ch)
    d6 = submit_distribution(c, [m6], channel=ch).json()["id"]
    _, body = submit_waitlist(c, [m6], channel=ch)
    w6 = body["id"]
    check("候补 pending", body["status"] == "pending", body)
    r = c.post(f"/api/v1/distributions/{d6}/revoke")
    check("撤销 200", r.status_code == 200, r.text)
    r = c.get(f"/api/v1/waitlists/{w6}")
    check("队首已成交", r.json()["status"] == "fulfilled", r.text)
    check("成交生成新申请", r.json()["distribution_id"] != d6, r.text)
    check("授权占用回到 1", auth_view(c, a6)["used_count"] == 1)

    print("== 按受理顺序成交（FIFO） ==")
    ch = "ch-fifo"
    m7 = make_material(c, "w-m7")
    a7 = make_auth(c, m7, 1, channel=ch)
    d7 = submit_distribution(c, [m7], channel=ch).json()["id"]
    _, body = submit_waitlist(c, [m7], channel=ch)
    w7a = body["id"]
    _, body = submit_waitlist(c, [m7], channel=ch)
    w7b = body["id"]
    c.post(f"/api/v1/distributions/{d7}/revoke")
    sa = waitlist_view(c, w7a)["status"]
    sb = waitlist_view(c, w7b)["status"]
    check("先受理者先成交", sa == "fulfilled" and sb == "pending", f"{sa}/{sb}")
    check("授权占用仍为 1", auth_view(c, a7)["used_count"] == 1)

    print("== 队首额度不足 → 停止本轮（后续条目不越序） ==")
    ch = "ch-block"
    m8a = make_material(c, "w-m8a")
    m8b = make_material(c, "w-m8b")
    m8c = make_material(c, "w-m8c")
    a8a = make_auth(c, m8a, 1, channel=ch)
    a8b = make_auth(c, m8b, 1, channel=ch)
    a8c = make_auth(c, m8c, 1, channel=ch)
    d8a = submit_distribution(c, [m8a], channel=ch).json()["id"]
    d8b = submit_distribution(c, [m8b], channel=ch).json()["id"]
    _, body = submit_waitlist(c, [m8a, m8b], channel=ch)  # a8a 满 → 入队
    w8_head = body["id"]
    _, body = submit_waitlist(c, [m8b], channel=ch)  # a8b 满 → 入队
    w8_second = body["id"]
    check("次条也入队", body["status"] == "pending", body)
    # 新直接申请（无关授权 a8c）触发一轮处理：队首不可承接 → 停止本轮
    r = submit_distribution(c, [m8c], channel=ch)
    check("无关直接申请仍可成交", r.status_code == 201, r.text)
    check("队首仍 pending", waitlist_view(c, w8_head)["status"] == "pending")
    check(
        "次条仍 pending（停止本轮）",
        waitlist_view(c, w8_second)["status"] == "pending",
    )
    # 释放次条所需额度：队首仍缺 a8a → 停止本轮，次条不得越序占额
    c.post(f"/api/v1/distributions/{d8b}/revoke")
    check(
        "释放 a8b 后队首仍 pending",
        waitlist_view(c, w8_head)["status"] == "pending",
    )
    check(
        "次条不越序（仍 pending）",
        waitlist_view(c, w8_second)["status"] == "pending",
    )
    check("a8b 额度未被越序占用", auth_view(c, a8b)["used_count"] == 0)
    # 释放队首所缺额度：队首整体可承接 → 成交；次条因 a8b 被队首占用仍候补
    c.post(f"/api/v1/distributions/{d8a}/revoke")
    check(
        "队首成交",
        waitlist_view(c, w8_head)["status"] == "fulfilled",
    )
    check(
        "次条仍 pending（a8b 被队首承接）",
        waitlist_view(c, w8_second)["status"] == "pending",
    )
    check("a8a 占用 1", auth_view(c, a8a)["used_count"] == 1)
    check("a8b 占用 1", auth_view(c, a8b)["used_count"] == 1)

    print("== 授权已不再匹配 → 标记失败并继续后项 ==")
    ch = "ch-fail"
    m9 = make_material(c, "w-m9")
    m10 = make_material(c, "w-m10")
    a9 = make_auth(c, m9, 1, channel=ch)
    a10 = make_auth(c, m10, 1, channel=ch)
    d9 = submit_distribution(c, [m9], channel=ch).json()["id"]
    d10 = submit_distribution(c, [m10], channel=ch).json()["id"]
    _, body = submit_waitlist(c, [m9], channel=ch)
    w9 = body["id"]
    _, body = submit_waitlist(c, [m10], channel=ch)
    w10 = body["id"]
    c.patch(f"/api/v1/authorizations/{a9}", json={"status": "inactive"})
    c.post(f"/api/v1/distributions/{d10}/revoke")  # 释放 a10 → 触发重算
    r = c.get(f"/api/v1/waitlists/{w9}")
    check("队首标记失败", r.json()["status"] == "failed", r.text)
    check(
        "失败原因码",
        r.json()["failure_reason"] == "authorization_unavailable",
        r.text,
    )
    check(
        "失败逐项明细",
        r.json()["failure_details"][0]["reason"] == "authorization_inactive",
        r.text,
    )
    check("failed_at 已记录", r.json()["failed_at"] is not None, r.text)
    check(
        "后项继续成交",
        waitlist_view(c, w10)["status"] == "fulfilled",
    )
    check("a10 占用 1", auth_view(c, a10)["used_count"] == 1)

    print("== 预留取消释放额度 → 队首成交 ==")
    ch = "ch-resv-cancel"
    m11 = make_material(c, "w-m11")
    a11 = make_auth(c, m11, 1, channel=ch)
    r = c.post(
        "/api/v1/reservations",
        json={
            "material_ids": [m11],
            "region": "CN",
            "channel": ch,
            "occur_at": NOW.isoformat(),
            "ttl_minutes": 30,
        },
    )
    resv11 = r.json()["id"]
    _, body = submit_waitlist(c, [m11], channel=ch)
    w11 = body["id"]
    check("预留占额后候补入队", body["status"] == "pending", body)
    c.post(f"/api/v1/reservations/{resv11}/cancel")
    r = c.get(f"/api/v1/waitlists/{w11}")
    check("取消预留后队首成交", r.json()["status"] == "fulfilled", r.text)
    av = auth_view(c, a11)
    check(
        "占用 1 / 预留 0",
        av["used_count"] == 1 and av["reserved_count"] == 0,
        av,
    )

    print("== 预留到期释放额度 → 队首成交（惰性结算触发） ==")
    ch = "ch-resv-expire"
    m12 = make_material(c, "w-m12")
    a12 = make_auth(c, m12, 1, channel=ch)
    r = c.post(
        "/api/v1/reservations",
        json={
            "material_ids": [m12],
            "region": "CN",
            "channel": ch,
            "occur_at": NOW.isoformat(),
            "ttl_minutes": 30,
        },
    )
    resv12 = r.json()["id"]
    _, body = submit_waitlist(c, [m12], channel=ch)
    w12 = body["id"]
    force_expire_reservation(resv12)
    r = c.get(f"/api/v1/waitlists/{w12}")  # 查询即触发到期结算 + 重算
    check("到期释放后队首成交", r.json()["status"] == "fulfilled", r.text)
    check(
        "预留已过期",
        c.get(f"/api/v1/reservations/{resv12}").json()["status"] == "expired",
    )

    print("== 改期释放原授权额度 → 队首成交 ==")
    ch = "ch-rsch"
    m13 = make_material(c, "w-m13")
    t2 = NOW + timedelta(hours=12)
    a13_old = make_auth(
        c, m13, 1, channel=ch,
        starts=NOW - timedelta(days=1), ends=NOW + timedelta(hours=6),
    )
    a13_new = make_auth(
        c, m13, 1, channel=ch,
        starts=NOW + timedelta(hours=6), ends=NOW + timedelta(days=2),
    )
    d13 = submit_distribution(c, [m13], channel=ch).json()["id"]
    _, body = submit_waitlist(c, [m13], channel=ch)
    w13 = body["id"]
    check("改期前候补 pending", body["status"] == "pending", body)
    r = c.post(
        f"/api/v1/distributions/{d13}/reschedule",
        json={"occur_at": t2.isoformat()},
    )
    check("改期 200", r.status_code == 200, r.text)
    r = c.get(f"/api/v1/waitlists/{w13}")
    check("改期释放后队首成交", r.json()["status"] == "fulfilled", r.text)
    check("原授权由候补承接", auth_view(c, a13_old)["used_count"] == 1)
    check("新授权承接改期申请", auth_view(c, a13_new)["used_count"] == 1)

    print("== 改挂：替代授权可承接 → 成交 ==")
    ch = "ch-mig-ok"
    m14 = make_material(c, "w-m14")
    a14 = make_auth(c, m14, 1, channel=ch)
    a14_rep = make_auth(c, m14, 5, channel=ch)
    d14 = submit_distribution(c, [m14], channel=ch).json()["id"]
    _, body = submit_waitlist(c, [m14], channel=ch)
    w14 = body["id"]
    r = c.post(
        "/api/v1/authorizations/migrate",
        json={
            "source_authorization_id": a14,
            "replacement_authorization_id": a14_rep,
        },
    )
    check("改挂 200", r.status_code == 200, r.text)
    r = c.get(f"/api/v1/waitlists/{w14}")
    check("改挂后队首经替代授权成交", r.json()["status"] == "fulfilled", r.text)
    check(
        "成交命中替代授权",
        r.json()["distribution"]["items"][0]["authorization"]["id"] == a14_rep,
        r.text,
    )
    check("替代授权占用 2", auth_view(c, a14_rep)["used_count"] == 2)

    print("== 改挂：授权不再匹配 → 失败 ==")
    ch = "ch-mig-fail"
    m15 = make_material(c, "w-m15")
    t15b = NOW + timedelta(hours=12)
    a15 = make_auth(c, m15, 1, channel=ch)  # 覆盖 NOW 与 NOW+12h
    d15 = submit_distribution(c, [m15], channel=ch).json()["id"]  # 占满 a15
    _, body = submit_waitlist(c, [m15], channel=ch, occur_at=t15b)
    w15 = body["id"]
    check("候补入队（a15 满）", body["status"] == "pending", body)
    # 替代授权在候补入队后登记：仅覆盖 NOW（d15 的时刻），不覆盖 t15b。
    a15_rep = make_auth(
        c, m15, 1, channel=ch,
        starts=NOW - timedelta(days=1), ends=NOW + timedelta(hours=6),
    )
    r = c.post(
        "/api/v1/authorizations/migrate",
        json={
            "source_authorization_id": a15,
            "replacement_authorization_id": a15_rep,
        },
    )
    check("改挂 200（替代授权覆盖既有申请）", r.status_code == 200, r.text)
    r = c.get(f"/api/v1/waitlists/{w15}")
    check(
        "改挂后授权不再匹配 → 失败",
        r.json()["status"] == "failed"
        and r.json()["failure_reason"] == "authorization_unavailable",
        r.text,
    )

    print("== 新直接申请须先处理可成交候补，不得抢走其额度 ==")
    ch = "ch-direct-a"
    m16 = make_material(c, "w-m16")
    a16 = make_auth(c, m16, 1, channel=ch)
    d16 = submit_distribution(c, [m16], channel=ch).json()["id"]
    _, body = submit_waitlist(c, [m16], channel=ch)
    w16 = body["id"]
    c.post(f"/api/v1/distributions/{d16}/revoke")  # 释放 → w16 成交
    check(
        "释放后候补成交",
        waitlist_view(c, w16)["status"] == "fulfilled",
    )
    r = submit_distribution(c, [m16], channel=ch)
    check("直接申请不抢候补额度 422", r.status_code == 422, r.text)
    check(
        "原因 quota_exhausted",
        r.json()["error"]["details"][0]["reason"] == "quota_exhausted",
        r.text,
    )

    print("== 新直接申请受理时先成交候补（新授权容量让给队首） ==")
    ch = "ch-direct-b"
    m17 = make_material(c, "w-m17")
    a17 = make_auth(c, m17, 1, channel=ch)
    d17 = submit_distribution(c, [m17], channel=ch).json()["id"]
    _, body = submit_waitlist(c, [m17], channel=ch)
    w17 = body["id"]
    a17b = make_auth(c, m17, 1, channel=ch)  # 登记新授权不触发重算
    check(
        "登记新授权后候补仍 pending",
        waitlist_view(c, w17)["status"] == "pending",
    )
    r = submit_distribution(c, [m17], channel=ch)
    check("直接申请让位候补 422", r.status_code == 422, r.text)
    r = c.get(f"/api/v1/waitlists/{w17}")
    check("候补经新授权成交", r.json()["status"] == "fulfilled", r.text)
    check(
        "候补命中新授权",
        r.json()["distribution"]["items"][0]["authorization"]["id"] == a17b,
        r.text,
    )
    check("原授权占用不变", auth_view(c, a17)["used_count"] == 1)

    # -------------------------------------------------------- 查询与取消
    print("== 查询：单条 / 列表过滤 / 非法过滤 / 404 ==")
    r = c.get("/api/v1/waitlists/999999")
    check("未知候补 404", r.status_code == 404, r.text)
    r = c.get("/api/v1/waitlists", params={"status": "fulfilled"})
    check(
        "列表过滤 fulfilled",
        r.status_code == 200
        and len(r.json()) >= 1
        and all(w["status"] == "fulfilled" for w in r.json()),
        r.text,
    )
    r = c.get("/api/v1/waitlists", params={"status": "pending"})
    check(
        "列表过滤 pending",
        r.status_code == 200
        and all(w["status"] == "pending" for w in r.json()),
        r.text,
    )
    r = c.get("/api/v1/waitlists", params={"status": "bogus"})
    check(
        "非法过滤 422",
        r.status_code == 422
        and r.json()["error"]["code"] == "invalid_waitlist_status",
        r.text,
    )
    r = c.get(f"/api/v1/waitlists/{w2}")
    check(
        "单条返回状态/失败原因/申请编号字段",
        r.status_code == 200
        and r.json()["status"] == "pending"
        and r.json()["distribution_id"] is None
        and r.json()["failure_reason"] is None,
        r.text,
    )

    print("== 取消：仅待处理候补可取消 ==")
    r = c.post(f"/api/v1/waitlists/{w2}/cancel")
    check("取消 pending 200", r.status_code == 200, r.text)
    check("状态 cancelled", r.json()["status"] == "cancelled", r.text)
    check("cancelled_at 已记录", r.json()["cancelled_at"] is not None, r.text)
    r = c.post(f"/api/v1/waitlists/{w2}/cancel")
    check(
        "重复取消 409",
        r.status_code == 409
        and r.json()["error"]["code"] == "waitlist_already_cancelled",
        r.text,
    )
    r = c.post(f"/api/v1/waitlists/{w6}/cancel")
    check(
        "已成交不可取消 409",
        r.status_code == 409
        and r.json()["error"]["code"] == "waitlist_already_fulfilled",
        r.text,
    )
    r = c.post(f"/api/v1/waitlists/{w9}/cancel")
    check(
        "已失败不可取消 409",
        r.status_code == 409
        and r.json()["error"]["code"] == "waitlist_already_failed",
        r.text,
    )
    r = c.post("/api/v1/waitlists/999999/cancel")
    check("取消未知候补 404", r.status_code == 404, r.text)

    print("== 取消队首后后续候补立即重算 ==")
    ch = "ch-advance"
    m18 = make_material(c, "w-m18")
    m19 = make_material(c, "w-m19")
    a18 = make_auth(c, m18, 1, channel=ch)
    a19 = make_auth(c, m19, 1, channel=ch)
    d18 = submit_distribution(c, [m18], channel=ch).json()["id"]
    d19 = submit_distribution(c, [m19], channel=ch).json()["id"]
    _, body = submit_waitlist(c, [m18], channel=ch)
    w18 = body["id"]
    _, body = submit_waitlist(c, [m19], channel=ch)
    w19 = body["id"]
    c.post(f"/api/v1/distributions/{d19}/revoke")  # 释放 a19，但队首 w18 阻塞
    check(
        "队首阻塞时次条仍 pending",
        waitlist_view(c, w19)["status"] == "pending",
    )
    c.post(f"/api/v1/waitlists/{w18}/cancel")  # 取消队首 → 次条立即重算
    check(
        "取消队首后次条成交",
        waitlist_view(c, w19)["status"] == "fulfilled",
    )
    check("a19 占用 1", auth_view(c, a19)["used_count"] == 1)

    print("== 已转申请沿用现有撤销规则（撤销释放 → 下一候补成交） ==")
    ch = "ch-revoke-rule"
    m20 = make_material(c, "w-m20")
    a20 = make_auth(c, m20, 1, channel=ch)
    _, body = submit_waitlist(c, [m20], channel=ch)
    w20a = body["id"]
    d20 = body["distribution_id"]
    _, body = submit_waitlist(c, [m20], channel=ch)
    w20b = body["id"]
    check("第二候补入队", body["status"] == "pending", body)
    r = c.post(f"/api/v1/distributions/{d20}/revoke")
    check("撤销成交申请 200", r.status_code == 200, r.text)
    r = c.post(f"/api/v1/distributions/{d20}/revoke")
    check(
        "重复撤销 409 already_revoked",
        r.status_code == 409
        and r.json()["error"]["code"] == "already_revoked",
        r.text,
    )
    check(
        "撤销释放后下一候补成交",
        waitlist_view(c, w20b)["status"] == "fulfilled",
    )
    check("授权占用仍为 1", auth_view(c, a20)["used_count"] == 1)

    print("== 候补生成的申请可改期（沿用既有规则） ==")
    w20b_dist = waitlist_view(c, w20b)["distribution_id"]
    r = c.post(
        f"/api/v1/distributions/{w20b_dist}/reschedule",
        json={"occur_at": NOW.isoformat()},  # 同一瞬间幂等
    )
    check("成交申请改期幂等 200", r.status_code == 200, r.text)

    # ---------------------------------------------------------- 并发
    print("== 并发：并发受理不超额、不重复成交 ==")
    ch = "ch-conc1"
    mc = make_material(c, "w-conc1")
    ac = make_auth(c, mc, 3, channel=ch)

    def submit_one(i: int):
        cc = TestClient(app)
        r = cc.post(
            "/api/v1/waitlists",
            json={
                "material_ids": [mc],
                "region": "CN",
                "channel": ch,
                "occur_at": NOW.isoformat(),
            },
        )
        return r.status_code, r.json()

    results = []
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = [pool.submit(submit_one, i) for i in range(10)]
        for f in as_completed(futures):
            results.append(f.result())
    fulfilled = [
        b for code, b in results if code == 201 and b["status"] == "fulfilled"
    ]
    pending = [
        b for code, b in results if code == 201 and b["status"] == "pending"
    ]
    check("并发受理：3 成交", len(fulfilled) == 3, f"{len(fulfilled)}")
    check("并发受理：7 入队", len(pending) == 7, f"{len(pending)}")
    dist_ids = {b["distribution_id"] for b in fulfilled}
    check("成交申请编号不重复", len(dist_ids) == 3, dist_ids)
    check("并发受理后占用 3", auth_view(c, ac)["used_count"] == 3)

    print("== 并发：并发释放与受理不重复生成、不超额 ==")
    ch = "ch-conc2"
    md = make_material(c, "w-conc2")
    ad = make_auth(c, md, 1, channel=ch)
    dd = submit_distribution(c, [md], channel=ch).json()["id"]
    wait_ids = []
    for _ in range(5):
        _, body = submit_waitlist(c, [md], channel=ch)
        assert body["status"] == "pending", body
        wait_ids.append(body["id"])

    def revoke_one():
        cc = TestClient(app)
        return cc.post(f"/api/v1/distributions/{dd}/revoke").status_code

    def direct_one(i: int):
        cc = TestClient(app)
        return cc.post(
            "/api/v1/distributions",
            json={
                "material_ids": [md],
                "region": "CN",
                "channel": ch,
                "occur_at": NOW.isoformat(),
            },
        ).status_code

    def waitlist_one(i: int):
        cc = TestClient(app)
        r = cc.post(
            "/api/v1/waitlists",
            json={
                "material_ids": [md],
                "region": "CN",
                "channel": ch,
                "occur_at": NOW.isoformat(),
            },
        )
        return r.status_code, r.json()["status"]

    with ThreadPoolExecutor(max_workers=11) as pool:
        f_revoke = pool.submit(revoke_one)
        f_directs = [pool.submit(direct_one, i) for i in range(5)]
        f_waits = [pool.submit(waitlist_one, i) for i in range(5)]
        revoke_code = f_revoke.result()
        direct_codes = [f.result() for f in f_directs]
        wait_results = [f.result() for f in f_waits]
    check("撤销成功", revoke_code == 200, revoke_code)
    check(
        "直接申请全部被拒（不抢候补额度）",
        all(code == 422 for code in direct_codes),
        direct_codes,
    )
    check(
        "新候补全部入队",
        all(code == 201 and st == "pending" for code, st in wait_results),
        wait_results,
    )
    r = c.get("/api/v1/waitlists", params={"status": "fulfilled"})
    fulfilled_md = [
        w
        for w in r.json()
        if w["distribution"]
        and any(it["material_id"] == md for it in w["distribution"]["items"])
    ]
    check("仅队首成交一次", len(fulfilled_md) == 1, fulfilled_md)
    check("成交者即最先受理者", fulfilled_md[0]["id"] == wait_ids[0], fulfilled_md)
    check("释放后占用回到 1", auth_view(c, ad)["used_count"] == 1)

    print("== 并发：多触发源并发重算同一队列，账实相符 ==")
    ch = "ch-conc3"
    me = make_material(c, "w-conc3")
    ae = make_auth(c, me, 2, channel=ch)
    d_ids = [
        submit_distribution(c, [me], channel=ch).json()["id"] for _ in range(2)
    ]
    w_ids = []
    for _ in range(4):
        _, body = submit_waitlist(c, [me], channel=ch)
        assert body["status"] == "pending", body
        w_ids.append(body["id"])

    def trigger_revoke(did: int):
        cc = TestClient(app)
        return cc.post(f"/api/v1/distributions/{did}/revoke").status_code

    def trigger_get(wid: int):
        cc = TestClient(app)
        return cc.get(f"/api/v1/waitlists/{wid}").status_code

    with ThreadPoolExecutor(max_workers=8) as pool:
        f1 = pool.submit(trigger_revoke, d_ids[0])
        f2 = pool.submit(trigger_revoke, d_ids[1])
        f_gets = [pool.submit(trigger_get, wid) for wid in w_ids * 2]
        revoke_codes = [f1.result(), f2.result()]
        get_codes = [f.result() for f in f_gets]
    check("两个撤销均 200", revoke_codes == [200, 200], revoke_codes)
    check("并发查询均 200", all(code == 200 for code in get_codes), get_codes)
    statuses = [waitlist_view(c, wid)["status"] for wid in w_ids]
    check(
        "释放两槽 → 前两条成交、后两条仍候补",
        statuses == ["fulfilled", "fulfilled", "pending", "pending"],
        statuses,
    )
    check("占用回到 2", auth_view(c, ae)["used_count"] == 2)

    # 账实相符：used_count == 该授权 approved 明细数；成交候补与申请一一对应。
    db = SessionLocal()
    try:
        for aid in (ac, ad, ae):
            used = db.scalar(
                select(models.Authorization.used_count).where(
                    models.Authorization.id == aid
                )
            )
            approved_items = db.scalar(
                select(func.count())
                .select_from(models.DistributionItem)
                .join(
                    models.Distribution,
                    models.DistributionItem.distribution_id
                    == models.Distribution.id,
                )
                .where(
                    models.DistributionItem.authorization_id == aid,
                    models.Distribution.status == "approved",
                )
            )
            check(
                f"授权 {aid} 账实相符",
                used == approved_items,
                f"used={used} items={approved_items}",
            )
        fulfilled_cnt = db.scalar(
            select(func.count())
            .select_from(models.WaitlistRequest)
            .where(models.WaitlistRequest.status == "fulfilled")
        )
        dist_cnt = db.scalar(
            select(
                func.count(func.distinct(models.WaitlistRequest.distribution_id))
            ).where(models.WaitlistRequest.status == "fulfilled")
        )
        check(
            "成交候补与申请一一对应（无重复生成）",
            fulfilled_cnt == dist_cnt,
            f"{fulfilled_cnt}/{dist_cnt}",
        )
    finally:
        db.close()

    print(f"\n结果：{passed} 通过，{failed} 失败")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
