"""ORCAS 쿼리·클릭 쌍을 검색 로그 이벤트 스트림으로 재생한다.

ORCAS에는 시각·세션·사용자·빈도가 없다. 아래 값은 모두 이 재생기가 만든 가정이다.
- 쿼리 인기도: 쿼리별 클릭 문서 수(n_docs)에 비례해 뽑는다(근사).
- 시각: 가상 시계. 하루 주기(04시 최저, 16시 최고)로 세션 시작 빈도를 바꾼다.
- 세션: 검색 횟수는 기하분포, 검색 뒤 클릭 확률 p_click, 클릭 문서는 그 쿼리의 문서 중 균등.

어뷰징은 정답 라벨과 함께 주입한다. 라벨은 이벤트에 넣지 않고 labels로만 내보낸다.
- query_flood       : 봇 1명이 같은 쿼리를 수 초 간격으로 반복(클릭 없음)
- click_spam        : 봇 1명이 같은 쿼리를 검색하고 같은 문서를 반복 클릭
- distributed_flood : 여러 계정이 같은 쿼리를 각자 몇 번씩만 검색(계정 단위 규칙으로는 잡기 어려움)

출력은 (key, value_bytes) 이다. key는 user_id(같은 사용자의 순서를 한 파티션에서 보존).
"""
from __future__ import annotations

import dataclasses
import heapq
import json
import math
import uuid
from datetime import datetime, timedelta, timezone
from typing import Iterator

import numpy as np
import pyarrow.parquet as pq

DAY = 86400.0


@dataclasses.dataclass
class Config:
    seed: int = 42
    start: str = "2026-01-05T00:00:00+00:00"  # 가상 시계 시작(실제 날짜 아님)
    sim_seconds: float = DAY
    sessions_per_day: int = 200_000
    n_users: int = 100_000
    p_click: float = 0.6
    mean_searches: float = 2.2
    late_fraction: float = 0.002      # 늦게 도착하는 이벤트 비율
    late_max_s: float = 600.0
    bad_fraction: float = 0.0005      # 형식 오류 이벤트 비율(DLQ 시험용)
    query_floods: int = 3             # 하루당 캠페인 수
    click_spams: int = 3
    distributed_floods: int = 2


@dataclasses.dataclass
class Corpus:
    qid: np.ndarray        # int64
    query: object          # pyarrow StringArray (파이썬 문자열 1천만 개를 만들지 않기 위해)
    offsets: np.ndarray    # docs list offsets (len = n + 1)
    docs: np.ndarray       # int32 doc_id
    cum_weight: np.ndarray # 인기도 누적합

    @classmethod
    def load(cls, path) -> "Corpus":
        t = pq.read_table(path, columns=["qid", "query", "n_docs", "docs"])
        docs = t.column("docs").combine_chunks()
        n_docs = t.column("n_docs").to_numpy().astype(np.float64)
        return cls(
            qid=t.column("qid").to_numpy(),
            query=t.column("query").combine_chunks(),
            offsets=docs.offsets.to_numpy(),
            docs=docs.values.to_numpy(),
            cum_weight=np.cumsum(n_docs),
        )

    def sample(self, rng: np.random.Generator, k: int) -> np.ndarray:
        return np.searchsorted(self.cum_weight, rng.random(k) * self.cum_weight[-1], side="right")

    def text(self, i: int) -> str:
        return self.query[i].as_py()

    def doc_of(self, rng: np.random.Generator, i: int) -> int:
        lo, hi = self.offsets[i], self.offsets[i + 1]
        return int(self.docs[lo + rng.integers(hi - lo)])


def diurnal(t: float) -> float:
    """세션 시작 빈도 배수(0.2~1.0). 04시 최저, 16시 최고."""
    h = (t % DAY) / 3600.0
    return 0.2 + 0.8 * (1 - math.cos(2 * math.pi * (h - 4) / 24)) / 2


class Generator:
    def __init__(self, corpus: Corpus, cfg: Config):
        self.c, self.cfg = corpus, cfg
        self.rng = np.random.default_rng(cfg.seed)
        self.t0 = datetime.fromisoformat(cfg.start)
        self.labels: list[dict] = []
        self.stats = {"search": 0, "click": 0, "bad": 0, "late": 0, "abuse_events": 0}
        self._next_user = cfg.n_users  # 봇 계정은 정상 범위 밖 번호를 쓰지만 형식은 같다
        # 사용자 활동량 편차: 순위^-0.8 가중
        w = 1.0 / np.arange(1, cfg.n_users + 1) ** 0.8
        self._user_cum = np.cumsum(w)

    # ---------- 공통 ----------
    def _uid(self, n: int) -> str:
        return f"u{n:07d}"

    def _new_bot(self) -> str:
        self._next_user += 1
        return self._uid(self._next_user)

    def _event(self, etype: str, t: float, user: str, session: str, i: int, doc: int | None) -> dict:
        e = {
            "event_id": str(uuid.UUID(bytes=self.rng.bytes(16))),
            "event_type": etype,
            "event_time": (self.t0 + timedelta(seconds=t)).isoformat(timespec="milliseconds"),
            "user_id": user,
            "session_id": session,
            "qid": int(self.c.qid[i]),
            "query": self.c.text(i),
        }
        if doc is not None:
            e["doc_id"] = doc
        return e

    # ---------- 정상 세션 ----------
    def _sessions(self) -> Iterator[tuple[float, dict]]:
        cfg, rng = self.cfg, self.rng
        lam_max = cfg.sessions_per_day / DAY / 0.6  # diurnal 평균이 0.6이므로 하루 총량을 맞춘다
        t = 0.0
        p_geo = 1.0 / cfg.mean_searches
        while True:
            t += rng.exponential(1.0 / lam_max)
            if t >= cfg.sim_seconds:
                return
            if rng.random() > diurnal(t):
                continue
            uid = int(np.searchsorted(self._user_cum, rng.random() * self._user_cum[-1], side="right"))
            user, session = self._uid(uid), f"s{rng.integers(0, 2**40):010x}"
            ts = t
            for i in self.c.sample(rng, int(rng.geometric(p_geo))):
                yield ts, self._event("search", ts, user, session, int(i), None)
                if rng.random() < cfg.p_click:
                    tc = ts + rng.exponential(8.0)
                    yield tc, self._event("click", tc, user, session, int(i), self.c.doc_of(rng, int(i)))
                ts += 2.0 + rng.exponential(20.0)

    # ---------- 어뷰징 ----------
    def _target(self) -> int:
        # 상위 인기 쿼리가 아니라 중간 인기 쿼리를 노린다(순위 조작 시나리오).
        while True:
            i = int(self.rng.integers(len(self.c.qid)))
            if self.c.text(i).strip() and 2 <= self.c.offsets[i + 1] - self.c.offsets[i] <= 20:
                return i

    def _campaign(self, kind: str) -> list[tuple[float, dict]]:
        rng, cfg = self.rng, self.cfg
        dur = float(rng.uniform(1200, 2400))
        start = float(rng.uniform(0, max(1.0, cfg.sim_seconds - dur)))
        i = self._target()
        out: list[tuple[float, dict]] = []
        users: list[str] = []
        doc = None
        if kind in ("query_flood", "click_spam"):
            user = self._new_bot()
            users = [user]
            session = f"s{rng.integers(0, 2**40):010x}"
            doc = self.c.doc_of(rng, i) if kind == "click_spam" else None
            t = start
            while t < start + dur:
                out.append((t, self._event("search", t, user, session, i, None)))
                if kind == "click_spam":
                    tc = t + rng.uniform(0.5, 2.0)
                    out.append((tc, self._event("click", tc, user, session, i, doc)))
                    t += rng.uniform(2.0, 6.0)
                else:
                    t += rng.uniform(1.0, 4.0)
        else:  # distributed_flood
            for _ in range(int(rng.integers(30, 61))):
                user = self._new_bot()
                users.append(user)
                session = f"s{rng.integers(0, 2**40):010x}"
                for t in np.sort(rng.uniform(start, start + dur, int(rng.integers(3, 9)))):
                    out.append((float(t), self._event("search", float(t), user, session, i, None)))
        self.labels.append({
            "campaign_id": len(self.labels) + 1, "type": kind, "user_ids": users,
            "qid": int(self.c.qid[i]), "query": self.c.text(i), "doc_id": doc,
            "start": (self.t0 + timedelta(seconds=start)).isoformat(timespec="seconds"),
            "end": (self.t0 + timedelta(seconds=start + dur)).isoformat(timespec="seconds"),
            "n_events": len(out),
        })
        self.stats["abuse_events"] += len(out)
        return sorted(out, key=lambda x: x[0])

    # ---------- 출력 ----------
    def _bad(self, e: dict) -> bytes:
        kind = self.rng.integers(3)
        if kind == 0:
            return json.dumps(e).encode()[:-7]          # 잘린 JSON
        if kind == 1:
            e = dict(e); e.pop("user_id"); return json.dumps(e).encode()  # 필수 필드 누락
        e = dict(e); e["event_time"] = "not-a-time"; return json.dumps(e).encode()  # 형식 오류

    def events(self) -> Iterator[tuple[str, bytes]]:
        """가상 시각 순서로 내보낸다. 단 late_fraction 만큼은 늦게 도착시킨다."""
        cfg, rng = self.cfg, self.rng
        days = cfg.sim_seconds / DAY
        campaigns = (["query_flood"] * round(cfg.query_floods * days)
                     + ["click_spam"] * round(cfg.click_spams * days)
                     + ["distributed_flood"] * round(cfg.distributed_floods * days))
        streams = [self._campaign(k) for k in campaigns]
        # 정상 세션 스트림은 세션 안에서만 시각이 정렬돼 있어 작은 버퍼로 다시 정렬한다.
        merged = heapq.merge(self._reorder(self._sessions()), *streams, key=lambda x: x[0])
        delayed: list[tuple[float, int, dict]] = []  # (도착 시각, 순번, 이벤트)
        seq = 0
        for t, e in merged:
            while delayed and delayed[0][0] <= t:
                yield self._emit(heapq.heappop(delayed)[2])
            if rng.random() < cfg.late_fraction:
                self.stats["late"] += 1
                seq += 1
                heapq.heappush(delayed, (t + rng.uniform(60, cfg.late_max_s), seq, e))
                continue
            yield self._emit(e)
        while delayed:
            yield self._emit(heapq.heappop(delayed)[2])

    def _reorder(self, it: Iterator[tuple[float, dict]], horizon: float = 900.0):
        # 세션 길이는 대부분 수 분 이내다. horizon보다 오래 걸린 이벤트는 순서가 어긋난 채 나갈 수 있다(실제 로그와 같음).
        buf: list[tuple[float, int, dict]] = []
        n = 0
        for t, e in it:
            n += 1
            heapq.heappush(buf, (t, n, e))
            while buf and buf[0][0] < t - horizon:
                bt, _, be = heapq.heappop(buf)
                yield bt, be
        while buf:
            bt, _, be = heapq.heappop(buf)
            yield bt, be

    def _emit(self, e: dict) -> tuple[str, bytes]:
        self.stats[e["event_type"]] += 1
        if self.rng.random() < self.cfg.bad_fraction:
            self.stats["bad"] += 1
            return e["user_id"], self._bad(e)
        return e["user_id"], json.dumps(e, ensure_ascii=False).encode()


def main():
    import argparse, sys, pathlib
    ap = argparse.ArgumentParser(description="ORCAS 검색 로그 재생기 (JSONL을 stdout으로)")
    ap.add_argument("--corpus", default="data/prepared/queries.parquet")
    ap.add_argument("--labels", default="data/labels.json")
    ap.add_argument("--hours", type=float, default=24)
    ap.add_argument("--sessions-per-day", type=int, default=Config.sessions_per_day)
    ap.add_argument("--seed", type=int, default=Config.seed)
    a = ap.parse_args()
    cfg = Config(seed=a.seed, sim_seconds=a.hours * 3600, sessions_per_day=a.sessions_per_day)
    g = Generator(Corpus.load(a.corpus), cfg)
    out = sys.stdout.buffer
    for _, v in g.events():
        out.write(v + b"\n")
    pathlib.Path(a.labels).write_text(json.dumps({"config": dataclasses.asdict(cfg), "stats": g.stats,
                                                   "campaigns": g.labels}, ensure_ascii=False, indent=1))
    print(json.dumps(g.stats), file=sys.stderr)


if __name__ == "__main__":
    main()
