"""docs/results/*.json → docs/img/*.png (README·포트폴리오용 정적 그림)."""
import json
import os

import matplotlib
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt

matplotlib.use("Agg")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
R = os.path.join(ROOT, "docs", "results")
OUT = os.path.join(ROOT, "docs", "img")
os.makedirs(OUT, exist_ok=True)

for f in fm.findSystemFonts():
    if "NotoSansCJK-Regular" in f:
        fm.fontManager.addfont(f)
        plt.rcParams["font.family"] = fm.FontProperties(fname=f).get_name()
        break

SURFACE, INK, INK2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
S = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]  # 범주 슬롯 1~4 (고정 순서)
plt.rcParams.update({"figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "axes.edgecolor": GRID,
                     "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2, "text.color": INK,
                     "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8, "axes.spines.top": False,
                     "axes.spines.right": False, "font.size": 10})


def load(name):
    return json.load(open(os.path.join(R, name + ".json")))


def chart_a():
    fig, ax = plt.subplots(figsize=(8, 4.2), dpi=150)
    series = [("a0_baseline_per_message", "1건씩 INSERT (기준선)"), ("a1_scaleout_6x_per_message", "소비자 6개 확장"),
              ("a2c_batch_flush_200ms", "배치 200ms (채택)"), ("a4_kafka_engine_mv", "Kafka 엔진 기본 flush")]
    for (n, label), col, dy in zip(series, S, (7, -7, 0, 0)):
        s = [x for x in load(n)["samples"] if x.get("lag") is not None]
        t, lag = [x["t"] for x in s], [x["lag"] for x in s]
        ax.plot(t, lag, color=col, lw=2, label=label)
        if lag[-1] > 1000:  # 끝까지 남은 lag만 직접 표기
            ax.annotate(f"{lag[-1]:,}", (t[-1], lag[-1]), xytext=(4, dy),
                        textcoords="offset points", va="center", color=INK2, fontsize=8)
    ax.axvline(60, color=INK2, lw=1, ls=(0, (3, 3)))
    ax.text(61, 150_000, "생산 종료", color=INK2, fontsize=8)
    ax.set_xlabel("경과 시간(초) · 초당 5,000건 유입")
    ax.set_ylabel("소비 그룹 lag (건)")
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
    ax.set_title("실험 A · 적재 방식별 lag", loc="left", fontsize=12, color=INK)
    ax.legend(frameon=False, loc="upper left")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "A-lag.png"))


def chart_b():
    rows = [("자동 커밋 · 200ms\n적재 직전 장애만", "failpoint_b0c_autocommit_before_insert_200ms"),
            ("자동 커밋 · 2초\n적재 직전 장애만", "failpoint_b0d_autocommit_before_insert_2s"),
            ("적재 후 커밋", "failpoint_b1_commit_after_insert"),
            ("적재 후 커밋\n+ 중복 제거 토큰(창 1000)", "failpoint_b3b_dedup_token_window"),
            ("적재 후 커밋\n+ ClickHouse 오프셋 재개", "failpoint_b4_resume_from_table")]
    fig, ax = plt.subplots(figsize=(8, 4.2), dpi=150)
    y = range(len(rows))
    lost = [load(n)["lost"] for _, n in rows]
    dup = [load(n)["duplicates"] for _, n in rows]
    h = 0.36
    ax.barh([i - h / 2 - 0.01 for i in y], lost, h, color=S[1], label="유실")
    ax.barh([i + h / 2 + 0.01 for i in y], dup, h, color=S[0], label="중복")
    for i in y:
        ax.text(lost[i], i - h / 2, f" {lost[i]:,}", va="center", fontsize=8, color=INK2)
        ax.text(dup[i], i + h / 2, f" {dup[i]:,}", va="center", fontsize=8, color=INK2)
    ax.set_yticks(list(y), [r[0] for r in rows], fontsize=8)
    ax.invert_yaxis()
    ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
    ax.set_xlabel("건수 (정답 event_id 709,950건과 대조)")
    ax.set_title("실험 B · 장애 지점 주입 후 유실·중복", loc="left", fontsize=12, color=INK)
    ax.legend(frameon=False, loc="lower right")
    ax.grid(axis="y", visible=False)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "B-loss-dup.png"))


def chart_c():
    d = load("c_late_events")["summary"]
    delays = sorted({int(k.split("+")[1][:-1]) for k in d if k.startswith("raw@")})
    miss = [d[f"raw@+{x}m"]["missing_events_total"] for x in delays]
    fig, ax = plt.subplots(figsize=(8, 4.2), dpi=150)
    ax.plot(delays, miss, color=S[0], lw=2, marker="o", ms=5)
    for x, m in zip(delays, miss):
        ax.annotate(f"{m:,}", (x, m), xytext=(0, 7), textcoords="offset points", ha="center", fontsize=8, color=INK2)
    ax.set_xlabel("시간 마감 후 집계까지 기다린 시간 (가상 분)")
    ax.set_ylabel("12개 시간 합계 누락 이벤트 (건)")
    ax.set_xscale("symlog", linthresh=1)
    ax.set_xticks(delays, [str(x) for x in delays])
    ax.set_title("실험 C · 허용 지연과 집계 누락", loc="left", fontsize=12, color=INK)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "C-lateness.png"))


if __name__ == "__main__":
    chart_a()
    chart_b()
    if os.path.exists(os.path.join(R, "c_late_events.json")):
        chart_c()
