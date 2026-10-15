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

# 한글 글꼴: 클라우드(Linux)는 Noto Sans CJK, macOS는 Apple SD Gothic Neo. 앞에 있는 것을 우선한다.
_fonts = fm.findSystemFonts()
for want in ("NotoSansCJK-Regular", "AppleSDGothicNeo"):
    f = next((f for f in _fonts if want in f), None)
    if f:
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



def chart_c2():
    d = load("metrics_dag_mac_m4")
    hours = [r["hour"] for r in d["per_hour"]]

    def miss(r, stage):
        return next(v["missing_events_excl_injected"] for v in r["versions"] if v["stage"] == stage)
    v1 = [miss(r, "provisional") for r in d["per_hour"]]
    v2 = [miss(r, "final") for r in d["per_hour"]]
    late = [max([x[1] for x in r["late_checks"]] or [0]) for r in d["per_hour"]]
    fig, ax = plt.subplots(figsize=(8, 4.2), dpi=150)
    w = 0.27
    for i, (vals, col, label) in enumerate(((v1, S[0], "잠정 v1 누락 (워터마크 +1분)"),
                                            (v2, S[2], "확정 v2 누락 (워터마크 +15분)"),
                                            (late, S[1], "확정 뒤 늦은 도착 감지 → v3 재확정 (의도적 주입)"))):
        xs = [h + (i - 1) * w for h in hours]
        ax.bar(xs, vals, w, color=col, label=label)
        for x, v in zip(xs, vals):
            if v:
                ax.text(x, v, f"{v}", ha="center", va="bottom", fontsize=7, color=INK2)
    ax.set_xticks(hours, [f"{h:02d}시" for h in hours], fontsize=8)
    ax.set_xlabel("가상 시간 H (day1, 60배속 재생)")
    ax.set_ylabel("건수 (재생 종료 후 원본 집계 대비)")
    ax.set_title("시간별 지표 DAG · 버전별 누락과 확정 후 늦은 도착", loc="left", fontsize=12, color=INK)
    top = max(v1 + v2 + late)
    ax.set_ylim(0, top * 1.8)
    if not any(v2):
        ax.text(0.99, 0.62, "확정 v2 누락: 12개 시간 모두 0건(주입분 제외)", transform=ax.transAxes, ha="right",
                fontsize=8, color=INK2)
    ax.legend(frameon=False, loc="upper right", fontsize=8)
    ax.grid(axis="x", visible=False)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT, "C2-metrics-dag.png"))


def chart_c3():
    d = load("mv_scale_mac_m4")
    ms = [m for m in d["measurements"] if m["state"] == "after_optimize"]
    methods = [("raw", "원본 events 집계"), ("mv", "MV(AggregatingMergeTree)"),
               ("mv_g1", "MV, index_granularity=1"), ("versioned", "hourly_metrics FINAL(미리 계산)")]
    fig, axes = plt.subplots(1, 2, figsize=(9, 4.2), dpi=150, sharey=True)
    w = 0.2
    for ax, (shape, title) in zip(axes, (("hour", "한 시간 조회"), ("series", "범위 전체 시간별 시계열"))):
        for i, ((m, label), col) in enumerate(zip(methods, S)):
            xs, ys = [], []
            for j, days in enumerate((1, 3, 7)):
                r = next(x for x in ms if (x["shape"], x["method"], x["days"]) == (shape, m, days))
                xs.append(j + (i - 1.5) * w)
                ys.append(r["server_ms_median"])
            ax.bar(xs, ys, w, color=col, label=label)
            for x, y in zip(xs, ys):
                ax.text(x, y * 1.08, f"{y:.1f}", ha="center", va="bottom", fontsize=6.5, color=INK2)
        ax.set_yscale("log")
        ax.set_xticks(range(3), ["1일", "3일", "7일"])
        ax.set_title(title, loc="left", fontsize=10, color=INK)
        ax.grid(axis="x", visible=False)
    axes[0].set_ylabel("서버 실행 시간 중앙값 (ms, 로그 축)")
    axes[0].set_xlabel("조회 범위 (한 시간 조회는 그 범위 마지막 날 16시)")
    axes[1].set_xlabel("조회 범위")
    fig.suptitle("MV 조회 비용 재측정 · 7일치 496만 건, OPTIMIZE FINAL 뒤, 웜 캐시", x=0.01, ha="left", fontsize=12,
                 color=INK)
    h, lb = axes[0].get_legend_handles_labels()
    fig.legend(h, lb, frameon=False, fontsize=7.5, loc="lower center", ncol=4)
    fig.tight_layout(rect=(0, 0.06, 1, 1))
    fig.savefig(os.path.join(OUT, "C3-mv-scale.png"))

if __name__ == "__main__":
    # 인자로 그릴 그림을 고른다(예: charts.py c2). 인자가 없으면 A·B·C만 그린다.
    # 이미 커밋한 그림을 다른 환경·글꼴로 다시 그리지 않도록 새 그림은 이름을 지정해서만 그린다.
    import sys
    names = sys.argv[1:] or ["a", "b", "c"]
    for n in names:
        if n == "c" and not os.path.exists(os.path.join(R, "c_late_events.json")):
            continue
        globals()[f"chart_{n}"]()
