"""측정 환경 스냅샷: 호스트·Docker 할당량·이미지 아키텍처·패키지 버전·ClickHouse 기본 설정·시계 차이.

클라우드 작업 공간 값(env_and_defaults.json)과 섞지 않도록 다른 이름으로 저장한다.
Docker Desktop(macOS)에서는 컨테이너가 VM 시계를, 호스트 프로세스가 호스트 시계를 쓴다.
kafka_ts(생산자 시각)와 inserted_at(ClickHouse 시각)을 빼는 지표에 두 시계 차이가 섞이므로 함께 기록한다.
"""
import argparse
import json
import platform
import statistics
import subprocess
import time
from importlib import metadata

from common import ROOT, save

from slp.infra import ch

PACKAGES = ["duckdb", "numpy", "pyarrow", "pytest", "confluent-kafka", "clickhouse-connect", "matplotlib",
            "prometheus-client"]


def sh(*cmd: str) -> str:
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=30, cwd=ROOT).stdout.strip()
    except (OSError, subprocess.TimeoutExpired):
        return ""


def host() -> dict:
    out = {"platform": platform.platform(), "machine": platform.machine()}
    if platform.system() == "Darwin":
        out["macos"] = sh("sw_vers", "-productVersion")
        out["cpu"] = sh("sysctl", "-n", "machdep.cpu.brand_string")
        out["ncpu"] = int(sh("sysctl", "-n", "hw.ncpu") or 0)
        out["mem_bytes"] = int(sh("sysctl", "-n", "hw.memsize") or 0)
    return out


def docker() -> dict:
    info = json.loads(sh("docker", "info", "--format", "{{json .}}") or "{}")
    images = {}
    for line in sh("docker", "compose", "ps", "--format", "{{.Service}} {{.Image}}").splitlines():
        svc, image = line.split(" ", 1)
        arch = sh("docker", "image", "inspect", "-f", "{{.Architecture}}", image)
        digest = sh("docker", "image", "inspect", "-f", "{{index .RepoDigests 0}}", image)
        images[svc] = {"image": image, "arch": arch, "digest": digest}
    return {"server_version": info.get("ServerVersion"), "os": info.get("OperatingSystem"),
            "kernel": info.get("KernelVersion"), "ncpu": info.get("NCPU"), "mem_total_bytes": info.get("MemTotal"),
            "compose": sh("docker", "compose", "version", "--short"), "running_images": images}


def clickhouse() -> dict:
    c = ch()
    mt = dict(c.query("""select name, value from system.merge_tree_settings where name in
        ('old_parts_lifetime', 'non_replicated_deduplication_window', 'parts_to_delay_insert',
         'parts_to_throw_insert')""").result_rows)
    st = dict(c.query("""select name, value from system.settings where name in
        ('max_insert_block_size', 'kafka_max_wait_ms', 'stream_flush_interval_ms',
         'async_insert_use_adaptive_busy_timeout', 'async_insert_busy_timeout_max_ms',
         'async_insert_busy_timeout_ms')""").result_rows)
    # 시계 차이 = ClickHouse now64() − 호스트 시각(요청 왕복의 중간 시점). 20회 측정 중앙값.
    samples = []
    for _ in range(20):
        t0 = time.time()
        server = c.query("select toUnixTimestamp64Milli(now64(3))").result_rows[0][0] / 1000
        t1 = time.time()
        samples.append((server - (t0 + t1) / 2, t1 - t0))
    offs = [s[0] * 1000 for s in samples]
    return {"version": c.query("select version()").result_rows[0][0], "merge_tree_settings": mt, "settings": st,
            "clock_offset_ms": {"median": round(statistics.median(offs), 2), "min": round(min(offs), 2),
                                "max": round(max(offs), 2),
                                "rtt_ms_median": round(statistics.median(s[1] * 1000 for s in samples), 2),
                                "note": "ClickHouse now64() − 호스트 time.time() 왕복 중간값. now64 해상도 1ms"}}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", default="env_mac_m4")
    ap.add_argument("--note", default="")
    a = ap.parse_args()
    pk = {}
    for p in PACKAGES:
        try:
            pk[p] = metadata.version(p)
        except metadata.PackageNotFoundError:
            pk[p] = None
    res = {"taken_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "host": host(),
           "python": platform.python_version(), "packages": pk, "docker": docker(), "clickhouse": clickhouse(),
           "git_head": sh("git", "rev-parse", "--short", "HEAD"), "note": a.note}
    print(json.dumps(res, ensure_ascii=False, indent=1))
    print(save(a.name, res))
