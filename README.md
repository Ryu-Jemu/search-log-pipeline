# search-log-pipeline

공개 검색 클릭 로그(MS MARCO ORCAS)를 실시간 이벤트로 재생해 수집 → 정제 → 저장 → 지표 → 어뷰징 탐지까지 잇는 로그 파이프라인 연습 프로젝트입니다.

> 진행 중입니다. 단계별 기록은 `docs/`에 있습니다.

- [1단계 · 원천 데이터와 재생기](docs/01-data-and-replay.md)

## 실행 (1단계)
```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
scripts/download_orcas.sh                      # 약 330MB, 크기·해시 확인
PYTHONPATH=src .venv/bin/python -m slp.prepare  # parquet 생성
PYTHONPATH=src .venv/bin/python -m slp.generator --hours 1 > data/events.jsonl
.venv/bin/pytest -q
```
