#!/usr/bin/env bash
# ORCAS 클릭 로그를 내려받고 크기·해시를 확인한다.
# 데이터는 비상업적 연구 목적으로만 쓸 수 있어 저장소에 포함하지 않는다.
# https://microsoft.github.io/msmarco/ORCAS.html
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p data
URL=https://msmarco.z22.web.core.windows.net/msmarcoranking/orcas.tsv.gz
SIZE=329653181                                                        # 공식 페이지 표기
SHA=b0345e7b61e410b0727a1f58ec5bfcded6f97fb8f1d59ead7b2e74b0fb3f30ba  # 2026-10-15 내려받은 파일 기준
[ -f data/orcas.tsv.gz ] || curl -fL -o data/orcas.tsv.gz "$URL"
[ "$(wc -c < data/orcas.tsv.gz | tr -d ' ')" = "$SIZE" ] || { echo "크기 불일치"; exit 1; }
if command -v sha256sum >/dev/null; then h=$(sha256sum data/orcas.tsv.gz); else h=$(shasum -a 256 data/orcas.tsv.gz); fi
[ "${h%% *}" = "$SHA" ] || { echo "해시 불일치"; exit 1; }
echo "ok: data/orcas.tsv.gz"
