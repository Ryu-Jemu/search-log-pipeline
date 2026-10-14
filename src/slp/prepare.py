"""ORCAS 원본(orcas.tsv.gz)을 재생기가 읽는 parquet 두 개로 바꾼다.

- queries.parquet : qid, query(원문 그대로), n_docs, docs(list<int32>)
- docs.parquet    : doc_id(int32), url

원문 쿼리는 정규화하지 않는다. 공백·대소문자·빈 쿼리 처리는 파이프라인의
정제 단계가 맡고, 그 동작을 실제 원본 데이터로 시험하기 위해서다.
같은 (qid, did) 중복 행만 여기서 제거한다(원본 270행).
"""
import argparse
import json
import pathlib

import duckdb

SRC_COLUMNS = "{'qid':'BIGINT','query':'VARCHAR','did':'VARCHAR','url':'VARCHAR'}"


def prepare(src: pathlib.Path, out: pathlib.Path) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    # quote/escape 비활성: 쿼리에 따옴표가 그대로 들어 있다. CRLF 줄끝은 DuckDB가 제거한다.
    con.execute(
        f"""create temp table raw as select * from read_csv('{src}', delim='\t', header=false,
            quote='', escape='', columns={SRC_COLUMNS})"""
    )
    con.execute(
        """create temp table pairs as
           select distinct qid, coalesce(query, '') as query,
                  cast(substr(did, 2) as integer) as doc_id, url
           from raw"""
    )
    con.execute(
        f"""copy (select qid, any_value(query) as query, count(*)::integer as n_docs,
                         list(doc_id order by doc_id) as docs
                  from pairs group by qid order by qid)
            to '{out / "queries.parquet"}' (format parquet)"""
    )
    con.execute(
        f"""copy (select distinct doc_id, url from pairs order by doc_id)
            to '{out / "docs.parquet"}' (format parquet)"""
    )
    stats = {
        "raw_rows": con.execute("select count(*) from raw").fetchone()[0],
        "dedup_pairs": con.execute("select count(*) from pairs").fetchone()[0],
        "queries": con.execute("select count(distinct qid) from pairs").fetchone()[0],
        "docs": con.execute("select count(distinct doc_id) from pairs").fetchone()[0],
        "empty_queries": con.execute("select count(distinct qid) from pairs where trim(query) = ''").fetchone()[0],
    }
    (out / "prepare_stats.json").write_text(json.dumps(stats, indent=2))
    return stats


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="data/orcas.tsv.gz", type=pathlib.Path)
    ap.add_argument("--out", default="data/prepared", type=pathlib.Path)
    a = ap.parse_args()
    print(json.dumps(prepare(a.src, a.out), indent=2))
