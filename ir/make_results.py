import json
import os
import sys
from typing import Any, Dict, List
import requests

# =======================
# 你只需要改这两个（一般不用改）
# =======================
ES_URL = "http://localhost:9200"     # 如果你的 Elasticsearch 不在本机，就改这里
INDEX = "student_index"              # 你的索引名：student_index

# 输入/输出文件（默认当前目录）
INPUT_QUERIES_FILE = "2507244_queries.json"
OUTPUT_RESULTS_FILE = "2507244_results.json"

TOPK = 40  # 必须前40（你说的 G5 自动检查就是这个）


def es_search(query_body: Dict[str, Any], size: int = TOPK) -> List[str]:
    """
    把 query_body 发给 Elasticsearch，返回 hits 里的 _id 列表（docid）。
    """
    # 如果 query_body 里没写 size，我们强行补上 size=40
    body = dict(query_body)
    body["size"] = size

    url = f"{ES_URL}/{INDEX}/_search"
    r = requests.get(url, json=body, timeout=30)

    # 如果 ES 返回 400/401/403/500，这里会直接告诉你错误内容
    if not r.ok:
        raise RuntimeError(f"ES request failed: {r.status_code}\n{r.text}")

    data = r.json()
    hits = data.get("hits", {}).get("hits", [])
    return [h.get("_id") for h in hits if "_id" in h]


def build_keyword_query(keyword_query: str) -> Dict[str, Any]:
    """
    把 keyword_query（字符串）包装成一个 ES 查询（multi_match best_fields）
    """
    return {
        "query": {
            "multi_match": {
                "query": keyword_query,
                "fields": ["title", "parsedParagraphs"],
                "type": "best_fields"
            }
        }
    }


def main():
    # 1) 读取 queries.json
    if not os.path.exists(INPUT_QUERIES_FILE):
        print(f"[ERROR] Cannot find {INPUT_QUERIES_FILE} in current folder.")
        print("你现在的目录里没有这个文件。请先 cd 到 M:\\ir 或把文件放同一目录。")
        sys.exit(1)

    with open(INPUT_QUERIES_FILE, "r", encoding="utf-8") as f:
        qdata = json.load(f)

    # 2) 准备输出结构（results.json）
    results: Dict[str, Any] = {
        "student_surname": qdata.get("student_surname", ""),
        "student_givenname": qdata.get("student_givenname", ""),
        "student_reg_number": qdata.get("student_reg_number", ""),
        "topic_keywords": qdata.get("topic_keywords", ""),
        "results": []
    }

    queries = qdata.get("queries", [])
    print(f"Loaded queries: {len(queries)}")
    print(f"ES_URL={ES_URL}, INDEX={INDEX}, TOPK={TOPK}")
    print("-" * 60)

    # 3) 每一题：跑 keyword_query + 跑 kibana_query
    for q in queries:
        number = q.get("number")
        original_query = q.get("original_query", "")
        keyword_query = q.get("keyword_query", "")
        kibana_query = q.get("kibana_query", None)

        if not number:
            print("[WARN] Found a query without 'number', skipped.")
            continue

        # --- keyword_query 跑出来的前40 docid（G5 会查这个）
        if not keyword_query:
            keyword_docids = []
        else:
            keyword_body = build_keyword_query(keyword_query)
            keyword_docids = es_search(keyword_body, size=TOPK)

        # --- kibana_query 跑出来的前40 docid（G7 你要提升 precision 就看这个）
        if not kibana_query:
            kibana_docids = []
        else:
            # kibana_query 本身通常长这样： {"query": {...}} 或者 {"query":{"bool":...}}
            # 我们直接丢给 ES，让 ES 跑
            kibana_docids = es_search(kibana_query, size=TOPK)

        # 写进 results.json
        results["results"].append({
            "number": number,
            "original_query": original_query,
            "keyword_query": keyword_query,
            "keyword_top40_docids": keyword_docids,
            "kibana_top40_docids": kibana_docids
        })

        print(f"Q{number:02d} OK | keyword_top40={len(keyword_docids)} | kibana_top40={len(kibana_docids)}")

    # 4) 保存输出文件
    with open(OUTPUT_RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print("-" * 60)
    print(f"[DONE] Wrote: {OUTPUT_RESULTS_FILE}")


if __name__ == "__main__":
    main()