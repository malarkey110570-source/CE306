import json

QUERIES_FILE = "2507244_queries.json"
RESULTS_FILE = "2507244_results.json"
OUT_FILE = "2507244_results_fixed_v2.json"

def load_json(path: str):
    with open(path, "r", encoding="utf-8-sig") as f:
        return json.loads(f.read())

def save_json(path: str, obj):
    # 用 utf-8（不带 BOM）写出
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

qfile = load_json(QUERIES_FILE)
rfile = load_json(RESULTS_FILE)

# 你的 results 文件可能顶层叫 results 或 queries（你之前替换过）
r_list = rfile.get("queries") or rfile.get("results")
if r_list is None:
    raise KeyError("你的 results 文件顶层既没有 'queries' 也没有 'results'，先打开检查一下结构。")

# 建一个 number -> query 的映射
qmap = {}
for q in qfile["queries"]:
    qmap[q["number"]] = q

# 把缺的字段补进去（最关键是 kibana_query）
for item in r_list:
    n = item["number"]
    if n not in qmap:
        raise KeyError(f"results 里有 number={n}，但 queries 文件里找不到对应条目。")

    src = qmap[n]

    # 必补：评分脚本会读
    if "kibana_query" not in item:
        item["kibana_query"] = src["kibana_query"]

    # 建议也补上这些（有些版本脚本会用到/打印）
    for k in ["original_query", "keyword_query", "answer_type", "exact_answers"]:
        if k in src and k not in item:
            item[k] = src[k]
    if "matches" not in item:
        item["matches"] = src["matches"]
# 顶层信息也补齐（可有可无，但规范）
for k in ["student_surname", "student_givenname", "student_reg_number", "topic_keywords"]:
    if k in qfile and k not in rfile:
        rfile[k] = qfile[k]

# 确保顶层用 queries（评分脚本就是 d["queries"]）
rfile["queries"] = r_list
if "results" in rfile:
    del rfile["results"]

save_json(OUT_FILE, rfile)
print(f"[DONE] Wrote: {OUT_FILE}")