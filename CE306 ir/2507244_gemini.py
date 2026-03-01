import argparse
import json
import os
import re
from typing import Any, Dict, List, Tuple

# Gemini SDK: pip install google-genai
from google import genai


def load_gold(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def pick_queries(gold: Dict[str, Any], pick_nums: List[int]) -> List[Dict[str, Any]]:
    qmap = {q["number"]: q for q in gold.get("queries", [])}
    picked = []
    for n in pick_nums:
        if n not in qmap:
            raise ValueError(f"Query number {n} not found in gold standard.")
        picked.append(qmap[n])
    return picked


def normalize_answer_type(t: str) -> str:
    t = (t or "").strip().upper()
    # keep as-is if already one of allowed labels
    allowed = {"PERSON", "FAC", "ORG", "GPE", "LOC", "EVENT", "DATE", "MONEY"}
    if t in allowed:
        return t
    return t  # fallback


def build_prompt(answer_type: str, exact_answers: List[str], supporting_text: str) -> str:
    # Keep it simple + force JSON output
    return f"""You are an information extraction assistant.

Task:
1) Extract all named entities of type {answer_type} from the text.
2) Return ONLY valid JSON (no markdown).
3) JSON schema:
{{
  "entities": ["..."], 
  "matches_exact_answers": true/false,
  "matched_answers": ["..."]
}}

Rules:
- "entities" must be unique strings in order of appearance.
- "matched_answers" should include items from exact_answers that appear in the text (case-insensitive substring match is OK).
- matches_exact_answers is true iff matched_answers is non-empty.

exact_answers: {json.dumps(exact_answers, ensure_ascii=False)}

TEXT:
{supporting_text}
"""


def safe_parse_json(s: str) -> Dict[str, Any]:
    # try direct
    try:
        return json.loads(s)
    except Exception:
        pass

    # try to extract first {...} block
    m = re.search(r"\{.*\}", s, flags=re.S)
    if not m:
        raise ValueError("Gemini response is not JSON and no JSON object found.")
    return json.loads(m.group(0))


def collect_supporting_text(query_obj: Dict[str, Any], max_chars: int = 6000) -> str:
    # Gold standard has: matches: [ {docid, sentences:[...]} ... ]
    matches = query_obj.get("matches", [])
    parts = []
    for m in matches:
        docid = m.get("docid", "")
        sents = m.get("sentences", [])
        if not sents:
            continue
        parts.append(f"[DocID {docid}] " + " ".join(sents))
    text = "\n\n".join(parts).strip()
    if len(text) > max_chars:
        text = text[:max_chars] + "\n...[truncated]..."
    return text


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gold", required=True, help="Path to your *_queries.json (gold standard)")
    ap.add_argument("--pick", nargs="+", type=int, required=True, help="Pick 5 query numbers, e.g. --pick 3 7 10 14 19")
    ap.add_argument("--model", default="models/gemini-2.5-flash", help="Gemini model name")
    args = ap.parse_args()

    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
    if not api_key:
        raise RuntimeError(
            "Missing API key. Set environment variable GEMINI_API_KEY (recommended) "
            "or GOOGLE_API_KEY before running."
        )

    client = genai.Client(api_key=api_key)

    gold = load_gold(args.gold)
    picked = pick_queries(gold, args.pick)

    print("=== Gemini NE Check ===")
    print(f"Gold: {args.gold}")
    print(f"Model: {args.model}")
    print(f"Picked queries: {args.pick}")
    print()

    for q in picked:
        qnum = q.get("number")
        original = q.get("original_query", "")
        answer_type = normalize_answer_type(q.get("answer_type", ""))
        exact_answers = q.get("exact_answers", [])
        supporting_text = collect_supporting_text(q)

        print("------------------------------------------------------------")
        print(f"Q{qnum}: {original}")
        print(f"answer_type: {answer_type}")
        print(f"exact_answers: {exact_answers}")
        print(f"supporting_text_chars: {len(supporting_text)}")

        if not supporting_text:
            print("No supporting sentences found in gold standard for this query (matches[] empty). Skipping.")
            continue

        prompt = build_prompt(answer_type, exact_answers, supporting_text)

        resp = client.models.generate_content(
            model=args.model,
            contents=prompt,
        )

        text = resp.text or ""
        try:
            data = safe_parse_json(text)
        except Exception as e:
            print("Gemini returned unparsable output.")
            print("RAW OUTPUT:")
            print(text)
            print("ERROR:", e)
            continue

        entities = data.get("entities", [])
        matched_answers = data.get("matched_answers", [])
        matches_exact = bool(data.get("matches_exact_answers", False))

        print("Gemini entities:", entities)
        print("Matched answers:", matched_answers)
        print("matches_exact_answers:", matches_exact)

        # simple automatic judgement: do we see at least one exact answer matched?
        auto_correct = len(matched_answers) > 0
        print("Auto-judgement (has matched answer):", auto_correct)

    print("\nDone.")


if __name__ == "__main__":
    main()