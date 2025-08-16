#!/usr/bin/env python3
"""
Daily report builder (recap + today's targets/fades).
Robust header parsing + logging + fallbacks so we don't silently default tiers/scores.
"""
from __future__ import annotations
import argparse, csv, json, os, sys
from pathlib import Path
from typing import Dict, List, Any, Tuple

__VERSION__ = "v2025-08-16a"

# --------------------------
# Normalization helpers
# --------------------------
def _norm_key(k): return (k or "").strip().lower()
def _norm_val(v): return (v or "").strip()

# --------------------------
# File readers
# --------------------------
def read_csv_rows(path: str) -> List[Dict[str, Any]]:
    if not path or not os.path.exists(path):
        print(f"[builder] WARN: CSV not found: {path}", file=sys.stderr)
        return []
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        rdr.fieldnames = [_norm_key(h) for h in rdr.fieldnames]
        for r in rdr:
            rows.append({_norm_key(k): _norm_val(v) for k, v in r.items()})
    return rows

def read_json(path: str) -> Dict[str, Any]:
    if not path or not os.path.exists(path):
        print(f"[builder] WARN: JSON not found: {path}", file=sys.stderr)
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

# --------------------------
# Converters
# --------------------------
def to_float(x, default=None) -> float | None:
    try:
        return float(x)
    except Exception:
        return default

def booly(x) -> bool:
    s = str(x or "").strip().lower()
    return s in ("1","true","t","yes","y")

def pick(r: Dict[str, Any], *keys: str):
    for k in keys:
        v = r.get(k)
        if v not in (None, ""):
            return v
    return None

# --------------------------
# Ranked lookup
# --------------------------
def build_ranked_lookup(ranked_rows: List[Dict[str, Any]]) -> Tuple[Dict[str,Any],Dict[str,Any]]:
    by_id, by_name = {}, {}
    for r in ranked_rows or []:
        rid = str(r.get("player_id") or r.get("id") or "").strip()
        nm  = (r.get("name") or r.get("player_name") or "").strip().lower()
        if rid:
            by_id[rid] = r
        if nm:
            by_name[nm] = r
    return by_id, by_name

# --------------------------
# Score extraction
# --------------------------
def extract_any_score(row: Dict[str, Any]) -> float | None:
    # common aliases
    s = to_float(pick(row,
                      "raw_score","score","final_score","composite_score",
                      "model_score","proj_score","pred_score","fantasy_score"), None)
    if s is not None:
        return s
    # any column containing 'score'
    for k, v in row.items():
        if "score" in (k or ""):
            fv = to_float(v, None)
            if fv is not None:
                return fv
    return None

# --------------------------
# Build today's lists
# --------------------------
def today_lists(tiers_hit, tiers_sp, structured, ht_min, sp_min, hf_max, spf_max,
                ranked_by_id, ranked_by_name):
    sp_ids = set()
    if isinstance(structured, dict):
        for v in structured.values():
            pid = str(v.get("player_id") or "")
            if not pid:
                continue
            if booly(v.get("is_probable_starter")) or booly(v.get("starter")) or booly(v.get("probable")):
                sp_ids.add(pid)

    def split(rows, pitcher=False):
        targets, fades = [], []
        for r in rows:
            tier  = to_float(pick(r, "tier", "tier_score"))
            pid   = str(pick(r, "player_id", "id") or "")
            name  = (pick(r, "name", "player_name") or "").strip()
            name_key = name.lower()

            if pitcher:
                is_sp_today = booly(r.get("starting_pitcher_today"))
                if not (is_sp_today or (pid and pid in sp_ids)):
                    continue
            if tier is None:
                continue

            # --- SCORE EXTRACTION w/ fallback ---
            score = extract_any_score(r)
            if score is None:
                rr = ranked_by_id.get(pid) if pid else None
                if rr is None and name_key:
                    rr = ranked_by_name.get(name_key)
                if rr is not None:
                    score = extract_any_score(rr)
            if score is None:
                for _k,_v in r.items():
                    if "score" in (_k or "") and to_float(_v) is not None:
                        score = float(_v); break
            if score is None or score == 0.0:
                score = float(tier)
            # -----------------------------------

            entry = {
                "player_id": pid,
                "name": name,
                "team": r.get("team"),
                "opp": pick(r, "opponent_team", "opp"),
                "pos": pick(r, "position", "pos"),
                "tier": float(tier),
                "score": float(score),
            }
            if pitcher:
                if sp_min is not None and tier >= sp_min:
                    targets.append(entry)
                if spf_max is not None and tier <= spf_max:
                    fades.append(entry)
            else:
                if ht_min is not None and tier >= ht_min:
                    targets.append(entry)
                if hf_max is not None and tier <= hf_max:
                    fades.append(entry)

        targets.sort(key=lambda x: (-x["tier"], x.get("name") or ""))
        fades.sort(key=lambda x: (x["tier"], x.get("name") or ""))
        return targets, fades

    h_t, h_f = split(tiers_hit, pitcher=False)
    p_t, p_f = split(tiers_sp,  pitcher=True)
    return {"hitters": {"targets": h_t, "fades": h_f},
            "pitchers":{"targets": p_t, "fades": p_f}}

# --------------------------
# Recap section
# --------------------------
def recap_section(eval_rows, buckets_today):
    recap = {"hitters":{"targets":[],"fades":[],"notable_non_picks":[]},
             "pitchers":{"targets":[],"fades":[]}}
    if not eval_rows:
        return recap, {"targets_precision": None, "fades_accuracy": None, "overall_hit_rate": None}

    picked = set()
    for sec in ("hitters","pitchers"):
        for lst in ("targets","fades"):
            for p in buckets_today.get(sec,{}).get(lst,[]):
                picked.add(((str(p.get("player_id") or "")).lower(), (p.get("name") or "").lower()))

    def bucket_for_pos(pos): return "pitchers" if (pos or "").upper() in ("SP","P") else "hitters"

    def to_row(r):
        return {
            "player_id": str(pick(r, "player_id", "id") or ""),
            "name": pick(r, "name", "player_name") or "",
            "pos": pick(r, "position", "pos") or "",
            "tier": pick(r, "tier", "tier_score"),
            "actual_hits": to_float(pick(r, "actual_hits", "hits", "h"), 0.0)
        }

    for r in eval_rows:
        r = { _norm_key(k): _norm_val(v) for k,v in r.items() }
        cat = (r.get("category") or "").lower()
        row = to_row(r)
        bucket = bucket_for_pos(row["pos"])
        if cat in ("target","targets"):
            recap[bucket]["targets"].append(row)
        elif cat in ("fade","fades"):
            recap[bucket]["fades"].append(row)

    # Nick Kurtz rule
    for r in eval_rows:
        r = { _norm_key(k): _norm_val(v) for k,v in r.items() }
        row = to_row(r)
        key = (row["player_id"].lower(), row["name"].lower())
        if row["actual_hits"] is not None and row["actual_hits"] >= 3 and key not in picked:
            recap["hitters"]["notable_non_picks"].append(
                {"player_id": row["player_id"], "name": row["name"], "actual_hits": row["actual_hits"], "reason": ">=3 hits"}
            )

    def rate(rows, pred):
        rows = rows or []
        return round(sum(1 for x in rows if pred(x))/len(rows), 4) if rows else None

    tgt_all = (recap["hitters"]["targets"] or []) + (recap["pitchers"]["targets"] or [])
    fade_all= (recap["hitters"]["fades"]   or []) + (recap["pitchers"]["fades"]   or [])
    metrics = {
        "targets_precision": rate(tgt_all, lambda x: (float(x.get("actual_hits") or 0) >= 1)),
        "fades_accuracy":    rate(fade_all, lambda x: (float(x.get("actual_hits") or 0) == 0)),
        "overall_hit_rate":  rate(tgt_all+fade_all, lambda x: (float(x.get("actual_hits") or 0) >= 1)) if (tgt_all or fade_all) else None
    }
    return recap, metrics

# --------------------------
# Sanity logging
# --------------------------
def log_sanity(tiers_hit, tiers_sp, ranked_by_id, ranked_by_name):
    def topn(rows, n=3):
        return [{"name": r.get("name"),
                 "tier": r.get("tier"),
                 "raw_score": r.get("raw_score") or r.get("score")} for r in rows[:n]]
    print(f"[builder] version {__VERSION__}")
    print(f"[builder] ranked_full: by_id={len(ranked_by_id)} by_name={len(ranked_by_name)}")
    print(f"[builder] hitters sample: {json.dumps(topn(tiers_hit), ensure_ascii=False)}")
    print(f"[builder] pitchers sample: {json.dumps(topn(tiers_sp), ensure_ascii=False)}")

# --------------------------
# Main
# --------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--yday", required=True)
    ap.add_argument("--structured", required=False, default=None)
    ap.add_argument("--tiers-hit", required=True, dest="tiers_hit")
    ap.add_argument("--tiers-sp", required=True, dest="tiers_sp")
    ap.add_argument("--eval-yday", required=True, dest="eval_yday")
    ap.add_argument("--out", required=True)
    ap.add_argument("--ranked-full", required=False, default=None)
    ap.add_argument("--hitter-target-min", type=float, default=6.0)
    ap.add_argument("--sp-target-min",     type=float, default=6.0)
    ap.add_argument("--hitter-fade-max",   type=float, default=4.0)
    ap.add_argument("--sp-fade-max",       type=float, default=4.0)
    ap.add_argument("--box-yday", required=False, default=None)
    args = ap.parse_args()

    tiers_hit  = read_csv_rows(args.tiers_hit)
    tiers_sp   = read_csv_rows(args.tiers_sp)
    structured = read_json(args.structured) if args.structured else {}
    eval_rows  = read_csv_rows(args.eval_yday)
    ranked_rows= read_csv_rows(args.ranked_full) if args.ranked_full else []
    ranked_by_id, ranked_by_name = build_ranked_lookup(ranked_rows)

    log_sanity(tiers_hit, tiers_sp, ranked_by_id, ranked_by_name)

    today = today_lists(
        tiers_hit, tiers_sp, structured,
        args.hitter_target_min, args.sp_target_min, args.hitter_fade_max, args.sp_fade_max,
        ranked_by_id, ranked_by_name
    )
    recap, metrics = recap_section(eval_rows, today)

    payload = {"version": __VERSION__, "date": args.date, "recap": recap, "today": today, "metrics": metrics}
    Path(os.path.dirname(args.out)).mkdir(parents=True, exist_ok=True)

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    print(f"[builder] Wrote {args.out}")
    print(f"[builder] metrics: {json.dumps(metrics)}")

if __name__ == "__main__":
    main()
