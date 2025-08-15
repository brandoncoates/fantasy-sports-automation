#!/usr/bin/env python3
"""
Daily report builder (recap + today's targets/fades).
Robust header parsing + logging so we don't silently default tiers/scores.
"""
from __future__ import annotations
import argparse, csv, json, os, sys
from pathlib import Path
from typing import Dict, List, Any

__VERSION__ = "v2025-08-15b"

def _norm_key(k): return (k or "").strip().lower()
def _norm_val(v): return (v or "").strip()

def read_csv_rows(path):
    if not path or not os.path.exists(path):
        print(f"[builder] WARN: CSV not found: {path}", file=sys.stderr)
        return []
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        # normalize headers for downstream lookups
        rdr.fieldnames = [_norm_key(h) for h in rdr.fieldnames]
        for r in rdr:
            rows.append({_norm_key(k): _norm_val(v) for k, v in r.items()})
    return rows

def read_json(path):
    if not path or not os.path.exists(path):
        print(f"[builder] WARN: JSON not found: {path}", file=sys.stderr)
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def to_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def booly(x):
    s = str(x).strip().lower()
    return s in ("1","true","t","yes","y")

def pick(r: Dict[str, Any], *keys: str):
    for k in keys:
        v = r.get(k)
        if v not in (None, ""):
            return v
    return None

def today_lists(tiers_hit, tiers_sp, structured, ht_min, sp_min, hf_max, spf_max):
    # probable SP ids from structured
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
            # support both 'raw_score' and 'score' in CSVs
            tier  = to_float(pick(r, "tier", "tier_score"))
            score = to_float(pick(r, "raw_score", "score"), 0.0)
            pid   = str(pick(r, "player_id", "id") or "")
            if pitcher:
                is_sp_today = booly(r.get("starting_pitcher_today"))
                if not (is_sp_today or (pid and pid in sp_ids)):
                    continue
            if tier is None:
                # skip if we truly can't read a tier from the row
                continue

            entry = {
                "player_id": pid,
                "name": pick(r, "name", "player_name"),
                "team": r.get("team"),
                "opp": pick(r, "opponent_team", "opp"),
                "pos": pick(r, "position", "pos"),
                "tier": float(tier),
                "score": float(score) if score is not None else 0.0,
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

def recap_section(eval_rows, buckets_today):
    recap = {"hitters":{"targets":[],"fades":[],"notable_non_picks":[]},
             "pitchers":{"targets":[],"fades":[]}};
    if not eval_rows:
        return recap, {"targets_precision": None, "fades_accuracy": None, "overall_hit_rate": None}

    picked = set()
    for sec in ("hitters","pitchers"):
        for lst in ("targets","fades"):
            for p in buckets_today.get(sec,{}).get(lst,[]):
                picked.add(((str(p.get("player_id") or "")).lower(), (p.get("name") or "").lower()))

    def bucket_for_pos(pos):
        return "pitchers" if (pos or "").upper() in ("SP","P") else "hitters"

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

    # Nick Kurtz rule: hitters we didn't pick but had big nights (>=3 hits)
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

def log_sanity(tiers_hit, tiers_sp):
    # Print a few sample rows to CI logs for sanity
    def topn(rows, n=3):
        out = []
        for r in rows[:n]:
            out.append({
                "name": r.get("name"), "tier": r.get("tier"),
                "raw_score": r.get("raw_score") or r.get("score")
            })
        return out
    print(f"[builder] version {__VERSION__}")
    print(f"[builder] hitters sample: {json.dumps(topn(tiers_hit), ensure_ascii=False)}")
    print(f"[builder] pitchers sample: {json.dumps(topn(tiers_sp), ensure_ascii=False)}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--yday", required=True)
    ap.add_argument("--structured", required=False, default=None)
    ap.add_argument("--tiers-hit", required=True, dest="tiers_hit")
    ap.add_argument("--tiers-sp", required=True, dest="tiers_sp")
    ap.add_argument("--eval-yday", required=True, dest="eval_yday")
    ap.add_argument("--out", required=True)
    ap.add_argument("--hitter-target-min", type=float, default=6.0)
    ap.add_argument("--sp-target-min",     type=float, default=6.0)
    ap.add_argument("--hitter-fade-max",   type=float, default=4.0)
    ap.add_argument("--sp-fade-max",       type=float, default=4.0)
    ap.add_argument("--box-yday", required=False, default=None)
    args = ap.parse_args()

    tiers_hit = read_csv_rows(args.tiers_hit)
    tiers_sp  = read_csv_rows(args.tiers_sp)
    structured = read_json(args.structured) if args.structured else {}
    eval_rows = read_csv_rows(args.eval_yday)

    log_sanity(tiers_hit, tiers_sp)

    today = today_lists(
        tiers_hit, tiers_sp, structured,
        args.hitter_target_min, args.sp_target_min, args.hitter_fade_max, args.sp_fade_max
    )
    recap, metrics = recap_section(eval_rows, today)

    payload = {"version": __VERSION__, "date": args.date, "recap": recap, "today": today, "metrics": metrics}
    Path(os.path.dirname(args.out)).mkdir(parents=True, exist_ok=True)

    # Guardrail: if everything in hitters targets is exactly (7.0, 0.0), warn loudly
    ht = today["hitters"]["targets"] or []
    if ht:
        n_bad = sum(1 for x in ht if float(x.get("tier", 0)) == 7.0 and float(x.get("score", 0)) == 0.0)
        share = n_bad/len(ht)
        if share >= 0.8:
            print(f"::warning::Suspicious output: {n_bad}/{len(ht)} hitter targets have (tier=7.0, score=0.0). Check CSV header mapping.", file=sys.stderr)

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    print(f"[builder] Wrote {args.out}")
    print(f"[builder] metrics: {json.dumps(metrics)}")

if __name__ == "__main__":
    main()
