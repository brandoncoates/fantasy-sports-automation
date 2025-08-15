#!/usr/bin/env python3
"""
Daily Report Builder (MLB)
- Recap (yesterday): Targets/Fades results + Notable Non-Picks (Nick Kurtz rule)
- Today: Targets/Fades for hitters and SP-only pitchers
Inputs are passed via CLI args so this works in GitHub Actions or locally.

Usage (example):
python tools/daily_report_builder.py \
  --date 2025-08-15 \
  --yday 2025-08-14 \
  --structured structured_players_2025-08-15.json \
  --tiers-hit data/analysis/tiers_hitters_2025-08-15.csv \
  --tiers-sp  data/analysis/tiers_starting_pitchers_2025-08-15.csv \
  --eval-yday data/analysis/evaluation_2025-08-15.csv \
  --out       data/analysis/daily_report_2025-08-15.json
"""
import argparse, csv, json, os
from collections import defaultdict

def read_csv(path):
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            # normalize keys to lowercase
            rows.append({(k or "").strip().lower(): (v or "").strip() for k, v in r.items()})
    return rows

def read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def to_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def pick_cols(row, keys):
    for k in keys:
        if k in row and row[k] not in ("", None, "NA", "null"):
            return row[k]
    return None

def build_today_section(tiers_hit_rows, tiers_sp_rows, structured):
    starters = set()
    if structured:
        # normalize keys once
        for p in structured:
            pl = { (k or "").lower(): v for k, v in p.items() }
            is_probable = str(pl.get("is_probable_starter", "")).lower() == "true"
            is_starter  = str(pl.get("starter", "")).lower() == "true"
            pid = pick_cols(pl, ["player_id","id"])
            if pid and (is_probable or is_starter):
                starters.add(str(pid))

    def rows_to_simple(rows, enforce_sp=False):
        out = []
        for r in rows:
            name = pick_cols(r, ["name","player_name"])
            team = pick_cols(r, ["team","team_code","team_name"])
            opp  = pick_cols(r, ["opponent_team","opp","opponent"])
            pos  = pick_cols(r, ["position","pos"])
            pid  = pick_cols(r, ["player_id","id"])
            tier = pick_cols(r, ["tier","score","rank"])
            # lightweight "why" tags from available hints
            why = []
            if to_float(r.get("implied_total") or r.get("itt")):
                why.append("high_itt")
            if to_float(r.get("form_z") or r.get("form")):
                why.append("hot_form")
            if r.get("park") or r.get("park_factor"):
                why.append("park_boost")

            if enforce_sp and starters:
                if not pid or str(pid) not in starters:
                    continue

            out.append({
                "player_id": pid, "name": name, "team": team, "opp": opp, "pos": pos,
                "tier_or_score": tier, "why": why
            })
        return out

    # infer list tags from common columns
    def is_target_row(r): return (str(r.get("is_target") or r.get("target") or r.get("list")).lower() in ("1","true","yes","t","target","targets"))
    def is_fade_row(r):   return (str(r.get("is_fade")   or r.get("fade")   or r.get("list")).lower() in ("1","true","yes","f","fade","fades"))

    today = {
        "hitters": {
            "targets": rows_to_simple([r for r in tiers_hit_rows if is_target_row(r)]),
            "fades":   rows_to_simple([r for r in tiers_hit_rows if is_fade_row(r)]),
        },
        "pitchers": {
            "targets": rows_to_simple([r for r in tiers_sp_rows if is_target_row(r)], enforce_sp=True),
            "fades":   rows_to_simple([r for r in tiers_sp_rows if is_fade_row(r)], enforce_sp=True),
        }
    }
    return today

def build_recap(eval_rows, todays_picks):
    if not eval_rows:
        return {"hitters":{"targets":[],"fades":[],"notable_non_picks":[]},
                "pitchers":{"targets":[],"fades":[]}}, {"targets_precision": None, "fades_accuracy": None, "overall_hit_rate": None}

    # detect key columns
    first = eval_rows[0]
    res_key = next((k for k in ("result","label","hit_miss","outcome") if k in first), None)
    fp_key  = next((k for k in ("fp","fantasy_points","points","fpts","fd_points","dk_points") if k in first), None)
    pos_key = "position" if "position" in first else ("pos" if "pos" in first else None)

    # Index today's picks for quick lookup (by id or name)
    picked_today = set()
    for sec in ("hitters","pitchers"):
        for lst in ("targets","fades"):
            for p in todays_picks.get(sec,{}).get(lst,[]):
                pid = (p.get("player_id") or "").lower()
                name = (p.get("name") or "").lower()
                picked_today.add(pid + "|" + name)

    def was_picked_today(name, pid):
        return ( (pid or "").lower() + "|" + (name or "").lower() ) in picked_today

    recap = {"hitters":{"targets":[],"fades":[],"notable_non_picks":[]},
             "pitchers":{"targets":[],"fades":[]}}

    # fill recap from eval rows (assumes eval file marks which list they were in with flags if present)
    for r in eval_rows:
        name = pick_cols(r, ["name","player_name"])
        team = pick_cols(r, ["team","team_code","team_name"])
        pos  = pick_cols(r, ["position","pos"])
        pid  = pick_cols(r, ["player_id","id","playerid"])
        res  = (r.get(res_key) or "").title() if res_key else ""
        fp   = to_float(r.get(fp_key), None) if fp_key else None

        bucket = "pitchers" if (pos and pos.upper() in ("SP","P")) else "hitters"
        lst = None
        if str(r.get("is_target") or r.get("target") or r.get("list")).lower() in ("1","true","yes","t","target","targets"):
            lst = "targets"
        elif str(r.get("is_fade") or r.get("fade") or r.get("list")).lower() in ("1","true","yes","f","fade","fades"):
            lst = "fades"

        if lst:
            recap[bucket][lst].append({
                "player_id": pid, "name": name, "team": team, "pos": pos,
                "result": res, "fp": fp
            })

    # Nick Kurtz: notable non-picks (95th percentile by pos, approximated without percentile lib)
    # Gather FP per pos
    fp_by_pos = defaultdict(list)
    for r in eval_rows:
        pos  = pick_cols(r, ["position","pos"])
        fp   = to_float(r.get(fp_key), None) if fp_key else None
        if pos and fp is not None:
            fp_by_pos[pos.upper()].append(fp)

    # crude 95th percentile: sort and pick index
    pct95 = {}
    for pos, vals in fp_by_pos.items():
        if len(vals) >= 10:
            vals.sort()
            idx = max(0, round(0.95 * (len(vals)-1)))
            pct95[pos] = vals[int(idx)]
        else:
            pct95[pos] = None

    for r in eval_rows:
        name = pick_cols(r, ["name","player_name"])
        pos  = pick_cols(r, ["position","pos"])
        pid  = pick_cols(r, ["player_id","id","playerid"])
        fp   = to_float(r.get(fp_key), None) if fp_key else None
        if not name or not pos or fp is None: 
            continue
        if was_picked_today(name, pid):  # skip players we picked
            continue
        th = pct95.get(pos.upper())
        if th is not None and fp >= th:
            # treat all non-pitcher as hitters for this bucket
            bucket = "pitchers" if pos.upper() in ("SP","P") else "hitters"
            recap[bucket]["notable_non_picks" if bucket=="hitters" else "targets"].append({
                "player_id": pid, "name": name, "pos": pos, "fp": fp, "reason": "95th_pct+"
            })

    # metrics (simple rates)
    def rate(rows, want=("Hit",)):
        rows = rows or []
        n = sum(1 for r in rows if (r.get("result") or "") in want)
        return round(n/len(rows), 4) if rows else None

    tgt_all = (recap["hitters"]["targets"] or []) + (recap["pitchers"]["targets"] or [])
    fade_all= (recap["hitters"]["fades"]   or []) + (recap["pitchers"]["fades"]   or [])
    metrics = {
        "targets_precision": rate(tgt_all, ("Hit",)),
        "fades_accuracy":    rate(fade_all, ("Hit","Neutral")),  # adjust if your eval uses different labels
        "overall_hit_rate":  rate(tgt_all+fade_all, ("Hit",))
    }
    return recap, metrics

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--yday", required=True)
    ap.add_argument("--structured", required=False, default=None)
    ap.add_argument("--tiers-hit", required=True)
    ap.add_argument("--tiers-sp", required=True)
    ap.add_argument("--eval-yday", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    structured = read_json(args.structured) if args.structured and os.path.exists(args.structured) else None
    tiers_hit_rows = read_csv(args.tiers_hit)
    tiers_sp_rows  = read_csv(args.tiers_sp)
    eval_rows      = read_csv(args.eval_yday)

    today = build_today_section(tiers_hit_rows, tiers_sp_rows, structured)
    recap, metrics = build_recap(eval_rows, today)

    payload = {
        "date": args.date,
        "recap": recap,
        "today": today,
        "metrics": metrics
    }
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f"✅ Wrote {args.out}")
    print(json.dumps(metrics, indent=2))

if __name__ == "__main__":
    main()
