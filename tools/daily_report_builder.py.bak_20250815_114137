#!/usr/bin/env python3
"""
Daily Report Builder (MLB)
Combines:
- Recap from evaluation_<DATE>.csv (yesterday's outcomes)
- Today's targets/fades from tiers_hitters_<DATE>.csv and tiers_starting_pitchers_<DATE>.csv
Only SPs are included for pitchers.

Assumptions from your files:
tiers_* columns: player_id,name,date,raw_score,tier,team,opponent_team,home_or_away,position,starting_pitcher_today
evaluation columns: player_id,date,tier,actual_hits,category
structured JSON: dict keyed by name -> object with fields including is_probable_starter/starter

Targets/Fades thresholds are configurable.
"""

import argparse, csv, json, os
from pathlib import Path

def read_csv_rows(path):
    if not path or not os.path.exists(path):
        return []
    with open(path, newline='', encoding='utf-8') as f:
        return list(csv.DictReader(f))

def read_json(path):
    if not path or not os.path.exists(path):
        return {}
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def to_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def booly(x):
    s = str(x).strip().lower()
    return s in ('1','true','t','yes','y')

def today_lists(tiers_hit, tiers_sp, structured, ht_min, sp_min, hf_max, spf_max):
    # set of SP player_ids from structured
    sp_ids = set()
    if isinstance(structured, dict):
        for v in structured.values():
            pid = str(v.get('player_id') or '')
            if not pid: 
                continue
            if booly(v.get('is_probable_starter')) or booly(v.get('starter')):
                sp_ids.add(pid)

    def split_rows(rows, pitcher=False):
        targets, fades = [], []
        for r in rows:
            tier = to_float(r.get('tier'))
            pid  = str(r.get('player_id') or '')
            if pitcher:
                # enforce SP: either tiers says starting_pitcher_today or structured set
                if not (booly(r.get('starting_pitcher_today')) or (pid and pid in sp_ids)):
                    continue
            minimal = {
                'player_id': pid,
                'name': r.get('name'),
                'team': r.get('team'),
                'opp': r.get('opponent_team'),
                'pos': r.get('position'),
                'tier': tier,
                'score': to_float(r.get('raw_score'))
            }
            if tier is not None:
                if pitcher:
                    if sp_min is not None and tier >= sp_min:
                        targets.append(minimal)
                    if spf_max is not None and tier <= spf_max:
                        fades.append(minimal)
                else:
                    if ht_min is not None and tier >= ht_min:
                        targets.append(minimal)
                    if hf_max is not None and tier <= hf_max:
                        fades.append(minimal)
        # stable sort by tier desc for targets, asc for fades
        targets.sort(key=lambda x: (-(x['tier'] if x['tier'] is not None else -999), x['name'] or ''))
        fades.sort(key=lambda x: ((x['tier'] if x['tier'] is not None else 999), x['name'] or ''))
        return targets, fades

    h_t, h_f = split_rows(tiers_hit, pitcher=False)
    p_t, p_f = split_rows(tiers_sp, pitcher=True)
    return {
        'hitters': {'targets': h_t, 'fades': h_f},
        'pitchers': {'targets': p_t, 'fades': p_f}
    }

def recap_section(eval_rows, buckets_today):
    # eval rows may be empty; schema: player_id,date,tier,actual_hits,category
    recap = {'hitters': {'targets': [], 'fades': [], 'notable_non_picks': []},
             'pitchers': {'targets': [], 'fades': []}}
    if not eval_rows:
        return recap, {'targets_precision': None, 'fades_accuracy': None, 'overall_hit_rate': None}

    # index today's picks (id or name)
    picked = set()
    for sec in ('hitters','pitchers'):
        for lst in ('targets','fades'):
            for p in buckets_today.get(sec,{}).get(lst,[]):
                picked.add((str(p.get('player_id') or '').lower(), (p.get('name') or '').lower()))

    # we may not have position in evaluation; treat as hitters by default
    def bucket_for_pos(pos):
        return 'pitchers' if (pos or '').upper() in ('SP','P') else 'hitters'

    for r in eval_rows:
        pid = str(r.get('player_id') or '')
        name = r.get('name') or r.get('player_name') or ''
        pos = r.get('position') or r.get('pos') or ''
        cat = (r.get('category') or '').strip().lower()  # 'target' or 'fade'
        actual = r.get('actual_hits')
        tier = r.get('tier')
        bucket = bucket_for_pos(pos)
        if cat in ('target','targets'):
            recap[bucket]['targets'].append({'player_id': pid, 'name': name, 'pos': pos, 'tier': tier, 'actual_hits': actual})
        elif cat in ('fade','fades'):
            recap[bucket]['fades'].append({'player_id': pid, 'name': name, 'pos': pos, 'tier': tier, 'actual_hits': actual})

    # Nick Kurtz: add hitters with >=3 hits that we did NOT pick
    for r in eval_rows:
        pid = str(r.get('player_id') or '')
        name = (r.get('name') or r.get('player_name') or '')
        key = (pid.lower(), name.lower())
        try:
            ah = float(r.get('actual_hits'))
        except Exception:
            ah = None
        if ah is not None and ah >= 3 and key not in picked:
            recap['hitters']['notable_non_picks'].append({'player_id': pid, 'name': name, 'actual_hits': ah, 'reason': '>=3 hits'})

    # basic metrics
    def rate(rows, predicate):
        rows = rows or []
        if not rows: 
            return None
        n = sum(1 for x in rows if predicate(x))
        return round(n/len(rows), 4)

    tgt_all = (recap['hitters']['targets'] or []) + (recap['pitchers']['targets'] or [])
    fade_all= (recap['hitters']['fades'] or []) + (recap['pitchers']['fades'] or [])
    metrics = {
        'targets_precision': rate(tgt_all, lambda x: (float(x.get('actual_hits') or 0) >= 1)),
        'fades_accuracy':    rate(fade_all, lambda x: (float(x.get('actual_hits') or 0) == 0)),
        'overall_hit_rate':  rate(tgt_all+fade_all, lambda x: (float(x.get('actual_hits') or 0) >= 1)) if (tgt_all or fade_all) else None
    }
    return recap, metrics

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--date', required=True)
    ap.add_argument('--yday', required=True)
    ap.add_argument('--structured', required=False, default=None)
    ap.add_argument('--tiers-hit', required=True)
    ap.add_argument('--tiers-sp', required=True)
    ap.add_argument('--eval-yday', required=True)
    ap.add_argument('--out', required=True)
    # thresholds
    ap.add_argument('--hitter-target-min', type=float, default=6.0)
    ap.add_argument('--sp-target-min', type=float, default=6.0)
    ap.add_argument('--hitter-fade-max', type=float, default=4.0)
    ap.add_argument('--sp-fade-max', type=float, default=4.0)
    # compatibility: accept but ignore
    ap.add_argument('--box-yday', required=False, default=None)
    args = ap.parse_args()

    tiers_hit = read_csv_rows(args.tiers_hit)
    tiers_sp  = read_csv_rows(args.tiers_sp)
    structured = read_json(args.structured) if args.structured else {}
    eval_rows = read_csv_rows(args.eval_yday)

    today = today_lists(
        tiers_hit, tiers_sp, structured,
        args.hitter_target_min, args.sp_target_min, args.hitter_fade_max, args.sp_fade_max
    )
    recap, metrics = recap_section(eval_rows, today)

    payload = {'date': args.date, 'recap': recap, 'today': today, 'metrics': metrics}

    Path(os.path.dirname(args.out)).mkdir(parents=True, exist_ok=True)
    with open(args.out, 'w', encoding='utf-8') as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

    print(f'Wrote {args.out}')
    print(json.dumps(metrics, indent=2))

if __name__ == '__main__':
    main()
