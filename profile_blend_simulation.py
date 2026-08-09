import argparse
import json
import math
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import numpy as np

import train_shadow_ml as training


# A profile overlay is allowed to nudge v4, not replace it.  The prospective
# rollout policy also starts at 15%, so analysis must not manufacture a gain
# with a blend that could never be deployed under that policy.
ALPHA_GRID = (0.0, 0.05, 0.10, 0.15)
GROUP_TOTAL_MINIMUM = 60
GROUP_INNER_MINIMUM = 12
GROUP_OUTER_MINIMUM = 12
PROFILE_TOTAL_MINIMUM = 30
PROFILE_INNER_MINIMUM = 6
PROFILE_OUTER_MINIMUM = 6


def _finite_float(value, default=None):
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def normalize_scores(values):
    values = [float(value) for value in values]
    if not values:
        return []
    low, high = min(values), max(values)
    if high - low <= 1e-12:
        return [0.5 for _ in values]
    return [(value - low) / (high - low) for value in values]


def visible_v4_component(rows):
    """Return an affine-normalized v4 score without cosmetically stretching it.

    The raw score must reproduce the visible ranking. A rank-derived fallback is
    returned only so diagnostics can continue; any race using it fails the
    promotion gate through ``faithful=False``.
    """
    raw_scores = []
    for row in rows:
        value = _finite_float(row.get("v4_score"))
        if value is None:
            value = _finite_float(row.get("ai_score"))
        raw_scores.append(value)
    baseline_ranks = training.rank_from_visible_v4(rows)
    if all(value is not None for value in raw_scores):
        normalized = normalize_scores(raw_scores)
        score_ranks = training.rank_from_scores(rows, normalized)
        if all(score_ranks.get(id(row)) == baseline_ranks.get(id(row)) for row in rows):
            return normalized, True

    field_size = max(1, len(rows) - 1)
    fallback = [
        (len(rows) - baseline_ranks.get(id(row), len(rows))) / field_size
        for row in rows
    ]
    return fallback, False


def predict_scores(model, rows, feature_cols):
    matrix = np.array(
        [
            [training.safe_float(training.feature_dict(row).get(col), 0.0) for col in feature_cols]
            for row in rows
        ],
        dtype=np.float32,
    )
    return normalize_scores([float(value) for value in model.predict(matrix)])


def blended_scores(model, rows, feature_cols, alpha):
    baseline, faithful = visible_v4_component(rows)
    ml_scores = predict_scores(model, rows, feature_cols)
    blended = [
        (1.0 - float(alpha)) * v4_score + float(alpha) * ml_score
        for v4_score, ml_score in zip(baseline, ml_scores)
    ]
    return blended, faithful


def evaluate_blend(model, races, feature_cols, alpha):
    def builder(rows):
        scores, _ = blended_scores(model, rows, feature_cols, alpha)
        return training.rank_from_scores(rows, scores)

    return training.evaluate_ranks(races, builder)


def compare_blend_to_existing(model, races, feature_cols, alpha):
    rescues = damages = baseline_top3 = candidate_top3 = 0
    baseline_top1 = candidate_top1 = 0
    baseline_gaps, candidate_gaps = [], []
    baseline_crowds, candidate_crowds = [], []
    fallback_races = race_count = 0
    for raw_rows in races.values():
        rows = [row for row in raw_rows if row.get("finish_pos") is not None]
        if len(rows) < 2:
            continue
        candidate_scores, faithful = blended_scores(model, rows, feature_cols, alpha)
        fallback_races += int(not faithful)
        candidate_ranks = training.rank_from_scores(rows, candidate_scores)
        baseline_ranks = training.rank_from_visible_v4(rows)
        winner = min(rows, key=lambda row: training.safe_float(row.get("finish_pos"), 999.0))
        baseline_rank = baseline_ranks.get(id(winner), 999)
        candidate_rank = candidate_ranks.get(id(winner), 999)
        baseline_hit = baseline_rank <= 3
        candidate_hit = candidate_rank <= 3
        baseline_top3 += int(baseline_hit)
        candidate_top3 += int(candidate_hit)
        baseline_top1 += int(baseline_rank == 1)
        candidate_top1 += int(candidate_rank == 1)
        rescues += int(not baseline_hit and candidate_hit)
        damages += int(baseline_hit and not candidate_hit)

        raw_baseline = [
            training.safe_float(row.get("v4_score"), training.safe_float(row.get("ai_score"), 0.0))
            for row in rows
        ]
        baseline_separation = training.normalized_boundary(raw_baseline)
        candidate_separation = training.normalized_boundary(candidate_scores)
        if baseline_separation["gap"] is not None:
            baseline_gaps.append(baseline_separation["gap"])
        if candidate_separation["gap"] is not None:
            candidate_gaps.append(candidate_separation["gap"])
        baseline_crowds.append(baseline_separation["cutoff_crowd"])
        candidate_crowds.append(candidate_separation["cutoff_crowd"])
        race_count += 1

    baseline_gap = training._median_or_none(baseline_gaps)
    candidate_gap = training._median_or_none(candidate_gaps)
    return {
        "races": race_count,
        "baselineWinnerTop3": baseline_top3,
        "candidateWinnerTop3": candidate_top3,
        "rescues": rescues,
        "damages": damages,
        "winnerTop3Net": rescues - damages,
        "baselineTop1": baseline_top1,
        "candidateTop1": candidate_top1,
        "top1Net": candidate_top1 - baseline_top1,
        "baselineBoundaryGapMedian": baseline_gap,
        "candidateBoundaryGapMedian": candidate_gap,
        "boundaryGapRatio": (
            candidate_gap / baseline_gap
            if baseline_gap is not None and baseline_gap > 1e-12 and candidate_gap is not None
            else None
        ),
        "baselineCutoffCrowdMedian": training._median_or_none(baseline_crowds),
        "candidateCutoffCrowdMedian": training._median_or_none(candidate_crowds),
        "v4ScoreFallbackRaces": fallback_races,
    }


def top3_transition_events(model, races, feature_cols, alpha):
    events = []
    for race_key, raw_rows in races.items():
        rows = [row for row in raw_rows if row.get("finish_pos") is not None]
        if len(rows) < 2:
            continue
        candidate_scores, _ = blended_scores(model, rows, feature_cols, alpha)
        candidate_ranks = training.rank_from_scores(rows, candidate_scores)
        baseline_ranks = training.rank_from_visible_v4(rows)
        winner = min(rows, key=lambda row: training.safe_float(row.get("finish_pos"), 999.0))
        baseline_rank = baseline_ranks.get(id(winner), 999)
        candidate_rank = candidate_ranks.get(id(winner), 999)
        baseline_hit = baseline_rank <= 3
        candidate_hit = candidate_rank <= 3
        if baseline_hit == candidate_hit:
            continue
        events.append({
            "event": "RESCUE" if candidate_hit else "DAMAGE",
            "raceKey": race_key,
            "raceId": winner.get("race_id"),
            "raceDate": winner.get("race_date"),
            "city": winner.get("city") or winner.get("hippodrome"),
            "raceNo": winner.get("race_no"),
            "track": training.track_bucket(winner.get("track")),
            "winner": winner.get("horse_name"),
            "baselineWinnerRank": baseline_rank,
            "candidateWinnerRank": candidate_rank,
        })
    return events


def segment_key(entry, kind):
    if kind == "GROUP":
        return f"GROUP:{training.category(entry)}"
    subtype = training.profile_subtype(entry)
    if kind == "PROFILE":
        return f"PROFILE:{subtype}"
    return f"PROFILE_TRACK:{subtype}|{training.track_bucket(entry.get('track'))}"


def segment_kind(key):
    return key.split(":", 1)[0]


def segment_races(races, key):
    kind = segment_kind(key)
    return {
        race_id: rows
        for race_id, rows in races.items()
        if rows and segment_key(rows[0], kind) == key
    }


def candidate_segment_keys(races):
    keys = set()
    for rows in races.values():
        if not rows:
            continue
        group = training.category(rows[0])
        subtype = training.profile_subtype(rows[0])
        if group != "GLOBAL":
            keys.add(segment_key(rows[0], "GROUP"))
        if subtype not in {"GLOBAL", group}:
            keys.add(segment_key(rows[0], "PROFILE"))
            keys.add(segment_key(rows[0], "PROFILE_TRACK"))
    return sorted(keys)


def evidence_minimums(kind):
    if kind == "GROUP":
        return GROUP_TOTAL_MINIMUM, GROUP_INNER_MINIMUM, GROUP_OUTER_MINIMUM
    return PROFILE_TOTAL_MINIMUM, PROFILE_INNER_MINIMUM, PROFILE_OUTER_MINIMUM


def choose_alpha(model, races, feature_cols, alpha_grid=ALPHA_GRID):
    evaluations = []
    for alpha in alpha_grid:
        comparison = compare_blend_to_existing(model, races, feature_cols, alpha)
        evaluations.append({"alpha": float(alpha), "comparison": comparison})
    return max(
        evaluations,
        key=lambda item: (
            item["comparison"]["winnerTop3Net"],
            -item["comparison"]["damages"],
            item["comparison"]["top1Net"],
            item["comparison"]["boundaryGapRatio"] or -999.0,
            -item["alpha"],
        ),
    ), evaluations


def build_segment_gate(key, total_races, inner, outer, alpha):
    kind = segment_kind(key)
    total_minimum, inner_minimum, outer_minimum = evidence_minimums(kind)

    def check(name, passed, detail):
        return {"name": name, "passed": bool(passed), "detail": detail}

    outer_boundary_ok = (
        outer.get("boundaryGapRatio") is not None and outer["boundaryGapRatio"] >= 0.90
    )
    outer_crowd_ok = (
        outer.get("candidateCutoffCrowdMedian") is not None
        and outer.get("baselineCutoffCrowdMedian") is not None
        and outer["candidateCutoffCrowdMedian"] <= outer["baselineCutoffCrowdMedian"] + 1.0
    )
    checks = [
        check("nonzero_blend", alpha > 0.0, f"alpha={alpha:.2f}"),
        check("total_evidence", total_races >= total_minimum, f"races={total_races}/{total_minimum}"),
        check("inner_minimum", inner.get("races", 0) >= inner_minimum,
              f"races={inner.get('races', 0)}/{inner_minimum}"),
        check("inner_winner_top3_plus_1", inner.get("winnerTop3Net", -999) >= 1,
              f"net={inner.get('winnerTop3Net')}"),
        check("outer_minimum", outer.get("races", 0) >= outer_minimum,
              f"races={outer.get('races', 0)}/{outer_minimum}"),
        check("outer_winner_top3_plus_1", outer.get("winnerTop3Net", -999) >= 1,
              f"net={outer.get('winnerTop3Net')}"),
        check("outer_no_damage", outer.get("damages", 999) == 0,
              f"damages={outer.get('damages')}"),
        check("outer_top1_loss_max_1", outer.get("top1Net", -999) >= -1,
              f"net={outer.get('top1Net')}"),
        check("outer_boundary_not_compressed", outer_boundary_ok,
              f"ratio={training.fmt(outer.get('boundaryGapRatio'))}"),
        check("outer_cutoff_crowd_not_worse", outer_crowd_ok,
              f"baseline={training.fmt(outer.get('baselineCutoffCrowdMedian'))},"
              f"candidate={training.fmt(outer.get('candidateCutoffCrowdMedian'))}"),
        check("raw_v4_score_faithful", outer.get("v4ScoreFallbackRaces", 999) == 0,
              f"fallback_races={outer.get('v4ScoreFallbackRaces')}"),
    ]
    passed = all(item["passed"] for item in checks)
    return {
        "decision": "SHADOW_CANDIDATE" if passed else "REJECTED",
        "checks": checks,
        "failedChecks": [item["name"] for item in checks if not item["passed"]],
    }


def simulate(entries, corpus_summary, validation_ratio=0.2, inner_validation_ratio=0.25):
    outer_train, outer_validation = training.split_races(entries, validation_ratio)
    inner_entries = [row for rows in outer_train.values() for row in rows]
    inner_train, inner_validation = training.split_races(inner_entries, inner_validation_ratio)
    if len(inner_validation) < 10 or len(outer_validation) < 10:
        raise ValueError("Not enough chronological validation races")

    inner_cols, _ = training.select_feature_cols(inner_train, training.FEATURE_COLS)
    inner_cols = training.without_agf_features(inner_cols)
    outer_cols, feature_coverage = training.select_feature_cols(outer_train, training.FEATURE_COLS)
    outer_cols = training.without_agf_features(outer_cols)
    inner_model = training.train_ranker(inner_train, inner_cols)
    outer_model = training.train_ranker(outer_train, outer_cols)

    all_races = dict(training._race_items(entries))
    segment_results = []
    for key in candidate_segment_keys(all_races):
        total_segment = segment_races(all_races, key)
        inner_segment = segment_races(inner_validation, key)
        outer_segment = segment_races(outer_validation, key)
        selected, alpha_evaluations = choose_alpha(inner_model, inner_segment, inner_cols)
        alpha = selected["alpha"]
        inner_comparison = selected["comparison"]
        outer_comparison = compare_blend_to_existing(
            outer_model, outer_segment, outer_cols, alpha
        )
        outer_metrics = evaluate_blend(outer_model, outer_segment, outer_cols, alpha)
        baseline_metrics = training.evaluate_existing(outer_segment, "v4_rank")
        gate = build_segment_gate(
            key, len(total_segment), inner_comparison, outer_comparison, alpha
        )
        segment_results.append({
            "segment": key,
            "kind": segment_kind(key),
            "totalRaces": len(total_segment),
            "innerValidationRaces": len(inner_segment),
            "outerValidationRaces": len(outer_segment),
            "selectedAlpha": alpha,
            "innerComparison": inner_comparison,
            "outerComparison": outer_comparison,
            "outerBaselineMetrics": baseline_metrics,
            "outerCandidateMetrics": outer_metrics,
            "innerTop3Transitions": top3_transition_events(
                inner_model, inner_segment, inner_cols, alpha
            ),
            "outerTop3Transitions": top3_transition_events(
                outer_model, outer_segment, outer_cols, alpha
            ),
            "alphaSearch": alpha_evaluations,
            "gate": gate,
        })

    segment_results.sort(
        key=lambda item: (
            item["gate"]["decision"] == "SHADOW_CANDIDATE",
            item["outerComparison"]["winnerTop3Net"],
            -item["outerComparison"]["damages"],
            item["totalRaces"],
        ),
        reverse=True,
    )
    return {
        "schemaVersion": "profile-blend-simulation-v1",
        "createdAt": datetime.now().astimezone().isoformat(timespec="seconds"),
        "visibleRankingImpact": False,
        "automaticDeployment": False,
        "modelVariant": "no-agf",
        "corpusSummary": corpus_summary,
        "split": {
            "innerTrainRaces": len(inner_train),
            "innerValidationRaces": len(inner_validation),
            "outerTrainRaces": len(outer_train),
            "outerValidationRaces": len(outer_validation),
            "validationRatio": validation_ratio,
            "innerValidationRatio": inner_validation_ratio,
        },
        "policy": {
            "alphaGrid": list(ALPHA_GRID),
            "groupMinimums": {
                "total": GROUP_TOTAL_MINIMUM,
                "inner": GROUP_INNER_MINIMUM,
                "outer": GROUP_OUTER_MINIMUM,
            },
            "profileMinimums": {
                "total": PROFILE_TOTAL_MINIMUM,
                "inner": PROFILE_INNER_MINIMUM,
                "outer": PROFILE_OUTER_MINIMUM,
            },
            "winnerTop3OuterGainRequired": 1,
            "outerDamageAllowed": 0,
            "top1LossAllowed": 1,
            "boundaryGapMinimumRatio": 0.90,
        },
        "featureCount": len(outer_cols),
        "featureSourceCoverage": feature_coverage,
        "segments": segment_results,
        "shadowCandidates": [
            item["segment"]
            for item in segment_results
            if item["gate"]["decision"] == "SHADOW_CANDIDATE"
        ],
    }


def write_report(path, result):
    lines = [
        "# Profile-aware v4 + ML Blend Simulation",
        "",
        f"- Created: {result['createdAt']}",
        "- Model signal: no-AGF global ranker, bounded per segment",
        "- Maximum ML contribution: 15%",
        "- Visible ranking impact: none",
        "- Alpha is selected on inner chronological validation and evaluated once on outer holdout.",
        f"- Shadow candidates: `{', '.join(result['shadowCandidates']) or 'none'}`",
        "",
        "| Segment | Total | Inner | Outer | Alpha | Inner W3 net | Outer W3 base->cand | Rescue | Damage | Top1 net | Gap ratio | Decision |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for item in result["segments"]:
        inner = item["innerComparison"]
        outer = item["outerComparison"]
        lines.append(
            f"| {item['segment']} | {item['totalRaces']} | {item['innerValidationRaces']} | "
            f"{item['outerValidationRaces']} | {item['selectedAlpha']:.2f} | "
            f"{inner['winnerTop3Net']} | {outer['baselineWinnerTop3']}->{outer['candidateWinnerTop3']} | "
            f"{outer['rescues']} | {outer['damages']} | {outer['top1Net']} | "
            f"{training.fmt(outer['boundaryGapRatio'])} | {item['gate']['decision']} |"
        )
    lines.extend([
        "",
        "## Gate failures",
        "",
    ])
    for item in result["segments"]:
        if item["gate"]["decision"] == "SHADOW_CANDIDATE":
            continue
        lines.append(
            f"- `{item['segment']}`: {', '.join(item['gate']['failedChecks'])}"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Simulate profile-bounded v4 + no-AGF ML blends.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-dir", default=".")
    parser.add_argument("--validation-ratio", type=float, default=0.2)
    parser.add_argument("--inner-validation-ratio", type=float, default=0.25)
    args = parser.parse_args()

    entries, corpus_summary = training.load_entries(
        SimpleNamespace(
            input=args.input,
            export_url=None,
            include_partial_races=False,
        ),
        with_summary=True,
    )
    result = simulate(
        entries,
        corpus_summary,
        validation_ratio=args.validation_ratio,
        inner_validation_ratio=args.inner_validation_ratio,
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    date_stamp = datetime.now().strftime("%Y%m%d")
    json_path = output_dir / f"profile_blend_simulation_{date_stamp}.json"
    report_path = output_dir / f"profile_blend_simulation_{date_stamp}.md"
    json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    write_report(report_path, result)
    print(json.dumps({
        "json": str(json_path),
        "report": str(report_path),
        "shadow_candidates": result["shadowCandidates"],
        "segments": len(result["segments"]),
        "visible_ranking_impact": False,
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
