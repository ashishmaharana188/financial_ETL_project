import math
from itertools import combinations


def extract_mapped_keys(current_mapping):
    """Helper to flatten the list of lists in your existing mapping dictionary."""
    mapped_keys = set()
    if not current_mapping:
        return mapped_keys

    for category, keys_list in current_mapping.items():
        if keys_list and isinstance(keys_list[0], list):
            for sub_list in keys_list:
                mapped_keys.update(sub_list)
        else:
            mapped_keys.update(keys_list)
    return mapped_keys


def execute_three_way_match(
    raw_data, ai_classified_keys, leak_type, target_gap, current_multiplier
):
    """
    Tests every combination of the AI's suggested keys to find the exact subset
    that mathematically perfectly fills the gap (checking both positive and negative signs).
    """
    boundary_map = {
        "OCF": "OperatingCashFlow",
        "ICF": "InvestingCashFlow",
        "FCF": "FinancingCashFlow",
    }

    ai_bucket = boundary_map.get(leak_type)
    ai_suggested_keys = ai_classified_keys.get(ai_bucket, [])

    # 1. Pull the raw values and scale them
    trace_values = {
        k: raw_data.get(k, 0.0) * current_multiplier
        for k in ai_suggested_keys
        if k in raw_data and isinstance(raw_data[k], (int, float))
    }

    if not trace_values:
        return {
            "status": "SOURCE_DATA_ANOMALY",
            "missing_keys_found": [],
            "evidence": {},
            "message": "AI found no valid numeric keys for this bucket.",
        }

    # 2. SUBSET SUM: Test all combinations of the AI's keys
    keys_list = list(trace_values.keys())

    # Safeguard against exponential computation time if API goes crazy
    if len(keys_list) > 10:
        keys_list = keys_list[:10]

    best_diff = float("inf")
    best_combo = []
    best_sum = 0.0

    # Test every possible combination size (1 key, 2 keys... up to all keys)
    for r in range(1, len(keys_list) + 1):
        for combo in combinations(keys_list, r):
            # NEW: Test all possible +/- sign combinations for these specific keys
            for signs in product([1, -1], repeat=r):
                combo_sum = sum(trace_values[k] * sign for k, sign in zip(combo, signs))

                if math.isclose(combo_sum, target_gap, abs_tol=2.0):
                    return {
                        "status": "SUCCESS",
                        "missing_keys_found": list(combo),
                        "evidence": {k: trace_values[k] for k in combo},
                        "message": f"Exact subset match found! Keys {list(combo)} sum perfectly to fix the gap.",
                    }

                # Track the closest match for our anomaly debugging log
                diff = abs(target_gap - combo_sum)
                if diff < best_diff:
                    best_diff = diff
                    best_combo = list(combo)
                    best_sum = combo_sum

    return {
        "status": "SOURCE_DATA_ANOMALY",
        "missing_keys_found": [],
        "evidence": {k: trace_values[k] for k in best_combo},
        "message": f"No valid subset found. Closest combo was {list(best_combo)} summing to {best_sum:.2f} (Gap: {target_gap:.2f}).",
    }
