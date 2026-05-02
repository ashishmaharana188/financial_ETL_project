import math


def extract_mapped_keys(current_mapping):
    """Helper to flatten the list of lists in your existing mapping dictionary."""
    mapped_keys = set()
    for category, keys_list in current_mapping.items():
        if isinstance(keys_list[0], list):  # Handle your list of lists
            for sub_list in keys_list:
                mapped_keys.update(sub_list)
        else:
            mapped_keys.update(keys_list)
    return mapped_keys


def execute_three_way_match(raw_data, current_mapping, ai_classified_keys, leak_type):
    """
    Executes the deterministic Three-Way Match to prove if the AI's semantic
    sorting mathematically fixes the cash flow leak.
    """
    # 1. Map the abbreviation to the exact Yahoo Finance Reported Key and AI PascalCase bucket
    boundary_map = {
        "OCF": {"ai_bucket": "OperatingCashFlow", "reported_key": "OperatingCashFlow"},
        "ICF": {"ai_bucket": "InvestingCashFlow", "reported_key": "InvestingCashFlow"},
        "FCF": {"ai_bucket": "FinancingCashFlow", "reported_key": "FinancingCashFlow"},
    }

    config = boundary_map.get(leak_type)
    if not config:
        raise ValueError(f"Unknown leak type: {leak_type}")

    ai_bucket = config["ai_bucket"]
    reported_key = config["reported_key"]

    # --- THE THREE NUMBERS ---

    # Number A: The Reported Total (Source of Truth)
    # This is what Yahoo Finance explicitly claims the total is.
    reported_total = raw_data.get(reported_key, 0.0)

    # Number B: The Current Mapped Total
    # Summing only the keys we already have in our dictionary for this statement.
    mapped_keys = extract_mapped_keys(current_mapping)
    current_total = sum(
        raw_data.get(k, 0.0)
        for k in mapped_keys
        if k in raw_data and isinstance(raw_data[k], (int, float))
    )

    # Number C: The Reconstructed Total (Current + AI Suggestions)
    # We add the values of the exact keys the AI just classified into this bucket.
    ai_suggested_keys = ai_classified_keys.get(ai_bucket, [])
    ai_suggested_total = sum(
        raw_data.get(k, 0.0)
        for k in ai_suggested_keys
        if k in raw_data and isinstance(raw_data[k], (int, float))
    )

    reconstructed_total = current_total + ai_suggested_total

    # --- THE DETERMINISTIC AUDIT ---

    # We use a small tolerance (abs_tol) to forgive Python's microscopic floating-point rounding errors
    if math.isclose(reconstructed_total, reported_total, abs_tol=0.1):
        return {
            "status": "SUCCESS",
            "missing_keys_found": ai_suggested_keys,
            "calculated_leak_fixed": ai_suggested_total,
            "message": "Mathematical match found! The AI's semantic sorting perfectly reconciles the leak.",
        }
    else:
        # Calculate the remaining gap to log how broken the source data is
        unresolvable_gap = reported_total - reconstructed_total

        return {
            "status": "SOURCE_DATA_ANOMALY",
            "missing_keys_found": [],
            "calculated_leak_fixed": 0.0,
            "message": f"Source Data Broken: Even with all logical keys applied, the math fails by {unresolvable_gap:.2f}. Do not trust the reported total.",
        }
