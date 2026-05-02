import json
from scripts.model_runtime import runtime


def analyze_key_with_phi3(unmapped_key, candidate_buckets):
    """
    Uses the localized Phi-3 model to assign an unmapped key to the correct bucket
    from a constrained list of candidates, returning strict JSON.
    """
    if not candidate_buckets:
        return None

    # Ensure Phi-3 is loaded
    runtime.load_models()

    if not runtime.llm:
        print("      [ERROR] Phi-3 Logic Engine failed to load.")
        return None

    print(f"      [REASONING] Asking Phi-3 to classify: '{unmapped_key}'...")

    # 1. Build the constrained prompt
    # We only pass the top candidate buckets, completely avoiding the 4k context limit
    prompt = f"""<|system|>
You are a senior accounting data engineer. Your job is to classify unmapped financial statement line items into standard accounting categories.
Respond ONLY with a valid JSON object. 
Format: {{"Category": "The selected bucket name"}}
<|end|>
<|user|>
Unmapped Key: "{unmapped_key}"
Candidate Buckets: {candidate_buckets}

Which candidate bucket represents the best accounting classification for this key? 
Respond strictly in JSON format.<|end|>
<|assistant|>"""

    # 2. Execute Phi-3 Inference
    try:
        response = runtime.llm(
            prompt,
            max_tokens=50,  # We only need a tiny JSON response
            temperature=0.0,  # 0.0 forces deterministic, purely logical accounting answers
            response_format={
                "type": "json_object"
            },  # Physical guardrail to enforce JSON
        )

        # 3. Extract the response text
        raw_text = response["choices"][0]["text"]
        result_json = json.loads(raw_text)

        # Determine the selected category
        selected_category = result_json.get("Category")

        if selected_category and selected_category in candidate_buckets:
            print(f"      [REASONING] Phi-3 selected: {selected_category}")
            return {selected_category: [unmapped_key]}
        else:
            print(
                f"      [WARNING] Phi-3 selected an invalid category or hallucinated: {selected_category}"
            )
            return None

    except Exception as e:
        print(f"      [ERROR] Phi-3 Inference Failed: {str(e)}")
        return None


# Example usage (for testing the file directly):
if __name__ == "__main__":
    test_key = "ProceedsFromMaturitiesOfInvestments"
    test_candidates = [
        "CapExPurchaseOfPPE",
        "PurchaseSaleOfInvestments",
        "NetDebtIssuedRepaid",
        "OtherInvestingActivities",
        "DividendsPaid",
    ]
    result = analyze_key_with_phi3(test_key, test_candidates)
    print(f"Final AI JSON: {result}")
