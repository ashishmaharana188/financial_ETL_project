import json
from sentence_transformers import util
from scripts.model_runtime import runtime


def get_top_buckets(
    unmapped_key, config_path="mapping_config.json", top_k=5, candidates=None
):

    print(f"[VECTOR SEARCH] Analyzing unmapped key: '{unmapped_key}'...")

    # 1. Load the dictionary categories
    with open(config_path, "r") as f:
        config = json.load(f)

    # We only care about the parent bucket names for routing
    target_map = config.get("normalized_indirect_cf_synonym_map", {})

    # --- TARGETED FILTERING LOGIC ---
    if candidates:
        bucket_names = [b for b in candidates if b in target_map]
    else:
        bucket_names = list(target_map.keys())

    if not bucket_names:
        print("      [ERROR] No valid buckets found for vectorization.")
        return []

    # Ensure models are loaded in VRAM
    runtime.load_models()

    if not runtime.embedder:
        print("[ERROR] BGE-M3 Embedder failed to load.")
        return []

    # 2. Vectorize directly on the GPU
    bucket_vectors = runtime.embedder.encode(bucket_names, convert_to_tensor=True)
    key_vector = runtime.embedder.encode(unmapped_key, convert_to_tensor=True)

    # 3. Calculate Cosine Similarity natively via sentence_transformers
    cosine_scores = util.cos_sim(key_vector, bucket_vectors)[0]

    # 4. Pair the scores with the bucket names
    similarities = []
    for i, score in enumerate(cosine_scores):
        similarities.append((bucket_names[i], score.item()))

    # 5. Sort by highest similarity and grab the top_k
    similarities.sort(key=lambda x: x[1], reverse=True)
    actual_k = min(top_k, len(similarities))
    top_matches = [match[0] for match in similarities[:actual_k]]

    print(f"[VECTOR SEARCH] Top {actual_k} matches: {top_matches}")
    return top_matches


if __name__ == "__main__":
    test_key = "ProceedsFromMaturitiesOfInvestments"

    print("\n--- Standard Search ---")
    matches = get_top_buckets(test_key)

    print("\n--- Targeted Search (Investing Only) ---")
    targeted_matches = get_top_buckets(
        test_key,
        candidates=[
            "CapExPurchaseOfPPE",
            "PurchaseSaleOfInvestments",
            "OtherInvestingActivities",
        ],
    )
