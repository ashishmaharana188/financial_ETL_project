import json
from sentence_transformers import util
from scripts.model_runtime import runtime


def get_top_buckets(unmapped_key, config_path="mapping_config.json", top_k=5):
    """
    Uses BGE-M3 (via PyTorch) to find the top mathematically similar buckets for a given key.
    """
    print(f"[VECTOR SEARCH] Analyzing unmapped key: '{unmapped_key}'...")

    # 1. Load the dictionary categories
    with open(config_path, "r") as f:
        config = json.load(f)

    # We only care about the parent bucket names for routing
    target_map = config.get("normalized_indirect_cf_synonym_map", {})
    bucket_names = list(target_map.keys())

    if not bucket_names:
        print("      [ERROR] No buckets found in mapping_config.json.")
        return []

    # Ensure models are loaded in VRAM
    runtime.load_models()

    if not runtime.embedder:
        print("[ERROR] BGE-M3 Embedder failed to load.")
        return []

    # 2. Vectorize directly on the GPU
    # convert_to_tensor=True keeps the math inside the VRAM, bypassing the CPU entirely
    bucket_vectors = runtime.embedder.encode(bucket_names, convert_to_tensor=True)
    key_vector = runtime.embedder.encode(unmapped_key, convert_to_tensor=True)

    # 3. Calculate Cosine Similarity natively via sentence_transformers
    # This returns a tensor of scores representing the distance
    cosine_scores = util.cos_sim(key_vector, bucket_vectors)[0]

    # 4. Pair the scores with the bucket names
    similarities = []
    for i, score in enumerate(cosine_scores):
        similarities.append((bucket_names[i], score.item()))

    # 5. Sort by highest similarity and grab the top_k
    similarities.sort(key=lambda x: x[1], reverse=True)
    top_matches = [match[0] for match in similarities[:top_k]]

    print(f"[VECTOR SEARCH] Top {top_k} matches: {top_matches}")
    return top_matches


# Example usage (for testing the file directly):
if __name__ == "__main__":
    test_key = "ProceedsFromMaturitiesOfInvestments"
    matches = get_top_buckets(test_key)
    print(f"Matches for {test_key}: {matches}")
