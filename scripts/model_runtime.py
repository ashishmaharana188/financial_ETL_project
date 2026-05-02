import json
import requests
import math


class PureOllamaRuntime:
    def __init__(self, config_path="mapping_config.json"):
        self.config_path = config_path
        self.embed_url = "http://localhost:11434/api/embeddings"
        self.generate_url = "http://localhost:11434/api/generate"

        self.dictionary_names = []
        self.dictionary_vectors = []
        self.is_loaded = False

    def get_embedding(self, text):
        """Fetches a vector embedding from Ollama."""
        payload = {
            "model": "bge-m3",
            "prompt": text,
            "keep_alive": "5m",  # Keeps BGE-M3 hot in VRAM
        }
        response = requests.post(self.embed_url, json=payload)
        response.raise_for_status()
        return response.json()["embedding"]

    def _cosine_similarity(self, v1, v2):
        """Pure Python cosine similarity (Zero dependencies required)."""
        dot_product = sum(a * b for a, b in zip(v1, v2))
        magnitude1 = math.sqrt(sum(a * a for a in v1))
        magnitude2 = math.sqrt(sum(b * b for b in v2))
        if magnitude1 == 0 or magnitude2 == 0:
            return 0.0
        return dot_product / (magnitude1 * magnitude2)

    def initialize_cognitive_engine(self):
        """PRE-VECTORIZES the dictionary ONCE using Ollama."""
        if self.is_loaded:
            return

        print("\n[AI ENGINE] Booting 100% Ollama Architecture...")
        print("  -> Pre-vectorizing dictionary via BGE-M3...")

        with open(self.config_path, "r") as f:
            config = json.load(f)

        self.dictionary_names = list(
            config.get("normalized_indirect_cf_synonym_map", {}).keys()
        )

        # Vectorize each bucket name once at startup
        for name in self.dictionary_names:
            vector = self.get_embedding(name)
            self.dictionary_vectors.append(vector)

        self.is_loaded = True
        print("[AI ENGINE] System Ready. Dictionary cached in RAM.\n")

    def find_nearest_buckets(self, unmapped_key, top_k=8):
        """Instantly compares a new key against the pre-computed dictionary."""
        # 1. Vectorize just the single missing key via Ollama
        key_vector = self.get_embedding(unmapped_key)

        # 2. Compare against cached dictionary vectors
        similarities = []
        for i, dict_vec in enumerate(self.dictionary_vectors):
            score = self._cosine_similarity(key_vector, dict_vec)
            similarities.append((self.dictionary_names[i], score))

        similarities.sort(key=lambda x: x[1], reverse=True)
        return [match[0] for match in similarities[:top_k]]

    def process_with_phi3(self, unmapped_key, candidate_buckets):
        """Passes the top candidates to Ollama and fuzzy-matches the result."""
        print(f"      [OLLAMA] Classifying: '{unmapped_key}'...")

        prompt = f"""<|system|>
You are an expert accounting data engineer. Your job is to map unmapped keys to the provided accounting buckets.
Respond ONLY with a valid JSON object. Format: {{"Category": "The selected bucket name"}}<|end|>
<|user|>
Unmapped Key: "{unmapped_key}"
Candidate Buckets: {candidate_buckets}<|end|>
<|assistant|>"""

        payload = {
            "model": "phi3:mini",
            "prompt": prompt,
            "format": "json",
            "stream": False,
            "keep_alive": "5m",
            "options": {"temperature": 0.0, "num_ctx": 2048},
        }

        try:
            response = requests.post(self.generate_url, json=payload)
            response.raise_for_status()

            result_json = json.loads(response.json()["response"])
            selected_category = str(result_json.get("Category", "")).strip()

            # 1. Try Exact Match First
            if selected_category in candidate_buckets:
                print(f"      [OLLAMA] Selected: {selected_category}")
                return {selected_category: [unmapped_key]}

            # 2. Try Fuzzy Match (Lowercased and spaces removed)
            clean_selected = selected_category.replace(" ", "").lower()
            for bucket in candidate_buckets:
                clean_bucket = bucket.replace(" ", "").lower()
                if clean_selected == clean_bucket or clean_selected in clean_bucket:
                    print(
                        f"      [OLLAMA] Fuzzy Matched: '{selected_category}' -> {bucket}"
                    )
                    return {bucket: [unmapped_key]}

            # 3. If it completely failed, tell us exactly what it said
            print(
                f"      [WARNING] Ollama Hallucinated/Failed: '{selected_category}' (Not in candidates)"
            )
            return None

        except Exception as e:
            print(f"      [ERROR] Ollama Inference Failed: {str(e)}")
            return None

    def purge_memory(self):
        """Forces Ollama to instantly drop both models from your 4GB VRAM."""
        if not self.is_loaded:
            return
        print("\n[AI ENGINE] Batch complete. Purging Ollama VRAM...")

        try:
            # Sending keep_alive: 0 instantly unloads the models
            requests.post(self.embed_url, json={"model": "bge-m3", "keep_alive": 0})
            requests.post(
                self.generate_url, json={"model": "phi3:mini", "keep_alive": 0}
            )
        except:
            pass

        self.dictionary_vectors = []
        self.is_loaded = False
        print("[AI ENGINE] VRAM clear. Hardware returned to OS.\n")


runtime = PureOllamaRuntime()
