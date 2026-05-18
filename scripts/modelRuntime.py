import os
import warnings

warnings.filterwarnings("ignore", category=FutureWarning, module="transformers")
os.environ["TRANSFORMERS_VERBOSITY"] = "error"
os.environ["TOKENIZERS_PARALLELISM"] = "false"
import gc
import requests
import torch
from sentence_transformers import SentenceTransformer


class HybridRuntime:
    def __init__(self):
        self.embedder = None
        self.generate_url = "http://localhost:11434/api/generate"
        self.is_loaded = False

    def load_models(self):
        """Loads PyTorch sentence-transformers on CUDA."""
        if not self.is_loaded:
            print("\n[AI ENGINE] Booting Hybrid Architecture (PyTorch + Ollama)...")

            # Auto-detect GPU: Force CUDA if available, fallback to CPU
            device = "cuda" if torch.cuda.is_available() else "cpu"
            print(f"  -> Loading BGE-M3 Embedder via PyTorch on {device.upper()}...")

            # Load the model directly into the chosen device VRAM
            self.embedder = SentenceTransformer("BAAI/bge-m3", device=device)

            self.is_loaded = True
            print("[AI ENGINE] System Ready.\n")

    def llm(self, prompt, max_tokens=50, temperature=0.0, response_format=None):
        """
        Wraps the Ollama API to seamlessly match the interface your reasoning.py expects.
        """
        payload = {
            "model": "phi3:mini",
            "prompt": prompt,
            "stream": False,
            "keep_alive": "5m",
            "options": {"temperature": temperature, "num_predict": max_tokens},
        }

        # Enforce JSON formatting if requested by reasoning.py
        if response_format and response_format.get("type") == "json_object":
            payload["format"] = "json"

        try:
            response = requests.post(self.generate_url, json=payload)
            response.raise_for_status()

            raw_text = response.json().get("response", "")

            # Mock the response structure your reasoning.py expects
            return {"choices": [{"text": raw_text}]}
        except Exception as e:
            print(f"      [ERROR] Ollama HTTP Request Failed: {e}")
            # Return empty JSON structure to prevent downstream crashes
            return {"choices": [{"text": "{}"}]}

    def purge_memory(self):
        """Clears PyTorch VRAM and tells Ollama to unload models."""
        if not self.is_loaded:
            return

        print("\n[AI ENGINE] Batch complete. Purging VRAM...")

        # 1. Clear PyTorch/CUDA VRAM
        if self.embedder is not None:
            del self.embedder
            self.embedder = None
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
                torch.cuda.ipc_collect()

        # 2. Clear Ollama VRAM
        try:
            requests.post(
                "http://localhost:11434/api/embeddings",
                json={"model": "bge-m3", "keep_alive": 0},
            )
            requests.post(
                self.generate_url, json={"model": "phi3:mini", "keep_alive": 0}
            )
        except:
            pass

        # 3. Python Garbage Collection
        gc.collect()

        self.is_loaded = False
        print("[AI ENGINE] VRAM clear. Hardware returned to OS.\n")


# Instantiate the singleton
runtime = HybridRuntime()
