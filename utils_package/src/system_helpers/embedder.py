from sentence_transformers import SentenceTransformer
from functools import lru_cache


@lru_cache()
def get_model(name_model: str = "all-MiniLM-L6-v2") -> SentenceTransformer:
    try:
        return SentenceTransformer(name_model)
    except Exception as e:
        raise RuntimeError(f"Error loading SentenceTransformer model: {e}")


def get_embedding(text: str):
    model = get_model()
    return model.encode(text)
