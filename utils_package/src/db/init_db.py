from qdrant_client import QdrantClient, models
from system_settings.db_settings import dbsettings as settings


def get_qdrant_client() -> QdrantClient:
    """
    Get qdrant client
    """
    if settings.qdrant_api_key:
        return QdrantClient(
            url=f"https://{settings.qdrant_host}",
            api_key=settings.qdrant_api_key,
        )
    return QdrantClient(
        host=settings.qdrant_host,
        port=settings.qdrant_port,
    )

