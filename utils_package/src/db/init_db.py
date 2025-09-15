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


# def ensure_collection(client: QdrantClient | None = None) -> None:
#     """
#         Ensure that a collection with given configuration exists in Qdrant.
#     """
#     if client is None:
#         client = get_qdrant_client()

#     cname = settings.qdrant_collection
#     desired = models.VectorParams(
#         size=settings.qdrant_embed_dim,
#         distance=getattr(models.Distance, settings.qdrant_distance.upper()),
#     )

#     # Exists collection ?
#     cols = client.get_collections().collections
#     names = {c.name for c in cols}
#     if cname not in names:
#         client.create_collection(collection_name=cname, vectors_config=desired)
#         return


#     info = client.get_collection(cname)
#     current = info.vectors_count

#     coll_params = info.config.params

#     vec_cfg = coll_params.vectors
#     if isinstance(vec_cfg, models.VectorParams):
#         same = (vec_cfg.size == desired.size) and (vec_cfg.distance == desired.distance)
#     else:

#         same = False

#     if same:
#         return

#     client.recreate_collection(collection_name=cname, vectors_config=desired)
