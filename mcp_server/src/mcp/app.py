import argparse
from mcp.server.fastmcp import FastMCP

from qdrant_client.models import Filter, FieldCondition, MatchValue
from db.init_db import get_qdrant_client
from system_settings.db_settings import dbsettings
from system_settings.mcp_settings import mcpsettings
from system_helpers.embedder import get_embedding


mcp = FastMCP(mcpsettings.get_mcp_uri())


@mcp.tool()
def semantic_search(
    query: str,
    limit: int = 5,
    type_filter: str | None = None,
    score_threshold: float = 0.8,
) -> list[dict[str, object]]:
    """
    Perform a semantic search on the Qdrant database with the given query and parameters.

    Args:
        query (str): The query to search for.
        limit (int, optional): The maximum number of results to return. Defaults to 5.
        type_filter (str | None, optional): The type of objects to filter the search to. Defaults to None.

    Returns:
        list[dict[str, object]]: A list of objects containing the search results.

    """
    # 1) Client
    client = get_qdrant_client()

    # 2) Embedding
    embeds = get_embedding([query])
    if not embeds or not embeds[0]:
        raise ValueError("get_embedding devolvió vacío.")
    qvec = embeds[0]

    # 3) Optional filter
    qfilter = None
    if type_filter:
        qfilter = Filter(
            must=[FieldCondition(key="type", match=MatchValue(value=type_filter))]
        )

    # 4) search
    hits = client.search(
        collection_name=dbsettings.qdrant_collection,
        query_vector=qvec,
        query_filter=qfilter,
        limit=limit,
        score_threshold=score_threshold,
        with_payload=True,
    )

    # 5) normalize
    results: list[dict[str, object]] = []
    for h in hits:
        payload = h.payload or {}
        results.append(
            {
                "id": str(getattr(h, "id", None)),
                "score": round(float(h.score), 4) if h.score is not None else None,
                "title": payload.get("title"),
                "type": payload.get("type"),
                "year": payload.get("year"),
                "genres": payload.get("genre"),
                "rating_num": payload.get("rating_num"),
                "content_rating": payload.get("content_rating"),
            }
        )
    return results


if __name__ == "__main__":
    # ensure_collection()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--server_type", type=str, default="sse", choices=["sse", "stdio"]
    )
    args = parser.parse_args()
    mcp.run(args.server_type)
