"""
Populate a Qdrant collection from CSV datasets (movies / movies+TV).

- Loads two datasets (paths or directories) with Polars.
- Unifies rows into `MediaItem` entries.
- Builds text corpora, generates embeddings, and upserts to Qdrant in batches.
- Creates (or validates) the Qdrant collection dimension automatically from the first batch.

You can run it as a script:
    python populate_qdrant.py --movies /path/to/movies_csv_or_dir --mixed /path/to/mixed_csv_or_dir

If no CLI args are provided, it will fall back to downloading Kaggle datasets.
"""

from pathlib import Path
from typing import Sequence
import argparse
import hashlib
import ast
import re

import numpy as np
import polars as pl
import kagglehub

from qdrant_client.models import (
    OptimizersConfigDiff,
    VectorParams,
    Distance,
    PointStruct,
)

from db.init_db import get_qdrant_client
from system_helpers.embedder import get_embedding
from system_config.entities import MediaItem
from system_settings.db_settings import dbsettings
from system_config.logger import get_logger

# ----------------------- Config -----------------------

log = get_logger("qdrant_populator")

BATCH_SIZE = 512
DURATION_RE = re.compile(r"(\d+)\s*min", re.IGNORECASE)

# ----------------------- Utils -----------------------


def as_path_glob(p: str | Path) -> str:
    """
    Return a glob string for CSV discovery.

    - If `p` is a file ending in .csv, returns the absolute file path.
    - If `p` is a directory, returns a recursive '**/*.csv' pattern under it.

    Args:
        p: File or directory path.

    Returns:
        Glob pattern or file path as string.
    """
    p = Path(p).expanduser().resolve()
    if p.is_file() and p.suffix.lower() == ".csv":
        return str(p)
    return str(p / "**" / "*.csv")


def is_null(v) -> bool:
    """
    NaN-safe null checker.

    Args:
        v: Any value.

    Returns:
        True if `v` is None or NaN (float); False otherwise.
    """
    return v is None or (isinstance(v, float) and np.isnan(v))


def split_csv_list(v) -> list[str]:
    """
    Split a CSV-like string into a list of trimmed tokens.

    Examples:
        'Action, Comedy, Drama' -> ['Action', 'Comedy', 'Drama']

    Args:
        v: Value to split.

    Returns:
        A list of strings (possibly empty).
    """
    return [] if is_null(v) else [x.strip() for x in str(v).split(",") if x.strip()]


def parse_listish(v) -> list[str]:
    """
    Parse list-like fields that may come as a Python-like list string or CSV.

    Examples:
        "[Actor1, Actor2, Actor3]" -> ['Actor1', 'Actor2', 'Actor3']
        "Actor1, Actor2"           -> ['Actor1', 'Actor2']

    Args:
        v: Value to parse.

    Returns:
        A list of trimmed strings (possibly empty).
    """
    if is_null(v):
        return []
    s = str(v).strip()
    try:
        parsed = ast.literal_eval(s)
        if isinstance(parsed, list):
            return [
                str(x).strip().rstrip(",") for x in parsed if str(x).strip().rstrip(",")
            ]
    except Exception:
        # Fallback to CSV split if not a literal list
        pass
    return [x.strip().rstrip(",") for x in s.split(",") if x.strip().rstrip(",")]


def parse_duration_minutes(v) -> int | None:
    """
    Extract minutes from strings like '30 min', '67 Min', etc.

    Notes:
        Returns None for season-based values ('1 Season', '2 Seasons', etc.) or when no minutes found.

    Args:
        v: Value with potential 'min' pattern.

    Returns:
        Integer minutes or None.
    """
    if is_null(v):
        return None
    m = DURATION_RE.search(str(v))
    return int(m.group(1)) if m else None


def build_corpus(item: MediaItem) -> str:
    """
    Compose a text corpus for embedding from a MediaItem.

    The corpus concatenates several fields to provide rich semantic context.

    Args:
        item: MediaItem instance.

    Returns:
        String corpus to be embedded.
    """
    parts = [
        item.title or "",
        item.director or "",
        ", ".join(item.cast or []),
        ", ".join(item.genre or []),
        item.description or "",
        item.type or "",
        f"{item.duration_min} min" if item.duration_min else "",
    ]
    # Filter empty parts to avoid redundant separators
    return " | ".join(p for p in parts if p)


def norm_embeddings(embeds, nrows: int) -> list[list[float]]:
    """
    Normalize embeddings into a `list[list[float]]` with shape (nrows, dim).

    Accepts:
        - numpy.ndarray (2D)
        - list[list[float]]
        - list[np.ndarray 1D] / list[Sequence[float]]

    Args:
        embeds: Embeddings as produced by the embedder.
        nrows: Expected number of rows.

    Returns:
        Embeddings as list of lists of floats.

    Raises:
        RuntimeError/TypeError if shape or types are not supported.
    """
    if embeds is None:
        raise RuntimeError("Embedding result is None.")

    if isinstance(embeds, np.ndarray):
        if embeds.ndim != 2 or embeds.shape[0] != nrows or embeds.shape[1] == 0:
            raise RuntimeError(
                f"Bad ndarray shape {embeds.shape}; expected (nrows, dim)=({nrows}, D>0)"
            )
        return embeds.astype(float).tolist()

    if isinstance(embeds, list):
        if len(embeds) != nrows or len(embeds) == 0:
            raise RuntimeError(f"Bad list length {len(embeds)} vs expected {nrows}")
        out: list[list[float]] = []
        for e in embeds:
            if isinstance(e, np.ndarray):
                if e.ndim != 1:
                    raise RuntimeError("Each embedding array must be 1D.")
                out.append(e.astype(float).tolist())
            else:
                vec = [float(x) for x in list(e)]
                if not vec:
                    raise RuntimeError("Zero-dimension embedding vector.")
                out.append(vec)
        return out

    raise TypeError(f"Unsupported embeddings type: {type(embeds)}")


def chunked(seq, size: int):
    """
    Yield fixed-size chunks from a sequence.

    Args:
        seq: Input sequence.
        size: Chunk size (> 0).

    Yields:
        Slices of `seq` with up to `size` elements.

    Raises:
        ValueError if `size <= 0`.
    """
    if size <= 0:
        raise ValueError("size must be > 0")
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


# ----------------------- Loading (Polars) -----------------------


def load_unified(csv_movies_only: str | Path, csv_mixed: str | Path) -> list[MediaItem]:
    """
    Load and unify two datasets into a list of MediaItem.

    Dataset 1 (movies only): expected columns ['title', 'stars', 'genre', 'description', 'duration'].
    Dataset 2 (mixed movies/TV): expected columns ['title', 'director', 'cast', 'listed_in', 'description', 'duration', 'type'].

    Both inputs accept either a single CSV file or a directory (recursively globbed).

    Args:
        csv_movies_only: Path to the movies-only CSV or directory.
        csv_mixed: Path to the mixed movies/TV CSV or directory.

    Returns:
        List of MediaItem instances (possibly empty on read failures).
    """
    pat1, pat2 = as_path_glob(csv_movies_only), as_path_glob(csv_mixed)

    try:
        df1 = pl.read_csv(
            pat1, glob=True, ignore_errors=True, quote_char='"', has_header=True
        )
    except Exception as e:
        log.warning("Failed to read dataset 1 (movies only): %s", e)
        df1 = pl.DataFrame()

    try:
        df2 = pl.read_csv(
            pat2, glob=True, ignore_errors=True, quote_char='"', has_header=True
        )
    except Exception as e:
        log.warning("Failed to read dataset 2 (mixed): %s", e)
        df2 = pl.DataFrame()

    unified: list[MediaItem] = []

    # ---- Dataset 1 (Movies) ----
    if df1.height > 0:
        cols1 = [
            c
            for c in ["title", "stars", "genre", "description", "duration"]
            if c in df1.columns
        ]
        for r in df1.select(cols1).to_dicts():
            unified.append(
                MediaItem(
                    title=r.get("title") or None,
                    director=None,  # Not available in this dataset
                    cast=parse_listish(r.get("stars")),
                    genre=split_csv_list(r.get("genre")),
                    description=r.get("description") or None,
                    duration_min=parse_duration_minutes(r.get("duration")),
                    type="Movie",
                )
            )

    # ---- Dataset 2 (Movies or TV) ----
    if df2.height > 0:
        cols2 = [
            c
            for c in [
                "title",
                "director",
                "cast",
                "listed_in",
                "description",
                "duration",
                "type",
            ]
            if c in df2.columns
        ]
        for r in df2.select(cols2).to_dicts():
            unified.append(
                MediaItem(
                    title=r.get("title") or None,
                    director=r.get("director") or None,
                    cast=parse_listish(r.get("cast")),
                    genre=split_csv_list(r.get("listed_in")),
                    description=r.get("description") or None,
                    duration_min=parse_duration_minutes(r.get("duration")),
                    type=r.get("type") or None,
                )
            )

    return unified


# ----------------------- Qdrant helpers -----------------------


def create_or_validate_collection(client, name: str, dim: int) -> None:
    """
    Ensure the Qdrant collection exists with the correct vector dimension.

    If the collection exists and the dimension differs, raises a RuntimeError.
    If it doesn't exist, it is created with COSINE distance and a memmap optimizer threshold.

    Args:
        client: Qdrant client instance.
        name: Collection name.
        dim: Embedding dimension.

    Raises:
        RuntimeError if an existing collection has a mismatched dimension.
    """
    try:
        info = client.get_collection(name)
        try:
            # Newer client/server
            existing = info.config.params.vectors.size
        except Exception:
            # Backward compat with dict-like params
            vectors = info.config.params.vectors
            existing = vectors.get("size") if isinstance(vectors, dict) else None

        if existing is not None and existing != dim:
            raise RuntimeError(
                f"Collection '{name}' has dim={existing}, embeddings dim={dim} (mismatch)."
            )

        log.info("Collection '%s' OK (dim=%s).", name, existing or dim)
    except Exception:
        log.info("Creating collection '%s' (dim=%d).", name, dim)
        client.recreate_collection(
            collection_name=name,
            vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
            optimizers_config=OptimizersConfigDiff(memmap_threshold=20000),
        )


def point_id(text: str) -> str:
    """
    Build a deterministic point ID from a text corpus (MD5 hex).

    Notes:
        Deterministic IDs avoid duplicates on re-runs. If you prefer non-deterministic
        IDs, swap this for `str(uuid4())`.

    Args:
        text: Text corpus used to compute the ID.

    Returns:
        Hexadecimal MD5 string.
    """
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def to_points(
    items: Sequence[MediaItem], embeds: Sequence[Sequence[float]]
) -> list[PointStruct]:
    """
    Convert MediaItems and their embeddings into Qdrant PointStruct list.

    - Cleans payload by dropping None/empty values.
    - Uses deterministic IDs based on the text corpus (see `point_id`).

    Args:
        items: Sequence of MediaItem objects.
        embeds: Sequence of embedding vectors (same length as items).

    Returns:
        List of PointStruct ready for upsert.

    Raises:
        ValueError if lengths differ.
    """
    if len(items) != len(embeds):
        raise ValueError(f"Rows {len(items)} != Embeds {len(embeds)}")

    pts: list[PointStruct] = []
    for it, vec in zip(items, embeds):
        corpus = build_corpus(it)
        # Ensure list[float]
        if isinstance(vec, np.ndarray):
            vec = vec.astype(float).tolist()
        else:
            vec = [float(x) for x in vec]

        payload = {
            "title": it.title,
            "director": it.director,
            "cast": it.cast,
            "genre": it.genre,
            "description": it.description,
            "duration_min": it.duration_min,
            "type": it.type,
        }
        # Drop empty/None to keep payload compact
        payload = {k: v for k, v in payload.items() if v not in (None, "", [], {})}

        pts.append(
            PointStruct(
                id=point_id(corpus),  # or: id=str(uuid4()) for non-deterministic IDs
                vector=vec,
                payload=payload,
            )
        )
    return pts


# ----------------------- Orchestrator -----------------------


def create_emb_db_from_csvs(
    csv_movies_only: str | Path, csv_mixed_movies_tv: str | Path
) -> int:
    """
    Load datasets, embed, and upsert to a Qdrant collection in batches.

    Steps:
        1) Load & unify rows into MediaItem.
        2) Generate embeddings for the first batch to get the dimension.
        3) Create or validate Qdrant collection.
        4) Upsert first batch, then the remaining data by chunks.

    Args:
        csv_movies_only: Path to movies-only dataset (file or directory).
        csv_mixed_movies_tv: Path to mixed dataset (file or directory).

    Returns:
        Total number of upserted items.
    """
    data = load_unified(csv_movies_only, csv_mixed_movies_tv)
    if not data:
        log.info("No records to index.")
        return 0

    client = get_qdrant_client()

    # First batch (at least 1 row) to determine embedding dimension
    first_batch = data[: min(BATCH_SIZE, len(data))]
    first_corpora = [build_corpus(x) for x in first_batch]
    first_emb = norm_embeddings(get_embedding(first_corpora), nrows=len(first_batch))
    dim = len(first_emb[0])

    # Ensure collection exists with correct dimension
    create_or_validate_collection(client, dbsettings.qdrant_collection, dim)

    # Upsert first batch
    client.upsert(
        collection_name=dbsettings.qdrant_collection,
        points=to_points(first_batch, first_emb),
        wait=True,
    )

    # Upsert remaining data
    remaining = data[len(first_batch) :]
    if remaining:
        # tqdm optional (not strictly required to import globally)
        try:
            import tqdm as _tqdm  # local import to avoid hard dependency if not installed

            iter_batches = _tqdm.tqdm(
                list(chunked(remaining, BATCH_SIZE)),
                desc="Upserting remaining batches",
                total=(len(remaining) + BATCH_SIZE - 1) // BATCH_SIZE,
            )
        except Exception:
            iter_batches = chunked(remaining, BATCH_SIZE)

        for batch in iter_batches:
            corpora = [build_corpus(x) for x in batch]
            embeds = norm_embeddings(get_embedding(corpora), nrows=len(batch))
            client.upsert(
                collection_name=dbsettings.qdrant_collection,
                points=to_points(batch, embeds),
                wait=True,
            )

    log.info(
        "Upsert completed in '%s' with %d elements.",
        dbsettings.qdrant_collection,
        len(data),
    )
    return len(data)


# ----------------------- CLI / Script entry -----------------------


def _download_default_kaggle() -> tuple[str, str]:
    """
    Download default Kaggle datasets if no CLI paths are provided.

    Returns:
        Tuple (movies_only_path, mixed_path) as directory paths returned by kagglehub.

    Notes:
        - 'payamamanat/imbd-dataset' (movies)
        - 'shivamb/netflix-shows'   (mixed movies/TV)
    """
    log.info("No CLI paths provided. Downloading Kaggle datasets...")
    imbd_path = kagglehub.dataset_download("payamamanat/imbd-dataset")
    netflix_path = kagglehub.dataset_download("shivamb/netflix-shows")
    return imbd_path, netflix_path


def main() -> None:
    """
    CLI entry-point.

    Options:
        -m/--movies: Path to movies-only CSV file or directory.
        -x/--mixed : Path to mixed movies/TV CSV file or directory.

    If neither is provided, default Kaggle datasets are downloaded.
    """
    parser = argparse.ArgumentParser(
        description="Populate Qdrant DB using CSV files (movies and mixed datasets)."
    )
    parser.add_argument(
        "-m", "--movies", type=str, help="Path to movies-only CSV file or directory"
    )
    parser.add_argument(
        "-x", "--mixed", type=str, help="Path to mixed movies/TV CSV file or directory"
    )
    args = parser.parse_args()

    if args.movies and args.mixed:
        movies_path, mixed_path = args.movies, args.mixed
    else:
        movies_path, mixed_path = _download_default_kaggle()

    total = create_emb_db_from_csvs(movies_path, mixed_path)
    log.info("Done. Total indexed: %d", total)


if __name__ == "__main__":
    main()
