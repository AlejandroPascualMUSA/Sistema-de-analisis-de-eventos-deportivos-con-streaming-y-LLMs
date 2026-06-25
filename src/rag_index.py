"""Construccion y consulta del indice RAG local.

Indexa documentos Markdown de data/docs y permite recuperar fragmentos
contextuales para enriquecer los informes sin depender de servicios externos.
"""

from __future__ import annotations

import argparse
import pickle
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

try:
    from src.config import DOCS_DIR, INDEX_FILE
except ModuleNotFoundError:
    from config import DOCS_DIR, INDEX_FILE


@dataclass
# Representa un fragmento documental indexable por el RAG.
# Representa un fragmento recuperable del corpus documental usado por el RAG.
class DocumentChunk:
    doc_id: str
    source: str
    title: str
    text: str


# Normaliza espacios para que los fragmentos queden compactos y comparables.
# Normaliza espacios y saltos de linea para mejorar el indexado del RAG.
def _clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


# Evita indexar encabezados o metadatos que no aportan contenido analitico.
# Detecta lineas que son solo encabezados o metadatos y no aportan contexto semantico.
def _is_metadata_or_heading(paragraph: str) -> bool:
    """Return True for chunks that should help neither retrieval nor reporting.

    Earlier versions indexed headings such as "# Barcelona" and metadata blocks
    such as "tipo_documento: equipo\nequipo: Barcelona". TF-IDF can rank those
    highly for team queries, which makes the final report display useless RAG
    snippets. This function removes those chunks before building the index.
    """
    raw = paragraph.strip()
    if not raw:
        return True

    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    if not lines:
        return True

    # Encabezado Markdown puro: "# Barcelona", "## Guia de calidad", etc.
    if len(lines) == 1 and lines[0].startswith("#"):
        return True

    normalized = _clean_text(raw.lower())

    # Bloques formados solo por metadatos clave-valor.
    metadata_prefixes = (
        "tipo_documento",
        "tipo documento",
        "equipo:",
        "equipo =",
        "categoria:",
        "categoría:",
        "fuente:",
        "source:",
        "tags:",
    )
    metadata_like_lines = 0
    for line in lines:
        lower = line.lower()
        if any(lower.startswith(prefix) for prefix in metadata_prefixes):
            metadata_like_lines += 1
        elif re.match(r"^[a-záéíóúüñ_ -]{2,30}\s*[:=]\s*.+$", lower):
            metadata_like_lines += 1
    if metadata_like_lines == len(lines):
        return True

    # Parrafos muy cortos con forma de titulo generan chunks poco utiles.
    word_count = len(re.findall(r"\w+", normalized))
    if word_count <= 3 and not any(term in normalized for term in ["xg", "obv", "spark", "kafka"]):
        return True

    return False


# Elimina lineas tecnicas de los documentos antes de guardarlas en el indice.
# Elimina encabezados/metadatos antes de crear chunks recuperables.
def _strip_non_content_lines(paragraph: str) -> str:
    """Elimina metadatos/encabezados de parrafos mixtos manteniendo el contenido."""
    kept: list[str] = []
    for line in paragraph.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        lower = stripped.lower()
        if stripped.startswith("#"):
            continue
        if lower.startswith((
            "tipo_documento:",
            "tipo documento:",
            "equipo:",
            "categoria:",
            "categoría:",
            "fuente:",
            "source:",
            "tags:",
        )):
            continue
        kept.append(stripped)
    return _clean_text(" ".join(kept))


# Lee documentos Markdown, los divide en fragmentos utiles y asigna metadatos.
# El corpus RAG se construye desde Markdown local, no desde internet.
# Lee data/docs y crea fragmentos documentales utiles para el indice.
def load_markdown_documents(docs_dir: str | Path) -> list[DocumentChunk]:
    docs_path = Path(docs_dir)
    chunks: list[DocumentChunk] = []
    for path in sorted(docs_path.glob("*.md")):
        text = path.read_text(encoding="utf-8")
        title = path.stem.replace("_", " ").title()
        first_heading = next((line.strip("# ").strip() for line in text.splitlines() if line.startswith("#")), None)
        if first_heading:
            title = first_heading

        paragraphs = []
        for part in re.split(r"\n\s*\n", text):
            cleaned = _strip_non_content_lines(part)
            if not cleaned:
                continue
            if _is_metadata_or_heading(part):
                continue
            paragraphs.append(cleaned)

        for index, paragraph in enumerate(paragraphs, start=1):
            chunks.append(
                DocumentChunk(
                    doc_id=f"{path.stem}_{index}",
                    source=str(path),
                    title=title,
                    text=paragraph,
                )
            )
    return chunks


# Construye el indice TF-IDF local a partir de los documentos de data/docs.
# Construye y guarda el indice TF-IDF usado por el RAG local.
def build_index(docs_dir: str | Path = DOCS_DIR, index_file: str | Path = INDEX_FILE) -> dict[str, Any]:
    chunks = load_markdown_documents(docs_dir)
    if not chunks:
        raise RuntimeError(f"No markdown documents with indexable content found in {docs_dir}")

    vectorizer = TfidfVectorizer(
        lowercase=True,
        ngram_range=(1, 2),
        min_df=1,
        stop_words=None,
    )
    matrix = vectorizer.fit_transform([chunk.text for chunk in chunks])
    payload = {
        "backend": "tfidf_vector_index",
        "chunks": [asdict(chunk) for chunk in chunks],
        "vectorizer": vectorizer,
        "matrix": matrix,
    }

    index_path = Path(index_file)
    index_path.parent.mkdir(parents=True, exist_ok=True)
    with index_path.open("wb") as file:
        pickle.dump(payload, file)
    return payload


# Carga el indice existente o lo reconstruye si falta o se fuerza la regeneracion.
# Carga el indice existente o lo reconstruye si no existe.
def load_index(index_file: str | Path = INDEX_FILE, docs_dir: str | Path = DOCS_DIR, force_rebuild: bool = False) -> dict[str, Any]:
    index_path = Path(index_file)
    if force_rebuild or not index_path.exists():
        return build_index(docs_dir=docs_dir, index_file=index_file)
    with index_path.open("rb") as file:
        return pickle.load(file)


# Recupera los fragmentos mas relevantes para una consulta concreta.
# Recupera los k fragmentos mas relevantes para una consulta textual.
def retrieve_context(
    query: str,
    k: int = 4,
    index_file: str | Path = INDEX_FILE,
    docs_dir: str | Path = DOCS_DIR,
) -> list[dict[str, Any]]:
    payload = load_index(index_file=index_file, docs_dir=docs_dir)
    raw_chunks = payload["chunks"]
    vectorizer: TfidfVectorizer = payload["vectorizer"]
    matrix = payload["matrix"]

    query_vector = vectorizer.transform([query])
    scores = cosine_similarity(query_vector, matrix).flatten()
    if scores.size == 0:
        return []

    best_indices = np.argsort(scores)[::-1][:k]
    results = []
    for idx in best_indices:
        chunk = raw_chunks[int(idx)]
        if isinstance(chunk, DocumentChunk):
            chunk_dict = asdict(chunk)
        else:
            chunk_dict = dict(chunk)
        results.append(
            {
                "doc_id": chunk_dict.get("doc_id", ""),
                "source": chunk_dict.get("source", ""),
                "title": chunk_dict.get("title", ""),
                "score": float(scores[int(idx)]),
                "text": chunk_dict.get("text", ""),
            }
        )
    return results


# Argumentos para reconstruir el indice desde consola.
# Define los argumentos de linea de comandos para ejecutar este modulo de forma flexible.
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build or query the local RAG index")
    parser.add_argument("--docs-dir", default=str(DOCS_DIR))
    parser.add_argument("--index-file", default=str(INDEX_FILE))
    parser.add_argument("--query", default="")
    parser.add_argument("--k", type=int, default=4)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


# Entrada CLI para generar o regenerar el indice RAG.
# Punto de entrada del modulo: conecta las piezas y ejecuta el flujo principal.
def main() -> None:
    args = parse_args()
    if args.force or not Path(args.index_file).exists():
        payload = build_index(args.docs_dir, args.index_file)
        print(f"Index written to {args.index_file}")
        print(f"Indexed chunks: {len(payload['chunks'])}")
    if args.query:
        for item in retrieve_context(query=args.query, k=args.k, index_file=args.index_file, docs_dir=args.docs_dir):
            print(f"[{item['score']:.3f}] {item['title']} - {item['source']}")
            print(item["text"])
            print()


if __name__ == "__main__":
    main()
