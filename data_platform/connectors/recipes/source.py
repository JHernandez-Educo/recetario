"""dlt source for extracted recipe JSON files.

The extraction pipeline (Julio's: OCR/vision over PDFs + transcripts) drops one
JSON file per recipe into data_lake/recipes/incoming/, following the contract
in recipe.schema.json (see docs/PRD-v2-recipes.md section 2).

This loader:
- validates every file against the JSON Schema contract
- invalid file -> moved to rejected/ + loud log (never silently dropped)
- valid rows -> raw_recipes dataset; `lines` and `sub_recipes` become dlt
  child tables (recipes__lines, recipes__sub_recipes) automatically
- after a SUCCESSFUL load the caller moves the read files to processed/
  via archive_incoming() — files are never moved before the load commits
"""

import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import dlt
import jsonschema

logger = logging.getLogger("recipes")

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RECIPES_DIR = PROJECT_ROOT / "data_lake" / "recipes"
INCOMING_DIR = RECIPES_DIR / "incoming"
PROCESSED_DIR = RECIPES_DIR / "processed"
REJECTED_DIR = RECIPES_DIR / "rejected"

_SCHEMA = json.loads((Path(__file__).parent / "recipe.schema.json").read_text())


def _reject(path: Path, reason: str) -> None:
    REJECTED_DIR.mkdir(parents=True, exist_ok=True)
    target = REJECTED_DIR / path.name
    shutil.move(str(path), str(target))
    logger.error("REJECTED recipe file %s -> %s (%s)", path.name, target, reason)


def archive_incoming() -> int:
    """Move every remaining incoming file to processed/. Call ONLY after a
    successful pipeline run: files still in incoming/ at that point were all
    validated and loaded (invalid ones were already moved to rejected/)."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    moved = 0
    for path in sorted(INCOMING_DIR.glob("*.json")):
        shutil.move(str(path), str(PROCESSED_DIR / f"{stamp}_{path.name}"))
        moved += 1
    return moved


@dlt.source(name="recipes")
def recipe_files():
    @dlt.resource(name="recipes", write_disposition="merge", primary_key=("recipe_name", "source_document"))
    def recipes() -> Iterator[dict]:
        INCOMING_DIR.mkdir(parents=True, exist_ok=True)
        for path in sorted(INCOMING_DIR.glob("*.json")):
            try:
                data = json.loads(path.read_text())
            except json.JSONDecodeError as exc:
                _reject(path, f"not valid JSON: {exc}")
                continue
            try:
                jsonschema.validate(data, _SCHEMA)
            except jsonschema.ValidationError as exc:
                _reject(path, f"contract violation: {exc.message}")
                continue

            yield {
                "recipe_name": data["recipe_name"],
                "component_role": data["component_role"],
                "category": data["category"],
                "yield_qty": data["yield"]["qty"],
                "yield_unit": data["yield"]["unit"],
                "lines": data["lines"],
                "sub_recipes": data.get("sub_recipes", []),
                "source_document": data["source_document"],
                "source_pages": data.get("source_pages"),
                "transcript_context": data.get("transcript_context"),
                "extracted_at": data["extracted_at"],
                "extractor_version": data["extractor_version"],
                "ingested_from_file": path.name,
                "ingested_at": datetime.now(timezone.utc),
            }

    return (recipes,)
