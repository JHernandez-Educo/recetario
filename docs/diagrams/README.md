# Diagrams

Every image in the README is generated from code, so it stays in sync with the platform.

| File | Source | Rebuild |
|---|---|---|
| `docs/images/platform-overview.svg` | `src/diagrams.py` | `uv run python docs/diagrams/src/diagrams.py` |
| `docs/erd/Recetario.png` | `star_schema.dbml` | Paste the DBML into [dbdiagram.io](https://dbdiagram.io) and export |

**Themes:** `midnight` is the default. Pass `light` or `dark` as extra arguments for variants.

**Logos:** the files in `src/logos/` come from three places:
- the Dagster project's tool icon set (Apache-2.0 repository);
- [Simple Icons](https://simpleicons.org) (CC0);
- the official DuckLake site.

Every logo is a trademark of its owner, used here for identification only.
