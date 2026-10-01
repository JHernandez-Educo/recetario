"""dlt source for Shopify storefronts (Mundo Huevo, La Vaquita, ...).

Shopify shops expose a public catalog feed:

    GET /products.json?limit=250&page=N   (pages until an empty page)

Unlike VTEX, every product carries `updated_at` — so this source uses dlt's
native incremental cursor: after the first full load, only products modified
since the stored watermark flow through. No custom pagination and no custom
state code; both are dlt built-ins here.

Known caveats (PRD 6.3):
- merchants can disable `products.json`; the failure mode is a 404 someday,
  and a zero-row run must be treated as an alert, never as "no changes"
- `variants[].grams` is often 0 — package size cannot be trusted from here
  (the product-mapping/alias layer owns net weight)
"""

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

logger = logging.getLogger("shopify")

PROJECT_ROOT = Path(__file__).resolve().parents[3]
LAKE_DIR = PROJECT_ROOT / "data_lake" / "shopify"

PRODUCTS_PATH = "/products.json"
PAGE_LIMIT = 250  # Shopify max per page
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
)
# Keys we drop before loading: bulky presentation data with no costing value.
_NOISE_KEYS = ("body_html", "images")


def _price_cop(value) -> Optional[int]:
    """Shopify prices arrive as strings like '12500.00' (COP)."""
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return None


@dlt.source(name="shopify", max_table_nesting=2)
def shopify_catalog(
    store: Optional[str] = None,
    stores: dict = dlt.config.value,
    request_delay: float = 1.0,
):
    """Catalog + price events for one Shopify store, or all configured stores
    when `store` is None.

    `stores` is injected from `[sources.shopify.stores]` in config.toml.
    """
    selected = [store] if store else list(stores.keys())
    captured_at = datetime.now(timezone.utc)
    run_stamp = captured_at.strftime("%Y-%m-%dT%H-%M-%SZ")

    @dlt.resource(name="products", write_disposition="merge", primary_key="id", columns={"store": {"partition": True}})
    def products(
        updated_at=dlt.sources.incremental("updated_at", initial_value="1970-01-01T00:00:00Z"),
    ) -> Iterator[dict]:
        """One row per product; dlt normalizes `variants` into a nested
        `products__variants` table automatically. The incremental cursor on
        `updated_at` means unchanged products are filtered out after page 1's
        watermark is established — true source-side change tracking."""
        for st in selected:
            client = RESTClient(
                base_url=stores[st]["base_url"],
                headers={"User-Agent": USER_AGENT},
                data_selector="products",
                paginator=PageNumberPaginator(base_page=1, page_param="page", total_path=None),
            )
            run_dir = LAKE_DIR / st / run_stamp
            for page_num, page in enumerate(client.paginate(PRODUCTS_PATH, params={"limit": PAGE_LIMIT})):
                run_dir.mkdir(parents=True, exist_ok=True)
                (run_dir / f"products_p{page_num}.json").write_bytes(page.response.content)
                if page_num == 0 and not page:
                    logger.warning("[%s] zero products returned — feed disabled or store empty?", st)
                for product in page:
                    row = {k: v for k, v in product.items() if k not in _NOISE_KEYS}
                    row["store"] = st
                    row["captured_at"] = captured_at
                    yield row
                time.sleep(request_delay)

    @dlt.transformer(data_from=products, name="price_events", write_disposition="append", columns={"store": {"partition": True}, "captured_date": {"partition": True}})
    def price_events(product_row: dict) -> Iterator[dict]:
        """Append-only price history at variant grain, same contract as the
        VTEX `price_events` table. The incremental cursor already limits us to
        changed products; the hash-diff in resource state further narrows to
        actual PRICE changes (updated_at moves on any product edit)."""
        rows = product_row if isinstance(product_row, list) else [product_row]
        hashes = dlt.current.resource_state().setdefault("price_hashes", {})
        for product in rows:
            for variant in product.get("variants", []):
                price_cop = _price_cop(variant.get("price"))
                if price_cop is None:
                    continue
                key = f"{product['store']}:{variant['id']}"
                fingerprint = f"{price_cop}|{_price_cop(variant.get('compare_at_price'))}"
                if hashes.get(key) != fingerprint:
                    hashes[key] = fingerprint
                    yield {
                        "store": product["store"],
                        "variant_id": variant["id"],
                        "product_id": product["id"],
                        "sku": variant.get("sku") or None,
                        "product_name": product.get("title"),
                        "variant_name": variant.get("title"),
                        "product_type": product.get("product_type"),
                        "grams": variant.get("grams") or None,  # unreliable, kept as a hint
                        "price_cop": price_cop,
                        "list_price_cop": _price_cop(variant.get("compare_at_price")),
                        "is_available": variant.get("available"),
                        "captured_at": product["captured_at"],
                        "captured_date": product["captured_at"].date(),
                        "source": "scrape",
                    }

    return products, price_events
