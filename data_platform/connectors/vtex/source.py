"""dlt source for VTEX-platform Colombian retailers (D1, Éxito, Euro, Jumbo, ...).

Every VTEX store exposes the same public catalog API, so one parameterized
source covers all of them — adding a store is a config entry in
`.dlt/config.toml`, not code.

    GET /api/catalog_system/pub/category/tree/<depth>
    GET /api/catalog_system/pub/products/search/?fq=C:/<categoryId>/&_from=0&_to=49

Constraints handled here (PRD section 6.2):
- `_from`/`_to` range pagination, max 50 items per window, ~2,500-result cap
  per query -> we crawl per leaf category, never one giant search
- monetary fields mix COP and centavos within one payload -> `price_cop` is
  normalized here and cross-checked against the centavos value embedded in
  `addToCartLink` (`price_check_ok` column; staging must assert on it)
- no change feed -> `price_events` emits a row only when a SKU's price hash
  differs from the last run, tracked in dlt resource state (append-only)
- every raw HTTP response body lands in `lake/` before parsing (audit trail;
  re-parse without re-fetch)

Retries/backoff are NOT implemented here: dlt's RESTClient retries on its own,
configured under `[runtime]` in `.dlt/config.toml`.
"""

import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, List, Optional

import dlt
from dlt.sources.helpers.requests import Request, Response
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import BasePaginator

logger = logging.getLogger("vtex")

PROJECT_ROOT = Path(__file__).resolve().parents[3]
LAKE_DIR = PROJECT_ROOT / "data_lake" / "vtex"

SEARCH_PATH = "/api/catalog_system/pub/products/search/"
CATEGORY_TREE_PATH = "/api/catalog_system/pub/category/tree/5"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
)
PAGE_SIZE = 50  # VTEX hard limit per request
WINDOW_CAP = 2500  # VTEX serves at most ~2,500 results per query

_CART_PRICE_RE = re.compile(r"[?&]price=(\d+)")


class VtexRangePaginator(BasePaginator):
    """Paginate VTEX catalog search via `_from`/`_to` index ranges.

    VTEX has no page numbers or cursors: you request item index ranges
    (inclusive), at most 50 wide, and a short or empty page means the end.
    This is the only custom pagination code in the whole project; everything
    else uses dlt built-ins.
    """

    def __init__(self, page_size: int = PAGE_SIZE):
        super().__init__()
        if not 1 <= page_size <= PAGE_SIZE:
            raise ValueError(f"VTEX allows 1..{PAGE_SIZE} items per page")
        self.page_size = page_size
        self.start = 0

    def init_request(self, request: Request) -> None:
        self.update_request(request)

    def update_state(self, response: Response, data: Optional[List[Any]] = None) -> None:
        if self.start == 0:
            # `resources: 0-49/1234` header carries the total result count.
            # Beyond WINDOW_CAP, VTEX silently serves nothing — that would be
            # invisible data loss, so shout and tell the operator the fix.
            total = response.headers.get("resources", "").rpartition("/")[-1]
            if total.isdigit() and int(total) > WINDOW_CAP:
                logger.warning(
                    "category has %s results, over the ~%d VTEX window — "
                    "products past the cap are UNREACHABLE; raise crawl_depth: %s",
                    total, WINDOW_CAP, response.url,
                )
        if len(response.json()) < self.page_size:
            self._has_next_page = False
        else:
            self.start += self.page_size
            if self.start + self.page_size > WINDOW_CAP:
                self._has_next_page = False  # never request past the window

    def update_request(self, request: Request) -> None:
        if request.params is None:
            request.params = {}
        request.params["_from"] = self.start
        request.params["_to"] = self.start + self.page_size - 1


def _flatten_tree(nodes: List[dict], parent_path: str = "", depth: int = 1) -> Iterator[dict]:
    """Walk the nested category tree, yielding one flat row per category.

    `url_path` (the slugged path, e.g. /despensa/pasta) is what category
    queries use: some stores (D1) assign products to category ids that do NOT
    match the tree's leaf ids, so id-facet (`fq=C:`) queries come back empty
    while path queries — the same style the storefront uses — always work.
    """
    for node in nodes:
        children = node.get("children") or []
        url_path = "/" + node.get("url", "").split("//")[-1].split("/", 1)[-1].strip("/")
        path = f"{parent_path}/{node['name']}"
        yield {
            "category_id": node["id"],
            "name": node["name"],
            "path": path,
            "url_path": url_path,
            "depth": depth,
            "is_leaf": not children,
        }
        yield from _flatten_tree(children, path, depth + 1)


def _select_crawl_categories(flat: List[dict], roots: List[str], crawl_depth: int) -> List[dict]:
    """Categories to crawl: everything at `crawl_depth`, plus shallower
    leaves. VTEX category queries are hierarchical (a parent path returns all
    products beneath it), so this covers the whole scoped tree while keeping
    each query under the ~2,500-result search window.

    `roots` scopes to food trees by case-insensitive substring match on the
    readable path; empty means the whole catalog.
    """
    crawl = [c for c in flat if c["depth"] == crawl_depth or (c["is_leaf"] and c["depth"] < crawl_depth)]
    if not roots:
        return crawl
    wanted = [r.lower() for r in roots]
    selected = [c for c in crawl if any(w in c["path"].lower() for w in wanted)]
    if not selected:
        top = sorted({c["path"].split("/")[1] for c in flat})
        logger.warning("category_roots %s matched nothing; available roots: %s", roots, top)
    return selected


def _product_rows(product: dict, category: dict, store: str, captured_at: datetime) -> Iterator[dict]:
    """One row per SKU (VTEX 'item') from the default seller's offer."""
    specs = {k: product.get(k) for k in product.get("allSpecifications", [])}
    for item in product.get("items", []):
        sellers = item.get("sellers") or []
        seller = next((s for s in sellers if s.get("sellerDefault")), sellers[0] if sellers else None)
        if seller is None:
            continue
        offer = seller.get("commertialOffer") or {}
        price = offer.get("Price")
        price_cop = int(round(price)) if price else None

        # Cross-check against the centavos price embedded in addToCartLink:
        # a missed x100 error is catastrophic for COGS (PRD R4).
        cart_match = _CART_PRICE_RE.search(seller.get("addToCartLink") or "")
        cart_centavos = int(cart_match.group(1)) if cart_match else None
        price_check_ok = (
            None if (price_cop is None or cart_centavos is None)
            else abs(cart_centavos - price_cop * 100) <= 100  # tolerate rounding
        )

        yield {
            "store": store,
            "sku": item["itemId"],
            "ean": item.get("ean") or None,
            "product_id": product.get("productId"),
            "product_name": product.get("productName"),
            "item_name": item.get("nameComplete"),
            "brand": product.get("brand"),
            "category_id": category["category_id"],
            "category_path": category["path"],
            "link": product.get("link"),
            "price_cop": price_cop,
            "list_price_cop": int(round(offer["ListPrice"])) if offer.get("ListPrice") else None,
            "cart_price_centavos": cart_centavos,
            "price_check_ok": price_check_ok,
            "available_qty": offer.get("AvailableQuantity"),
            "is_available": bool(offer.get("IsAvailable", offer.get("AvailableQuantity"))),
            "measurement_unit": item.get("measurementUnit"),
            "unit_multiplier": item.get("unitMultiplier"),
            "specs_json": json.dumps(specs, ensure_ascii=False) if specs else None,
            "captured_at": captured_at,
        }


@dlt.source(name="vtex")
def vtex_catalog(
    store: Optional[str] = None,
    stores: dict = dlt.config.value,
    request_delay: float = 1.0,
    max_categories: Optional[int] = None,
):
    """Catalog + price events for one VTEX store, or all configured stores
    when `store` is None (stores run sequentially inside each resource).

    `stores` is injected by dlt from `[sources.vtex.stores]` in config.toml:
    each entry carries `base_url` and `category_roots` (names scoping the
    crawl to food categories). `max_categories` is a dev knob to cap a run.
    """
    selected = [store] if store else list(stores.keys())
    captured_at = datetime.now(timezone.utc)
    run_stamp = captured_at.strftime("%Y-%m-%dT%H-%M-%SZ")
    clients = {
        st: RESTClient(
            base_url=stores[st]["base_url"],
            headers={"User-Agent": USER_AGENT},
            data_selector="$",  # VTEX responses are bare JSON arrays
        )
        for st in selected
    }
    _trees: dict = {}

    def _save_raw(st: str, response: Response, label: str) -> None:
        run_dir = LAKE_DIR / st / run_stamp
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / f"{label}.json").write_bytes(response.content)

    def _category_tree(st: str) -> List[dict]:
        if st not in _trees:
            response = clients[st].get(CATEGORY_TREE_PATH)
            response.raise_for_status()
            _save_raw(st, response, "category_tree")
            _trees[st] = list(_flatten_tree(response.json()))
        return _trees[st]

    @dlt.resource(name="categories", write_disposition="merge", primary_key=("store", "category_id"), columns={"store": {"partition": True}})
    def categories() -> Iterator[dict]:
        """Each store's full category tree, flattened — reference table used
        to pick `category_roots` and to keep crawl scope visible in SQL."""
        for st in selected:
            for row in _category_tree(st):
                yield {"store": st, "captured_at": captured_at, **row}

    @dlt.resource(name="products", write_disposition="merge", primary_key=("store", "sku"), columns={"store": {"partition": True}})
    def products() -> Iterator[dict]:
        """Current catalog snapshot: one row per (store, SKU) with normalized
        prices. Full pull of scoped categories on every run (VTEX has no
        change feed); cheap at this scale per PRD 6.4."""
        for st in selected:
            cfg = stores[st]
            crawl = _select_crawl_categories(
                _category_tree(st),
                cfg.get("category_roots", []),
                cfg.get("crawl_depth", 2),
            )
            if max_categories:
                crawl = crawl[:max_categories]
            logger.info("[%s] crawling %d categories", st, len(crawl))

            for i, cat in enumerate(crawl, start=1):
                n_items = 0
                segments = cat["url_path"].strip("/").count("/") + 1
                for page_num, page in enumerate(
                    clients[st].paginate(
                        SEARCH_PATH.rstrip("/") + cat["url_path"],
                        params={"map": ",".join(["c"] * segments)},
                        paginator=VtexRangePaginator(),
                    )
                ):
                    _save_raw(st, page.response, f"products_cat{cat['category_id']}_p{page_num}")
                    for product in page:
                        for row in _product_rows(product, cat, st, captured_at):
                            n_items += 1
                            yield row
                    time.sleep(request_delay)  # PRD hygiene: ~1 req/s
                logger.info("[%s] category %d/%d %s: %d SKUs", st, i, len(crawl), cat["path"], n_items)

    @dlt.transformer(data_from=products, name="price_events", write_disposition="append", columns={"store": {"partition": True}, "captured_date": {"partition": True}})
    def price_events(product_row: dict) -> Iterator[dict]:
        """Append-only price history: emits a row only when a SKU's price
        changed since the last run. The last-seen hashes live in dlt resource
        state, committed atomically with the load."""
        rows = product_row if isinstance(product_row, list) else [product_row]
        hashes = dlt.current.resource_state().setdefault("price_hashes", {})
        for row in rows:
            if row["price_cop"] is None:
                continue
            key = f"{row['store']}:{row['sku']}"
            fingerprint = f"{row['price_cop']}|{row['list_price_cop']}"
            if hashes.get(key) != fingerprint:
                hashes[key] = fingerprint
                yield {
                    "store": row["store"],
                    "sku": row["sku"],
                    "ean": row["ean"],
                    "product_id": row["product_id"],
                    "product_name": row["product_name"],
                    "price_cop": row["price_cop"],
                    "list_price_cop": row["list_price_cop"],
                    "available_qty": row["available_qty"],
                    "captured_at": row["captured_at"],
                    "captured_date": row["captured_at"].date(),
                    "source": "scrape",
                }

    return categories, products, price_events
