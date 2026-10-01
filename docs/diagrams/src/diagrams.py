"""Generate README diagrams as self-contained SVGs (logos embedded), light + dark."""

import base64
import html
import re
import sys
from pathlib import Path

LOGOS = Path(__file__).parent / "logos"
FONT = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif"

THEMES = {
    "light": {"bg": "#ffffff", "ink": "#1b1f24", "muted": "#59636e", "label": "#8a94a0",
              "card": "#ffffff", "card_line": "#d8dee4", "band": "#f6f8fa", "arrow": "#59636e",
              "accent": "#5b5bd6", "live": "#1f883d", "road": "#9a6700"},
    "midnight": {"bg": "#0b1220", "ink": "#e8eefc", "muted": "#a3b1cc", "label": "#7f8db0",
                 "card": "#ffffff", "card_line": "#26324d", "band": "#111a2e", "arrow": "#8fa3c8",
                 "accent": "#7c8cff", "live": "#34d399", "road": "#fbbf24"},
    "dark": {"bg": "#0d1117", "ink": "#e6edf3", "muted": "#9da7b3", "label": "#7d8590",
             "card": "#ffffff", "card_line": "#30363d", "band": "#161b22", "arrow": "#9da7b3",
             "accent": "#8b8bf0", "live": "#3fb950", "road": "#d29922"},
}
CARD_INK, CARD_MUTED = "#1b1f24", "#59636e"  # cards stay white in both themes so every logo reads


def logo(name: str, recolor: str | None = None) -> str:
    svg = (LOGOS / f"{name}.svg").read_text()
    if recolor:
        svg = re.sub(r"<svg ", f'<svg fill="{recolor}" ', svg, count=1)
    return "data:image/svg+xml;base64," + base64.b64encode(svg.encode()).decode()


class Canvas:
    def __init__(self, w: int, h: int, theme: str):
        self.w, self.h, self.t = w, h, THEMES[theme]
        self.parts: list[str] = []

    def add(self, s: str):
        self.parts.append(s)

    def text(self, x, y, s, size=13, weight=400, fill=None, anchor="start", spacing=0, italic=False):
        style = f' letter-spacing="{spacing}"' if spacing else ""
        style += ' font-style="italic"' if italic else ""
        self.add(f'<text x="{x}" y="{y}" font-family="{FONT}" font-size="{size}" font-weight="{weight}" '
                 f'fill="{fill or self.t["ink"]}" text-anchor="{anchor}"{style}>{html.escape(s)}</text>')

    def card(self, x, y, w, h, dashed=False, fill=None, line=None, r=14):
        dash = ' stroke-dasharray="7 5"' if dashed else ""
        self.add(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{r}" fill="{fill or self.t["card"]}" '
                 f'stroke="{line or self.t["card_line"]}" stroke-width="1.5"{dash} filter="url(#shadow)"/>')

    def image(self, x, y, size, href):
        self.add(f'<image href="{href}" x="{x}" y="{y}" width="{size}" height="{size}"/>')

    def arrow(self, x1, y1, x2, y2, dashed=False, color=None, label=None, label_dy=-8):
        c = color or self.t["arrow"]
        dash = ' stroke-dasharray="6 5"' if dashed else ""
        mx = (x1 + x2) / 2
        path = f"M{x1},{y1} C{mx},{y1} {mx},{y2} {x2},{y2}"
        self.add(f'<path d="{path}" fill="none" stroke="{c}" stroke-width="2"{dash} marker-end="url(#head-{"road" if dashed else "live"})"/>')
        if label:
            self.text(mx, (y1 + y2) / 2 + label_dy, label, size=11.5, weight=600, fill=c, anchor="middle")

    def svg(self, title: str) -> str:
        t = self.t
        defs = f'''<defs>
  <filter id="shadow" x="-10%" y="-10%" width="120%" height="130%"><feDropShadow dx="0" dy="2" stdDeviation="3" flood-color="#000" flood-opacity="0.08"/></filter>
  <marker id="head-live" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="{t["arrow"]}"/></marker>
  <marker id="head-road" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="{t["road"]}"/></marker>
</defs>'''
        return (f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.w}" height="{self.h}" viewBox="0 0 {self.w} {self.h}" role="img" aria-label="{html.escape(title)}">'
                f'<title>{html.escape(title)}</title>{defs}<rect width="100%" height="100%" fill="{t["bg"]}"/>'
                + "".join(self.parts) + "</svg>")


def bullets(c: Canvas, x, y, items, size=12.5, gap=19, color=CARD_MUTED):
    for i, s in enumerate(items):
        c.text(x, y + i * gap, "• " + s, size=size, fill=color)


def platform_overview(theme: str) -> str:
    c = Canvas(1600, 860, theme)
    t = c.t
    c.text(40, 52, "Recetario — the platform at a glance", size=26, weight=700)
    c.text(40, 78, "Prices from five retailers land in a lake, the tables that earn it are promoted to a tested warehouse, "
                   "and metrics feed data products.", size=14, fill=t["muted"])

    # Governance band (roadmap): OpenMetadata touches every layer
    c.card(300, 104, 1260, 58, dashed=True, fill=t["band"], line=t["road"], r=12)
    c.image(318, 113, 40, logo("openmetadata"))
    c.text(368, 131, "OpenMetadata — one catalog for every layer", size=15, weight=700)
    c.text(368, 150, "discovery · glossary & metrics catalog · column-level lineage · data quality · built-in MCP for AI",
           size=12.5, fill=t["muted"])

    labels = [(150, "SOURCES"), (400, "INGEST"), (665, "LAKE"), (985, "WAREHOUSE"), (1255, "SEMANTIC LAYER"), (1475, "DATA PRODUCTS")]
    for x, s in labels:
        c.text(x, 195, s, size=12, weight=700, fill=t["label"], anchor="middle", spacing=1.5)

    # Sources
    c.card(40, 210, 220, 150)
    c.image(56, 224, 34, logo("vtex", recolor="#F71963"))
    c.text(100, 247, "VTEX", size=17, weight=700, fill=CARD_INK)
    bullets(c, 56, 280, ["D1 · Éxito", "Euro · Jumbo", "one connector, 4 stores"])
    c.card(40, 375, 220, 112)
    c.image(56, 389, 34, logo("shopify"))
    c.text(100, 412, "Shopify", size=17, weight=700, fill=CARD_INK)
    bullets(c, 56, 444, ["Mundo Huevo", "incremental on updated_at"])
    c.card(40, 502, 220, 122, dashed=True, line=t["road"])
    c.image(56, 516, 34, logo("claude"))
    c.text(100, 539, "User input", size=17, weight=700, fill=CARD_INK)
    bullets(c, 56, 565, ["photos & PDFs + her note", "Claude → structured JSON", "FastAPI → her catalog DB"], gap=17)

    # Ingest
    c.card(310, 300, 180, 210)
    c.image(373, 316, 54, logo("dlthub"))
    c.text(400, 396, "dlt", size=19, weight=700, fill=CARD_INK, anchor="middle")
    bullets(c, 326, 424, ["paginated REST", "retries + backoff", "price-change state", "schema evolution"], gap=19)

    # Lake
    c.card(545, 210, 240, 404)
    c.image(563, 226, 46, logo("ducklake"))
    c.image(729, 230, 40, logo("azure"))
    c.text(563, 296, "DuckLake", size=19, weight=700, fill=CARD_INK)
    c.text(563, 317, "on Azure Data Lake Storage", size=13, fill=CARD_MUTED)
    bullets(c, 563, 352, ["open Parquet files", "ACID transactions", "snapshots & time travel", "schema evolution",
                          "SQL catalog in Postgres", "partitioned by store + date", "full raw history, cheap"], gap=22)
    c.image(563, 518, 64, logo("parquet_folder"))
    c.text(637, 553, "raw_vtex/price_events/", size=10.5, fill=CARD_MUTED)
    c.text(637, 570, "store=d1/captured_date=…", size=10.5, fill=CARD_MUTED)

    # Warehouse
    c.card(855, 210, 260, 404)
    c.image(871, 226, 46, logo("postgres"))
    c.text(927, 256, "Postgres", size=19, weight=700, fill=CARD_INK)
    c.text(927, 276, "only promoted tables", size=13, fill=CARD_MUTED)
    c.card(871, 300, 228, 296, fill="#fbfbfd", line="#e4e7eb", r=10)
    c.image(887, 314, 34, logo("dbt"))
    c.text(931, 337, "dbt Core", size=16, weight=700, fill=CARD_INK)
    for i, (name, note) in enumerate([("clean", "typed · deduplicated"), ("snapshots", "SCD2 history"), ("marts", "star schema")]):
        y = 362 + i * 70
        c.card(887, y, 196, 50, fill="#ffffff", line="#d0d7de", r=8)
        c.text(903, y + 22, name, size=14, weight=700, fill=CARD_INK)
        c.text(903, y + 40, note, size=12, fill=CARD_MUTED)
        if i < 2:
            c.text(985, y + 63, "▼ tests gate", size=10.5, weight=600, fill=t["live"], anchor="middle")
    c.text(985, 586, "32 blocking tests", size=11.5, weight=700, fill=t["live"], anchor="middle")

    # Semantic layer (roadmap)
    c.card(1175, 300, 160, 210, dashed=True, line=t["road"])
    c.image(1230, 316, 50, logo("cube"))
    c.text(1255, 392, "Cube", size=19, weight=700, fill=CARD_INK, anchor="middle")
    bullets(c, 1189, 420, ["metrics defined once", "cost per gram", "recipe cost · margin"], gap=19)

    # Data products (roadmap)
    for i, (name, note, icon) in enumerate([("BI dashboards", "financial & ops reporting", None),
                                            ("AI agents", "FastMCP · WhatsApp", "modelcontextprotocol"),
                                            ("Apps & APIs", "her phone page · pricing", None)]):
        y = 230 + i * 128
        c.card(1390, y, 170, 108, dashed=True, line=t["road"])
        if icon:
            c.image(1406, y + 14, 28, logo(icon))
        else:
            glyph = "▤" if i == 0 else "⌘"
            c.text(1420, y + 38, glyph, size=24, fill=t["accent"], anchor="middle")
        c.text(1444, y + 35, name, size=14.5, weight=700, fill=CARD_INK)
        c.text(1406, y + 72, note, size=12, fill=CARD_MUTED)

    # Flows
    c.arrow(260, 285, 310, 405)
    c.arrow(260, 431, 310, 405)
    c.arrow(260, 558, 310, 405, dashed=True)
    c.arrow(490, 405, 545, 405, label="land")
    c.arrow(785, 405, 855, 405, label="promote")
    c.arrow(1115, 405, 1175, 405, dashed=True)
    for y in (284, 412, 540):
        c.arrow(1335, 405, 1390, y, dashed=True)

    # Orchestration band (live)
    c.card(300, 650, 850, 66, fill=t["band"], line=t["accent"], r=12)
    c.image(318, 660, 46, logo("dagster"))
    c.text(374, 679, "Dagster — orchestrates every step as one asset graph", size=15, weight=700)
    c.text(374, 700, "schedules · sensors · lineage lake → marts · dbt tests as asset checks · run history · failure alerts",
           size=12.5, fill=t["muted"])
    for x in (400, 665, 985):
        c.add(f'<line x1="{x}" y1="650" x2="{x}" y2="{614 if x != 400 else 510}" stroke="{t["accent"]}" stroke-width="1.5" stroke-dasharray="2 4"/>')

    # Runtime strip
    c.text(40, 772, "RUNS ON", size=12, weight=700, fill=t["label"], spacing=1.5)
    c.card(40, 784, 1520, 46, fill=t["band"], r=10)
    for i, (icon, s) in enumerate([("docker", "Docker Compose — one image for dlt + dbt + Dagster"),
                                   ("azure", "Azure VM + Data Lake Storage"),
                                   ("githubactions", "GitHub Actions CI on every push")]):
        x = 60 + i * 510
        c.add(f'<rect x="{x - 6}" y="790" width="34" height="34" rx="7" fill="#ffffff"/>')
        c.image(x - 2, 794, 26, logo(icon))
        c.text(x + 38, 813, s, size=13.5, weight=600)

    # Legend
    c.add(f'<line x1="1180" y1="676" x2="1214" y2="676" stroke="{t["arrow"]}" stroke-width="2"/>')
    c.text(1222, 680, "live today", size=12.5, fill=t["muted"])
    c.add(f'<line x1="1180" y1="700" x2="1214" y2="700" stroke="{t["road"]}" stroke-width="2" stroke-dasharray="6 5"/>')
    c.text(1222, 704, "roadmap", size=12.5, fill=t["muted"])
    c.text(1560, 852, "Logos are trademarks of their respective owners; used for identification only.",
           size=10.5, fill=t["label"], anchor="end")
    return c.svg("Recetario platform at a glance")


DIAGRAMS = {"platform-overview": platform_overview}

if __name__ == "__main__":
    # Usage: python docs/diagrams/src/diagrams.py [out_dir] [theme ...]   (defaults: docs/images, midnight)
    repo = Path(__file__).resolve().parents[3]
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else repo / "docs" / "images"
    themes = sys.argv[2:] or ["midnight"]
    out.mkdir(parents=True, exist_ok=True)
    for name, fn in DIAGRAMS.items():
        for theme in themes:
            suffix = "" if theme == "midnight" else f"-{theme}"
            (out / f"{name}{suffix}.svg").write_text(fn(theme))
            print("wrote", out / f"{name}{suffix}.svg")
