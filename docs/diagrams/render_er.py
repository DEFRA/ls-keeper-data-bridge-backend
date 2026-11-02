"""Render the krds-db read-model ER diagram to SVG (no dependencies).

Run:  python docs/diagrams/render_er.py
Out:  docs/diagrams/krds-read-model-er.svg
"""

import html
import os

W, H = 1560, 880
COL_W = 330
ROW_H = 20
HEADER_H = 28
PAD = 8

NEW_FILL = "#eef6ee"      # new tables
NEW_EDGE = "#2e7d32"
OLD_FILL = "#f4f6f8"      # existing tables
OLD_EDGE = "#607d8b"
KEY = "#b71c1c"
FK = "#1565c0"
JOIN = "#8e24aa"          # join key (not a real FK)
TXT = "#212121"
MUTED = "#757575"


def esc(s):
    return html.escape(s, quote=True)


class Table:
    def __init__(self, name, x, y, rows, new=False, note=None):
        self.name = name
        self.x, self.y = x, y
        self.rows = rows            # [(col, tag)] tag in {'pk','fk','jk',''}
        self.new = new
        self.note = note            # grey note line under title
        self.w = COL_W
        extra = 18 if note else 0
        self.h = PAD + HEADER_H + extra + len(rows) * ROW_H + PAD

    def bottom(self):
        return self.y + self.h

    def right(self):
        return self.x + self.w

    def center_x(self):
        return self.x + self.w / 2


TABLES = [
    Table("Party", 60, 60, [
        ("Id  PK", "pk"), ("SourcePartyId", ""), ("Email", ""), ("…", "")]),
    Table("PartyRole", 60, 300, [
        ("Id  PK", "pk"), ("PartyId  FK", "fk"), ("HoldingId  FK", "fk"),
        ("HerdId  FK", "fk"), ("Role", "")]),
    Table("Holding", 520, 320, [
        ("Id  PK", "pk"), ("Cph  UNIQUE", "pk"), ("FeatureName", ""),
        ("CphType", ""), ("StartDate / EndDate", ""), ("…", "")]),
    Table("Herd", 60, 540, [
        ("Id  PK", "pk"), ("HoldingId  FK", "fk"), ("Herdmark", ""),
        ("Cphh", ""), ("…", "")]),
    Table("HoldingAnimalProfile", 60, 700, [
        ("Id  PK", "pk"), ("HoldingId  FK", "fk"), ("AnimalSpeciesCode", ""), ("…", "")]),
    Table("CtsOpenLocation", 520, 640, [
        ("LocationNumber  PK", "pk"), ("Cph  join key", "jk"), ("…", "")]),

    Table("CommonLand", 980, 60, [
        ("Id  PK", "pk"), ("CommonCph  UNIQUE", "pk"), ("PremiseId", ""),
        ("BusinessUsage", ""), ("PremisesName", ""),
        ("AddressLine1", ""), ("AddressLine2", ""), ("AddressLine3", ""),
        ("LocalAuthorityName", ""), ("Country", ""), ("Postcode", ""),
        ("Easting", ""), ("Northing", "")],
        new=True, note="one row per 00-series CPH (the common itself)"),
    Table("CommonLandHolding", 980, 420, [
        ("Id  PK", "pk"), ("CommonLandId  FK", "fk"), ("MainCph  join key", "jk"),
        ("CommonCph  join key", "jk"), ("IsContiguous", ""),
        ("StartDate / EndDate", ""), ("LinkId", "")],
        new=True, note="one row per keeper-holding / common link"),
]

# edges: (from_table, to_table, label, style) style in {'fk','jk'}
EDGES = [
    ("PartyRole", "Party", "PartyId", "fk"),
    ("PartyRole", "Holding", "HoldingId", "fk"),
    ("PartyRole", "Herd", "HerdId", "fk"),
    ("Herd", "Holding", "HoldingId", "fk"),
    ("HoldingAnimalProfile", "Holding", "HoldingId", "fk"),
    ("CtsOpenLocation", "Holding", "Cph → Cph  (join key)", "jk"),
    ("CommonLandHolding", "CommonLand", "CommonLandId", "fk"),
    ("CommonLandHolding", "Holding", "MainCph → Cph  (join key)", "jk"),
    ("CommonLandHolding", "Holding", "CommonCph → Cph  (join key)", "jk"),
]

# edge routing, indexed in the same order as EDGES:
# (side_out, side_in, from_dy, to_dy) — dy fans edges sharing a side apart.
ROUTE = [
    ("t", "l", 0, 0),      # PartyRole -> Party
    ("r", "l", 0, -40),    # PartyRole -> Holding
    ("l", "t", 0, 0),      # PartyRole -> Herd
    ("r", "l", 0, 40),     # Herd -> Holding
    ("r", "b", 0, -20),    # HoldingAnimalProfile -> Holding
    ("t", "b", 0, 20),     # CtsOpenLocation -> Holding
    ("t", "b", 0, 0),      # CommonLandHolding -> CommonLand
    ("l", "r", -50, -30),  # CommonLandHolding -> Holding (MainCph)
    ("l", "r", 50, 40),    # CommonLandHolding -> Holding (CommonCph)
]

BY_NAME = {t.name: t for t in TABLES}


def anchor(table, side, dy=0):
    if side == "r":
        return table.right(), table.y + table.h / 2 + dy
    if side == "l":
        return table.x, table.y + table.h / 2 + dy
    if side == "t":
        return table.center_x(), table.y + dy
    return table.center_x(), table.bottom() + dy


def edge_path(f, t, fs, ts, fdy, tdy):
    x1, y1 = anchor(f, fs, fdy)
    x2, y2 = anchor(t, ts, tdy)
    # simple orthogonal route with one bend
    if fs in ("l", "r") and ts in ("l", "r"):
        xm = (x1 + x2) / 2
        return f"M {x1} {y1} L {xm} {y1} L {xm} {y2} L {x2} {y2}", (x1, y1), (x2, y2)
    if fs in ("t", "b") and ts in ("t", "b"):
        ym = (y1 + y2) / 2
        return f"M {x1} {y1} L {x1} {ym} L {x2} {ym} L {x2} {y2}", (x1, y1), (x2, y2)
    if fs in ("l", "r"):
        return f"M {x1} {y1} L {x2} {y1} L {x2} {y2}", (x1, y1), (x2, y2)
    return f"M {x1} {y1} L {x1} {y2} L {x2} {y2}", (x1, y1), (x2, y2)


def render():
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
        f'font-family="Segoe UI, Arial, sans-serif" font-size="13">',
        f'<rect width="{W}" height="{H}" fill="#ffffff"/>',
    ]

    # edges first (under tables)
    for i, (fname, tname, label, style) in enumerate(EDGES):
        f, t = BY_NAME[fname], BY_NAME[tname]
        fs, ts, fdy, tdy = ROUTE[i]
        d, (x1, y1), (x2, y2) = edge_path(f, t, fs, ts, fdy, tdy)
        if style == "fk":
            parts.append(f'<path d="{d}" fill="none" stroke="{FK}" stroke-width="1.6"/>')
        else:
            parts.append(f'<path d="{d}" fill="none" stroke="{JOIN}" stroke-width="1.4" '
                         f'stroke-dasharray="6 4"/>')
        lx, ly = (x1 + x2) / 2, (y1 + y2) / 2
        parts.append(f'<text x="{lx}" y="{ly - 6}" font-size="11" fill="{MUTED}" '
                     f'text-anchor="middle">{esc(label)}</text>')
        parts.append(f'<text x="{x1 + (10 if fs == "r" else -10 if fs == "l" else 12)}" '
                     f'y="{y1 - 6}" font-size="11" fill="{MUTED}">N</text>')
        parts.append(f'<text x="{x2 + (10 if ts == "r" else -10 if ts == "l" else 12)}" '
                     f'y="{y2 - 6}" font-size="11" fill="{MUTED}">1</text>')

    for t in TABLES:
        edge = NEW_EDGE if t.new else OLD_EDGE
        fill = NEW_FILL if t.new else OLD_FILL
        parts.append(f'<rect x="{t.x}" y="{t.y}" width="{t.w}" height="{t.h}" rx="6" '
                     f'fill="{fill}" stroke="{edge}" stroke-width="1.8"/>')
        parts.append(f'<rect x="{t.x}" y="{t.y}" width="{t.w}" height="{HEADER_H}" rx="6" '
                     f'fill="{edge}"/>')
        parts.append(f'<rect x="{t.x}" y="{t.y + HEADER_H - 8}" width="{t.w}" height="8" '
                     f'fill="{edge}"/>')
        title = t.name + ("  (new)" if t.new else "")
        parts.append(f'<text x="{t.x + 10}" y="{t.y + 19}" font-weight="bold" '
                     f'fill="#ffffff">{esc(title)}</text>')
        yy = t.y + HEADER_H + (18 if t.note else 0)
        if t.note:
            parts.append(f'<text x="{t.x + 10}" y="{t.y + HEADER_H + 13}" font-size="11" '
                         f'fill="{MUTED}">{esc(t.note)}</text>')
        for i, (col, tag) in enumerate(t.rows):
            cy = yy + 14 + i * ROW_H
            colour = KEY if tag == "pk" else FK if tag == "fk" else JOIN if tag == "jk" else TXT
            parts.append(f'<text x="{t.x + 10}" y="{cy}" fill="{colour}">{esc(col)}</text>')

    # legend
    lx, ly = 980, H - 132
    parts.append(f'<rect x="{lx - 14}" y="{ly - 24}" width="540" height="122" rx="6" '
                 f'fill="#fafafa" stroke="#ccc"/>')
    parts.append(f'<text x="{lx}" y="{ly}" font-weight="bold" fill="{TXT}">krds-db.sqlite read model</text>')
    parts.append(f'<text x="{lx}" y="{ly + 22}" fill="{KEY}">PK/UNIQUE</text>'
                 f'<text x="{lx + 90}" y="{ly + 22}" fill="{FK}">FK (resolves within model)</text>'
                 f'<text x="{lx + 300}" y="{ly + 22}" fill="{JOIN}">join key (may not resolve)</text>')
    parts.append(f'<text x="{lx}" y="{ly + 44}" fill="{MUTED}">'
                 f'Solid = FK-style (same deterministic guid). Dashed = CPH</text>')
    parts.append(f'<text x="{lx}" y="{ly + 62}" fill="{MUTED}">'
                 f'text join; the CPH may not exist in sam_cph_holdings.</text>')
    parts.append(f'<text x="{lx}" y="{ly + 84}" fill="{MUTED}">'
                 f'Source: amls2_common_land extract via the common-land view part.</text>')

    parts.append('</svg>')
    return "\n".join(parts)


if __name__ == "__main__":
    out = os.path.join(os.path.dirname(__file__), "krds-read-model-er.svg")
    with open(out, "w", encoding="utf-8") as fh:
        fh.write(render())
    print(out)
