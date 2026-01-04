#!/usr/bin/env python3
"""Generate a lightweight PDF from report.md without external dependencies.
The converter treats the markdown as plain text (heading markers are stripped)
and lays out lines on 8.5x11 pages using Helvetica.
"""
from __future__ import annotations

import textwrap
from pathlib import Path
from typing import List

PAGE_WIDTH = 612  # 8.5 in * 72
PAGE_HEIGHT = 792  # 11 in * 72
MARGIN = 48
FONT_SIZE = 11
LEADING = 14  # line height


def _escape(text: str) -> str:
    """Escape parentheses and backslashes for PDF text blocks."""
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _normalize_markdown(md: str) -> List[str]:
    lines: List[str] = []
    for raw in md.splitlines():
        line = raw.lstrip()
        if line.startswith("#"):
            # Strip markdown heading markers and upper-case to stand out.
            stripped = raw.lstrip("# ").upper()
            lines.append(stripped)
            continue
        if not line:
            lines.append("")
            continue
        wrapped = textwrap.wrap(line, width=110) or [""]
        lines.extend(wrapped)
    return lines


def _chunk_lines(lines: List[str]) -> List[List[str]]:
    max_lines = int((PAGE_HEIGHT - 2 * MARGIN) // LEADING)
    return [lines[i : i + max_lines] for i in range(0, len(lines), max_lines)]


def _make_page_content(lines: List[str]) -> str:
    y = PAGE_HEIGHT - MARGIN
    parts = ["BT", f"/F1 {FONT_SIZE} Tf"]
    for line in lines:
        y -= LEADING
        parts.append(f"1 0 0 1 {MARGIN} {y:.2f} Tm ({_escape(line)}) Tj")
    parts.append("ET")
    return "\n".join(parts)


def _build_pdf(pages: List[str]) -> bytes:
    objs: List[str] = []

    # Font object
    objs.append("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")  # id 1

    # Content streams and page objects
    page_objs = []
    content_ids = []
    for content in pages:
        stream = f"<< /Length {len(content.encode('utf-8'))} >>\nstream\n{content}\nendstream"
        content_ids.append(len(objs) + 2)  # +2 because catalog/pages come later
        objs.append(stream)
    pages_id = len(objs) + 2
    catalog_id = len(objs) + 3

    for idx, cid in enumerate(content_ids):
        page_obj = (
            f"<< /Type /Page /Parent {pages_id} 0 R /MediaBox [0 0 {PAGE_WIDTH} {PAGE_HEIGHT}] "
            f"/Resources << /Font << /F1 1 0 R >> >> /Contents {cid} 0 R >>"
        )
        page_objs.append(page_obj)

    objs.extend(page_objs)

    # Pages tree
    kids = " ".join(f"{pages_id + 1 + i} 0 R" for i in range(len(page_objs)))
    pages_obj = f"<< /Type /Pages /Kids [ {kids} ] /Count {len(page_objs)} >>"
    objs.append(pages_obj)

    # Catalog
    catalog_obj = f"<< /Type /Catalog /Pages {pages_id} 0 R >>"
    objs.append(catalog_obj)

    # Build xref
    offsets = []
    pdf_parts = ["%PDF-1.4\n"]
    for obj_id, body in enumerate(objs, start=1):
        offsets.append(len("".join(pdf_parts).encode("utf-8")))
        pdf_parts.append(f"{obj_id} 0 obj\n{body}\nendobj\n")

    xref_pos = len("".join(pdf_parts).encode("utf-8"))
    xref_lines = ["xref", f"0 {len(objs) + 1}", "0000000000 65535 f "]
    for off in offsets:
        xref_lines.append(f"{off:010d} 00000 n ")
    trailer = (
        f"trailer\n<< /Size {len(objs)+1} /Root {catalog_id} 0 R >>\nstartxref\n{xref_pos}\n%%EOF"
    )
    pdf_parts.extend(xref_lines)
    pdf_parts.append(trailer)
    return "\n".join(pdf_parts).encode("utf-8")


def generate(md_path: Path, pdf_path: Path) -> None:
    md_text = md_path.read_text(encoding="utf-8")
    lines = _normalize_markdown(md_text)
    pages = [_make_page_content(chunk) for chunk in _chunk_lines(lines)]
    pdf_bytes = _build_pdf(pages)
    pdf_path.write_bytes(pdf_bytes)
    print(f"[Report] Wrote {pdf_path} ({len(pages)} page(s))")


if __name__ == "__main__":
    root = Path(__file__).resolve().parent.parent
    md_file = root / "report.md"
    pdf_file = root / "project_report.pdf"
    generate(md_file, pdf_file)
