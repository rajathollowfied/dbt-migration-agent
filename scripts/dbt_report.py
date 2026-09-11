"""dbt Run Results Reporter

Parses standard dbt artifacts (run_results.json + manifest.json) from any dbt project
and exports a human-readable Excel report.

Usage:
    python dbt_report.py <path_to_dbt_project> [--output report.xlsx]

Works with any dbt project regardless of adapter or platform.
No custom dependencies beyond openpyxl and pandas.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import openpyxl
from openpyxl.styles import Alignment, Font, PatternFill, Border, Side
from openpyxl.utils import get_column_letter


# ── Colours ────────────────────────────────────────────────────────────────────
GREEN  = "FF92D050"   # pass
RED    = "FFFF0000"   # error
YELLOW = "FFFFC000"   # skip / warn
BLUE   = "FF4472C4"   # header fill
WHITE  = "FFFFFFFF"
GREY   = "FFF2F2F2"


def _fill(hex_colour: str) -> PatternFill:
    return PatternFill("solid", fgColor=hex_colour)


def _font(bold: bool = False, colour: str = "FF000000", size: int = 11) -> Font:
    return Font(name="Arial", bold=bold, color=colour, size=size)


def _border() -> Border:
    thin = Side(style="thin", color="FFD9D9D9")
    return Border(left=thin, right=thin, top=thin, bottom=thin)


def _header_font() -> Font:
    return Font(name="Arial", bold=True, color=WHITE, size=11)


# ── Parsers ────────────────────────────────────────────────────────────────────

def load_json(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def parse_artifacts(project_path: Path) -> tuple[list[dict], dict]:
    """Return (model_rows, summary) from run_results.json + manifest.json."""
    target = project_path / "target"

    run_results_path = target / "run_results.json"
    manifest_path    = target / "manifest.json"

    if not run_results_path.exists():
        sys.exit(f"ERROR: run_results.json not found at {run_results_path}\n"
                 "Run `dbt run` or `dbt build` first.")

    rr = load_json(run_results_path)
    manifest = load_json(manifest_path) if manifest_path.exists() else {}
    nodes = manifest.get("nodes", {})

    generated_at = rr.get("metadata", {}).get("generated_at", "unknown")
    dbt_version  = rr.get("metadata", {}).get("dbt_schema_version", "unknown")
    elapsed      = rr.get("elapsed_time", 0)

    rows = []
    status_counts: dict[str, int] = {}

    for result in rr.get("results", []):
        uid    = result.get("unique_id", "")
        status = result.get("status", "unknown")
        msg    = result.get("message", "") or ""
        timing = result.get("execution_time", 0) or 0

        # skip non-model results (tests, seeds, snapshots)
        if not uid.startswith("model."):
            continue

        node   = nodes.get(uid, {})
        config = node.get("config", {})
        fqn    = node.get("fqn", [])

        # derive layer from fqn: project.layer.subpath.model_name
        layer = fqn[1] if len(fqn) > 1 else "unknown"

        rows.append({
            "Model Name":      node.get("name", uid.split(".")[-1]),
            "Layer":           layer,
            "Materialization": config.get("materialized", "unknown"),
            "Status":          status,
            "Execution (s)":   round(timing, 2),
            "Error / Message": _clean_message(msg),
            "Tags":            ", ".join(node.get("tags", [])),
            "Description":     node.get("description", ""),
            "Path":            node.get("original_file_path", ""),
        })

        status_counts[status] = status_counts.get(status, 0) + 1

    # sort: errors first, then skips, then passes
    priority = {"error": 0, "fail": 0, "skipped": 1, "skip": 1, "success": 2}
    rows.sort(key=lambda r: (priority.get(r["Status"], 1), r["Layer"], r["Model Name"]))

    total = len(rows)
    passed  = status_counts.get("success", 0)
    errors  = status_counts.get("error", 0) + status_counts.get("fail", 0)
    skipped = status_counts.get("skipped", 0) + status_counts.get("skip", 0)
    pass_rate = f"{round(passed / total * 100, 1)}%" if total else "N/A"

    summary = {
        "Project":       project_path.name,
        "Generated At":  generated_at,
        "dbt Version":   dbt_version,
        "Elapsed (s)":   round(elapsed, 1),
        "Total Models":  total,
        "Passed":        passed,
        "Errors":        errors,
        "Skipped":       skipped,
        "Pass Rate":     pass_rate,
    }

    return rows, summary


def _clean_message(msg: str) -> str:
    """Trim very long error messages to fit in a cell."""
    msg = msg.strip().replace("\n", " | ")
    return msg[:500] + "…" if len(msg) > 500 else msg


# ── Excel builder ──────────────────────────────────────────────────────────────

def build_excel(rows: list[dict], summary: dict, output_path: Path) -> None:
    wb = openpyxl.Workbook()

    _build_summary_sheet(wb.active, summary)
    _build_detail_sheet(wb.create_sheet("Model Detail"), rows)
    _build_errors_sheet(wb.create_sheet("Errors & Skips"), rows)

    wb.active.title = "Summary"
    wb.save(output_path)


def _apply_header_row(ws, headers: list[str], row: int = 1) -> None:
    for col, header in enumerate(headers, 1):
        cell = ws.cell(row=row, column=col, value=header)
        cell.font      = _header_font()
        cell.fill      = _fill(BLUE)
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border    = _border()


def _style_data_row(ws, row_num: int, num_cols: int, status: str = "", alternate: bool = False) -> None:
    bg = GREY if alternate else WHITE
    for col in range(1, num_cols + 1):
        cell = ws.cell(row=row_num, column=col)
        cell.font      = _font()
        cell.border    = _border()
        cell.alignment = Alignment(vertical="top", wrap_text=True)
        if status in ("error", "fail"):
            cell.fill = _fill("FFFFF0F0")
        elif status in ("skipped", "skip"):
            cell.fill = _fill("FFFFFBE6")
        else:
            cell.fill = _fill(bg)


def _status_badge(status: str) -> str:
    return {
        "success": "✅ Pass",
        "error":   "❌ Error",
        "fail":    "❌ Fail",
        "skipped": "⏭️ Skip",
        "skip":    "⏭️ Skip",
    }.get(status, status)


def _build_summary_sheet(ws, summary: dict) -> None:
    ws.column_dimensions["A"].width = 22
    ws.column_dimensions["B"].width = 30

    # Title
    ws.merge_cells("A1:B1")
    title = ws["A1"]
    title.value     = "dbt Migration Run Report"
    title.font      = _font(bold=True, colour=WHITE, size=14)
    title.fill      = _fill(BLUE)
    title.alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[1].height = 30

    # Spacer
    ws.append([])

    # Summary KPIs
    kpi_rows = [
        ("Project",      summary["Project"]),
        ("Generated At", summary["Generated At"]),
        ("dbt Version",  summary["dbt Version"]),
        ("Elapsed (s)",  summary["Elapsed (s)"]),
        ("",             ""),
        ("Total Models", summary["Total Models"]),
        ("✅ Passed",    summary["Passed"]),
        ("❌ Errors",    summary["Errors"]),
        ("⏭️ Skipped",   summary["Skipped"]),
        ("Pass Rate",    summary["Pass Rate"]),
    ]

    for i, (label, value) in enumerate(kpi_rows, start=3):
        ws.cell(row=i, column=1, value=label).font = _font(bold=True)
        cell = ws.cell(row=i, column=2, value=value)
        cell.font      = _font()
        cell.alignment = Alignment(horizontal="left")

        # Colour pass rate cell
        if label == "Pass Rate" and isinstance(value, str) and value != "N/A":
            pct = float(value.replace("%", ""))
            cell.fill = _fill(GREEN if pct >= 80 else (YELLOW if pct >= 50 else "FFFF0000"))
            cell.font = _font(bold=True, colour=WHITE)

    # Generated timestamp footer
    footer_row = 3 + len(kpi_rows) + 2
    ws.cell(row=footer_row, column=1, value="Report generated").font = _font(colour="FF888888")
    ws.cell(row=footer_row, column=2, value=datetime.now().strftime("%Y-%m-%d %H:%M")).font = _font(colour="FF888888")


def _build_detail_sheet(ws, rows: list[dict]) -> None:
    headers = ["Model Name", "Layer", "Materialization", "Status",
               "Execution (s)", "Tags", "Description", "Error / Message", "Path"]

    col_widths = [30, 12, 16, 12, 14, 20, 30, 60, 40]
    for i, w in enumerate(col_widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = w
    ws.row_dimensions[1].height = 24

    _apply_header_row(ws, headers)

    for i, row in enumerate(rows, start=2):
        status = row["Status"]
        ws.row_dimensions[i].height = 18
        data = [
            row["Model Name"],
            row["Layer"],
            row["Materialization"],
            _status_badge(status),
            row["Execution (s)"],
            row["Tags"],
            row["Description"],
            row["Error / Message"],
            row["Path"],
        ]
        for col, value in enumerate(data, 1):
            ws.cell(row=i, column=col, value=value)
        _style_data_row(ws, i, len(headers), status, alternate=(i % 2 == 0))

    # Freeze header
    ws.freeze_panes = "A2"


def _build_errors_sheet(ws, rows: list[dict]) -> None:
    error_rows = [r for r in rows if r["Status"] in ("error", "fail", "skipped", "skip")]

    headers = ["Model Name", "Layer", "Status", "Error / Message", "Path"]
    col_widths = [30, 12, 12, 80, 40]
    for i, w in enumerate(col_widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = w
    ws.row_dimensions[1].height = 24

    _apply_header_row(ws, headers)

    if not error_rows:
        ws.cell(row=2, column=1, value="🎉 No errors or skips — all models passed!").font = _font(bold=True)
        return

    for i, row in enumerate(error_rows, start=2):
        status = row["Status"]
        ws.row_dimensions[i].height = 18
        data = [
            row["Model Name"],
            row["Layer"],
            _status_badge(status),
            row["Error / Message"],
            row["Path"],
        ]
        for col, value in enumerate(data, 1):
            ws.cell(row=i, column=col, value=value)
        _style_data_row(ws, i, len(headers), status)

    ws.freeze_panes = "A2"


# ── CLI ────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Parse dbt run_results.json + manifest.json and export an Excel report."
    )
    parser.add_argument(
        "project_path",
        help="Path to the dbt project directory (must contain target/run_results.json)",
    )
    parser.add_argument(
        "--output", "-o",
        default="dbt_run_report.xlsx",
        help="Output Excel file path (default: dbt_run_report.xlsx)",
    )
    args = parser.parse_args()

    project_path = Path(args.project_path).resolve()
    output_path  = Path(args.output).resolve()

    print(f"Parsing dbt artifacts from: {project_path}")
    rows, summary = parse_artifacts(project_path)

    print(f"Found {summary['Total Models']} models — "
          f"{summary['Passed']} passed, {summary['Errors']} errors, "
          f"{summary['Skipped']} skipped ({summary['Pass Rate']} pass rate)")

    print(f"Building Excel report...")
    build_excel(rows, summary, output_path)
    print(f"Report saved to: {output_path}")


if __name__ == "__main__":
    main()
