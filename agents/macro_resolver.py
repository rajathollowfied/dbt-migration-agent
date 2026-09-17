"""Agent 2 — Macro Resolver Agent.

Purpose: make all macros dialect-agnostic before any model touches them.
See AGENT_DESIGN.md Section 4 (Agent 2) and MACRO_ANALYSIS.md for the full spec.

Steps:
  1. Read `project_name` from dbt_project.yml (dispatch namespace)
  2. Inventory macros + assess packages.yml compatibility
  3. Classify every macro: auto_resolve / flag / hard_stop
  4. Apply adapter.dispatch() pattern (or an in-place portable rewrite where no
     dispatch is needed) — always overwrites the original file
  5. Fix `data_type:` fields in yml (number->bigint, varchar->string, timestamp_ntz->timestamp)
  6. Comment out live `dbt_constraints.*` references in yml (block-aware, Python — not sed)
  7. Run `dbt deps` + `dbt compile` to verify macro resolution
  8. Write results to dbt_migration.audit.macro_resolution

Failure behavior: soft fail — flags problem macros, continues with resolvable ones.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError

from agents.common.db import StatementError, execute_sql, get_client
from agents.common.config import DEFAULT_CATALOG, DEFAULT_PROFILE, DEFAULT_WAREHOUSE_ID
from agents.common.workspace import ensure_workspace_copy

# ---------------------------------------------------------------------------
# Package compatibility (MACRO_ANALYSIS.md Section 3)
# ---------------------------------------------------------------------------

PACKAGE_COMPAT = {
    "dbt-labs/dbt_utils": "compatible",
    "brooklyn-data/dbt_artifacts": "compatible",
    "dbt-labs/dbt_project_evaluator": "compatible",
    "Snowflake-Labs/dbt_constraints": "incompatible",  # Snowflake/Postgres/Oracle only
}

# ---------------------------------------------------------------------------
# Classification heuristics for macro bodies
# ---------------------------------------------------------------------------

HARD_STOP_PATTERNS = [
    (re.compile(r"\bSHOW\s+STREAMS\b", re.IGNORECASE), "Snowflake Streams (SHOW STREAMS)"),
    (re.compile(r"metadata\$\w+", re.IGNORECASE), "Stream metadata columns (metadata$...)"),
    (re.compile(r"\bCREATE\s+(OR\s+REPLACE\s+)?SEQUENCE\b", re.IGNORECASE), "CREATE SEQUENCE"),
    (re.compile(r"\.nextval\b", re.IGNORECASE), "sequence .nextval"),
]

# Architecturally different but side-effect-only (safe to no-op on Databricks —
# nothing downstream consumes a return value from these).
SIDE_EFFECT_PATTERNS = [
    (re.compile(r"\bALTER\s+SESSION\b", re.IGNORECASE), "ALTER SESSION"),
    (re.compile(r"\bCURRENT_WAREHOUSE\s*\(", re.IGNORECASE), "CURRENT_WAREHOUSE()"),
    (re.compile(r"\bEXECUTE\s+IMMEDIATE\b", re.IGNORECASE), "EXECUTE IMMEDIATE"),
    (re.compile(r"\bCREATE\s+MASKING\s+POLICY\b", re.IGNORECASE), "CREATE MASKING POLICY"),
    (re.compile(r"\bCREATE\s+(OR\s+REPLACE\s+)?(TEMPORARY\s+)?STAGE\b", re.IGNORECASE), "Snowflake STAGE"),
    (re.compile(r"\bPUT\s+file://", re.IGNORECASE), "PUT file:// (stage upload)"),
    (re.compile(r"\bUSE\s+WAREHOUSE\b", re.IGNORECASE), "USE WAREHOUSE"),
]

# Pure syntax differences fixable with an in-place, dialect-portable rewrite —
# no dispatch needed because the fixed text is valid on both Snowflake and Databricks.
INLINE_FIX_PATTERNS = [
    (re.compile(r"\bsysdate\s*\(\s*\)", re.IGNORECASE), "sysdate()"),
    (re.compile(r"\biff\s*\(", re.IGNORECASE), "iff()"),
    (re.compile(r"\bdecode\s*\(", re.IGNORECASE), "decode()"),
    (re.compile(r"::\s*(varchar|number|integer|timestamp_ntz|date|timestamp)\b", re.IGNORECASE), "Snowflake ::type cast"),
]

CAST_TYPE_MAP = {
    "varchar": "STRING",
    "number": "DECIMAL",
    "integer": "INTEGER",
    "timestamp_ntz": "TIMESTAMP",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
}


def classify_macro_body(body: str) -> tuple[str, list[str]]:
    """Returns (category, matched_constructs). category is one of:
    'hard_stop', 'flag_sideeffect', 'flag_inline', 'auto_resolve'.
    """
    constructs = []
    for pattern, desc in HARD_STOP_PATTERNS:
        if pattern.search(body):
            constructs.append(desc)
    if constructs:
        return "hard_stop", constructs

    for pattern, desc in SIDE_EFFECT_PATTERNS:
        if pattern.search(body):
            constructs.append(desc)
    if constructs:
        return "flag_sideeffect", constructs

    for pattern, desc in INLINE_FIX_PATTERNS:
        if pattern.search(body):
            constructs.append(desc)
    if constructs:
        return "flag_inline", constructs

    return "auto_resolve", []


def apply_inline_dialect_fix(body: str) -> tuple[str, bool]:
    """Rewrite Snowflake-only syntax to syntax valid on both dialects. No dispatch needed."""
    new_body = body
    new_body = re.sub(r"\bsysdate\s*\(\s*\)", "current_timestamp()", new_body, flags=re.IGNORECASE)
    new_body = re.sub(r"\bSYSDATE\s*\(\s*\)", "current_timestamp()", new_body)

    def cast_repl(m: re.Match) -> str:
        target = m.group(2)
        db_type = CAST_TYPE_MAP.get(target.lower(), target.upper())
        return f"CAST({m.group('expr')} AS {db_type})"

    # `([\w.'"-]+)::type` — qualified names, string/numeric/date literals, and null.
    new_body = re.sub(
        r"(?P<expr>[\w.'\"-]+)::\s*(varchar|number|integer|timestamp_ntz|date|timestamp)\b(\(\d+(,\d+)?\))?",
        cast_repl,
        new_body,
        flags=re.IGNORECASE,
    )
    return new_body, new_body != body


# ---------------------------------------------------------------------------
# Macro parsing — split a file's text into individual {% macro %}...{% endmacro %} blocks
# ---------------------------------------------------------------------------

MACRO_START_HEAD_RE = re.compile(r"\{%-?\s*macro\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(")
MACRO_TAG_CLOSE_RE = re.compile(r"\s*-?%\}")
MACRO_END_RE = re.compile(r"\{%-?\s*endmacro\s*-?%\}")


def _find_matching_paren(text: str, open_paren_pos: int) -> int | None:
    """Index of the ')' matching the '(' at open_paren_pos, skipping parens inside
    quoted string literals. None if unbalanced."""
    depth = 1
    i = open_paren_pos + 1
    n = len(text)
    while i < n:
        ch = text[i]
        if ch in ("'", '"'):
            quote = ch
            i += 1
            while i < n and text[i] != quote:
                i += 2 if text[i] == "\\" else 1
            i += 1
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return None


def _next_macro_start(text: str, pos: int):
    """(name, args, tag_start, tag_end) for the next `{% macro name(...) %}` tag at
    or after pos — a hand-rolled balanced-paren scan, not a single regex, because a
    macro's default-argument values routinely use parens for grouping (e.g.
    `stream_name=( (this.alias or this.name) ~ "_ST" )`). A naive `\\(([^)]*)\\)`
    regex stops at the first ')' and silently fails to match the whole macro tag —
    confirmed this caused `get_stream` and the sequence macros to never be logged
    or re-verified by this agent on any run, ever, in this project.
    """
    m = MACRO_START_HEAD_RE.search(text, pos)
    while m:
        close = _find_matching_paren(text, m.end() - 1)
        if close is not None:
            tag_close = MACRO_TAG_CLOSE_RE.match(text, close + 1)
            if tag_close:
                return m.group(1), text[m.end():close].strip(), m.start(), tag_close.end()
        m = MACRO_START_HEAD_RE.search(text, m.end())
    return None


@dataclass
class MacroBlock:
    name: str
    args: str
    start: int  # char offset of the macro-start tag
    end: int  # char offset just past the matching endmacro tag
    body_start: int  # char offset just past the macro-start tag (start of body)
    body_end: int  # char offset of the endmacro tag (end of body)

    @property
    def full_text(self) -> str:
        return _FILE_TEXT_CACHE[self.body_start:self.body_end]


_FILE_TEXT_CACHE = ""  # set per-file during parse_macro_blocks; avoids threading text through every call


def parse_macro_blocks(text: str) -> list[MacroBlock]:
    global _FILE_TEXT_CACHE
    _FILE_TEXT_CACHE = text
    blocks = []
    pos = 0
    while True:
        found = _next_macro_start(text, pos)
        if not found:
            break
        name, args, tag_start, tag_end = found
        end_m = MACRO_END_RE.search(text, tag_end)
        if not end_m:
            break  # malformed file — stop rather than guess
        blocks.append(
            MacroBlock(
                name=name,
                args=args,
                start=tag_start,
                end=end_m.end(),
                body_start=tag_end,
                body_end=end_m.start(),
            )
        )
        pos = end_m.end()
    return blocks


def macro_names_in_file(text: str) -> list[str]:
    return [b.name for b in parse_macro_blocks(text)]


def has_dispatch_for(text: str, macro_name: str) -> bool:
    return bool(re.search(rf"adapter\.dispatch\(\s*['\"]{re.escape(macro_name)}['\"]", text)) or (
        f"databricks__{macro_name}" in text
    )


# ---------------------------------------------------------------------------
# Dispatch-pattern code generation
# ---------------------------------------------------------------------------

def build_dispatch_wrapper(
    macro_name: str,
    args: str,
    project_name: str,
    default_body: str,
    databricks_body: str,
    header_note: str,
) -> str:
    call_args = ", ".join(a.split("=")[0].strip() for a in args.split(",") if a.strip())
    return f"""{{#
  MACRO: {macro_name}
  {header_note}
#}}

{{% macro {macro_name}({args}) %}}
    {{{{ return(adapter.dispatch('{macro_name}', '{project_name}')({call_args})) }}}}
{{% endmacro %}}

{{% macro default__{macro_name}({args}) %}}
{default_body}
{{% endmacro %}}

{{% macro databricks__{macro_name}({args}) %}}
{databricks_body}
{{% endmacro %}}
"""


def noop_stub_body(macro_name: str, constructs: list[str]) -> str:
    reason = ", ".join(constructs)
    return (
        f'    {{%- do log("{macro_name}: no-op on Databricks ({reason}). '
        f'No direct equivalent — safe to skip, this macro has no return value consumed by SQL.", info=true) -%}}'
    )


def hard_stop_stub_body(macro_name: str, constructs: list[str]) -> str:
    reason = ", ".join(constructs)
    return (
        f'    {{%- do log("HARD STOP: {macro_name} uses {reason} — no direct Databricks equivalent. '
        f'Model requires architectural redesign. See MACRO_ANALYSIS.md.", info=true) -%}}\n'
        f"    {{{{ return(none) }}}}"
    )


# ---------------------------------------------------------------------------
# Known-macro registry — bespoke, validated handlers for macros this project's
# manual migration already proved out (see FINDINGS.md / MACRO_ANALYSIS.md).
# Each handler receives the macro's raw body text and returns the Databricks
# implementation body. Unknown macros fall back to the generic classifier above.
# ---------------------------------------------------------------------------

def _databricks_body_get_stream(block: MacroBlock) -> str:
    return hard_stop_stub_body("get_stream", ["Snowflake Streams — see MACRO_ANALYSIS.md Section 4.5"]) + \
        "\n    {{ return(table) }}"


def _databricks_body_integration_key(block: MacroBlock) -> str:
    return (
        "    {#- Databricks: replace ::VARCHAR with CAST(... AS STRING) -#}\n"
        "    {%- set fields = [] -%}\n"
        "    {%- for field in field_list -%}\n"
        '        {%- set _ = fields.append("COALESCE(CAST(" ~ field ~ " AS STRING), \'\')") -%}\n'
        "    {%- endfor -%}\n"
        '    {{ fields|join(" || \'~\' || ") }}'
    )


def _databricks_body_sequence_get_nextval() -> str:
    return (
        '    {%- do log("sequence_get_nextval: Snowflake sequence replaced with row_number() on Databricks.", info=true) -%}\n'
        '    {{ return("row_number() OVER (ORDER BY (SELECT NULL))") }}'
    )


def _databricks_body_sequence_nextval_as_surrogate_key() -> str:
    return (
        '    {%- do log("sequence_nextval_as_surrogate_key: Snowflake sequence replaced with row_number() + max() on Databricks.", info=true) -%}\n'
        "    {% if is_incremental() %}\n"
        "        row_number() OVER (ORDER BY (SELECT NULL))\n"
        "        + coalesce((SELECT max({{ column_name }}) FROM {{ this }}), 0)\n"
        "        as {{ column_name }}\n"
        "    {% else %}\n"
        "        row_number() OVER (ORDER BY (SELECT NULL)) as {{ column_name }}\n"
        "    {% endif %}"
    )


def _databricks_body_create_masking_policies() -> str:
    return noop_stub_body(
        "create_masking_policies",
        ["CREATE MASKING POLICY — implement via Unity Catalog column masks post-migration"],
    )


KNOWN_MACROS: dict[str, dict] = {
    "get_stream": {"category": "hard_stop", "needs_dispatch": True, "databricks_body": _databricks_body_get_stream,
                   "review_notes": "Architectural redesign needed — Delta Change Data Feed. See MACRO_ANALYSIS.md Section 4.5."},
    "integration_key": {"category": "auto_resolve", "needs_dispatch": True, "databricks_body": _databricks_body_integration_key,
                         "review_notes": "Validated — CAST(field AS STRING) replaces ::VARCHAR."},
    "sequence_get_nextval": {"category": "flag", "needs_dispatch": True,
                              "databricks_body": lambda block: _databricks_body_sequence_get_nextval(),
                              "review_notes": "No sequences on Databricks — replaced with row_number(). Validate downstream usage."},
    "sequence_nextval_as_surrogate_key": {"category": "flag", "needs_dispatch": True,
                                           "databricks_body": lambda block: _databricks_body_sequence_nextval_as_surrogate_key(),
                                           "review_notes": "No sequences on Databricks — row_number() + max() preserves high watermark. Validate surrogate key values."},
    "create_masking_policies": {"category": "auto_resolve", "needs_dispatch": True,
                                 "databricks_body": lambda block: _databricks_body_create_masking_policies(),
                                 "review_notes": "No-op stub — implement Unity Catalog column masks post-migration."},
}

# dbt built-in macro overrides (name already has the `<adapter>__` prefix baked in by
# dbt's own dispatch mechanism — no adapter.dispatch() wrapper macro needed, just add
# a sibling `databricks__` implementation next to the existing `snowflake__` one).
BUILTIN_OVERRIDE_RE = re.compile(r"^snowflake__(\w+)$")

BUILTIN_DATABRICKS_BODIES = {
    "snapshot_hash_arguments": (
        "    {#- Databricks: replace varchar with STRING, remove ::varchar cast -#}\n"
        "    ({%- for arg in args -%}\n"
        '        coalesce(cast({{ arg }} as STRING), \'\')\n'
        "        {% if not loop.last %} || '|' || {% endif %}\n"
        "    {%- endfor -%})"
    ),
}


# ---------------------------------------------------------------------------
# File-level resolution
# ---------------------------------------------------------------------------

@dataclass
class MacroResolution:
    macro_name: str
    macro_file: str
    category: str  # auto_resolve / flag / hard_stop
    snowflake_construct: str
    databricks_action: str
    dispatch_applied: bool
    requires_human_review: bool
    review_notes: str


def resolve_file(path: Path, project_name: str) -> tuple[str | None, list[MacroResolution]]:
    """Returns (new_text_or_None, resolutions). new_text is None if nothing changed."""
    text = path.read_text()
    blocks = parse_macro_blocks(text)
    if not blocks:
        return None, []

    resolutions: list[MacroResolution] = []
    replacements: list[tuple[int, int, str]] = []  # (start, end, new_text) applied in reverse
    body_by_name = {b.name: text[b.body_start:b.body_end] for b in blocks}

    for block in blocks:
        body = text[block.body_start:block.body_end]

        # Already resolved (dispatch or databricks__ sibling already present)?
        builtin_match = BUILTIN_OVERRIDE_RE.match(block.name)
        if builtin_match:
            canonical = builtin_match.group(1)
            already = f"databricks__{canonical}" in text
            if already:
                resolutions.append(MacroResolution(
                    block.name, str(path), "flag", "dbt built-in adapter override",
                    "databricks__ sibling already present", True, True,
                    "Already resolved — no further code change needed, but this is a "
                    "flag-category macro and should still be reviewed if not already done.",
                ))
                continue
            if canonical in BUILTIN_DATABRICKS_BODIES:
                new_macro_text = (
                    f"\n\n{{% macro databricks__{canonical}(args) -%}}\n"
                    f"{BUILTIN_DATABRICKS_BODIES[canonical]}\n"
                    f"{{%- endmacro %}}\n"
                )
                replacements.append((block.end, block.end, new_macro_text))
                resolutions.append(MacroResolution(
                    block.name, str(path), "flag", "dbt built-in adapter override",
                    f"Added databricks__{canonical} sibling implementation", True, True,
                    "Validated pattern — verify STRING cast matches downstream hash comparisons.",
                ))
            continue

        if has_dispatch_for(text, block.name) or block.name.startswith("default__") or block.name.startswith("databricks__"):
            if not block.name.startswith(("default__", "databricks__")):
                # Reclassify from the KNOWN_MACROS registry when this is a bespoke
                # macro (ground truth for what it was resolved with), or from the
                # databricks__ implementation's body when it isn't — NOT from this
                # block's own body, which is just the one-line dispatcher stub
                # (`{{ return(adapter.dispatch(...)) }}`) and contains none of the
                # patterns the classifier looks for. Classifying that one-liner
                # silently downgraded every already-resolved flag/hard_stop macro
                # to requires_human_review=False on every rerun — confirmed bug.
                known = KNOWN_MACROS.get(block.name)
                if known:
                    category = known["category"]
                else:
                    impl_body = body_by_name.get(f"databricks__{block.name}", body)
                    raw_category, constructs = classify_macro_body(impl_body)
                    category = "flag" if raw_category in ("flag_inline", "flag_sideeffect") else raw_category
                resolutions.append(MacroResolution(
                    block.name, str(path), category,
                    "already resolved", "dispatch pattern present", True,
                    category != "auto_resolve", "Already resolved — no further code change needed.",
                ))
            continue

        known = KNOWN_MACROS.get(block.name)
        if known:
            databricks_body = known["databricks_body"](block)
            new_text = build_dispatch_wrapper(
                block.name, block.args, project_name, body.rstrip("\n"), databricks_body,
                f"Category: {known['category']} | validated pattern from FINDINGS.md/MACRO_ANALYSIS.md",
            )
            replacements.append((block.start, block.end, new_text.strip()))
            resolutions.append(MacroResolution(
                block.name, str(path), known["category"], "known pattern (see MACRO_ANALYSIS.md)",
                "Applied validated adapter.dispatch() pattern", True,
                known["category"] in ("flag", "hard_stop"), known["review_notes"],
            ))
            continue

        # Generic heuristic path — unknown macro.
        category, constructs = classify_macro_body(body)
        if category == "auto_resolve":
            resolutions.append(MacroResolution(
                block.name, str(path), "auto_resolve", "none",
                "Portable as-is — no Snowflake-specific constructs found", False, False,
                "No action needed.",
            ))
            continue

        if category == "flag_inline":
            new_body, changed = apply_inline_dialect_fix(body)
            if changed:
                replacements.append((block.body_start, block.body_end, new_body))
            resolutions.append(MacroResolution(
                block.name, str(path), "flag", ", ".join(constructs),
                "Applied in-place dialect rewrite (portable on both dialects, no dispatch needed)",
                False, True, "Heuristic fix — validate output before relying on it in production.",
            ))
            continue

        # flag_sideeffect or hard_stop -> dispatch + stub
        stub = hard_stop_stub_body(block.name, constructs) if category == "hard_stop" else noop_stub_body(block.name, constructs)
        new_text = build_dispatch_wrapper(
            block.name, block.args, project_name, body.rstrip("\n"), stub,
            f"Category: {category} | generated by Macro Resolver Agent — heuristic classification",
        )
        replacements.append((block.start, block.end, new_text.strip()))
        resolutions.append(MacroResolution(
            block.name, str(path), "hard_stop" if category == "hard_stop" else "flag",
            ", ".join(constructs),
            "Generated dispatch stub (hard stop, logged)" if category == "hard_stop" else "Generated no-op dispatch stub",
            True, True,
            "Generated by heuristic classifier — human review required before trusting in production."
            if category != "hard_stop" else "Architectural blocker — requires human redesign decision.",
        ))

    if not replacements:
        return None, resolutions

    replacements.sort(key=lambda r: r[0], reverse=True)
    new_text = text
    for start, end, repl in replacements:
        new_text = new_text[:start] + repl + new_text[end:]
    return new_text, resolutions


# ---------------------------------------------------------------------------
# packages.yml handling
# ---------------------------------------------------------------------------

PACKAGE_LINE_RE = re.compile(r"^(\s*)-\s*package:\s*([\w.\-/]+)\s*$")


def classify_packages(packages_yml_text: str) -> list[dict]:
    results = []
    lines = packages_yml_text.splitlines()
    for i, line in enumerate(lines):
        m = PACKAGE_LINE_RE.match(line)
        if not m:
            continue
        commented = line.strip().startswith("#")
        pkg = m.group(2).lstrip("#").strip()
        compat = PACKAGE_COMPAT.get(pkg, "unknown")
        results.append({"package": pkg, "commented_out": commented, "compatibility": compat})
    return results


def fix_packages_yml(text: str) -> tuple[str, list[str]]:
    lines = text.splitlines(keepends=True)
    fixes = []
    out = []
    skip_next_version = False
    for line in lines:
        stripped = line.strip()
        if skip_next_version:
            if re.match(r"^#?\s*version\s*:", stripped) and not stripped.startswith("#"):
                out.append("    #" + line.lstrip())
                fixes.append(f"commented out orphaned version line: {stripped}")
                skip_next_version = False
                continue
            skip_next_version = False

        m = re.match(r"^(\s*)-\s*package:\s*([\w.\-/]+)\s*$", line.rstrip("\n"))
        if m and PACKAGE_COMPAT.get(m.group(2)) == "incompatible":
            out.append("#" + line if not line.lstrip().startswith("#") else line)
            fixes.append(f"commented out incompatible package: {m.group(2)}")
            skip_next_version = True
            continue

        out.append(line)
    return "".join(out), fixes


# ---------------------------------------------------------------------------
# yml data_type fixes
# ---------------------------------------------------------------------------

DATA_TYPE_MAP = {"number": "decimal(38,10)", "varchar": "string", "timestamp_ntz": "timestamp"}
# Not `bigint`: confirmed this causes real, silent failures. A bare `data_type:
# number` (no precision/scale) in a Snowflake yml doc doesn't say whether the
# underlying column is truly integer-only or a decimal/currency value — and
# Snowflake's NUMBER is used for both. Found this the hard way: `total_price`
# columns were mapped to `bigint`, but the actual built columns are
# DECIMAL(18,2). dbt-databricks's `materialized_view` materialization emits an
# EXPLICIT column-type DDL sourced from these yml docs (unlike table/
# incremental, which infer types from the query) — the wrong `bigint`
# declaration then conflicts with the real DECIMAL data at creation time
# (DELTA_MERGE_INCOMPATIBLE_DATATYPE), a failure that only surfaces for
# materialized views, not other materializations. `decimal(38,10)` is a safe
# superset for genuinely-integer columns too (no precision lost either way).
DATA_TYPE_RE = re.compile(r"^(\s*data_type:\s*)(number|varchar|timestamp_ntz)(\s*)$", re.IGNORECASE)


def fix_data_types_in_yml(text: str) -> tuple[str, list[str]]:
    lines = text.splitlines(keepends=True)
    fixes = []
    out = []
    for line in lines:
        if line.lstrip().startswith("#"):
            out.append(line)
            continue
        body = line.rstrip("\n")
        m = DATA_TYPE_RE.match(body)
        if m:
            old_val = m.group(2).lower()
            new_val = DATA_TYPE_MAP[old_val]
            newline = "\n" if line.endswith("\n") else ""
            out.append(f"{m.group(1)}{new_val}{m.group(3)}{newline}")
            fixes.append(f"data_type: {old_val} -> {new_val}")
        else:
            out.append(line)
    return "".join(out), fixes


# ---------------------------------------------------------------------------
# dbt_constraints yml reference commenting (block-aware, no over-commenting)
# ---------------------------------------------------------------------------

TEST_ITEM_RE = re.compile(r"^(\s*)-\s*(dbt_constraints\.\w+)\s*:?\s*$")
TESTS_KEY_RE = re.compile(r"^(\s*)tests\s*:\s*$")


def _line_indent(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def comment_out_dbt_constraints_blocks(text: str) -> tuple[str, list[str]]:
    """Comment out only live `dbt_constraints.*` test items (and their `arguments:`
    children), then comment out any `tests:` key left with zero remaining children.
    Never touches unrelated columns/tests/descriptions — see FINDINGS.md Section 5.
    """
    lines = text.splitlines(keepends=True)
    n = len(lines)
    commented = [False] * n
    fixes: list[str] = []

    i = 0
    while i < n:
        line = lines[i]
        if line.lstrip().startswith("#"):
            i += 1
            continue
        m = TEST_ITEM_RE.match(line.rstrip("\n"))
        if not m:
            i += 1
            continue
        indent = len(m.group(1))
        commented[i] = True
        fixes.append(f"commented out line {i + 1}: {line.strip()}")
        j = i + 1
        while j < n:
            nxt = lines[j]
            if nxt.strip() == "":
                j += 1
                continue
            if _line_indent(nxt) <= indent:
                break
            commented[j] = True
            j += 1
        i = j

    # Orphan pass: any `tests:` key whose children are all blank/commented -> comment it too.
    i = 0
    while i < n:
        line = lines[i]
        if not line.lstrip().startswith("#"):
            m = TESTS_KEY_RE.match(line.rstrip("\n"))
            if m:
                indent = len(m.group(1))
                j = i + 1
                has_live_child = False
                child_idxs = []
                while j < n:
                    nxt = lines[j]
                    if nxt.strip() == "":
                        j += 1
                        continue
                    if _line_indent(nxt) <= indent:
                        break
                    child_idxs.append(j)
                    if not commented[j] and not nxt.lstrip().startswith("#"):
                        has_live_child = True
                    j += 1
                if child_idxs and not has_live_child:
                    commented[i] = True
                    fixes.append(f"commented out orphaned 'tests:' key at line {i + 1}")
        i += 1

    out = []
    for idx, line in enumerate(lines):
        if commented[idx] and not line.lstrip().startswith("#"):
            stripped = line.lstrip(" ")
            leading = line[: len(line) - len(stripped)]
            out.append(f"{leading}#{stripped}")
        else:
            out.append(line)
    return "".join(out), fixes


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

@dataclass
class MacroResolverReport:
    project_name: str
    package_assessment: list[dict]
    packages_yml_fixes: list[str]
    macro_resolutions: list[MacroResolution]
    yml_data_type_fixes: dict[str, list[str]]
    yml_dbt_constraints_fixes: dict[str, list[str]]
    dbt_deps_ok: bool
    dbt_compile_ok: bool
    dbt_output_tail: str
    files_written: list[str]
    compile_error_in_macro_layer: bool

    def to_dict(self) -> dict:
        d = asdict(self)
        return d


class MacroResolverAgent:
    def __init__(
        self,
        project_path: str,
        profile: str | None = DEFAULT_PROFILE,
        catalog: str = DEFAULT_CATALOG,
        warehouse_id: str | None = DEFAULT_WAREHOUSE_ID,
        dbt_target: str = "dev",
        apply_yml_fixes: bool = True,
        reset_workspace: bool = False,
    ):
        self.source_path = Path(project_path).resolve()
        self.project_path = ensure_workspace_copy(self.source_path, reset=reset_workspace)
        self.profile = profile
        self.catalog = catalog
        self.warehouse_id = warehouse_id
        self.dbt_target = dbt_target
        self.apply_yml_fixes = apply_yml_fixes
        self.client: WorkspaceClient | None = None

    def read_project_name(self) -> str:
        yml_path = self.project_path / "dbt_project.yml"
        text = yml_path.read_text()
        m = re.search(r'^name:\s*"?([A-Za-z0-9_]+)"?\s*$', text, re.MULTILINE)
        if not m:
            raise ValueError(f"Could not find `name:` field in {yml_path}")
        return m.group(1)

    def resolve_packages(self) -> tuple[list[dict], list[str]]:
        pkg_path = self.project_path / "packages.yml"
        if not pkg_path.exists():
            return [], []
        text = pkg_path.read_text()
        assessment = classify_packages(text)
        fixes: list[str] = []
        if self.apply_yml_fixes:
            new_text, fixes = fix_packages_yml(text)
            if fixes:
                pkg_path.write_text(new_text)
        return assessment, fixes

    def resolve_macros(self, project_name: str) -> tuple[list[MacroResolution], list[str]]:
        macros_dir = self.project_path / "macros"
        if not macros_dir.exists():
            return [], []
        all_resolutions: list[MacroResolution] = []
        written: list[str] = []
        for sql_file in sorted(macros_dir.glob("*.sql")):
            new_text, resolutions = resolve_file(sql_file, project_name)
            if new_text is not None:
                sql_file.write_text(new_text)
                written.append(str(sql_file.relative_to(self.project_path)))
            all_resolutions.extend(resolutions)
        return all_resolutions, written

    def resolve_yml_files(self) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
        data_type_fixes: dict[str, list[str]] = {}
        dbt_constraints_fixes: dict[str, list[str]] = {}
        if not self.apply_yml_fixes:
            return data_type_fixes, dbt_constraints_fixes
        for yml_file in sorted((self.project_path / "models").rglob("*.yml")):
            text = yml_file.read_text()
            new_text, dt_fixes = fix_data_types_in_yml(text)
            if dt_fixes:
                data_type_fixes[str(yml_file.relative_to(self.project_path))] = dt_fixes
                text = new_text
            new_text2, dc_fixes = comment_out_dbt_constraints_blocks(text)
            if dc_fixes:
                dbt_constraints_fixes[str(yml_file.relative_to(self.project_path))] = dc_fixes
                text = new_text2
            if dt_fixes or dc_fixes:
                yml_file.write_text(text)
        return data_type_fixes, dbt_constraints_fixes

    def run_dbt_deps_and_compile(self) -> tuple[bool, bool, str]:
        try:
            deps = subprocess.run(
                ["dbt", "deps", "--project-dir", str(self.project_path)],
                capture_output=True, text=True, timeout=180,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired) as e:
            return False, False, str(e)
        deps_ok = deps.returncode == 0
        try:
            compile_ = subprocess.run(
                ["dbt", "compile", "--project-dir", str(self.project_path), "--target", self.dbt_target],
                capture_output=True, text=True, timeout=300,
            )
        except (FileNotFoundError, subprocess.TimeoutExpired) as e:
            return deps_ok, False, str(e)
        compile_ok = compile_.returncode == 0
        tail = (deps.stdout + deps.stderr + compile_.stdout + compile_.stderr)[-4000:]
        return deps_ok, compile_ok, tail

    def write_audit(self, resolutions: list[MacroResolution]) -> None:
        if not resolutions:
            return
        self.client = self.client or get_client(self.profile)
        now = datetime.now(timezone.utc).isoformat()
        rows_sql = []
        for r in resolutions:
            def esc(s: str) -> str:
                return s.replace("'", "''")
            rows_sql.append(
                "(" + ", ".join([
                    f"'{esc(r.macro_name)}'",
                    f"'{esc(r.macro_file)}'",
                    f"'{esc(r.category)}'",
                    f"'{esc(r.snowflake_construct)}'",
                    f"'{esc(r.databricks_action)}'",
                    str(r.dispatch_applied).upper(),
                    str(r.requires_human_review).upper(),
                    f"'{esc(r.review_notes)}'",
                    f"TIMESTAMP'{now}'",
                ]) + ")"
            )
        stmt = (
            f"INSERT INTO {self.catalog}.audit.macro_resolution "
            "(macro_name, macro_file, category, snowflake_construct, databricks_action, "
            "dispatch_applied, requires_human_review, review_notes, processed_at) VALUES "
            + ", ".join(rows_sql)
        )
        execute_sql(self.client, self.warehouse_id, stmt, catalog=self.catalog, schema="audit")

    def run(self) -> MacroResolverReport:
        project_name = self.read_project_name()
        package_assessment, packages_yml_fixes = self.resolve_packages()
        macro_resolutions, files_written = self.resolve_macros(project_name)
        data_type_fixes, dbt_constraints_fixes = self.resolve_yml_files()

        try:
            self.write_audit(macro_resolutions)
        except (StatementError, DatabricksError) as e:
            print(f"[warn] could not write to audit table: {e}")

        deps_ok, compile_ok, tail = self.run_dbt_deps_and_compile()

        # A compile failure only belongs to THIS agent if it traces back to a macro
        # file we touched (duplicate macro name, bad Jinja, etc). Model-level errors
        # (e.g. a `{{ config(snowflake_warehouse=...) }}` block) are the Transpiler
        # Agent's job — Macro Resolver reports them but doesn't fail on them.
        macro_layer_error = False
        if not compile_ok:
            lowered = tail.lower()
            if "duplicate macro" in lowered or "ambiguous macro" in lowered:
                macro_layer_error = True
            for f in files_written:
                if Path(f).name.lower() in lowered:
                    macro_layer_error = True

        return MacroResolverReport(
            project_name=project_name,
            package_assessment=package_assessment,
            packages_yml_fixes=packages_yml_fixes,
            macro_resolutions=macro_resolutions,
            yml_data_type_fixes=data_type_fixes,
            yml_dbt_constraints_fixes=dbt_constraints_fixes,
            dbt_deps_ok=deps_ok,
            dbt_compile_ok=compile_ok,
            dbt_output_tail=tail,
            files_written=files_written,
            compile_error_in_macro_layer=macro_layer_error,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Macro Resolver Agent")
    parser.add_argument("project_path")
    parser.add_argument("--profile", default=DEFAULT_PROFILE)
    parser.add_argument("--catalog", default=DEFAULT_CATALOG)
    parser.add_argument("--warehouse-id", default=DEFAULT_WAREHOUSE_ID)
    parser.add_argument("--dbt-target", default="dev")
    parser.add_argument("--no-yml-fixes", action="store_true")
    parser.add_argument("--reset-workspace", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    agent = MacroResolverAgent(
        project_path=args.project_path,
        profile=args.profile,
        catalog=args.catalog,
        warehouse_id=args.warehouse_id,
        dbt_target=args.dbt_target,
        apply_yml_fixes=not args.no_yml_fixes,
        reset_workspace=args.reset_workspace,
    )
    report = agent.run()

    if args.json:
        print(json.dumps(report.to_dict(), indent=2, default=str))
    else:
        print("Macro Resolver Agent")
        print(f"workspace copy: {agent.project_path}")
        print("=" * 60)
        print(f"project_name: {report.project_name}")
        print("\nPackages:")
        for p in report.package_assessment:
            print(f"  - {p['package']}: {p['compatibility']} (commented_out={p['commented_out']})")
        if report.packages_yml_fixes:
            print("  fixes:", report.packages_yml_fixes)

        print(f"\nMacros processed: {len(report.macro_resolutions)}")
        for r in report.macro_resolutions:
            flag = " [REVIEW]" if r.requires_human_review else ""
            print(f"  [{r.category}] {r.macro_name} ({Path(r.macro_file).name}){flag} — {r.databricks_action}")

        if report.yml_data_type_fixes:
            print("\nyml data_type fixes:")
            for f, fixes in report.yml_data_type_fixes.items():
                print(f"  {f}: {len(fixes)} fix(es)")

        if report.yml_dbt_constraints_fixes:
            print("\nyml dbt_constraints fixes:")
            for f, fixes in report.yml_dbt_constraints_fixes.items():
                print(f"  {f}: {len(fixes)} fix(es)")

        print(f"\ndbt deps: {'OK' if report.dbt_deps_ok else 'FAILED'}")
        print(f"dbt compile: {'OK' if report.dbt_compile_ok else 'FAILED'}")
        if not report.dbt_compile_ok:
            if report.compile_error_in_macro_layer:
                print("  -> error traces back to a macro file this agent touched")
            else:
                print("  -> error is model-level (e.g. a config() block) — out of scope for "
                      "Macro Resolver, expected until the Transpiler Agent runs")
            print(report.dbt_output_tail[-2000:])
        print("=" * 60)

    return 1 if report.compile_error_in_macro_layer else 0


if __name__ == "__main__":
    sys.exit(main())
