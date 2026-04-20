#!/usr/bin/env python3
"""
Evaluate sample tweets with the local two-stage filter.

Supports:
- CSV files
- XLSX files

Notes:
- Apple Numbers `.numbers` files are not parsed directly. Export to CSV or XLSX first.
- The script tries to auto-detect common tweet text / label columns.
"""

from __future__ import annotations

import argparse
import csv
import re
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET

from scraper import (
    PREFILTER_THRESHOLD,
    compute_composite_scores,
    llm_filter_tweets,
    load_config,
    score_tweets_batch,
)


TEXT_CANDIDATES = (
    "tweet text",
    "text",
    "tweet",
    "content",
    "post",
    "body",
)

AUTHOR_CANDIDATES = (
    "handle",
    "author",
    "username",
    "screen name",
)

NAME_CANDIDATES = (
    "author name",
    "display name",
    "name",
)

URL_CANDIDATES = (
    "tweet url",
    "url",
    "link",
    "tweet link",
)

LABEL_CANDIDATES = (
    "label",
    "expected",
    "expected keep",
    "keep",
    "relevant",
    "ground truth",
    "human label",
)


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (value or "").strip().lower()).strip()


def _pick_column(headers: list[str], candidates: tuple[str, ...], explicit: str | None = None) -> str | None:
    if explicit:
        for header in headers:
            if header == explicit:
                return header
        lowered = {_slug(h): h for h in headers}
        return lowered.get(_slug(explicit))

    normalized = {_slug(h): h for h in headers}
    for candidate in candidates:
        if candidate in normalized:
            return normalized[candidate]

    for header in headers:
        slug = _slug(header)
        for candidate in candidates:
            if candidate in slug:
                return header
    return None


def _coerce_label(value) -> bool | None:
    if value is None:
        return None
    raw = str(value).strip().lower()
    if not raw:
        return None
    truthy = {"1", "true", "yes", "y", "keep", "relevant", "pass", "include"}
    falsy = {"0", "false", "no", "n", "drop", "irrelevant", "fail", "exclude"}
    if raw in truthy:
        return True
    if raw in falsy:
        return False
    return None


def _read_csv(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    return [{k.strip(): v for k, v in row.items()} for row in rows]


def _xlsx_cell_value(cell: ET.Element, shared: list[str], ns: dict[str, str]) -> str:
    cell_type = cell.attrib.get("t")
    if cell_type == "inlineStr":
        return "".join(node.text or "" for node in cell.iterfind(".//a:t", ns))

    value_node = cell.find("a:v", ns)
    if value_node is None:
        return ""

    raw = value_node.text or ""
    if cell_type == "s":
        return shared[int(raw)]
    return raw


def _read_xlsx(path: Path) -> list[dict]:
    ns = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    rel_ns = {"r": "http://schemas.openxmlformats.org/package/2006/relationships"}

    with zipfile.ZipFile(path) as zf:
        shared = []
        if "xl/sharedStrings.xml" in zf.namelist():
            root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for item in root.findall("a:si", ns):
                shared.append("".join(node.text or "" for node in item.iterfind(".//a:t", ns)))

        workbook = ET.fromstring(zf.read("xl/workbook.xml"))
        first_sheet = workbook.find("a:sheets/a:sheet", ns)
        if first_sheet is None:
            return []

        rel_id = first_sheet.attrib["{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"]
        rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        target = None
        for rel in rels.findall("r:Relationship", rel_ns):
            if rel.attrib.get("Id") == rel_id:
                target = rel.attrib["Target"].lstrip("/")
                if not target.startswith("xl/"):
                    target = f"xl/{target}"
                break
        if target is None:
            return []

        sheet = ET.fromstring(zf.read(target))
        rows = []
        headers = None
        for row in sheet.findall(".//a:sheetData/a:row", ns):
            values = [_xlsx_cell_value(cell, shared, ns) for cell in row.findall("a:c", ns)]
            if headers is None:
                headers = values
                continue
            if not any(str(v).strip() for v in values):
                continue
            padded = values + [""] * max(0, len(headers) - len(values))
            rows.append(dict(zip(headers, padded)))
        return rows


def load_rows(path: Path) -> list[dict]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return _read_csv(path)
    if suffix == ".xlsx":
        return _read_xlsx(path)
    if suffix == ".numbers":
        raise RuntimeError("Numbers files are not supported directly. Export the sheet to CSV or XLSX first.")
    raise RuntimeError(f"Unsupported file type: {path.suffix}")


def build_tweets(rows: list[dict], text_col: str, author_col: str | None, name_col: str | None,
                 url_col: str | None, label_col: str | None) -> list[dict]:
    tweets = []
    for idx, row in enumerate(rows, 1):
        text = str(row.get(text_col, "") or "").strip()
        if not text:
            continue
        tweet = {
            "sample_row": idx,
            "text": text,
            "author": str(row.get(author_col, "") or "").strip() if author_col else "",
            "author_name": str(row.get(name_col, "") or "").strip() if name_col else "",
            "url": str(row.get(url_col, "") or "").strip() if url_col else "",
        }
        if label_col:
            tweet["expected_keep"] = _coerce_label(row.get(label_col))
        tweets.append(tweet)
    return tweets


def write_results(path: Path, tweets: list[dict]) -> None:
    headers = [
        "sample_row",
        "author_name",
        "author",
        "text",
        "url",
        "expected_keep",
        "mpnet_score",
        "prefilter_pass",
        "llm_relevant",
        "llm_topic_fit_score",
        "llm_philosophical_depth_score",
        "llm_relevance_score",
        "relevance_score",
        "quote_style",
        "quote_style_reason",
        "quote_docked_to_zero",
        "composite_score",
        "llm_reason",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for tweet in tweets:
            writer.writerow({
                "sample_row": tweet.get("sample_row", ""),
                "author_name": tweet.get("author_name", ""),
                "author": tweet.get("author", ""),
                "text": tweet.get("text", ""),
                "url": tweet.get("url", ""),
                "expected_keep": tweet.get("expected_keep", ""),
                "mpnet_score": tweet.get("score", ""),
                "prefilter_pass": tweet.get("prefilter_pass", ""),
                "llm_relevant": tweet.get("llm_relevant", ""),
                "llm_topic_fit_score": tweet.get("llm_topic_fit_score", ""),
                "llm_philosophical_depth_score": tweet.get("llm_philosophical_depth_score", ""),
                "llm_relevance_score": tweet.get("llm_relevance_score", ""),
                "relevance_score": tweet.get("relevance_score", ""),
                "quote_style": tweet.get("quote_style", ""),
                "quote_style_reason": tweet.get("quote_style_reason", ""),
                "quote_docked_to_zero": tweet.get("quote_docked_to_zero", ""),
                "composite_score": tweet.get("composite_score", ""),
                "llm_reason": tweet.get("llm_reason", ""),
            })


def print_summary(tweets: list[dict], threshold: float) -> None:
    total = len(tweets)
    prefiltered = sum(1 for t in tweets if t.get("prefilter_pass"))
    kept = sum(1 for t in tweets if t.get("llm_relevant"))
    labeled = [t for t in tweets if t.get("expected_keep") is not None]

    print(f"Loaded tweets:        {total}")
    print(f"MPNet threshold:      {threshold:.2f}")
    print(f"Prefilter passed:     {prefiltered}")
    print(f"LLM kept:             {kept}")

    if labeled:
        correct = sum(1 for t in labeled if bool(t.get("llm_relevant")) == bool(t.get("expected_keep")))
        positives = sum(1 for t in labeled if t.get("expected_keep"))
        print(f"Labeled rows:         {len(labeled)}")
        print(f"Expected keep rows:   {positives}")
        print(f"Agreement:            {correct}/{len(labeled)} ({(100.0 * correct / len(labeled)):.1f}%)")

        mismatches = [t for t in labeled if bool(t.get("llm_relevant")) != bool(t.get("expected_keep"))]
        if mismatches:
            print("\nFirst mismatches:")
            for tweet in mismatches[:10]:
                text = tweet.get("text", "").replace("\n", " ")
                if len(text) > 120:
                    text = text[:117] + "..."
                print(
                    f"- row {tweet.get('sample_row')}: expected={tweet.get('expected_keep')} "
                    f"got={tweet.get('llm_relevant')} score={tweet.get('llm_relevance_score', '')} | {text}"
                )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("input_file", nargs="?", default="sample tweets.xlsx",
                        help="Path to sample CSV or XLSX file.")
    parser.add_argument("--text-column", help="Explicit text column name.")
    parser.add_argument("--label-column", help="Explicit expected-label column name.")
    parser.add_argument("--output", help="CSV file to write scored results to.")
    args = parser.parse_args()

    input_path = Path(args.input_file)
    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        return 1

    rows = load_rows(input_path)
    if not rows:
        print(f"No rows found in {input_path}")
        return 1

    headers = list(rows[0].keys())
    text_col = _pick_column(headers, TEXT_CANDIDATES, args.text_column)
    label_col = _pick_column(headers, LABEL_CANDIDATES, args.label_column)
    author_col = _pick_column(headers, AUTHOR_CANDIDATES)
    name_col = _pick_column(headers, NAME_CANDIDATES)
    url_col = _pick_column(headers, URL_CANDIDATES)

    if not text_col:
        print("Could not auto-detect a tweet text column.")
        print(f"Available columns: {headers}")
        print("Pass --text-column \"Your Column Name\"")
        return 1

    print(f"Using text column:    {text_col}")
    if label_col:
        print(f"Using label column:   {label_col}")

    tweets = build_tweets(rows, text_col, author_col, name_col, url_col, label_col)
    if not tweets:
        print("No usable tweets found after reading the sample file.")
        return 1

    config = load_config()
    threshold = config.get("filtering", {}).get("semantic_threshold", PREFILTER_THRESHOLD)
    filter_model = config.get("llm_filter", {}).get("model", "phi3:mini")

    try:
        score_tweets_batch(tweets)
    except ImportError as exc:
        print(f"\nMissing Python dependency while scoring: {exc}")
        print("Install the project requirements first: pip install -r requirements.txt")
        return 1

    prefiltered = []
    for tweet in tweets:
        passed = tweet.get("score", -999.0) > threshold
        tweet["prefilter_pass"] = passed
        if passed:
            prefiltered.append(tweet)
        else:
            tweet["llm_relevant"] = False
            tweet["llm_reason"] = "[prefiltered out]"
            tweet["llm_topic_fit_score"] = 0.0
            tweet["llm_philosophical_depth_score"] = 0.0
            tweet["llm_relevance_score"] = 0.0

    print(f"Running local LLM filter with model: {filter_model}")
    try:
        kept = llm_filter_tweets(prefiltered, model=filter_model)
    except Exception as exc:
        print(f"\nLocal LLM filter failed: {exc}")
        print("Make sure Ollama is running and the configured model is pulled.")
        print("Example:")
        print("  ollama serve")
        print("  ollama pull phi3:mini")
        return 1
    compute_composite_scores(kept, threshold)

    kept_keys = {(tweet.get("author", ""), tweet.get("text", "")) for tweet in kept}
    for tweet in tweets:
        if (tweet.get("author", ""), tweet.get("text", "")) not in kept_keys and tweet.get("prefilter_pass"):
            tweet["llm_relevant"] = bool(tweet.get("llm_relevant", False))
            tweet["llm_topic_fit_score"] = float(tweet.get("llm_topic_fit_score", 0.0) or 0.0)
            tweet["llm_philosophical_depth_score"] = float(tweet.get("llm_philosophical_depth_score", 0.0) or 0.0)
            tweet["llm_relevance_score"] = float(tweet.get("llm_relevance_score", 0.0) or 0.0)
            tweet["relevance_score"] = float(tweet.get("relevance_score", 0.0) or 0.0)
            tweet["composite_score"] = float(tweet.get("composite_score", 0.0) or 0.0)

    output_path = Path(args.output) if args.output else input_path.with_name(f"{input_path.stem}_eval_results.csv")
    write_results(output_path, tweets)
    print_summary(tweets, threshold)
    print(f"\nResults written to:   {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
