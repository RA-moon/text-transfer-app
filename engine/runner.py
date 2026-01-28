from __future__ import annotations
import pathlib
import json
import re
from datetime import datetime
from difflib import SequenceMatcher
import pandas as pd

from .models import JobConfig, ReportRow
from .source_reader import list_files, read_table
from .extractors import extract_customer_key, extract_row_items
from .matcher import find_matches
from .writer import write_table

def _required_target_columns(job: JobConfig) -> list[str]:
    cols = []
    tgt_cfg = job.customer_match.target
    if tgt_cfg.type == "column":
        if tgt_cfg.column:
            cols.append(tgt_cfg.column)
        elif job.target.match.column:
            cols.append(job.target.match.column)
    for lang_cfg in job.source.content.languages.values():
        cols.append(lang_cfg.target_column)
    return sorted(set(cols))

def _validate_targets(job: JobConfig, target_paths: list[pathlib.Path]) -> None:
    required_cols = _required_target_columns(job)
    for p in target_paths:
        df = read_table(p)
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"Target file {p.name} missing columns: {missing}")

def _apply_updates(df: pd.DataFrame, mask: pd.Series, texts: dict[str, str | None], job: JobConfig) -> pd.DataFrame:
    df2 = df.copy()
    write_only_if_present = job.target.behavior.write_only_if_present

    for lang, text in texts.items():
        target_col = job.source.content.languages[lang].target_column
        if target_col not in df2.columns:
            raise ValueError(f"Target missing write column: {target_col}")
        if text is not None or not write_only_if_present:
            if text is not None:
                df2.loc[mask, target_col] = text

    return df2

def _normalize_key(value: str) -> str:
    s = str(value).lower()
    s = re.sub(r"[_\\-]+", " ", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _target_label_from_path(path: pathlib.Path) -> str:
    stem = path.stem
    parts = stem.split("_")
    if len(parts) >= 2:
        return parts[-2]
    return stem

def _build_target_label_map(target_paths: list[pathlib.Path]) -> dict[str, tuple[str, pathlib.Path]]:
    label_map: dict[str, tuple[str, pathlib.Path]] = {}
    for p in target_paths:
        label = _target_label_from_path(p)
        key = _normalize_key(label)
        if key not in label_map:
            label_map[key] = (label, p)
    return label_map

def _detect_social_platform(text_de: str | None, text_fr: str | None, social_cfg=None) -> str | None:
    hay = " ".join([t or "" for t in (text_de, text_fr)]).lower()
    urls = re.findall(r"https?://[^\s)\]]+", hay) + re.findall(r"\bwww\.[^\s)\]]+", hay)
    if social_cfg and getattr(social_cfg, "platforms", None):
        for platform, cfg in social_cfg.platforms.items():
            domains = [str(d).lower() for d in (cfg.domains or [])]
            keywords = [str(k).lower() for k in (cfg.keywords or [])]
            if domains:
                for url in urls:
                    if any(d in url for d in domains):
                        return platform
            if keywords and any(k in hay for k in keywords):
                return platform
    for url in urls:
        if "youtu.be" in url or "youtube.com" in url:
            return "YouTube"
        if "instagram.com" in url or "instagr.am" in url:
            return "Instagram"
        if "linkedin.com" in url:
            return "LinkedIn"
        if "facebook.com" in url or "fb.com" in url:
            return "Facebook"
        if "twitter.com" in url or "x.com" in url:
            return "X"
    if "instagram" in hay:
        return "Instagram"
    if "linkedin" in hay:
        return "LinkedIn"
    if "youtube" in hay or "youtu" in hay:
        return "YouTube"
    if "facebook" in hay or "fb" in hay:
        return "Facebook"
    if "twitter" in hay or (re.search(r"\bx\b", hay) and ("http" in hay or "www" in hay)):
        return "X"
    return None

def _is_social_element(element_label: str) -> bool:
    return "link social media" in element_label.lower()

def _pick_link_value(text_de: str | None, text_fr: str | None) -> str | None:
    if text_de:
        return text_de
    if text_fr:
        return text_fr
    return None

def _normalize_collision_text(value: str) -> str:
    s = str(value).replace("\r\n", "\n").replace("\r", "\n").strip()
    return s

def _value_has_content(value) -> bool:
    if value is None:
        return False
    if isinstance(value, float) and pd.isna(value):
        return False
    s = str(value).strip()
    if s == "" or s.lower() == "nan":
        return False
    return True

def _rule_contains_all(hay: str, needles: list[str]) -> bool:
    for n in needles:
        if n not in hay:
            return False
    return True

def _extract_benefit_number(text: str) -> str | None:
    m = re.search(r"benefit\\s*(\\d+)", text, flags=re.IGNORECASE)
    if m:
        return m.group(1)
    return None

def _resolve_target_by_rules(
    element_label: str,
    label_map: dict[str, tuple[str, pathlib.Path]],
    name_map: dict[str, pathlib.Path],
    rules: list[dict],
) -> tuple[str, pathlib.Path] | None:
    if not rules:
        return None
    elem_norm = _normalize_key(element_label)
    elem_lower = element_label.lower()
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        when = rule.get("when", {}) or {}
        when_contains = when.get("contains", [])
        when_regex = when.get("regex")
        if isinstance(when_contains, str):
            when_contains = [when_contains]
        when_contains_norm = [str(x).lower() for x in when_contains]
        if when_contains_norm and not _rule_contains_all(elem_lower, when_contains_norm):
            continue
        if when_regex:
            try:
                if re.search(when_regex, element_label) is None:
                    continue
            except re.error:
                continue

        target = rule.get("target", {}) or {}
        filename_contains = target.get("filename_contains", [])
        label_contains = target.get("label_contains", [])
        filename_regex = target.get("filename_regex")
        if isinstance(filename_contains, str):
            filename_contains = [filename_contains]
        if isinstance(label_contains, str):
            label_contains = [label_contains]
        filename_contains_norm = [str(x).lower() for x in filename_contains]
        label_contains_norm = [str(x).lower() for x in label_contains]

        prefer_benefit_number = bool(target.get("prefer_benefit_number", False))
        benefit_num = _extract_benefit_number(element_label) if prefer_benefit_number else None

        candidates = []
        for p in name_map.values():
            name_norm = _normalize_key(p.stem)
            label_norm = _normalize_key(_target_label_from_path(p))
            if filename_contains_norm and not _rule_contains_all(name_norm, filename_contains_norm):
                continue
            if label_contains_norm and not _rule_contains_all(label_norm, label_contains_norm):
                continue
            if filename_regex:
                try:
                    if re.search(filename_regex, p.name) is None and re.search(filename_regex, p.stem) is None:
                        continue
                except re.error:
                    continue
            if benefit_num:
                if f"benefit {benefit_num}" not in name_norm and f"benefit {benefit_num}" not in label_norm:
                    continue
            candidates.append(p)

        if not candidates:
            continue

        fuzzy = rule.get("fuzzy", {}) or {}
        fuzzy_threshold = fuzzy.get("threshold")
        fuzzy_mode = str(fuzzy.get("mode", "label")).lower()
        if fuzzy_threshold is not None:
            try:
                threshold = float(fuzzy_threshold)
            except (TypeError, ValueError):
                threshold = None
            if threshold is not None:
                best = None
                best_score = 0.0
                for p in candidates:
                    cand = _target_label_from_path(p) if fuzzy_mode == "label" else p.stem
                    score = SequenceMatcher(None, elem_norm, _normalize_key(cand)).ratio()
                    if score > best_score:
                        best_score = score
                        best = p
                if best is not None and best_score >= threshold:
                    return (_target_label_from_path(best), best)
                continue

        if len(candidates) == 1:
            p = candidates[0]
            return (_target_label_from_path(p), p)
        if len(candidates) > 1 and benefit_num:
            p = sorted(candidates, key=lambda x: x.name)[0]
            return (_target_label_from_path(p), p)
    return None

def _resolve_target_path(
    element_label: str,
    text_de: str | None,
    text_fr: str | None,
    label_map: dict[str, tuple[str, pathlib.Path]],
    name_map: dict[str, pathlib.Path],
    element_map: dict,
    fuzzy_threshold: float,
    element_rules: list[dict],
    social_cfg=None,
) -> tuple[str, pathlib.Path] | None:
    if not element_label:
        return None
    elem_key = _normalize_key(element_label)

    mapped = element_map.get(elem_key)
    if mapped:
        if mapped in name_map:
            p = name_map[mapped]
            return (_target_label_from_path(p), p)
        for p in name_map.values():
            if mapped == p.stem:
                return (_target_label_from_path(p), p)
        elem_key = _normalize_key(mapped)

    rule_match = _resolve_target_by_rules(element_label, label_map, name_map, element_rules)
    if rule_match is not None:
        return rule_match

    if "link social media" in element_label.lower():
        platform = _detect_social_platform(text_de, text_fr, social_cfg)
        if platform is None:
            return None
        pkey = _normalize_key(platform)
        if pkey in label_map:
            return label_map[pkey]
        for p in name_map.values():
            if pkey in _normalize_key(p.name) or pkey in _normalize_key(p.stem):
                return (_target_label_from_path(p), p)
        return None

    if elem_key in label_map:
        return label_map[elem_key]

    best = None
    best_score = 0.0
    for key, value in label_map.items():
        score = SequenceMatcher(None, elem_key, key).ratio()
        if score > best_score:
            best_score = score
            best = value
    if best is not None and best_score >= fuzzy_threshold:
        return best
    return None

def dry_run(job: JobConfig, source_dir: str, target_dir: str, strict_single_match_override: bool | None = None) -> pd.DataFrame:
    src_paths = list_files(source_dir, job.source.file_types)
    tgt_paths = list_files(target_dir, job.target.file_types)

    _validate_targets(job, tgt_paths)

    strict = job.target.behavior.strict_single_match
    if strict_single_match_override is not None:
        strict = strict_single_match_override

    targets = {p.name: read_table(p) for p in tgt_paths}
    target_label_map = _build_target_label_map(tgt_paths)
    target_name_map = {p.name: p for p in tgt_paths}
    element_map = { _normalize_key(k): v for k, v in (job.source.element.map or {}).items() }

    rows: list[ReportRow] = []
    for sp in src_paths:
        cust_cached = None
        for elem, texts, row in extract_row_items(sp, job):
            if job.customer_match.source.type == "column":
                cust = extract_customer_key(sp, job, row=row)
            else:
                if cust_cached is None:
                    cust_cached = extract_customer_key(sp, job)
                cust = cust_cached
            if cust is None:
                rows.append(ReportRow(sp.name, elem, None, None, None, False, False, None, "BLOCK_NO_CUSTOMER", "customer_name_missing", 0))
                continue

            has_de = texts.get("DE") is not None
            has_fr = texts.get("FR") is not None
            languages_present = ",".join(sorted([k for k, v in texts.items() if v is not None])) or None
            if all(v is None for v in texts.values()):
                rows.append(ReportRow(sp.name, elem, None, cust, None, False, False, languages_present, "SKIP_NO_TEXT", "no_text", 0))
                continue

            resolved = _resolve_target_path(
                elem,
                texts.get("DE"),
                texts.get("FR"),
                target_label_map,
                target_name_map,
                element_map,
                job.source.element.fuzzy_threshold,
                job.source.element.rules,
                job.social,
            )
            if resolved is None:
                continue
            target_label, target_path = resolved
            df_t = targets[target_path.name]
            mask = find_matches(df_t, cust, job, target_path)
            mcount = int(mask.sum())

            if mcount == 0:
                rows.append(ReportRow(sp.name, elem, target_path.name, cust, None, has_de, has_fr, languages_present, "BLOCK_NO_MATCH", "no_target_row_found", 0))
            elif strict and mcount != 1:
                rows.append(ReportRow(sp.name, elem, target_path.name, cust, None, has_de, has_fr, languages_present, "BLOCK_MULTI_MATCH", "strict_single_match_failed", 0))
            else:
                written = mcount if not strict else 1
                rows.append(ReportRow(sp.name, elem, target_path.name, cust, None, has_de, has_fr, languages_present, "READY", "", written))

    return pd.DataFrame([r.__dict__ for r in rows])

def run(job: JobConfig, source_dir: str, target_dir: str, output_base_dir: str,
        strict_single_match_override: bool | None = None,
        write_reports_override: bool | None = None,
        write_collisions_override: bool | None = None) -> dict:
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_base = pathlib.Path(output_base_dir) / f"run_{run_id}"
    out_targets = out_base / "updated_targets"
    out_base.mkdir(parents=True, exist_ok=True)

    report_df = dry_run(job, source_dir, target_dir, strict_single_match_override=strict_single_match_override)
    blocked_df = report_df[report_df["status"].isin(["BLOCK_NO_CUSTOMER","BLOCK_NO_MATCH","BLOCK_MULTI_MATCH"])].copy()

    hard_blocks = blocked_df[blocked_df["status"].isin(["BLOCK_NO_CUSTOMER","BLOCK_NO_MATCH","BLOCK_MULTI_MATCH"])]
    do_reports = job.output.write_reports if write_reports_override is None else write_reports_override
    do_collisions = job.output.write_collisions if write_collisions_override is None else write_collisions_override

    if not hard_blocks.empty:
        if do_reports:
            report_df.to_csv(out_base / "report.csv", index=False, encoding="utf-8")
            blocked_df.to_csv(out_base / "blocked.csv", index=False, encoding="utf-8")
            audit = {
                "run_id": run_id,
                "timestamp": datetime.now().isoformat(),
                "status": "BLOCKED",
                "counts": {
                    "sources": int(len(report_df)),
                    "blocked_hard": int(len(hard_blocks)),
                    "blocked_total": int(len(blocked_df)),
                }
            }
            with open(out_base / "audit.json", "w", encoding="utf-8") as f:
                json.dump(audit, f, ensure_ascii=False, indent=2)
        return {"status": "BLOCKED", "output_dir": str(out_base)}

    tgt_paths = list_files(target_dir, job.target.file_types)
    targets = {p.name: read_table(p) for p in tgt_paths}
    target_label_map = _build_target_label_map(tgt_paths)
    target_name_map = {p.name: p for p in tgt_paths}
    element_map = { _normalize_key(k): v for k, v in (job.source.element.map or {}).items() }

    src_paths = list_files(source_dir, job.source.file_types)
    strict = job.target.behavior.strict_single_match
    if strict_single_match_override is not None:
        strict = strict_single_match_override

    total_rows_touched = 0
    social_unmapped = []
    unmapped_inputs = []
    unused_inputs = []
    collisions = []
    summary_log = []
    seen_updates = {}
    touched_columns = {}
    for sp in src_paths:
        cust_cached = None
        cust_for_log = None
        any_text = False
        any_mapped = False
        example_element = None
        source_wrote = False
        for elem, texts, row in extract_row_items(sp, job):
            if job.customer_match.source.type == "column":
                cust = extract_customer_key(sp, job, row=row)
            else:
                if cust_cached is None:
                    cust_cached = extract_customer_key(sp, job)
                cust = cust_cached
            if cust is None:
                continue
            if cust_for_log is None:
                cust_for_log = cust

            if all(v is None for v in texts.values()):
                continue
            languages_present = ",".join(sorted([k for k, v in texts.items() if v is not None])) or None
            any_text = True
            if example_element is None:
                example_element = elem
            if _is_social_element(elem) and _detect_social_platform(texts.get("DE"), texts.get("FR"), job.social) is None:
                social_unmapped.append({
                    "source_file": sp.name,
                    "customer_name": cust,
                    "element": elem,
                    "link_value": _pick_link_value(texts.get("DE"), texts.get("FR")),
                })
                unused_inputs.append({
                    "source_file": sp.name,
                    "customer_name": cust,
                    "element": elem,
                    "target_file": None,
                    "target_column": None,
                    "reason": "social_link_unmapped",
                    "languages_present": languages_present,
                })
                continue
            resolved = _resolve_target_path(
                elem,
                texts.get("DE"),
                texts.get("FR"),
                target_label_map,
                target_name_map,
                element_map,
                job.source.element.fuzzy_threshold,
                job.source.element.rules,
                job.social,
            )
            if resolved is None:
                unused_inputs.append({
                    "source_file": sp.name,
                    "customer_name": cust,
                    "element": elem,
                    "target_file": None,
                    "target_column": None,
                    "reason": "no_target_file_match",
                    "languages_present": languages_present,
                })
                continue
            any_mapped = True
            _, target_path = resolved
            df_t = targets[target_path.name]
            mask = find_matches(df_t, cust, job, target_path)
            mcount = int(mask.sum())
            if mcount == 0:
                unused_inputs.append({
                    "source_file": sp.name,
                    "customer_name": cust,
                    "element": elem,
                    "target_file": target_path.name,
                    "target_column": None,
                    "reason": "no_target_row_match",
                    "languages_present": languages_present,
                })
                continue
            if strict and mcount != 1:
                unused_inputs.append({
                    "source_file": sp.name,
                    "customer_name": cust,
                    "element": elem,
                    "target_file": target_path.name,
                    "target_column": None,
                    "reason": "strict_single_match_failed",
                    "languages_present": languages_present,
                })
                continue
            row_indices = df_t.index[mask].tolist()
            for row_idx in row_indices:
                for lang, text in texts.items():
                    if text is None:
                        continue
                    text_norm = _normalize_collision_text(text)
                    if text_norm == "":
                        continue
                    target_col = job.source.content.languages[lang].target_column
                    touched_columns.setdefault(target_path.name, set()).add(target_col)
                    key = (target_path.name, row_idx, target_col)
                    if key in seen_updates:
                        prev = seen_updates[key]
                        if prev["text_norm"] != text_norm:
                            collisions.append({
                                "target_file": target_path.name,
                                "row_index": row_idx,
                                "target_column": target_col,
                                "language": lang,
                                "customer_name": cust,
                                "source_file": sp.name,
                                "element": elem,
                                "previous_source_file": prev["source_file"],
                                "previous_element": prev["element"],
                                "old_content": prev["text_raw"],
                                "new_content": text,
                            })
                            seen_updates[key] = {
                                "source_file": sp.name,
                                "element": elem,
                                "text_norm": text_norm,
                                "text_raw": text,
                            }
                    else:
                        seen_updates[key] = {
                            "source_file": sp.name,
                            "element": elem,
                            "text_norm": text_norm,
                            "text_raw": text,
                        }
            targets[target_path.name] = _apply_updates(df_t, mask, texts, job)
            total_rows_touched += (mcount if not strict else 1)
            source_wrote = True
        if any_text and not any_mapped:
            unmapped_inputs.append({
                "source_file": sp.name,
                "customer_name": cust_for_log,
                "example_element": example_element,
                "reason": "no_target_file_match"
            })
        if any_text and not source_wrote:
            summary_log.append({
                "category": "source_no_transfer",
                "source_file": sp.name,
                "customer_name": cust_for_log,
                "example_element": example_element,
            })

    for tname, df in targets.items():
        write_table(df, out_targets / tname)

    if do_reports:
        report_df.to_csv(out_base / "report.csv", index=False, encoding="utf-8")
        blocked_df.to_csv(out_base / "blocked.csv", index=False, encoding="utf-8")
        if social_unmapped:
            pd.DataFrame(social_unmapped).to_csv(out_base / "social_unmapped.csv", index=False, encoding="utf-8")
        if unmapped_inputs:
            pd.DataFrame(unmapped_inputs).to_csv(out_base / "unmapped_inputs.csv", index=False, encoding="utf-8")
        target_columns = [cfg.target_column for cfg in job.source.content.languages.values()]
        for p in tgt_paths:
            touched = touched_columns.get(p.name, set())
            for col in target_columns:
                if col not in touched:
                    unused_inputs.append({
                        "source_file": None,
                        "customer_name": None,
                        "element": None,
                        "target_file": p.name,
                        "target_column": col,
                        "reason": "target_column_unused",
                        "languages_present": None,
                    })
        if unused_inputs:
            pd.DataFrame(unused_inputs).to_csv(out_base / "unused_inputs.csv", index=False, encoding="utf-8")
        if do_collisions and collisions:
            pd.DataFrame(collisions).to_csv(out_base / "collisions.csv", index=False, encoding="utf-8")
        empty_targets = []
        for tname, df in targets.items():
            cols = [cfg.target_column for cfg in job.source.content.languages.values()]
            has_any = False
            for col in cols:
                if col in df.columns and df[col].map(_value_has_content).any():
                    has_any = True
                    break
            if not has_any:
                empty_targets.append(tname)
        for tname in empty_targets:
            summary_log.append({
                "category": "target_empty",
                "target_file": tname,
            })
        if summary_log:
            pd.DataFrame(summary_log).to_csv(out_base / "summary_log.csv", index=False, encoding="utf-8")
        audit = {
            "run_id": run_id,
            "timestamp": datetime.now().isoformat(),
            "status": "OK",
            "counts": {
                "sources": int(len(report_df)),
                "blocked_total": int(len(blocked_df)),
                "targets": int(len(tgt_paths)),
                "rows_touched_total": int(total_rows_touched),
            }
        }
        with open(out_base / "audit.json", "w", encoding="utf-8") as f:
            json.dump(audit, f, ensure_ascii=False, indent=2)

    return {"status": "OK", "output_dir": str(out_base), "updated_targets_dir": str(out_targets)}
