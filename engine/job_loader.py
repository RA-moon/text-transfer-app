from __future__ import annotations
import yaml
from .models import (
    JobConfig, SourceConfig, ElementConfig, CustomerConfig, CustomerConfigCsv,
    ContentConfig, ContentLangConfig, ContentLanguageConfig,
    CustomerKeySpec, CustomerMatchConfig,
    TargetConfig, TargetMatchConfig, TargetWriteConfig, TargetBehaviorConfig,
    OutputConfig
)

class JobConfigError(ValueError):
    pass

def _require(d: dict, key: str):
    if key not in d:
        raise JobConfigError(f"Missing key: {key}")
    return d[key]

def _as_list(value) -> list[str]:
    if isinstance(value, list):
        return [str(v) for v in value]
    return [str(value)]

def _parse_customer_csv(cust_csv: dict) -> CustomerConfigCsv:
    row_match = _require(cust_csv, "row_match")
    row_match_column = _require(row_match, "column")
    row_match_equals = _require(row_match, "equals")
    value_column_preference = _require(cust_csv, "value_column_preference")
    return CustomerConfigCsv(
        row_match_column=row_match_column,
        row_match_equals=row_match_equals,
        value_column_preference=value_column_preference
    )

def _parse_key_spec(raw: dict, default_type: str | None = None) -> CustomerKeySpec:
    if raw is None:
        raise JobConfigError("customer_match source/target missing")
    t = str(raw.get("type", default_type or "")).strip()
    if not t:
        raise JobConfigError("customer_match source/target type missing")
    csv_cfg = None
    if "csv" in raw and raw["csv"] is not None:
        csv_cfg = _parse_customer_csv(raw["csv"])
    return CustomerKeySpec(
        type=t,
        column=raw.get("column"),
        filename_regex=raw.get("filename_regex"),
        excel_cell=raw.get("excel_cell"),
        csv=csv_cfg
    )

def load_job_from_raw(raw: dict) -> JobConfig:
    if not isinstance(raw, dict):
        raise JobConfigError("Job config must be a mapping/object")
    job_name = _require(raw, "job_name")

    src = _require(raw, "source")
    src_file_types = _require(src, "file_types")

    elem_raw = src.get("element", {}) or {}
    elem_column = str(elem_raw.get("column", "Element"))
    elem_map = elem_raw.get("map", {}) or {}
    if not isinstance(elem_map, dict):
        raise JobConfigError("source.element.map must be a mapping")
    elem_fuzzy_threshold = float(elem_raw.get("fuzzy_threshold", 0.78))
    elem_rules = elem_raw.get("rules", []) or []
    if not isinstance(elem_rules, list):
        raise JobConfigError("source.element.rules must be a list")

    cust = src.get("customer")
    if cust:
        cust_method = _require(cust, "method")
        excel_cell = _require(cust, "excel_cell")
        cust_csv = _require(cust, "csv")
        customer_cfg = CustomerConfig(
            method=cust_method,
            excel_cell=excel_cell,
            csv=_parse_customer_csv(cust_csv)
        )
    else:
        customer_cfg = CustomerConfig(method="excel_cell_or_csv_row", excel_cell="E5", csv=CustomerConfigCsv("Element","Firmenname",["Unnamed: 4","col_index:4"]))

    tgt = _require(raw, "target")
    tgt_file_types = [str(x).lower() for x in _require(tgt, "file_types")]
    match = _require(tgt, "match")
    write = _require(tgt, "write")
    behavior = _require(tgt, "behavior")

    target_cfg = TargetConfig(
        file_types=tgt_file_types,
        match=TargetMatchConfig(
            column=_require(match, "column"),
            normalize=list(_require(match, "normalize")),
            mode=_require(match, "mode"),
        ),
        write=TargetWriteConfig(
            de_column=_require(write, "de_column"),
            fr_column=_require(write, "fr_column"),
        ),
        behavior=TargetBehaviorConfig(
            overwrite_existing=bool(_require(behavior, "overwrite_existing")),
            write_only_if_present=bool(_require(behavior, "write_only_if_present")),
            strict_single_match=bool(_require(behavior, "strict_single_match")),
        )
    )

    out = _require(raw, "output")
    output_cfg = OutputConfig(
        write_reports=bool(_require(out, "write_reports")),
        reports_exclude_text=bool(_require(out, "reports_exclude_text")),
    )

    content = _require(src, "content")
    if "languages" in content:
        mode = str(content.get("mode", "row_columns"))
        langs_raw = content.get("languages", {}) or {}
        languages: dict[str, ContentLanguageConfig] = {}
        for lang, cfg in langs_raw.items():
            if cfg is None:
                continue
            cols = cfg.get("columns")
            if cols is None:
                cols = cfg.get("column")
            if cols is None:
                raise JobConfigError(f"content.languages.{lang} missing columns")
            target_col = cfg.get("target_column")
            if target_col is None:
                raise JobConfigError(f"content.languages.{lang} missing target_column")
            languages[str(lang)] = ContentLanguageConfig(
                columns=_as_list(cols),
                target_column=str(target_col),
            )
        content_cfg = ContentConfig(mode=mode, languages=languages)
    else:
        de = _require(content, "de")
        fr = _require(content, "fr")
        de_cfg = ContentLangConfig(method=_require(de, "method"), column=_as_list(_require(de, "column")))
        fr_cfg = ContentLangConfig(method=_require(fr, "method"), column=_as_list(_require(fr, "column")))
        content_cfg = ContentConfig(
            mode="row_columns" if de_cfg.method == "row_columns" or fr_cfg.method == "row_columns" else "join_column",
            languages={
                "DE": ContentLanguageConfig(columns=de_cfg.column, target_column=target_cfg.write.de_column),
                "FR": ContentLanguageConfig(columns=fr_cfg.column, target_column=target_cfg.write.fr_column),
            }
        )

    source_cfg = SourceConfig(
        file_types=[str(x).lower() for x in src_file_types],
        element=ElementConfig(
            column=elem_column,
            map=elem_map,
            fuzzy_threshold=elem_fuzzy_threshold,
            rules=elem_rules,
        ),
        customer=customer_cfg,
        content=content_cfg
    )

    cm_raw = raw.get("customer_match")
    if cm_raw:
        cm_source = _parse_key_spec(cm_raw.get("source"), default_type="excel_cell_or_csv_row")
        cm_target = _parse_key_spec(cm_raw.get("target"), default_type="column")
        cm_normalize = _as_list(cm_raw.get("normalize", match.get("normalize", [])))
        cm_mode = str(cm_raw.get("mode", match.get("mode", "exact")))
        cm_fuzzy_threshold = cm_raw.get("fuzzy_threshold")
    else:
        cm_source = CustomerKeySpec(
            type="excel_cell_or_csv_row",
            excel_cell=customer_cfg.excel_cell,
            csv=customer_cfg.csv
        )
        cm_target = CustomerKeySpec(
            type="column",
            column=match.get("column")
        )
        cm_normalize = list(match.get("normalize", []))
        cm_mode = str(match.get("mode", "exact"))
        cm_fuzzy_threshold = None

    customer_match_cfg = CustomerMatchConfig(
        source=cm_source,
        target=cm_target,
        normalize=cm_normalize,
        mode=cm_mode,
        fuzzy_threshold=float(cm_fuzzy_threshold) if cm_fuzzy_threshold is not None else None,
    )

    return JobConfig(
        job_name=job_name,
        source=source_cfg,
        target=target_cfg,
        output=output_cfg,
        customer_match=customer_match_cfg,
    )

def load_job(path: str) -> JobConfig:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return load_job_from_raw(raw)
