from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Dict

@dataclass(frozen=True)
class CustomerConfigCsv:
    row_match_column: str
    row_match_equals: str
    value_column_preference: List[str]

@dataclass(frozen=True)
class CustomerConfig:
    method: str
    excel_cell: str
    csv: CustomerConfigCsv

@dataclass(frozen=True)
class ContentLangConfig:
    method: str
    column: List[str]

@dataclass(frozen=True)
class ContentLanguageConfig:
    columns: List[str]
    target_column: str

@dataclass(frozen=True)
class ContentConfig:
    mode: str
    languages: Dict[str, ContentLanguageConfig]

@dataclass(frozen=True)
class CustomerKeySpec:
    type: str
    column: Optional[str] = None
    filename_regex: Optional[str] = None
    excel_cell: Optional[str] = None
    csv: Optional[CustomerConfigCsv] = None

@dataclass(frozen=True)
class CustomerMatchConfig:
    source: CustomerKeySpec
    target: CustomerKeySpec
    normalize: List[str]
    mode: str
    fuzzy_threshold: Optional[float] = None

@dataclass(frozen=True)
class ElementConfig:
    column: str
    map: dict
    fuzzy_threshold: float
    rules: List[dict]

@dataclass(frozen=True)
class SourceConfig:
    file_types: List[str]
    element: ElementConfig
    customer: CustomerConfig
    content: ContentConfig

@dataclass(frozen=True)
class TargetMatchConfig:
    column: str
    normalize: List[str]
    mode: str

@dataclass(frozen=True)
class TargetWriteConfig:
    de_column: str
    fr_column: str

@dataclass(frozen=True)
class TargetBehaviorConfig:
    overwrite_existing: bool
    write_only_if_present: bool
    strict_single_match: bool

@dataclass(frozen=True)
class TargetConfig:
    file_types: List[str]
    match: TargetMatchConfig
    write: TargetWriteConfig
    behavior: TargetBehaviorConfig

@dataclass(frozen=True)
class OutputConfig:
    write_reports: bool
    reports_exclude_text: bool
    write_collisions: bool

@dataclass(frozen=True)
class SocialPlatformConfig:
    keywords: List[str]
    domains: List[str]

@dataclass(frozen=True)
class SocialConfig:
    platforms: Dict[str, SocialPlatformConfig]

@dataclass(frozen=True)
class JobConfig:
    job_name: str
    source: SourceConfig
    target: TargetConfig
    output: OutputConfig
    customer_match: CustomerMatchConfig
    social: Optional[SocialConfig] = None

@dataclass
class ReportRow:
    source_file: str
    element: Optional[str]
    target_file: Optional[str]
    customer_name: Optional[str]
    link_value: Optional[str]
    has_de: bool
    has_fr: bool
    languages_present: Optional[str]
    status: str
    reason: str
    written_rows_total: int
