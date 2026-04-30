from __future__ import annotations

from dataclasses import dataclass, field as dataclass_field
from datetime import date, datetime, timezone
from enum import Enum

from dataclasses_json import dataclass_json


class Priority(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class InsightSource(str, Enum):
    TEMPLATE = "template"
    AGENTIC = "agentic"


class AnalysisStatus(str, Enum):
    SUCCESS = "success"
    PARTIAL = "partial"
    ERROR = "error"


@dataclass_json
@dataclass
class Signal:
    metric_id: str = ""
    description: str = ""
    value: float = 0.0
    change: float = 0.0
    period_days: int = 0


@dataclass_json
@dataclass
class Action:
    description: str = ""
    priority: Priority = Priority.MEDIUM


@dataclass_json
@dataclass
class RevenueImpact:
    value: float = 0.0
    description: str = ""


@dataclass_json
@dataclass
class Counterfactual:
    value: float = 0.0
    metric_id: str = ""
    metric_name: str = ""
    description: str = ""
    revenue_impact: RevenueImpact = dataclass_field(
        default_factory=RevenueImpact
    )


@dataclass_json
@dataclass
class Confidence:
    score: float = 0.0
    description: str = ""


@dataclass_json
@dataclass
class Insight:
    id: str = ""
    headline: str = ""
    description: str = ""
    detected_at: date = dataclass_field(default_factory=date.today)
    signals: list[Signal] = dataclass_field(default_factory=list)
    actions: list[Action] = dataclass_field(default_factory=list)
    counterfactual: Counterfactual = dataclass_field(
        default_factory=Counterfactual
    )
    revenue_impact: RevenueImpact = dataclass_field(
        default_factory=RevenueImpact
    )
    confidence: Confidence = dataclass_field(
        default_factory=Confidence
    )
    source: InsightSource = InsightSource.TEMPLATE


@dataclass_json
@dataclass
class AnalysisResult:
    insights: list[Insight] = dataclass_field(default_factory=list)
    analyzed_at: datetime = dataclass_field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    status: AnalysisStatus = AnalysisStatus.SUCCESS
    status_message: str = ""
