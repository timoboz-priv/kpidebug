from __future__ import annotations

from dataclasses import dataclass, field as dataclass_field
from datetime import datetime, timezone
from enum import Enum

from dataclasses_json import dataclass_json


class Priority(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


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
class UpsidePotential:
    value: float = 0.0
    metric_id: str = ""
    metric_name: str = ""
    description: str = ""


@dataclass_json
@dataclass
class Insight:
    headline: str = ""
    description: str = ""
    signals: list[Signal] = dataclass_field(default_factory=list)
    actions: list[Action] = dataclass_field(default_factory=list)
    upside_potential: UpsidePotential = dataclass_field(
        default_factory=UpsidePotential
    )


@dataclass_json
@dataclass
class AnalysisResult:
    insights: list[Insight] = dataclass_field(default_factory=list)
    analyzed_at: datetime = dataclass_field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
