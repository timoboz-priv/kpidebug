import json
import uuid
from datetime import date

from kpidebug.analysis.insight_store import AbstractInsightStore
from kpidebug.analysis.types import (
    Action, Confidence, Counterfactual, Insight, InsightSource,
    Priority, RevenueImpact, Signal,
)
from kpidebug.common.db import ConnectionPoolManager


class PostgresInsightStore(AbstractInsightStore):
    def __init__(self, pool_manager: ConnectionPoolManager):
        self.pool = pool_manager.pool()

    def ensure_tables(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS insights (
                    id TEXT PRIMARY KEY,
                    project_id TEXT NOT NULL,
                    detected_at TEXT NOT NULL,
                    headline TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    source TEXT NOT NULL DEFAULT 'template',
                    data JSONB NOT NULL DEFAULT '{}',
                    UNIQUE (project_id, headline, detected_at)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_insights_project_date
                ON insights(project_id, detected_at DESC)
            """)
            conn.execute("""
                ALTER TABLE insights
                ADD COLUMN IF NOT EXISTS source TEXT
                NOT NULL DEFAULT 'template'
            """)

    def drop_tables(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("DROP TABLE IF EXISTS insights CASCADE")

    def clean(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("DELETE FROM insights")

    def store_insights(self, project_id: str, insights: list[Insight]) -> None:
        if not insights:
            return
        with self.pool.connection() as conn:
            for insight in insights:
                insight_id = insight.id or str(uuid.uuid4())
                detected_at = insight.detected_at.isoformat()
                data = _insight_to_json(insight)
                conn.execute(
                    """
                    INSERT INTO insights
                        (id, project_id, detected_at, headline,
                         description, source, data)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (project_id, headline, detected_at)
                    DO UPDATE SET
                        description = EXCLUDED.description,
                        source = EXCLUDED.source,
                        data = EXCLUDED.data
                    """,
                    (insight_id, project_id, detected_at,
                     insight.headline, insight.description,
                     insight.source.value,
                     json.dumps(data)),
                )

    def list_insights(self, project_id: str, limit: int = 20) -> list[Insight]:
        with self.pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT id, project_id, detected_at, headline,
                       description, source, data
                FROM insights
                WHERE project_id = %s
                ORDER BY detected_at DESC, headline
                LIMIT %s
                """,
                (project_id, limit),
            ).fetchall()
        return [_row_to_insight(r) for r in rows]


def _insight_to_json(insight: Insight) -> dict:
    return {
        "signals": [
            {"metric_id": s.metric_id, "description": s.description,
             "value": s.value, "change": s.change, "period_days": s.period_days}
            for s in insight.signals
        ],
        "actions": [
            {"description": a.description, "priority": a.priority.value}
            for a in insight.actions
        ],
        "counterfactual": {
            "value": insight.counterfactual.value,
            "metric_id": insight.counterfactual.metric_id,
            "metric_name": insight.counterfactual.metric_name,
            "description": insight.counterfactual.description,
            "revenue_impact": {
                "value": insight.counterfactual.revenue_impact.value,
                "description": insight.counterfactual.revenue_impact.description,
            },
        },
        "revenue_impact": {
            "value": insight.revenue_impact.value,
            "description": insight.revenue_impact.description,
        },
        "confidence": {
            "score": insight.confidence.score,
            "description": insight.confidence.description,
        },
    }


def _row_to_insight(row: tuple) -> Insight:
    source_str = row[5] or "template"
    source = InsightSource(source_str)
    data = row[6] if isinstance(row[6], dict) else json.loads(row[6])

    signals = [
        Signal(
            metric_id=s.get("metric_id", ""),
            description=s.get("description", ""),
            value=float(s.get("value", 0)),
            change=float(s.get("change", 0)),
            period_days=int(s.get("period_days", 0)),
        )
        for s in data.get("signals", [])
    ]

    actions = [
        Action(
            description=a.get("description", ""),
            priority=Priority(a.get("priority", "medium")),
        )
        for a in data.get("actions", [])
    ]

    cf_data = data.get("counterfactual", {})
    cf_ri = cf_data.get("revenue_impact", {})
    counterfactual = Counterfactual(
        value=float(cf_data.get("value", 0)),
        metric_id=cf_data.get("metric_id", ""),
        metric_name=cf_data.get("metric_name", ""),
        description=cf_data.get("description", ""),
        revenue_impact=RevenueImpact(
            value=float(cf_ri.get("value", 0)),
            description=cf_ri.get("description", ""),
        ),
    )

    ri_data = data.get("revenue_impact", {})
    revenue_impact = RevenueImpact(
        value=float(ri_data.get("value", 0)),
        description=ri_data.get("description", ""),
    )

    conf_data = data.get("confidence", {})
    confidence = Confidence(
        score=float(conf_data.get("score", 0)),
        description=conf_data.get("description", ""),
    )

    detected_at_str = row[2] or ""
    detected_at = date.fromisoformat(detected_at_str) if detected_at_str else date.today()

    return Insight(
        id=row[0],
        headline=row[3],
        description=row[4],
        detected_at=detected_at,
        signals=signals,
        actions=actions,
        counterfactual=counterfactual,
        revenue_impact=revenue_impact,
        confidence=confidence,
        source=source,
    )
