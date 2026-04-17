from kpidebug.metrics.builtin_metrics import builtin_registry, MetricComputeResult


class TestBuiltinMetricRegistry:
    def test_all_metrics_registered(self):
        keys = [m.key for m in builtin_registry.list_all()]
        assert "stripe.gross_revenue" in keys
        assert "stripe.net_revenue" in keys
        assert "stripe.customer_count" in keys
        assert "stripe.mrr" in keys
        assert "stripe.refund_rate" in keys

    def test_get_unknown_returns_none(self):
        assert builtin_registry.get("nonexistent") is None

    def test_list_for_table(self):
        charge_metrics = builtin_registry.list_for_table("charges")
        keys = [m.key for m in charge_metrics]
        assert "stripe.gross_revenue" in keys
        assert "stripe.refund_rate" in keys
        assert "stripe.net_revenue" not in keys


class TestGrossRevenue:
    def test_sums_paid_charges(self):
        rows = [
            {"amount": 5000, "paid": True, "currency": "usd"},
            {"amount": 3000, "paid": True, "currency": "eur"},
            {"amount": 1000, "paid": False, "currency": "usd"},
        ]
        results = builtin_registry.compute("stripe.gross_revenue", rows)
        assert len(results) == 1
        assert results[0].value == 8000.0

    def test_grouped_by_currency(self):
        rows = [
            {"amount": 5000, "paid": True, "currency": "usd"},
            {"amount": 3000, "paid": True, "currency": "eur"},
            {"amount": 2000, "paid": True, "currency": "usd"},
        ]
        results = builtin_registry.compute("stripe.gross_revenue", rows, group_by=["currency"])
        by_group = {r.groups["currency"]: r.value for r in results}
        assert by_group["usd"] == 7000.0
        assert by_group["eur"] == 3000.0

    def test_empty_rows(self):
        results = builtin_registry.compute("stripe.gross_revenue", [])
        assert results[0].value == 0.0


class TestNetRevenue:
    def test_sums_net_for_charges(self):
        rows = [
            {"net": 4800, "type": "charge", "currency": "usd"},
            {"net": -500, "type": "refund", "currency": "usd"},
            {"net": 2900, "type": "charge", "currency": "eur"},
        ]
        results = builtin_registry.compute("stripe.net_revenue", rows)
        assert results[0].value == 7700.0


class TestCustomerCount:
    def test_counts_all_customers(self):
        rows = [
            {"id": "cus_1", "country": "US"},
            {"id": "cus_2", "country": "DE"},
            {"id": "cus_3", "country": "US"},
        ]
        results = builtin_registry.compute("stripe.customer_count", rows)
        assert results[0].value == 3.0

    def test_grouped_by_country(self):
        rows = [
            {"id": "cus_1", "country": "US"},
            {"id": "cus_2", "country": "DE"},
            {"id": "cus_3", "country": "US"},
        ]
        results = builtin_registry.compute("stripe.customer_count", rows, group_by=["country"])
        by_group = {r.groups["country"]: r.value for r in results}
        assert by_group["US"] == 2.0
        assert by_group["DE"] == 1.0


class TestMRR:
    def test_sums_active_subscriptions(self):
        rows = [
            {"amount": 2000, "status": "active", "currency": "usd"},
            {"amount": 5000, "status": "active", "currency": "usd"},
            {"amount": 3000, "status": "canceled", "currency": "usd"},
        ]
        results = builtin_registry.compute("stripe.mrr", rows)
        assert results[0].value == 7000.0


class TestRefundRate:
    def test_computes_rate(self):
        rows = [
            {"amount": 10000, "amount_refunded": 2000, "paid": True},
            {"amount": 5000, "amount_refunded": 0, "paid": True},
            {"amount": 3000, "amount_refunded": 0, "paid": False},
        ]
        results = builtin_registry.compute("stripe.refund_rate", rows)
        assert abs(results[0].value - 13.33) < 0.1

    def test_zero_when_no_charges(self):
        results = builtin_registry.compute("stripe.refund_rate", [])
        assert results[0].value == 0.0
