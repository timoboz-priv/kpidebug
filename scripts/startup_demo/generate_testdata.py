"""Generate synthetic test data for a small SaaS startup.

Produces CSV files in testdata/startup_demo/ for all Stripe and
Google Analytics tables, with embedded anomaly events that trigger
all 10 analysis templates (one anomaly per template).

Usage:
    python scripts/startup_demo/generate_testdata.py
"""

import csv
import os
import random
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

SEED = 42
OUTPUT_DIR = Path(__file__).resolve().parent.parent.parent / "testdata" / "startup_demo"
NUM_DAYS = 365
END_DATE = date(2026, 4, 28)
START_DATE = END_DATE - timedelta(days=NUM_DAYS - 1)

# --- Product catalog ---

PRODUCTS = [
    {"id": "prod_starter", "name": "Starter Plan", "description": "Basic analytics for small teams", "price_cents": 2900, "interval": "month"},
    {"id": "prod_growth", "name": "Growth Plan", "description": "Advanced features for growing startups", "price_cents": 3900, "interval": "month"},
    {"id": "prod_pro", "name": "Pro Plan", "description": "Full-featured plan for professionals", "price_cents": 4900, "interval": "month"},
    {"id": "prod_team", "name": "Team Plan", "description": "Collaboration features for teams", "price_cents": 7900, "interval": "month"},
    {"id": "prod_enterprise", "name": "Enterprise Plan", "description": "Custom solution for large organizations", "price_cents": 14900, "interval": "month"},
]

PRODUCT_WEIGHTS = [0.30, 0.28, 0.22, 0.13, 0.07]

# --- Traffic channels ---

CHANNELS = [
    {"source": "google", "medium": "organic", "channel_group": "Organic Search", "campaign": "(not set)"},
    {"source": "(direct)", "medium": "(none)", "channel_group": "Direct", "campaign": "(not set)"},
    {"source": "google", "medium": "cpc", "channel_group": "Paid Search", "campaign": "brand_awareness"},
    {"source": "facebook", "medium": "social", "channel_group": "Organic Social", "campaign": "(not set)"},
    {"source": "partner-blog.com", "medium": "referral", "channel_group": "Referral", "campaign": "(not set)"},
    {"source": "mailchimp", "medium": "email", "channel_group": "Email", "campaign": "weekly_newsletter"},
]

CHANNEL_WEIGHTS = [0.40, 0.25, 0.15, 0.10, 0.07, 0.03]

# --- Website pages ---

PAGES = [
    {"path": "/", "title": "Home - Quantifiction"},
    {"path": "/features", "title": "Features - Quantifiction"},
    {"path": "/pricing", "title": "Pricing - Quantifiction"},
    {"path": "/blog", "title": "Blog - Quantifiction"},
    {"path": "/blog/analytics-guide", "title": "The Complete Analytics Guide"},
    {"path": "/blog/startup-metrics", "title": "Key Startup Metrics to Track"},
    {"path": "/signup", "title": "Sign Up - Quantifiction"},
    {"path": "/login", "title": "Login - Quantifiction"},
    {"path": "/dashboard", "title": "Dashboard - Quantifiction"},
    {"path": "/docs", "title": "Documentation - Quantifiction"},
]

PAGE_WEIGHTS = [0.20, 0.12, 0.10, 0.08, 0.08, 0.05, 0.12, 0.08, 0.10, 0.07]

LANDING_PAGES = ["/", "/features", "/pricing", "/blog", "/blog/analytics-guide", "/signup"]
LANDING_WEIGHTS = [0.30, 0.15, 0.15, 0.15, 0.15, 0.10]

HOSTNAME = "www.quantifiction.io"

# --- Geography ---

COUNTRIES = [
    {"country": "United States", "city": "San Francisco", "region": "California", "continent": "Americas", "language": "en-us"},
    {"country": "United States", "city": "New York", "region": "New York", "continent": "Americas", "language": "en-us"},
    {"country": "United Kingdom", "city": "London", "region": "England", "continent": "Europe", "language": "en-gb"},
    {"country": "Germany", "city": "Berlin", "region": "Berlin", "continent": "Europe", "language": "de"},
    {"country": "Canada", "city": "Toronto", "region": "Ontario", "continent": "Americas", "language": "en-ca"},
    {"country": "Australia", "city": "Sydney", "region": "New South Wales", "continent": "Oceania", "language": "en-au"},
    {"country": "France", "city": "Paris", "region": "Ile-de-France", "continent": "Europe", "language": "fr"},
    {"country": "India", "city": "Bangalore", "region": "Karnataka", "continent": "Asia", "language": "en-in"},
]

GEO_WEIGHTS = [0.30, 0.15, 0.12, 0.10, 0.08, 0.07, 0.06, 0.12]

# --- Devices ---

DEVICES = [
    {"category": "desktop", "browser": "Chrome", "os": "Windows", "platform": "WEB"},
    {"category": "desktop", "browser": "Chrome", "os": "Macintosh", "platform": "WEB"},
    {"category": "desktop", "browser": "Safari", "os": "Macintosh", "platform": "WEB"},
    {"category": "mobile", "browser": "Safari", "os": "iOS", "platform": "WEB"},
    {"category": "mobile", "browser": "Chrome", "os": "Android", "platform": "WEB"},
    {"category": "tablet", "browser": "Safari", "os": "iOS", "platform": "WEB"},
]

DEVICE_WEIGHTS = [0.30, 0.18, 0.12, 0.20, 0.15, 0.05]

# --- Event types ---

EVENT_NAMES = ["page_view", "scroll", "click", "sign_up", "purchase", "add_to_cart", "begin_checkout", "view_item"]
EVENT_WEIGHTS = [0.40, 0.20, 0.15, 0.05, 0.03, 0.04, 0.03, 0.10]

# --- Card brands ---

CARD_BRANDS = ["visa", "mastercard", "amex", "discover"]
CARD_WEIGHTS = [0.55, 0.25, 0.12, 0.08]

# --- Names for customers ---

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Chris", "Lisa", "Daniel", "Nancy",
    "Alex", "Maria", "Ryan", "Emma", "Kevin", "Olivia", "Brian", "Sophia",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Wilson", "Anderson", "Thomas",
    "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White",
    "Harris", "Clark", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
]


def iso(d: date, hour: int = 12, minute: int = 0) -> str:
    return datetime(d.year, d.month, d.day, hour, minute, 0, tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def date_iso(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def uid(prefix: str = "") -> str:
    short = uuid.uuid4().hex[:24]
    return f"{prefix}_{short}" if prefix else short


def weighted_choice(rng: random.Random, items: list, weights: list):
    return rng.choices(items, weights=weights, k=1)[0]


def day_offset(d: date) -> int:
    return (d - START_DATE).days


# --- Anomaly events ---

class AnomalyEvent:
    start: date
    duration_days: int
    event_type: str
    params: dict

    def __init__(self, start: date, duration_days: int, event_type: str, **params):
        self.start = start
        self.duration_days = duration_days
        self.event_type = event_type
        self.params = params

    def is_active(self, d: date) -> bool:
        return self.start <= d < self.start + timedelta(days=self.duration_days)


ANOMALY_EVENTS = [
    AnomalyEvent(END_DATE - timedelta(days=330), 14, "acquisition_drop", channel="Organic Search", drop_factor=0.50),
    AnomalyEvent(END_DATE - timedelta(days=300), 14, "conversion_drop", drop_factor=0.35),
    AnomalyEvent(END_DATE - timedelta(days=270), 14, "segment_drop", country="United States", geo_drop=0.50, global_drop=0.14),
    AnomalyEvent(END_DATE - timedelta(days=240), 14, "returning_user_drop", drop_factor=0.40),
    AnomalyEvent(END_DATE - timedelta(days=210), 14, "payment_failure", failure_rate=0.40),
    AnomalyEvent(END_DATE - timedelta(days=180), 14, "onboarding_break", signup_boost=0.25, s2p_drop=0.35),
    AnomalyEvent(END_DATE - timedelta(days=150), 14, "pricing_shift", conv_drop=0.25, rev_multiplier=1.50, session_boost=0.06),
    AnomalyEvent(END_DATE - timedelta(days=120), 14, "s2p_decline", drop_factor=0.55),
    AnomalyEvent(END_DATE - timedelta(days=90), 35, "engagement_decay", max_drop=0.12),
    AnomalyEvent(END_DATE - timedelta(days=20), 14, "vanity_traffic", session_boost=0.30, conv_drop=0.25),
]


def get_session_multiplier(d: date) -> dict[str, float]:
    multipliers: dict[str, float] = {ch["channel_group"]: 1.0 for ch in CHANNELS}
    for event in ANOMALY_EVENTS:
        if not event.is_active(d):
            continue
        if event.event_type == "acquisition_drop":
            channel = event.params["channel"]
            drop = event.params["drop_factor"]
            if channel == "all":
                for k in multipliers:
                    multipliers[k] *= (1.0 - drop)
            elif channel in multipliers:
                multipliers[channel] *= (1.0 - drop)
        elif event.event_type == "segment_drop":
            for k in multipliers:
                multipliers[k] *= (1.0 - event.params["global_drop"])
        elif event.event_type == "pricing_shift":
            for k in multipliers:
                multipliers[k] *= (1.0 + event.params["session_boost"])
        elif event.event_type == "vanity_traffic":
            for k in multipliers:
                multipliers[k] *= (1.0 + event.params["session_boost"])
    return multipliers


def get_conversion_multiplier(d: date) -> float:
    mult = 1.0
    for event in ANOMALY_EVENTS:
        if not event.is_active(d):
            continue
        if event.event_type == "conversion_drop":
            mult *= (1.0 - event.params["drop_factor"])
        elif event.event_type == "onboarding_break":
            mult *= (1.0 + event.params["signup_boost"])
        elif event.event_type == "pricing_shift":
            mult *= (1.0 - event.params["conv_drop"])
        elif event.event_type == "vanity_traffic":
            mult *= (1.0 - event.params["conv_drop"])
    return mult


def get_geo_multiplier(d: date) -> dict[str, float]:
    multipliers: dict[str, float] = {}
    for event in ANOMALY_EVENTS:
        if event.event_type == "segment_drop" and event.is_active(d):
            country = event.params["country"]
            geo_drop = event.params["geo_drop"]
            global_drop = event.params["global_drop"]
            extra = (1.0 - geo_drop) / (1.0 - global_drop)
            multipliers[country] = multipliers.get(country, 1.0) * extra
    return multipliers


def get_returning_user_multiplier(d: date) -> float:
    mult = 1.0
    for event in ANOMALY_EVENTS:
        if event.event_type == "returning_user_drop" and event.is_active(d):
            mult *= (1.0 - event.params["drop_factor"])
    return mult


def get_payment_failure_rate(d: date) -> float:
    rate = 0.0
    for event in ANOMALY_EVENTS:
        if event.event_type == "payment_failure" and event.is_active(d):
            rate = max(rate, event.params["failure_rate"])
    return rate


def get_s2p_multiplier(d: date) -> float:
    mult = 1.0
    for event in ANOMALY_EVENTS:
        if not event.is_active(d):
            continue
        if event.event_type == "onboarding_break":
            mult *= (1.0 - event.params["s2p_drop"])
        elif event.event_type == "s2p_decline":
            mult *= (1.0 - event.params["drop_factor"])
    return mult


def get_revenue_multiplier(d: date) -> float:
    mult = 1.0
    for event in ANOMALY_EVENTS:
        if event.event_type == "pricing_shift" and event.is_active(d):
            mult *= event.params["rev_multiplier"]
    return mult


def get_engagement_multiplier(d: date) -> float:
    mult = 1.0
    for event in ANOMALY_EVENTS:
        if event.event_type == "engagement_decay" and event.is_active(d):
            days_in = (d - event.start).days
            progress = days_in / event.duration_days
            max_drop = event.params["max_drop"]
            mult *= (1.0 - max_drop * progress)
    return mult


def generate() -> None:
    rng = random.Random(SEED)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # --- State accumulators ---
    all_customers: list[dict] = []
    all_subscriptions: list[dict] = []
    all_charges: list[dict] = []
    all_invoices: list[dict] = []
    all_refunds: list[dict] = []
    all_balance_txns: list[dict] = []
    all_disputes: list[dict] = []
    all_payouts: list[dict] = []

    # GA accumulators (per-day rows)
    ga_sessions_daily: list[dict] = []
    ga_traffic_sources: list[dict] = []
    ga_pages: list[dict] = []
    ga_landing_pages: list[dict] = []
    ga_events: list[dict] = []
    ga_geography: list[dict] = []
    ga_devices: list[dict] = []
    ga_users: list[dict] = []
    ga_user_acquisition: list[dict] = []
    ga_conversions: list[dict] = []
    ga_ecommerce: list[dict] = []

    active_subscriptions: list[dict] = []
    customer_ids: set[str] = set()

    # Pre-create products and prices
    products_rows = []
    prices_rows = []
    product_created = iso(START_DATE - timedelta(days=30))
    for p in PRODUCTS:
        price_id = uid("price")
        products_rows.append({
            "id": p["id"],
            "name": p["name"],
            "description": p["description"],
            "active": True,
            "default_price": price_id,
            "type": "service",
            "created": product_created,
            "updated": product_created,
        })
        prices_rows.append({
            "id": price_id,
            "product": p["id"],
            "active": True,
            "currency": "usd",
            "unit_amount": p["price_cents"],
            "type": "recurring",
            "recurring_interval": p["interval"],
            "recurring_interval_count": 1,
            "nickname": p["name"],
            "tax_behavior": "exclusive",
            "created": product_created,
        })

    # --- Day-by-day simulation ---
    base_sessions = 1200
    base_conversion_rate = 0.035
    signup_to_paid_rate = 0.25
    monthly_churn_rate = 0.05
    daily_churn_rate = 1 - (1 - monthly_churn_rate) ** (1 / 30)

    for day_i in range(NUM_DAYS):
        d = START_DATE + timedelta(days=day_i)
        progress = day_i / NUM_DAYS

        # Organic growth curve (sessions grow ~40% over the year)
        growth_factor = 1.0 + 0.4 * progress

        # Weekend dip
        weekday = d.weekday()
        weekend_factor = 0.72 if weekday >= 5 else 1.0

        session_multipliers = get_session_multiplier(d)
        conversion_mult = get_conversion_multiplier(d)
        geo_mult = get_geo_multiplier(d)
        returning_mult = get_returning_user_multiplier(d)
        failure_rate = get_payment_failure_rate(d)
        s2p_mult = get_s2p_multiplier(d)
        rev_mult = get_revenue_multiplier(d)
        engagement_mult = get_engagement_multiplier(d)

        # --- GA: Traffic Sources (per channel per day) ---
        total_sessions = 0
        total_users = 0
        total_new_users = 0
        total_page_views = 0
        total_engaged = 0
        total_events = 0
        total_conversions = 0
        total_ga_revenue = 0.0
        daily_bounce_rate_sum = 0.0
        daily_engagement_rate_sum = 0.0
        daily_duration_sum = 0.0
        day_signups = 0

        channel_sessions: dict[str, int] = {}

        for ch_i, ch in enumerate(CHANNELS):
            ch_base = int(base_sessions * CHANNEL_WEIGHTS[ch_i] * growth_factor * weekend_factor)
            ch_noise = rng.gauss(1.0, 0.08)
            ch_mult = session_multipliers.get(ch["channel_group"], 1.0)
            sessions = max(1, int(ch_base * ch_noise * ch_mult))
            channel_sessions[ch["channel_group"]] = sessions

            users = int(sessions * rng.uniform(0.75, 0.90))
            new_u = int(users * rng.uniform(0.30, 0.50))
            pvs = int(sessions * rng.uniform(2.5, 4.0))
            engaged = int(sessions * rng.uniform(0.50, 0.65))
            bounce_base = rng.uniform(0.38, 0.52)
            bounce = 1.0 - (1.0 - bounce_base) * engagement_mult
            engage_rate = (1.0 - bounce_base + rng.uniform(-0.02, 0.02)) * engagement_mult
            avg_dur = rng.uniform(90, 200) * engagement_mult
            evts = int(sessions * rng.uniform(3.0, 6.0))

            effective_conv_rate = base_conversion_rate * conversion_mult
            convs = max(0, int(sessions * effective_conv_rate * rng.uniform(0.8, 1.2)))
            ch_revenue = convs * rng.uniform(30, 60)

            total_sessions += sessions
            total_users += users
            total_new_users += new_u
            total_page_views += pvs
            total_engaged += engaged
            total_events += evts
            total_conversions += convs
            total_ga_revenue += ch_revenue
            daily_bounce_rate_sum += bounce * sessions
            daily_engagement_rate_sum += engage_rate * sessions
            daily_duration_sum += avg_dur * sessions
            day_signups += convs

            ga_traffic_sources.append({
                "date": iso(d, 0, 0),
                "session_source": ch["source"],
                "session_medium": ch["medium"],
                "session_campaign": ch["campaign"],
                "session_channel_group": ch["channel_group"],
                "sessions": sessions,
                "total_users": users,
                "new_users": new_u,
                "page_views": pvs,
                "bounce_rate": round(bounce * 100, 2),
                "engagement_rate": round(engage_rate * 100, 2),
                "engaged_sessions": engaged,
                "avg_session_duration": round(avg_dur, 1),
                "event_count": evts,
                "conversions": convs,
                "total_revenue": round(ch_revenue, 2),
            })

        # --- GA: Sessions Daily ---
        avg_bounce = (daily_bounce_rate_sum / total_sessions * 100) if total_sessions else 45.0
        avg_engagement = (daily_engagement_rate_sum / total_sessions * 100) if total_sessions else 55.0
        avg_duration = (daily_duration_sum / total_sessions) if total_sessions else 120.0

        ga_sessions_daily.append({
            "date": iso(d, 0, 0),
            "sessions": total_sessions,
            "total_users": total_users,
            "new_users": total_new_users,
            "page_views": total_page_views,
            "avg_session_duration": round(avg_duration, 1),
            "bounce_rate": round(avg_bounce, 2),
            "engagement_rate": round(avg_engagement, 2),
            "engaged_sessions": total_engaged,
            "sessions_per_user": round(total_sessions / max(total_users, 1), 2),
            "page_views_per_session": round(total_page_views / max(total_sessions, 1), 2),
            "event_count": total_events,
            "conversions": total_conversions,
            "total_revenue": round(total_ga_revenue, 2),
        })

        # --- GA: Pages ---
        for pg_i, pg in enumerate(PAGES):
            pg_pvs = int(total_page_views * PAGE_WEIGHTS[pg_i] * rng.uniform(0.85, 1.15))
            pg_sessions = int(pg_pvs * rng.uniform(0.5, 0.8))
            pg_users = int(pg_sessions * rng.uniform(0.7, 0.9))
            pg_bounce = rng.uniform(0.30, 0.55)
            pg_engage = 1.0 - pg_bounce + rng.uniform(-0.02, 0.02)
            pg_dur = rng.uniform(60, 250)
            pg_engaged = int(pg_sessions * pg_engage)
            pg_evts = int(pg_sessions * rng.uniform(2, 5))
            pg_convs = int(pg_sessions * base_conversion_rate * conversion_mult * rng.uniform(0.5, 1.5)) if pg["path"] in ("/pricing", "/signup", "/features") else 0

            ga_pages.append({
                "date": iso(d, 0, 0),
                "page_path": pg["path"],
                "page_title": pg["title"],
                "hostname": HOSTNAME,
                "page_views": pg_pvs,
                "sessions": pg_sessions,
                "total_users": pg_users,
                "avg_session_duration": round(pg_dur, 1),
                "bounce_rate": round(pg_bounce * 100, 2),
                "engagement_rate": round(pg_engage * 100, 2),
                "engaged_sessions": pg_engaged,
                "event_count": pg_evts,
                "conversions": pg_convs,
            })

        # --- GA: Landing Pages ---
        for lp_i, lp in enumerate(LANDING_PAGES):
            lp_sessions = int(total_sessions * LANDING_WEIGHTS[lp_i] * rng.uniform(0.85, 1.15))
            lp_users = int(lp_sessions * rng.uniform(0.75, 0.90))
            lp_new = int(lp_users * rng.uniform(0.35, 0.55))
            lp_bounce = rng.uniform(0.35, 0.55)
            lp_engage = 1.0 - lp_bounce + rng.uniform(-0.02, 0.02)
            lp_dur = rng.uniform(80, 200)
            lp_convs = int(lp_sessions * base_conversion_rate * conversion_mult * rng.uniform(0.6, 1.4))
            lp_rev = lp_convs * rng.uniform(30, 60)

            ga_landing_pages.append({
                "date": iso(d, 0, 0),
                "landing_page": lp,
                "sessions": lp_sessions,
                "total_users": lp_users,
                "new_users": lp_new,
                "bounce_rate": round(lp_bounce * 100, 2),
                "engagement_rate": round(lp_engage * 100, 2),
                "avg_session_duration": round(lp_dur, 1),
                "conversions": lp_convs,
                "total_revenue": round(lp_rev, 2),
            })

        # --- GA: Events ---
        for ev_i, ev_name in enumerate(EVENT_NAMES):
            ev_count = int(total_events * EVENT_WEIGHTS[ev_i] * rng.uniform(0.85, 1.15))
            ev_users = int(ev_count * rng.uniform(0.3, 0.7))
            ev_per_user = round(ev_count / max(ev_users, 1), 2)
            ev_value = round(ev_count * rng.uniform(0.5, 2.0), 2) if ev_name in ("purchase", "add_to_cart") else 0.0
            ev_convs = total_conversions if ev_name == "sign_up" else (int(total_conversions * 0.6 * s2p_mult * rng.uniform(0.8, 1.2)) if ev_name == "purchase" else 0)
            ev_rev = round(ev_convs * rng.uniform(30, 60) * rev_mult, 2) if ev_name == "purchase" else 0.0

            ga_events.append({
                "date": iso(d, 0, 0),
                "event_name": ev_name,
                "event_count": ev_count,
                "total_users": ev_users,
                "event_count_per_user": ev_per_user,
                "event_value": ev_value,
                "conversions": ev_convs,
                "total_revenue": ev_rev,
            })

        # --- GA: Geography ---
        for geo_i, geo in enumerate(COUNTRIES):
            g_geo_mult = geo_mult.get(geo["country"], 1.0)
            g_sessions = int(total_sessions * GEO_WEIGHTS[geo_i] * rng.uniform(0.80, 1.20) * g_geo_mult)
            g_users = int(g_sessions * rng.uniform(0.70, 0.90))
            g_new = int(g_users * rng.uniform(0.30, 0.50))
            g_pvs = int(g_sessions * rng.uniform(2.5, 4.0))
            g_bounce = rng.uniform(0.38, 0.52)
            g_engage = 1.0 - g_bounce + rng.uniform(-0.02, 0.02)
            g_dur = rng.uniform(80, 200)
            g_convs = int(g_sessions * base_conversion_rate * conversion_mult * rng.uniform(0.6, 1.4))

            ga_geography.append({
                "date": iso(d, 0, 0),
                "country": geo["country"],
                "city": geo["city"],
                "region": geo["region"],
                "continent": geo["continent"],
                "language": geo["language"],
                "sessions": g_sessions,
                "total_users": g_users,
                "new_users": g_new,
                "page_views": g_pvs,
                "bounce_rate": round(g_bounce * 100, 2),
                "engagement_rate": round(g_engage * 100, 2),
                "avg_session_duration": round(g_dur, 1),
                "conversions": g_convs,
            })

        # --- GA: Devices ---
        for dev_i, dev in enumerate(DEVICES):
            dv_sessions = int(total_sessions * DEVICE_WEIGHTS[dev_i] * rng.uniform(0.85, 1.15))
            dv_users = int(dv_sessions * rng.uniform(0.70, 0.90))
            dv_new = int(dv_users * rng.uniform(0.30, 0.50))
            dv_pvs = int(dv_sessions * rng.uniform(2.0, 4.5))
            dv_bounce = rng.uniform(0.35, 0.55)
            dv_engage = 1.0 - dv_bounce + rng.uniform(-0.02, 0.02)
            dv_dur = rng.uniform(70, 220)
            dv_convs = int(dv_sessions * base_conversion_rate * conversion_mult * rng.uniform(0.6, 1.4))

            ga_devices.append({
                "date": iso(d, 0, 0),
                "device_category": dev["category"],
                "browser": dev["browser"],
                "operating_system": dev["os"],
                "platform": dev["platform"],
                "sessions": dv_sessions,
                "total_users": dv_users,
                "new_users": dv_new,
                "page_views": dv_pvs,
                "bounce_rate": round(dv_bounce * 100, 2),
                "engagement_rate": round(dv_engage * 100, 2),
                "avg_session_duration": round(dv_dur, 1),
                "conversions": dv_convs,
            })

        # --- GA: Users (new vs returning) ---
        new_sessions = int(total_sessions * rng.uniform(0.35, 0.45))
        ret_sessions = int((total_sessions - new_sessions) * returning_mult)
        for user_type, u_sessions in [("new", new_sessions), ("returning", ret_sessions)]:
            u_users = int(u_sessions * rng.uniform(0.75, 0.90))
            u_new = u_users if user_type == "new" else 0
            u_engaged = int(u_sessions * rng.uniform(0.45, 0.65))
            u_engage_rate = round(u_engaged / max(u_sessions, 1) * 100, 2)
            u_dur = rng.uniform(80, 200) if user_type == "returning" else rng.uniform(60, 150)
            u_pvs = int(u_sessions * rng.uniform(2.0, 4.0))
            u_evts = int(u_sessions * rng.uniform(3.0, 6.0))
            u_convs = int(u_sessions * base_conversion_rate * conversion_mult * rng.uniform(0.8, 1.2))
            u_rev = round(u_convs * rng.uniform(30, 60), 2)

            ga_users.append({
                "date": iso(d, 0, 0),
                "new_vs_returning": user_type,
                "total_users": u_users,
                "new_users": u_new,
                "sessions": u_sessions,
                "engaged_sessions": u_engaged,
                "engagement_rate": u_engage_rate,
                "avg_session_duration": round(u_dur, 1),
                "page_views": u_pvs,
                "event_count": u_evts,
                "conversions": u_convs,
                "total_revenue": u_rev,
            })

        # --- GA: User Acquisition ---
        for ch_i, ch in enumerate(CHANNELS):
            ua_new = int(total_new_users * CHANNEL_WEIGHTS[ch_i] * rng.uniform(0.80, 1.20))
            ua_users = int(ua_new * rng.uniform(1.0, 1.3))
            ua_sessions = int(ua_users * rng.uniform(1.0, 1.5))
            ua_engaged = int(ua_sessions * rng.uniform(0.45, 0.65))
            ua_engage_rate = round(ua_engaged / max(ua_sessions, 1) * 100, 2)
            ua_evts = int(ua_sessions * rng.uniform(3.0, 5.0))
            ua_convs = int(ua_sessions * base_conversion_rate * conversion_mult * rng.uniform(0.6, 1.4))
            ua_rev = round(ua_convs * rng.uniform(30, 60), 2)

            ga_user_acquisition.append({
                "date": iso(d, 0, 0),
                "first_user_source": ch["source"],
                "first_user_medium": ch["medium"],
                "first_user_campaign": ch["campaign"],
                "first_user_channel_group": ch["channel_group"],
                "new_users": ua_new,
                "total_users": ua_users,
                "sessions": ua_sessions,
                "engaged_sessions": ua_engaged,
                "engagement_rate": ua_engage_rate,
                "event_count": ua_evts,
                "conversions": ua_convs,
                "total_revenue": ua_rev,
            })

        # --- GA: Conversions ---
        conv_event_names = ["sign_up", "purchase", "begin_checkout"]
        for conv_ev in conv_event_names:
            if conv_ev == "sign_up":
                c_convs = day_signups
            elif conv_ev == "purchase":
                c_convs = int(day_signups * signup_to_paid_rate * s2p_mult * rng.uniform(0.8, 1.2))
            else:
                c_convs = int(day_signups * rng.uniform(0.6, 1.0))

            for ch_i, ch in enumerate(CHANNELS):
                ch_frac = CHANNEL_WEIGHTS[ch_i]
                cc = max(0, int(c_convs * ch_frac * rng.uniform(0.7, 1.3)))
                cc_users = max(1, int(cc * rng.uniform(0.8, 1.0)))
                cc_sessions = int(cc_users * rng.uniform(1.0, 1.5))
                cc_evts = int(cc_sessions * rng.uniform(2, 5))
                cc_rev = round(cc * rng.uniform(30, 60) * rev_mult, 2) if conv_ev == "purchase" else 0.0

                ga_conversions.append({
                    "date": iso(d, 0, 0),
                    "event_name": conv_ev,
                    "session_source": ch["source"],
                    "session_medium": ch["medium"],
                    "session_channel_group": ch["channel_group"],
                    "conversions": cc,
                    "total_users": cc_users,
                    "sessions": cc_sessions,
                    "event_count": cc_evts,
                    "total_revenue": cc_rev,
                })

        # --- Stripe: new customers & subscriptions for the day ---
        new_paid = int(day_signups * signup_to_paid_rate * s2p_mult * rng.uniform(0.85, 1.15))
        new_paid = max(0, new_paid)

        daily_new_customers = []
        for _ in range(new_paid):
            cust_id = uid("cus")
            geo = weighted_choice(rng, COUNTRIES, GEO_WEIGHTS)
            first = rng.choice(FIRST_NAMES)
            last = rng.choice(LAST_NAMES)
            cust = {
                "id": cust_id,
                "name": f"{first} {last}",
                "email": f"{first.lower()}.{last.lower()}{rng.randint(1,999)}@example.com",
                "phone": None,
                "description": None,
                "balance": 0,
                "currency": "usd",
                "delinquent": False,
                "country": geo["country"],
                "city": geo["city"],
                "tax_exempt": "none",
                "created": iso(d, rng.randint(8, 22), rng.randint(0, 59)),
            }
            all_customers.append(cust)
            customer_ids.add(cust_id)
            daily_new_customers.append(cust)

            # Create subscription
            product = weighted_choice(rng, PRODUCTS, PRODUCT_WEIGHTS)
            sub_id = uid("sub")
            inv_id = uid("in")
            period_end = d + timedelta(days=30)
            sub = {
                "id": sub_id,
                "customer": cust_id,
                "status": "active",
                "amount": product["price_cents"],
                "currency": "usd",
                "interval": "month",
                "cancel_at_period_end": False,
                "canceled_at": None,
                "cancel_at": None,
                "trial_start": None,
                "trial_end": None,
                "current_period_start": iso(d, 0, 0),
                "current_period_end": iso(period_end, 0, 0),
                "description": product["name"],
                "default_payment_method": uid("pm"),
                "latest_invoice": inv_id,
                "start_date": iso(d, 0, 0),
                "created": cust["created"],
                "_product": product,
                "_day": d,
            }
            all_subscriptions.append(sub)
            active_subscriptions.append(sub)

            # Create charge
            charge_id = uid("ch")
            card_brand = weighted_choice(rng, CARD_BRANDS, CARD_WEIGHTS)
            charge_time = cust["created"]
            charge = {
                "id": charge_id,
                "amount": product["price_cents"],
                "amount_captured": product["price_cents"],
                "amount_refunded": 0,
                "currency": "usd",
                "status": "succeeded",
                "paid": True,
                "captured": True,
                "refunded": False,
                "disputed": False,
                "description": f"Subscription to {product['name']}",
                "customer": cust_id,
                "invoice": inv_id,
                "payment_intent": uid("pi"),
                "payment_method_type": "card",
                "card_brand": card_brand,
                "card_last4": f"{rng.randint(1000, 9999)}",
                "failure_code": None,
                "failure_message": None,
                "receipt_email": cust["email"],
                "created": charge_time,
            }
            all_charges.append(charge)

            # Balance transaction for charge
            fee = int(product["price_cents"] * 0.029) + 30
            net = product["price_cents"] - fee
            all_balance_txns.append({
                "id": uid("txn"),
                "amount": product["price_cents"],
                "fee": fee,
                "net": net,
                "currency": "usd",
                "type": "charge",
                "status": "available",
                "source": charge_id,
                "description": f"Payment for {product['name']}",
                "reporting_category": "charge",
                "available_on": iso(d + timedelta(days=2), 0, 0),
                "created": charge_time,
            })

            # Invoice
            all_invoices.append({
                "id": inv_id,
                "number": f"INV-{len(all_invoices) + 1:06d}",
                "amount_due": product["price_cents"],
                "amount_paid": product["price_cents"],
                "amount_remaining": 0,
                "subtotal": product["price_cents"],
                "total": product["price_cents"],
                "currency": "usd",
                "status": "paid",
                "paid": True,
                "customer": cust_id,
                "subscription": sub_id,
                "collection_method": "charge_automatically",
                "attempt_count": 1,
                "due_date": None,
                "period_start": iso(d, 0, 0),
                "period_end": iso(period_end, 0, 0),
                "created": charge_time,
            })

        # --- Stripe: subscription renewals ---
        for sub in active_subscriptions:
            if sub["status"] != "active":
                continue
            period_end_str = sub["current_period_end"]
            pe = datetime.strptime(period_end_str, "%Y-%m-%dT%H:%M:%SZ").date()
            if pe != d:
                continue

            product = sub["_product"]
            charge_id = uid("ch")
            inv_id = uid("in")
            charge_time = iso(d, rng.randint(0, 6), rng.randint(0, 59))
            new_period_end = d + timedelta(days=30)

            payment_failed = failure_rate > 0 and rng.random() < failure_rate

            if payment_failed:
                charge = {
                    "id": charge_id,
                    "amount": product["price_cents"],
                    "amount_captured": 0,
                    "amount_refunded": 0,
                    "currency": "usd",
                    "status": "failed",
                    "paid": False,
                    "captured": False,
                    "refunded": False,
                    "disputed": False,
                    "description": f"Subscription renewal - {product['name']}",
                    "customer": sub["customer"],
                    "invoice": inv_id,
                    "payment_intent": uid("pi"),
                    "payment_method_type": "card",
                    "card_brand": weighted_choice(rng, CARD_BRANDS, CARD_WEIGHTS),
                    "card_last4": f"{rng.randint(1000, 9999)}",
                    "failure_code": rng.choice(["card_declined", "expired_card", "insufficient_funds"]),
                    "failure_message": "Your card was declined.",
                    "receipt_email": None,
                    "created": charge_time,
                }
                all_charges.append(charge)

                all_invoices.append({
                    "id": inv_id,
                    "number": f"INV-{len(all_invoices) + 1:06d}",
                    "amount_due": product["price_cents"],
                    "amount_paid": 0,
                    "amount_remaining": product["price_cents"],
                    "subtotal": product["price_cents"],
                    "total": product["price_cents"],
                    "currency": "usd",
                    "status": "open",
                    "paid": False,
                    "customer": sub["customer"],
                    "subscription": sub["id"],
                    "collection_method": "charge_automatically",
                    "attempt_count": 1,
                    "due_date": None,
                    "period_start": iso(d, 0, 0),
                    "period_end": iso(new_period_end, 0, 0),
                    "created": charge_time,
                })
            else:
                charge = {
                    "id": charge_id,
                    "amount": product["price_cents"],
                    "amount_captured": product["price_cents"],
                    "amount_refunded": 0,
                    "currency": "usd",
                    "status": "succeeded",
                    "paid": True,
                    "captured": True,
                    "refunded": False,
                    "disputed": False,
                    "description": f"Subscription renewal - {product['name']}",
                    "customer": sub["customer"],
                    "invoice": inv_id,
                    "payment_intent": uid("pi"),
                    "payment_method_type": "card",
                    "card_brand": weighted_choice(rng, CARD_BRANDS, CARD_WEIGHTS),
                    "card_last4": f"{rng.randint(1000, 9999)}",
                    "failure_code": None,
                    "failure_message": None,
                    "receipt_email": None,
                    "created": charge_time,
                }
                all_charges.append(charge)

                fee = int(product["price_cents"] * 0.029) + 30
                net = product["price_cents"] - fee
                all_balance_txns.append({
                    "id": uid("txn"),
                    "amount": product["price_cents"],
                    "fee": fee,
                    "net": net,
                    "currency": "usd",
                    "type": "charge",
                    "status": "available",
                    "source": charge_id,
                    "description": f"Renewal for {product['name']}",
                    "reporting_category": "charge",
                    "available_on": iso(d + timedelta(days=2), 0, 0),
                    "created": charge_time,
                })

                all_invoices.append({
                    "id": inv_id,
                    "number": f"INV-{len(all_invoices) + 1:06d}",
                    "amount_due": product["price_cents"],
                    "amount_paid": product["price_cents"],
                    "amount_remaining": 0,
                    "subtotal": product["price_cents"],
                    "total": product["price_cents"],
                    "currency": "usd",
                    "status": "paid",
                    "paid": True,
                    "customer": sub["customer"],
                    "subscription": sub["id"],
                    "collection_method": "charge_automatically",
                    "attempt_count": 1,
                    "due_date": None,
                    "period_start": iso(d, 0, 0),
                    "period_end": iso(new_period_end, 0, 0),
                    "created": charge_time,
                })

            sub["current_period_start"] = iso(d, 0, 0)
            sub["current_period_end"] = iso(new_period_end, 0, 0)
            sub["latest_invoice"] = inv_id

        # --- Stripe: churn ---
        still_active = []
        for sub in active_subscriptions:
            if sub["status"] != "active":
                still_active.append(sub)
                continue
            if rng.random() < daily_churn_rate:
                sub["status"] = "canceled"
                sub["canceled_at"] = iso(d, rng.randint(0, 23), rng.randint(0, 59))
                sub["cancel_at_period_end"] = False
            else:
                still_active.append(sub)
        active_subscriptions = still_active

        # --- Stripe: refunds (small %) ---
        todays_charges = [c for c in all_charges if c["created"].startswith(d.isoformat())]
        for charge in todays_charges:
            if rng.random() < 0.02:
                refund_amount = charge["amount"]
                ref_id = uid("re")
                ref_time = iso(d, rng.randint(12, 23), rng.randint(0, 59))
                all_refunds.append({
                    "id": ref_id,
                    "amount": refund_amount,
                    "currency": "usd",
                    "status": "succeeded",
                    "reason": rng.choice(["duplicate", "requested_by_customer", "fraudulent"]),
                    "charge": charge["id"],
                    "payment_intent": charge["payment_intent"],
                    "failure_reason": None,
                    "description": None,
                    "created": ref_time,
                })
                charge["amount_refunded"] = refund_amount
                charge["refunded"] = True

                all_balance_txns.append({
                    "id": uid("txn"),
                    "amount": -refund_amount,
                    "fee": 0,
                    "net": -refund_amount,
                    "currency": "usd",
                    "type": "refund",
                    "status": "available",
                    "source": ref_id,
                    "description": f"Refund for {charge['id']}",
                    "reporting_category": "refund",
                    "available_on": iso(d + timedelta(days=5), 0, 0),
                    "created": ref_time,
                })

        # --- Stripe: disputes (very rare) ---
        for charge in todays_charges:
            if not charge["refunded"] and rng.random() < 0.003:
                disp_id = uid("dp")
                disp_time = iso(d, rng.randint(10, 20), rng.randint(0, 59))
                all_disputes.append({
                    "id": disp_id,
                    "amount": charge["amount"],
                    "currency": "usd",
                    "charge": charge["id"],
                    "payment_intent": charge["payment_intent"],
                    "reason": rng.choice(["general", "fraudulent", "product_not_received"]),
                    "status": rng.choice(["evidence_under_review", "won", "lost"]),
                    "is_charge_refundable": True,
                    "created": disp_time,
                })
                charge["disputed"] = True

        # --- Stripe: weekly payouts ---
        if weekday == 4:
            week_charges = [c for c in all_charges if d - timedelta(days=7) <= datetime.strptime(c["created"][:10], "%Y-%m-%d").date() <= d and c["paid"]]
            payout_amount = sum(c["amount"] for c in week_charges)
            payout_fee = sum(int(c["amount"] * 0.029) + 30 for c in week_charges)
            net_payout = payout_amount - payout_fee
            if net_payout > 0:
                payout_id = uid("po")
                payout_time = iso(d, 6, 0)
                all_payouts.append({
                    "id": payout_id,
                    "amount": net_payout,
                    "currency": "usd",
                    "status": "paid",
                    "type": "bank_account",
                    "method": "standard",
                    "description": "STRIPE PAYOUT",
                    "arrival_date": iso(d + timedelta(days=2), 0, 0),
                    "created": payout_time,
                })

                all_balance_txns.append({
                    "id": uid("txn"),
                    "amount": -net_payout,
                    "fee": 0,
                    "net": -net_payout,
                    "currency": "usd",
                    "type": "payout",
                    "status": "available",
                    "source": payout_id,
                    "description": "STRIPE PAYOUT",
                    "reporting_category": "payout",
                    "available_on": iso(d + timedelta(days=2), 0, 0),
                    "created": payout_time,
                })

        # --- GA: Ecommerce (matches Stripe purchases) ---
        for cust in daily_new_customers:
            product = None
            for sub in all_subscriptions:
                if sub["customer"] == cust["id"]:
                    product = sub["_product"]
                    break
            if product is None:
                continue

            ga_ecommerce.append({
                "date": iso(d, 0, 0),
                "transaction_id": uid("txn"),
                "item_name": product["name"],
                "item_brand": "Quantifiction",
                "item_category": "SaaS Subscription",
                "ecommerce_purchases": 1,
                "item_purchase_quantity": 1,
                "item_revenue": round(product["price_cents"] / 100, 2),
                "purchase_revenue": round(product["price_cents"] / 100, 2),
                "total_purchasers": 1,
                "add_to_carts": 1,
                "checkouts": 1,
            })

    # --- Clean up subscription rows (remove internal fields) ---
    for sub in all_subscriptions:
        sub.pop("_product", None)
        sub.pop("_day", None)

    # --- Write CSVs ---
    csv_map = {
        "stripe_products.csv": products_rows,
        "stripe_prices.csv": prices_rows,
        "stripe_customers.csv": all_customers,
        "stripe_subscriptions.csv": all_subscriptions,
        "stripe_charges.csv": all_charges,
        "stripe_invoices.csv": all_invoices,
        "stripe_refunds.csv": all_refunds,
        "stripe_balance_transactions.csv": all_balance_txns,
        "stripe_disputes.csv": all_disputes,
        "stripe_payouts.csv": all_payouts,
        "ga_sessions_daily.csv": ga_sessions_daily,
        "ga_traffic_sources.csv": ga_traffic_sources,
        "ga_pages.csv": ga_pages,
        "ga_landing_pages.csv": ga_landing_pages,
        "ga_events.csv": ga_events,
        "ga_geography.csv": ga_geography,
        "ga_devices.csv": ga_devices,
        "ga_users.csv": ga_users,
        "ga_user_acquisition.csv": ga_user_acquisition,
        "ga_conversions.csv": ga_conversions,
        "ga_ecommerce.csv": ga_ecommerce,
    }

    for filename, rows in csv_map.items():
        path = OUTPUT_DIR / filename
        if not rows:
            print(f"  {filename}: 0 rows (skipped)")
            continue
        fieldnames = list(rows[0].keys())
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"  {filename}: {len(rows)} rows")

    # --- Write parameters doc ---
    write_parameters_doc(all_customers, all_subscriptions, all_charges)

    print(f"\nGenerated {len(csv_map)} CSV files in {OUTPUT_DIR}")


def write_parameters_doc(customers: list, subscriptions: list, charges: list) -> None:
    total_revenue = sum(c["amount"] for c in charges if c["paid"]) / 100
    active_subs = sum(1 for s in subscriptions if s["status"] == "active")
    canceled_subs = sum(1 for s in subscriptions if s["status"] == "canceled")

    content = f"""# Startup Demo Test Data Parameters

## Overview
Synthetic data for a small SaaS startup ("Quantifiction") covering {NUM_DAYS} days
from {START_DATE.isoformat()} to {END_DATE.isoformat()}.

## Company Profile
- **Product**: SaaS analytics platform
- **Target**: ~$10M ARR
- **Currency**: USD

## Products & Pricing
| Plan | Monthly Price |
|------|--------------|
| Starter | $29/mo |
| Growth | $39/mo |
| Pro | $49/mo |
| Team | $79/mo |
| Enterprise | $149/mo |

Product distribution weights: Starter 30%, Growth 28%, Pro 22%, Team 13%, Enterprise 7%

## Customer Metrics
- **Total customers created**: {len(customers)}
- **Active subscriptions (end)**: {active_subs}
- **Canceled subscriptions**: {canceled_subs}
- **Monthly churn rate**: ~5%
- **Total gross revenue**: ${total_revenue:,.2f}

## Traffic Parameters
- **Base daily sessions**: ~1,200 (growing ~40% over the year)
- **Weekend factor**: 0.72x
- **Conversion rate (session to signup)**: ~3.5%
- **Signup to paid rate**: ~25%

### Traffic Channels
| Channel | Weight |
|---------|--------|
| Organic Search | 40% |
| Direct | 25% |
| Paid Search | 15% |
| Organic Social | 10% |
| Referral | 7% |
| Email | 3% |

### Engagement
- **Bounce rate**: ~38-52% (varies by channel)
- **Engagement rate**: ~50-65%
- **Avg session duration**: ~90-200s

## Embedded Anomaly Events (1 per template)

| # | Template | Date Range | Description |
|---|----------|-----------|-------------|
| 1 | AcquisitionDrop | {(END_DATE - timedelta(days=330)).isoformat()} to {(END_DATE - timedelta(days=316)).isoformat()} | Organic Search -50% |
| 2 | ConversionBreakdown | {(END_DATE - timedelta(days=300)).isoformat()} to {(END_DATE - timedelta(days=286)).isoformat()} | Conversion rate -35% |
| 3 | SegmentFailure | {(END_DATE - timedelta(days=270)).isoformat()} to {(END_DATE - timedelta(days=256)).isoformat()} | US geo -50%, global -14% |
| 4 | ReturningUserDrop | {(END_DATE - timedelta(days=240)).isoformat()} to {(END_DATE - timedelta(days=226)).isoformat()} | Returning users -40% |
| 5 | InvoluntaryChurn | {(END_DATE - timedelta(days=210)).isoformat()} to {(END_DATE - timedelta(days=196)).isoformat()} | 40% renewals fail |
| 6 | OnboardingFailure | {(END_DATE - timedelta(days=180)).isoformat()} to {(END_DATE - timedelta(days=166)).isoformat()} | Signups +25%, s2p -35% |
| 7 | PricingMismatch | {(END_DATE - timedelta(days=150)).isoformat()} to {(END_DATE - timedelta(days=136)).isoformat()} | Conv -25%, rev/conv x1.5 |
| 8 | CohortQuality | {(END_DATE - timedelta(days=120)).isoformat()} to {(END_DATE - timedelta(days=106)).isoformat()} | s2p -55% |
| 9 | ProductFriction | {(END_DATE - timedelta(days=90)).isoformat()} to {(END_DATE - timedelta(days=55)).isoformat()} | Gradual engagement -12% |
| 10 | MetricIllusion | {(END_DATE - timedelta(days=20)).isoformat()} to {(END_DATE - timedelta(days=6)).isoformat()} | Sessions +30%, conv -25% |

## Simulation Dates for Verification
Best detection date is event_start + 6 (7d window fully inside event).

```bash
python scripts/simulate.py test-startup-demo --date 2025-06-11  # AcquisitionDrop
python scripts/simulate.py test-startup-demo --date 2025-07-07  # ConversionBreakdown
python scripts/simulate.py test-startup-demo --date 2025-08-07  # SegmentFailure
python scripts/simulate.py test-startup-demo --date 2025-09-07  # ReturningUserDrop
python scripts/simulate.py test-startup-demo --date 2025-10-07  # InvoluntaryChurn
python scripts/simulate.py test-startup-demo --date 2025-11-05  # OnboardingFailure
python scripts/simulate.py test-startup-demo --date 2025-12-05  # PricingMismatch
python scripts/simulate.py test-startup-demo --date 2026-01-04  # CohortQuality
python scripts/simulate.py test-startup-demo --date 2026-03-04  # ProductFriction
python scripts/simulate.py test-startup-demo --date 2026-04-09  # MetricIllusion
```

## Random Seed
All data is generated with seed `{SEED}` for reproducibility.
"""
    with open(OUTPUT_DIR / "parameters.md", "w") as f:
        f.write(content)
    print("  parameters.md: written")


if __name__ == "__main__":
    generate()
