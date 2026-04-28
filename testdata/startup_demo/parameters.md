# Startup Demo Test Data Parameters

## Overview
Synthetic data for a small SaaS startup ("Quantifiction") covering 365 days
from 2025-04-29 to 2026-04-28.

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
- **Total customers created**: 3584
- **Active subscriptions (end)**: 2686
- **Canceled subscriptions**: 898
- **Monthly churn rate**: ~5%
- **Total gross revenue**: $952,506.00

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

## Embedded Anomaly Events

### AcquisitionDrop events (sessions drop + conversion stable)
| # | Date Range | Description | Drop |
|---|-----------|-------------|------|
| 1 | 2026-01-28 to 2026-02-11 | Organic Search traffic drop (algorithm change) | -35% |
| 2 | 2025-10-30 to 2025-11-13 | Paid Search traffic drop (budget cut) | -50% |
| 3 | 2025-08-01 to 2025-08-15 | Overall session drop (holiday dip) | -20% |

### ConversionBreakdown events (conversion drops + sessions stable)
| # | Date Range | Description | Drop |
|---|-----------|-------------|------|
| 1 | 2026-02-27 to 2026-03-13 | Conversion rate drop (checkout bug) | -30% |
| 2 | 2025-11-29 to 2025-12-13 | Conversion rate drop (pricing page change) | -20% |

## Simulation Dates for Verification
Best detection date is event_start + 6: recent 7 days fully inside the event,
previous 7 days fully outside.

```bash
# AcquisitionDrop (event 1 - organic search drop)
python scripts/simulate.py test-startup-demo --date 2026-02-03

# AcquisitionDrop (event 2 - paid search drop)
python scripts/simulate.py test-startup-demo --date 2025-11-05

# ConversionBreakdown (event 1 - checkout bug)
python scripts/simulate.py test-startup-demo --date 2026-03-05

# ConversionBreakdown (event 2 - pricing page change)
python scripts/simulate.py test-startup-demo --date 2025-12-05
```

## Random Seed
All data is generated with seed `42` for reproducibility.
