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
- **Total customers created**: 3469
- **Active subscriptions (end)**: 2637
- **Canceled subscriptions**: 832
- **Monthly churn rate**: ~5%
- **Total gross revenue**: $920,384.00

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
| 1 | AcquisitionDrop | 2025-06-02 to 2025-06-16 | Organic Search -50% |
| 2 | ConversionBreakdown | 2025-07-02 to 2025-07-16 | Conversion rate -35% |
| 3 | SegmentFailure | 2025-08-01 to 2025-08-15 | US geo -50%, global -14% |
| 4 | ReturningUserDrop | 2025-08-31 to 2025-09-14 | Returning users -40% |
| 5 | InvoluntaryChurn | 2025-09-30 to 2025-10-14 | 40% renewals fail |
| 6 | OnboardingFailure | 2025-10-30 to 2025-11-13 | Signups +25%, s2p -35% |
| 7 | PricingMismatch | 2025-11-29 to 2025-12-13 | Conv -25%, rev/conv x1.5 |
| 8 | CohortQuality | 2025-12-29 to 2026-01-12 | s2p -55% |
| 9 | ProductFriction | 2026-01-28 to 2026-03-04 | Gradual engagement -12% |
| 10 | MetricIllusion | 2026-04-08 to 2026-04-22 | Sessions +30%, conv -25% |

## Simulation Dates for Verification
Best detection date is event_start + 6 (7d window fully inside event).

```bash
python scripts/simulate.py test-startup-demo --date 2025-06-08  # AcquisitionDrop
python scripts/simulate.py test-startup-demo --date 2025-07-08  # ConversionBreakdown
python scripts/simulate.py test-startup-demo --date 2025-08-07  # SegmentFailure
python scripts/simulate.py test-startup-demo --date 2025-09-06  # ReturningUserDrop
python scripts/simulate.py test-startup-demo --date 2025-10-06  # InvoluntaryChurn
python scripts/simulate.py test-startup-demo --date 2025-11-05  # OnboardingFailure
python scripts/simulate.py test-startup-demo --date 2025-12-05  # PricingMismatch
python scripts/simulate.py test-startup-demo --date 2026-01-04  # CohortQuality
python scripts/simulate.py test-startup-demo --date 2026-03-04  # ProductFriction
python scripts/simulate.py test-startup-demo --date 2026-04-14  # MetricIllusion
```

## Random Seed
All data is generated with seed `42` for reproducibility.
