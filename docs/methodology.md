# TransitPulse Methodology & Analytical Specifications

**Chennai Unified Metropolitan Transport Authority (CUMTA) — Mobility Decision Support Workbench**  
*Problem Statement 6: From Chennai One Data to Mobility Decisions*

---

## 1. Executive Summary & Core Philosophy

TransitPulse is a decision-support system designed to convert user search sessions and ticket transactions from the **Chennai One** mobile application into defensible public transit operational interventions. 

### Fundamental Analytic Tenets
1. **Search Intent vs. Physical Reality**: A search represents intent to travel; it is not proof of a physical journey. Similarly, a ticket purchase serves as a verifiable proxy for travel, but does not provide onboard vehicle telemetry.
2. **Strict Data Grounding**: No external fleet telemetry, timetable schedules, or Automated Passenger Counter (APC) data is fabricated. Every metric, chart, and table derives directly from the March 2026 Chennai One dataset.
3. **Privacy First**: Complete omission of personal identifiers (`commuter_id`) and suppression of small sample sizes ($< 10$ unique searchers).
4. **Unified Mathematical Engine**: All revenue projections, capture scenarios, and priority scores across the dashboard, diagnosis matrix, simulator, and data exports are driven by a single calculation engine (`revenue.py`).

---

## 2. Transit Demand Funnel Methodology

```
┌────────────────────────────────────────────────────────┐
│   Raw Single-Leg Searches: 4,178,481                   │
│   [ -945 Circular / Test Station Records ]             │
│   ▼                                                    │
│   Valid Single-Leg Intent: 4,177,536 (100.0%)          │
├──────────────────────────┬─────────────────────────────┤
│   Matched Tickets        │   Lost Searches             │
│   (Proxy for Travel)     │   (Unmet Transit Demand)    │
│   2,097,052 (50.2%)      │   2,080,484 (49.8%)         │
└──────────────────────────┴─────────────────────────────┘
  *Footnote: 7,406 ticketed journeys had no preceding search
   record and are segregated from the conversion funnel.
```

### Cleansing and Segregation Rules
- **Circular & Test Records**: 945 searches where the origin station matched the destination station, or where internal QA stations were recorded, are filtered out ($4,178,481 \to 4,177,536$).
- **Multi-Leg Cleansing**: 10 circular transfers are pruned ($267,951 \to 267,941$).
- **Orphan Ticketed Journeys (7,406 records)**: In 7,406 instances, a passenger purchased a ticket directly (e.g., via saved favorite routes, direct QR scanning, or conductor quick-issue) without logging an origin-destination search query. These records are intentionally excluded from the search conversion denominator to prevent artificial inflation of conversion ratios.
- **Conversion Rate Formula**:
  $$\text{Conversion Rate} = \frac{\text{Matched Tickets}}{\text{Valid Single-Leg Searches}} = \frac{2,097,052}{4,177,536} = 50.20\%$$
- **Unmet Demand (Lost Searches)**:
  $$\text{Lost Searches} = \text{Valid Single-Leg Searches} - \text{Matched Tickets} = 2,080,484 \ (49.80\%)$$

---

## 3. Operational Time Windows

To capture Chennai's distinct diurnal travel rhythms, data is partitioned into four continuous non-overlapping buckets:

| Time Bucket | Hours (IST) | Operational Context |
|---|---|---|
| **Morning Peak** | 07:00 – 10:59 (07, 08, 09, 10) | Morning commute to Central Business Districts, IT corridors, and educational hubs. |
| **Midday** | 11:00 – 15:59 (11, 12, 13, 14, 15) | Inter-peak discretionary, shopping, and business travel. |
| **Evening Peak** | 16:00 – 20:59 (16, 17, 18, 19, 20) | Return peak with high congestion, multi-modal transfer strain, and bus bunching. |
| **Night** | 21:00 – 06:59 (21, 22, 23, 00..06) | Shift workers, late-night intercity arrivals, and early morning freight/wholesale activity. |

*Completeness Guarantee*: $\sum \text{Hours} = 4 + 5 + 5 + 10 = 24$ hours. Every hour $h \in [0, 23]$ maps to exactly one bucket.

---

## 4. Root-Cause Diagnostic Logic

For each corridor window, TransitPulse evaluates search volumes, ticket conversion, repeat attempts, and multi-leg transfer penalties to assign a deterministic root-cause diagnosis:

1. **Crowding / Capacity Overrun (Peak Deficit)**:
   - *Criteria*: Assigned during Peak windows (Morning / Evening) when search demand is high ($\ge 80\text{th}$ percentile), but conversion rate drops below $40\%$ with elevated repeated attempts.
   - *Operational Implication*: Passenger demand outstrips vehicle capacity. Buses or trains arrive fully loaded, forcing riders to abandon app ticketing.
2. **Off-Peak Frequency Deficit**:
   - *Criteria*: Assigned during Midday or Night windows when conversion rate is low ($< 45\%$) despite consistent search queries.
   - *Operational Implication*: Excess headway between services. Long waiting times deter riders from finalizing digital bookings.
3. **Fare Sensitivity (Premium Route Dropoff)**:
   - *Criteria*: Detected exclusively on premium or AC express corridors where parallel standard alternatives exist, and users drop off after seeing higher fare stages.
   - *Operational Implication*: Fares exceed willingness-to-pay for daily commuters; requires fare rationalization or standard service expansion.
4. **Transfer Penalty & Hub Friction**:
   - *Criteria*: Evaluated on multi-leg journeys where the transfer penalty ratio $\ge 1.40$ or connection wait times exceed 20 minutes.
   - *Operational Implication*: Unsynchronized timetables and lack of physical interchange infrastructure at key transfer hubs.
5. **Coverage & Network Void**:
   - *Criteria*: Corridors with sustained search volumes but zero or near-zero matched tickets across all time windows.
   - *Operational Implication*: No direct route exists; passengers are forced to find unmapped informal transit or private modes.

---

## 5. Revenue Recovery Engine (`revenue.py`)

TransitPulse implements an explicit, transparent economic model without hidden multipliers:

### Single Source of Truth Formula
$$\text{Projected Monthly Upside} = \sum_{c \in \text{Selected Corridors}} \left( \text{Lost Trips}_c \times \text{Tariff}_c \times \text{Capture Rate} \times 30\text{ days} \right)$$

### Standard Parameters
- **Capture Rate**: $35\%$ conservative capture benchmark (representing the realistic fraction of lost searches that public transit operational improvements can re-capture).
- **Tariff Hierarchy**:
  - Direct Bus Corridor: **₹15.00** (Official MTC base stage fare)
  - Rail / Metro Corridor: **₹20.00** (Base suburban / metro tier)
  - Multimodal Transfer: **₹25.00** (Combined stage fare)
  - Designated Premium Routes (`543001`, `546001`, `918001`, `598001`): Mapped directly to parallel standard route fares (e.g. `543`, `546`, `918`, `598` at ₹15) to prevent unrealizable revenue inflation.

### System Revenue Totals (March 2026)
- **High Confidence Alone (214 Corridors)**: **₹524,902.79** (~₹5.2 Lakhs / month)
- **High + Medium Actionable Set (466 Corridors)**: **₹902,237.23** (~₹9.0 Lakhs / month)
- **Upper Bound (All 1,210 Corridors @ 35% Capture)**: **₹1,044,195.02** (~₹10.4 Lakhs / month)

### Invariant Monotonicity Assertion
$$\text{Revenue}(\text{High}) \le \text{Revenue}(\text{High} + \text{Medium}) \le \text{Revenue}(\text{All})$$
$$\text{Corridors}(\text{High}) \le \text{Corridors}(\text{High} + \text{Medium}) \le \text{Corridors}(\text{All})$$
These assertions are programmatically validated at runtime in `app.py` and across all unit tests in `tests/test_transitpulse.py`.

---

## 6. Premium Corridor Sibling Check

To avoid misclassifying standard routes as "premium", the following strict rules apply:
1. **Designated Premium Routes**: Only routes `543001`, `546001`, `918001`, and `598001` may carry the action `"Review premium fare tier"`. Ordinary routes (e.g., `5174`, `991`, `961`) are strictly assigned neutral capacity or frequency actions.
2. **Sibling Share Substitution Analysis**:
   - For each search on a premium route, the pipeline queries whether the same rider searched the parallel standard route (`543`, `546`, `918`, `598`) on the same OD pair within a 1-minute window.
   - Where riders consistently substituted to standard buses due to price sensitivity, the premium corridor's recoverable revenue is calculated at the standard fare to reflect true commuter willingness-to-pay.

---

## 7. Transfer Hub Analysis & Coordinates

- **Aggregated OD Pairs**: Multi-leg transfers are deduplicated by collapsing distinct intermediate hubs into unique Origin $\to$ Destination corridors with average transfer penalties and lists of hubs used.
- **Coordinate Attribution**: All hub and station geographic coordinates originate from a curated manual reference list (`data_reference/stop_coords.csv`) covering 54 major transit hubs in the Chennai Metropolitan Area.
- **Data Source Disclaimer**: Every spatial point is tagged with `coord_source=manual_approx`. No false precision is implied.

---

## 8. Limitations & Methodological Constraints

1. **No Fleet GPS / AVL Data**: Bus bunching and physical headway delays are inferred from user search conversion patterns and diurnal variance, not real-time GPS telemetry.
2. **No Onboard Load Data**: Crowding is inferred from high search volume with low conversion and repeated search attempts; actual physical occupancy is not measured.
3. **Temporal Bounds**: Dataset represents a single month (March 2026). Seasonal variations (monsoon, school holidays) require multi-month historical baselines.
4. **App Self-Selection**: Chennai One users skew towards smartphone-equipped daily commuters. Cash-paying passengers are not reflected in search intent telemetry.

