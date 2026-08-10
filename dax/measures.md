# DAX Measures

This document records the reusable DAX measures implemented in the **Water Safety Analytics Model**.

The semantic model uses five Gold tables:

- `gold_fact_event_delivery`
- `gold_dim_date`
- `gold_dim_location`
- `gold_dim_educator`
- `gold_bridge_event_educator`

The measures are organised into five display folders. Unless otherwise stated, measures respond dynamically to the active date, location, programme, event, language, venue, and other report filter contexts.

## Measure inventory

| Display folder | Measure count |
|---|---:|
| `01 - Core KPIs` | 5 |
| `02 - Attendance` | 3 |
| `03 - Satisfaction` | 2 |
| `04 - Educator` | 2 |
| `05 - Time & Outreach` | 3 |
| **Total** | **15** |

## 01 - Core KPIs

### Total Delivery Records

**Home table:** `gold_fact_event_delivery`  
**Format:** Whole number  
**Purpose:** Counts event-delivery records. Multi-day activities contribute one record for each delivery day.

```DAX
Total Delivery Records =
SUM ( gold_fact_event_delivery[delivery_count] )
```

Expected overall result: **47**.

### Total Event Groups

**Home table:** `gold_fact_event_delivery`  
**Format:** Whole number  
**Purpose:** Counts distinct event groups. A multi-day activity such as a boat show is counted once at event-group level.

```DAX
Total Event Groups =
DISTINCTCOUNT ( gold_fact_event_delivery[event_group_id] )
```

Expected overall result: **34**.

### Total People Reached

**Home table:** `gold_fact_event_delivery`  
**Format:** Whole number  
**Purpose:** Returns the total estimated number of people reached through delivered water-safety activities.

```DAX
Total People Reached =
SUM ( gold_fact_event_delivery[people_reached] )
```

Expected overall result: **3,565**.

### Total Registered

**Home table:** `gold_fact_event_delivery`  
**Format:** Whole number  
**Purpose:** Returns the total registered participant count. Blank registration values are ignored by `SUM`.

```DAX
Total Registered =
SUM ( gold_fact_event_delivery[registered_count] )
```

Expected overall result: **191**.

### Total Attended

**Home table:** `gold_fact_event_delivery`  
**Format:** Whole number  
**Purpose:** Returns total recorded attendance across all event deliveries with attendance data.

```DAX
Total Attended =
SUM ( gold_fact_event_delivery[attended_count] )
```

> `Total Attended` can include attendance recorded for activities that did not require registration. For registration-to-attendance comparison, use `Registered Event Attended`.

## 02 - Attendance

### Registered Event Attended

**Home table:** `gold_fact_event_delivery`  
**Format:** Whole number  
**Purpose:** Returns attendance only for event deliveries that contain a registration count, ensuring that the numerator and denominator of the attendance-rate calculation cover the same population.

```DAX
Registered Event Attended =
CALCULATE (
    [Total Attended],
    gold_fact_event_delivery[registered_count] <> BLANK()
)
```

Expected overall result: **171**.

### Attendance Variance

**Home table:** `gold_fact_event_delivery`  
**Format:** Whole number  
**Purpose:** Shows the difference between actual attendance and registrations for registration-applicable event deliveries.

```DAX
Attendance Variance =
[Registered Event Attended] - [Total Registered]
```

Expected overall result: **-20**.

A positive result is valid and means that actual attendance exceeded registrations.

### Attendance Rate

**Home table:** `gold_fact_event_delivery`  
**Format:** Percentage, 1 decimal place  
**Purpose:** Calculates the overall attendance rate for registration-applicable event deliveries.

```DAX
Attendance Rate =
DIVIDE (
    [Registered Event Attended],
    [Total Registered]
)
```

Expected overall result: **89.5%** (`171 / 191`).

This measure uses a **ratio of totals**, rather than averaging the row-level `attendance_rate` values. This weights the result by participant registrations and recalculates correctly in the current filter context.

## 03 - Satisfaction

### Average Satisfaction

**Home table:** `gold_fact_event_delivery`  
**Format:** Decimal number, 2 decimal places  
**Purpose:** Returns the average satisfaction score across rated event-delivery records. Blank scores are ignored by `AVERAGE`.

```DAX
Average Satisfaction =
AVERAGE ( gold_fact_event_delivery[satisfaction_score] )
```

This is an event-delivery-level average, not a participant-weighted average. A participant-weighted measure would require a reliable response-count field, which is not available in the current dataset.

### Rated Delivery Records

**Home table:** `gold_fact_event_delivery`  
**Format:** Whole number  
**Purpose:** Counts event-delivery records that contain a satisfaction score and provides context for the average satisfaction result.

```DAX
Rated Delivery Records =
CALCULATE (
    [Total Delivery Records],
    gold_fact_event_delivery[satisfaction_score] <> BLANK()
)
```

## 04 - Educator

### Events Delivered by Educator

**Home table:** `gold_bridge_event_educator`  
**Format:** Whole number  
**Purpose:** Counts distinct event deliveries associated with an educator through the event-educator bridge table.

```DAX
Events Delivered by Educator =
DISTINCTCOUNT ( gold_bridge_event_educator[event_id] )
```

This measure must use the bridge table because one event delivery can involve multiple educators or ambassadors. It should be used instead of `Total Delivery Records` when educator names are placed on a visual.

### Educator Hours

**Home table:** `gold_bridge_event_educator`  
**Format:** Decimal number, 1 decimal place  
**Purpose:** Returns total hours contributed by educators and ambassadors.

```DAX
Educator Hours =
SUM ( gold_bridge_event_educator[hours_worked] )
```

`Educator Hours` is a staffing-effort measure. It differs from `Total Session Hours`: a two-hour event involving six educators contributes two session hours but may contribute twelve educator hours.

> An event-level metric such as `people_reached` must not be summed directly across the bridge by educator because the same event total would be repeated for every participating educator, causing double counting. Educator-level reach requires an explicit attribution rule.

## 05 - Time & Outreach

### Total Session Hours

**Home table:** `gold_fact_event_delivery`  
**Format:** Decimal number, 1 decimal place  
**Purpose:** Returns the total duration of delivered sessions, independently of the number of educators staffing each session.

```DAX
Total Session Hours =
SUM ( gold_fact_event_delivery[session_duration_hours] )
```

### Average People Reached per Delivery

**Home table:** `gold_fact_event_delivery`  
**Format:** Decimal number, 1 decimal place  
**Purpose:** Measures average outreach per event-delivery record.

```DAX
Average People Reached per Delivery =
DIVIDE (
    [Total People Reached],
    [Total Delivery Records]
)
```

Expected overall result: approximately **75.9** (`3,565 / 47`). The measure can be analysed by month, programme, event type, region, delivery language, or venue category.

### Locations Reached

**Home table:** `gold_fact_event_delivery`  
**Format:** Whole number  
**Purpose:** Counts distinct locations with delivered activity in the current filter context.

```DAX
Locations Reached =
DISTINCTCOUNT ( gold_fact_event_delivery[location_id] )
```

Expected overall result: **27**.

The measure uses the fact-table location key instead of counting every row in the location dimension. Consequently, a date or programme filter returns only locations that had relevant delivered activity.

## Modelling notes

- Measures in `01`, `02`, `03`, and `05` are stored in `gold_fact_event_delivery`.
- Measures in `04 - Educator` are stored in `gold_bridge_event_educator`.
- Date and location dimensions filter the event-delivery fact table using active one-to-many, single-direction relationships.
- The fact table filters the event-educator bridge, and the educator dimension filters the bridge.
- `lead_educator_id` is retained as an event attribute but is not used as the primary relationship for educator participation analysis.
- Percentage formatting belongs to the semantic-model measure. A web DAX Query View result can display the underlying decimal value differently from a Power BI visual.
- The source data is synthetic and anonymised for portfolio demonstration purposes.

