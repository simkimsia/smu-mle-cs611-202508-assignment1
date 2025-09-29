# Silver Table Validation Strategy Summary

## Overview

This document summarizes the data validation strategies implemented across all silver table processors. The approach prioritizes **data preservation** with **FLAG_ONLY** as the default strategy, using more aggressive strategies only for critical data integrity issues or extreme outliers.

## Validation Strategies

### 🏷️ FLAG_ONLY (Default Strategy)
- **Action**: Keep all records, add validation flags
- **Purpose**: Preserve data for downstream analysis
- **Usage**: 95% of all fields

### 🚨 DROP_ROW (Critical Data Integrity)
- **Action**: Delete entire records
- **Purpose**: Cannot process without these fields
- **Usage**: Only for essential identifiers

### 📦 QUARANTINE (Extreme Outliers)
- **Action**: Move records to separate quarantine tables
- **Purpose**: Handle data that's clearly erroneous
- **Usage**: Only for statistically impossible values

### 🔧 DEFAULT_VALUE (Feature Engineering)
- **Action**: Replace invalid values with sensible defaults
- **Purpose**: Enable downstream processing
- **Usage**: Only when defaults make business sense

### 🔄 NULL_VALUE (Temporal Data)
- **Action**: Set invalid values to null
- **Purpose**: Handle missing temporal information
- **Usage**: Date fields in some processors

## Fields by Validation Strategy

### DROP_ROW (Record Deletion)
| Field | Processors | Rationale |
|-------|------------|-----------|
| `Customer_ID` | LoanDailySilver, FinancialsSilver | Cannot process without valid customer identifier |
| `snapshot_date` | LoanDailySilver, FinancialsSilver | Cannot process without valid date for temporal analysis |

### QUARANTINE (Extreme Outliers Only)
| Field | Processor | Threshold | Rationale |
|-------|-----------|-----------|-----------|
| `Annual_Income` | FinancialsSilver | < $1K or > $10M | Statistically impossible income ranges |
| `Monthly_Inhand_Salary` | FinancialsSilver | < $100 or > $1M | Statistically impossible salary ranges |

### NULL_VALUE (Date Handling)
| Field | Processors | Rationale |
|-------|------------|-----------|
| `snapshot_date` | BaseSilver (default), ClickstreamSilver, AttributesSilver | Prefer null over record deletion for dates |

### DEFAULT_VALUE (Feature Defaults)
| Fields | Processor | Default | Rationale |
|--------|-----------|---------|-----------|
| `fe_1` through `fe_20` | ClickstreamSilver | 0 | No activity = zero features |
| `Outstanding_Debt`, `Monthly_Balance`, `Total_EMI_per_month`, `Amount_invested_monthly` | FinancialsSilver | 0.0 | Financial amounts can reasonably be zero |

## Data Quality Improvements

### Numeric Field Cleaning
- **Conservative cleaning** applied to all Float/Integer fields
- **Removes**: underscores, commas, spaces, dollar signs
- **Examples**:
  - `"_50000"` → `"50000"` → `50000.0` ✅
  - `"50,000"` → `"50000"` → `50000.0` ✅
  - `"$50000"` → `"50000"` → `50000.0` ✅

### Categorical Field Validation
#### Payment_Behaviour (6 valid values)
- `High_spent_Medium_value_payments`
- `High_spent_Small_value_payments`
- `Low_spent_Medium_value_payments`
- `Low_spent_Small_value_payments`
- `High_spent_Large_value_payments`
- `Low_spent_Large_value_payments`

#### Payment_of_Min_Amount (2 valid values)
- `Yes` (50% of data)
- `No` (39% of data)
- `NM` (11% of data) → **Flagged as invalid**

#### Credit_Mix (3 valid values + special handling)
- `Bad`, `Good`, `Standard` → Valid
- `"_"` (20.9% of data) → Converted to `null` → **Flagged as invalid**

#### Type_of_Loan (Light Touch Approach)
- **Issue**: Multi-value field with complex formatting
- **Strategy**: Minimal cleaning, preserve all data
- **Validation**: Flag empty strings, nulls, and "Not Specified"
- **Examples**:
  - `""` → `null` → Invalid
  - `"Not Specified"` → Invalid
  - `"Personal Loan, Auto Loan, and Credit Card"` → Valid (keep as-is)

### Amount Validation Logic
#### Positive Amounts (must be > 0)
- `loan_amt`: Cannot have $0 loans

#### Non-Negative Amounts (can be ≥ 0)
- `due_amt`: $0 = no payment due (valid)
- `paid_amt`: $0 = no payment made (valid)
- `overdue_amt`: $0 = current loan (valid - majority case!)
- `balance`: $0 = fully paid loan (valid)

## Data Preservation Statistics

### Before vs After Approach
| Issue | Before (Aggressive) | After (Conservative) | Impact |
|-------|-------------------|---------------------|---------|
| Credit_Mix "_" values | Lost 20.9% of records | Flagged, kept all records | +2,611 records preserved |
| Due amounts = $0 | Flagged as invalid | Correctly validated | Majority of loans now valid |
| Formatting issues | Lost data | Cleaned and preserved | Higher data retention |

### Overall Strategy Benefits
- **Data Retention**: ~95% of records preserved
- **Quality Tracking**: Validation flags on all fields
- **Downstream Flexibility**: Gold layer can choose inclusion/exclusion
- **Business Alignment**: Conservative approach suitable for financial data

## Validation Flags Created

Each processor creates validation flags for data quality tracking:

### Common Flags
- `valid_customer_id`
- `valid_snapshot_date`

### Loan-Specific Flags
- `valid_loan_amt`
- `valid_due_amt`
- `valid_paid_amt`
- `valid_overdue_amt`
- `valid_balance`

### Financial-Specific Flags
- `valid_annual_income`
- `valid_monthly_salary`
- `valid_credit_utilization`
- `valid_interest_rate`
- `valid_credit_mix`
- `valid_payment_behaviour`
- `valid_payment_of_min_amount`
- `valid_type_of_loan`

### Clickstream-Specific Flags
- `valid_fe_1` through `valid_fe_20`

### Attributes-Specific Flags
- `valid_age`
- `valid_ssn`

## Usage in Gold Layer

The validation flags enable flexible processing in the gold layer:

```python
# High-quality subset only
clean_data = df.filter(
    (col("valid_customer_id") == True) &
    (col("valid_annual_income") == True) &
    (col("valid_credit_mix") == True)
)

# Include all data with quality metrics
full_data = df  # All records preserved

# Data quality analysis
quality_report = df.select(
    [F.avg(col(c).cast("int")).alias(c.replace("valid_", "pct_valid_"))
     for c in df.columns if c.startswith("valid_")]
)
```

## Key Design Principles

1. **Data Preservation First**: FLAG_ONLY as default strategy
2. **Conservative Quarantine**: Only for statistically impossible values
3. **Business Logic Alignment**: Zero amounts are often valid in financial data
4. **Audit Trail**: Original values preserved with `_original` columns
5. **Downstream Flexibility**: Let gold layer make inclusion/exclusion decisions
6. **Real-World Practicality**: Handle messy data gracefully rather than rejecting it