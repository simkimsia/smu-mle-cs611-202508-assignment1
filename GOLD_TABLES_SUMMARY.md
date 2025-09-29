# Gold Layer: ML-Ready Label & Feature Stores

## Overview

The Gold layer represents the final, business-ready data stores optimized for machine learning model training. This layer implements a **dual-store architecture** with separate **Label Store** and **Feature Store** to ensure temporal consistency and prevent data leakage in loan default prediction models.

## Architecture Design

### 🏷️ Label Store (`gold/label_store/`)
- **Purpose**: Provides ground truth labels for supervised learning
- **Target**: Binary classification for loan default prediction
- **Temporal Window**: 6-month observation period (MOB=6)
- **Default Definition**: DPD ≥ 30 days past due

### 🎯 Feature Store (`gold/feature_store/`)
- **Purpose**: Provides ML-ready features at point-in-time
- **Scope**: Comprehensive customer profile at snapshot date
- **Sources**: Integrated data from all silver tables
- **ML Compatibility**: Prevents temporal and target leakage

## Label Store Implementation

### Label Definition Strategy
```
Label = 1 if DPD ≥ 30 at MOB = 6 months
Label = 0 if DPD < 30 at MOB = 6 months
```

### Business Logic
| Metric | Value | Rationale |
|--------|-------|-----------|
| **DPD Threshold** | 30 days | Industry standard for identifying problematic loans |
| **MOB Window** | 6 months | Sufficient time to observe payment behavior patterns |
| **Label Type** | Binary (0/1) | Simplified classification suitable for business decisions |
| **Scope** | Customer-Loan Level | Each loan has a unique prediction scenario |

### Schema Structure
```
gold_label_store_YYYY_MM_DD.parquet
├── loan_id: String              # Unique loan identifier
├── Customer_ID: String          # Customer identifier
├── label: Integer               # Binary target (0=good, 1=default)
├── label_def: String           # Definition metadata ("30dpd_6mob")
└── snapshot_date: Date         # Point-in-time reference
```

### Temporal Consistency
- **Snapshot Date**: Reference point for feature extraction
- **Observation Window**: 6 months forward from snapshot date
- **Label Generation**: Based on loan performance after snapshot date
- **No Future Leakage**: Labels reflect information available only after the prediction point

## Feature Store Implementation

### Integration Strategy
The feature store implements **point-in-time joins** across all silver tables to create comprehensive customer profiles:

```
Base: loan_daily (Customer_ID, snapshot_date)
  ├── + attributes (demographics, account info)
  ├── + financials (income, debt, credit metrics)
  └── + clickstream (behavioral features)
```

### Join Logic & Conflict Resolution
```python
# Systematic column conflict handling
for each silver_table in [attributes, financials, clickstream]:
    existing_columns = feature_store.columns
    duplicate_columns = find_overlaps(silver_table.columns, existing_columns)
    clean_table = silver_table.drop(duplicate_columns - ["Customer_ID"])
    feature_store = feature_store.join(clean_table, on="Customer_ID", how="left")
```

### Data Leakage Prevention

#### Temporal Leakage Protection
- **Strict Snapshot Dating**: All features use data ≤ snapshot_date
- **No Future Information**: Zero tolerance for post-snapshot data
- **Consistent Time Windows**: All silver tables aligned to same snapshot_date

#### Target Leakage Protection
- **Feature Independence**: No loan performance metrics in features
- **Separation of Concerns**: Features describe customer state, not loan outcomes
- **Clean Data Lineage**: Features and labels derived from different data sources

## Feature Categories & Business Value

### 📊 Demographic Features (from `attributes`)
- **Age, SSN validation status**
- **Account characteristics**
- **Business Value**: Foundational customer profiling for risk assessment

### 💰 Financial Features (from `financials`)
- **Income, salary, debt ratios**
- **Credit utilization, payment behavior**
- **Investment patterns, EMI obligations**
- **Business Value**: Core creditworthiness indicators for default prediction

### 🖱️ Behavioral Features (from `clickstream`)
- **Digital engagement patterns (fe_1 to fe_20)**
- **Platform interaction frequency**
- **Business Value**: Modern behavioral risk signals complementing traditional credit metrics

### 🏦 Loan Context Features (from `loan_daily`)
- **Loan characteristics, MOB status**
- **Current account standing**
- **Business Value**: Loan-specific context for prediction scenarios

## Data Quality Integration

### Validation Flag Inheritance
The feature store inherits comprehensive validation flags from silver tables:

#### Customer Validation
- `valid_customer_id`: Essential for model training
- `valid_snapshot_date`: Temporal integrity assurance

#### Financial Validation
- `valid_annual_income`, `valid_monthly_salary`: Income reliability
- `valid_credit_utilization`, `valid_interest_rate`: Credit metrics quality
- `valid_payment_behaviour`, `valid_credit_mix`: Categorical feature quality

#### Behavioral Validation
- `valid_fe_1` through `valid_fe_20`: Digital behavior data quality

### Quality-Based Model Training
```python
# High-quality training set
premium_features = feature_store.filter(
    (col("valid_customer_id") == True) &
    (col("valid_annual_income") == True) &
    (col("valid_credit_mix") == True)
)

# Inclusive training with quality awareness
full_features = feature_store  # All records with quality flags for analysis
```

## ML Pipeline Compatibility

### Training Data Assembly
```python
# Join features with labels on Customer_ID and snapshot_date
training_data = feature_store.join(
    label_store,
    on=["Customer_ID", "snapshot_date"],
    how="inner"
)
```

### Temporal Splitting Strategy
- **Training**: Snapshot dates 2023-01 to 2023-09
- **Validation**: Snapshot dates 2023-10 to 2023-11
- **Test**: Snapshot dates 2023-12 to 2024-01
- **Forward-Looking**: No data leakage across splits

### Feature Engineering Ready
- **Categorical Encoding**: Payment behavior, credit mix categories
- **Numerical Scaling**: Income, debt ratios, utilization metrics
- **Missing Value Handling**: Validation flags guide imputation strategies
- **Feature Selection**: Quality flags enable systematic feature filtering

## Business Impact & Production Readiness

### Credit Risk Assessment Enhancement
- **Traditional + Modern**: Combines credit bureau data with digital behavioral signals
- **Real-Time Capable**: Point-in-time features support operational deployment
- **Regulatory Compliant**: Explainable features align with financial regulation requirements

### Model Performance Optimization
- **Balanced Dataset**: Label distribution analysis guides sampling strategies
- **Quality Segmentation**: Validation flags enable quality-based model variants
- **Feature Richness**: Multi-domain features improve predictive power

### Operational Excellence
- **Scalable Architecture**: Parquet format optimized for big data processing
- **Audit Trail**: Complete data lineage from raw data to ML features
- **Quality Monitoring**: Built-in validation flags support production monitoring

## File Structure & Naming Convention

```
datamart/{timestamp}/gold/
├── label_store/
│   ├── gold_label_store_2023_01_01.parquet
│   ├── gold_label_store_2023_02_01.parquet
│   └── ... (monthly partitions)
└── feature_store/
    ├── gold_feature_store_2023_01_01.parquet
    ├── gold_feature_store_2023_02_01.parquet
    └── ... (monthly partitions)
```

### Partition Strategy Benefits
- **Efficient Processing**: Date-based partitioning enables incremental processing
- **Temporal Isolation**: Each partition represents a complete prediction scenario
- **Scalable Storage**: Distributed processing friendly format
- **Easy Validation**: Partition-level quality checks and monitoring

## Key Design Principles

1. **Temporal Integrity**: Strict point-in-time feature extraction prevents data leakage
2. **Business Alignment**: Default definitions match industry standards (30 DPD, 6 MOB)
3. **ML Optimization**: Separate stores optimized for specific ML pipeline stages
4. **Quality Transparency**: Comprehensive validation flags enable informed model decisions
5. **Production Readiness**: Scalable architecture supports operational deployment
6. **Regulatory Compliance**: Explainable features and clear data lineage support audit requirements

## Slideument Preparation Key Points

### Technical Excellence
- **Medallion Architecture Compliance**: Clear bronze → silver → gold progression
- **Data Engineering Best Practices**: Parquet format, partitioning, schema evolution
- **ML Engineering Standards**: Temporal consistency, leakage prevention, quality tracking

### Business Value Proposition
- **Risk Management**: Enhanced default prediction capability
- **Operational Efficiency**: Automated, scalable ML data preparation
- **Regulatory Readiness**: Transparent, auditable ML pipeline
- **Competitive Advantage**: Modern behavioral signals complement traditional credit metrics

### Production Deployment Ready
- **Scalable Architecture**: Handles enterprise-scale data volumes
- **Quality Monitoring**: Built-in validation supports production monitoring
- **Incremental Processing**: Date-partitioned design enables efficient updates
- **Cross-Functional Accessibility**: Business-friendly parquet format supports various analytics tools