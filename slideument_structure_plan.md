# Slideument Structure Plan: ETL Data Pipeline for Loan Default Prediction

## Overview
10-slide slideument covering data pipeline implementation following Medallion Architecture for ML-ready feature and label stores. Target audience: Technical and non-technical stakeholders in financial ML projects.

---

## Slide 1: Executive Summary & Business Problem

### Content

- **Problem**: Financial institute needs ML-ready data for loan default prediction
- **Solution**: Production-grade ETL pipeline following Medallion Architecture
- **Business Value**: Enable data-driven loan decisions and risk management
- **Key Achievement**: Processing 24 months of data across 4 data sources into ML-ready format

### Visual Needs
- High-level architecture diagram showing data flow from raw CSVs → Bronze → Silver → Gold → ML Models

### Key Messages
- End-to-end data pipeline solution
- Production-ready architecture
- Enables ML model development

---

## Slide 2: Data Strategy & Architecture Decision

### Content
- **Why Medallion Architecture**: Progressive data refinement, data quality assurance, schema evolution support
- **Layer Purpose**:
  - Bronze: Raw data ingestion with date partitioning
  - Silver: Data validation, cleaning, and standardization
  - Gold: ML-ready feature and label stores
- **Technology Choice**: PySpark for scalability, Parquet for efficiency and schema evolution

### Visual Needs
- Detailed Medallion Architecture diagram with data volumes and processing logic at each layer

### Key Messages
- Deliberate architecture choice for financial data
- Scalable technology stack
- Clear separation of concerns

---

## Slide 3: Business Logic & Prediction Framework

### Content
- **Target Definition**: Default = 30+ DPD (Days Past Due) at month 6
- **Why Month 6**: Feature stabilization point where default patterns become predictable *(include your screenshot/data here)*
- **Prediction Use Case**: Application-time risk assessment using 6-month behavior patterns
- **Feature Strategy**: Multi-source data fusion (demographics, financials, clickstream, loan history)

### Visual Needs
- Timeline diagram showing loan lifecycle, feature availability, and prediction point
- Your default rate stabilization chart/screenshot

### Key Messages
- Business-driven target definition
- Data-supported timing choice (month 6)
- Comprehensive feature approach

---

## Slide 4: Data Sources & Integration Strategy

### Content
- **4 Key Data Sources**:
  - `lms_loan_daily.csv`: Loan performance and payment behavior (24K+ records)
  - `features_financials.csv`: Credit profile and financial capacity
  - `features_attributes.csv`: Demographics and customer profile
  - `feature_clickstream.csv`: Digital engagement patterns (fe_1 to fe_20)
- **Integration Approach**: Customer_ID based joins with left-join strategy to preserve loan records

### Visual Needs
- Data source diagram with sample schemas and join relationships

### Key Messages
- Comprehensive data coverage
- Customer-centric integration
- Loan record preservation strategy

---

## Slide 5: Modular Data Quality Architecture

### Content
- **Innovative Validation Framework**: Factory pattern with `InvalidDataStrategy` enum for flexible data handling
- **Conservative Approach**: "Flag but preserve" philosophy - maintain data for ML while tracking quality
- **Strategy Options**:
  - `FLAG_ONLY`: Keep data, add validation flags (default for most fields)
  - `DROP_ROW`: Only for critical fields (Customer_ID, snapshot_date)
  - `QUARANTINE`: Extreme outliers only (>$10M income, <$100 salary)
  - `DEFAULT_VALUE`: Missing behavioral data (clickstream features)
- **Business Value**: Maximizes training data while ensuring quality transparency

### Technical Highlight Box
```python
# Example: Conservative financial validation
"Annual_Income": InvalidDataStrategy.FLAG_ONLY  # Flag but keep
# vs extreme outlier handling
if salary > 10M: quarantine()  # Only quarantine obvious errors
```

### Visual Needs
- Validation strategy decision tree
- Before/after data retention statistics

### Key Messages
- Modular, configurable validation
- Conservative data preservation
- Quality transparency

---

## Slide 6: Silver Layer Modular Processing Architecture

### Content
- **Modular Design Innovation**:
  - `BaseSilverProcessor` with `ValidationMixin` for code reuse
  - Specialized processors: `LoanDailySilverProcessor`, `FinancialsSilverProcessor`, etc.
  - `SilverProcessorFactory` for clean processor instantiation
- **Conservative Data Handling**:
  - Financial data: Strict validation for critical fields, flagging for analysis fields
  - Clickstream: Lenient defaults (missing clicks = 0)
  - Demographics: Flag edge cases but preserve for ML feature engineering
  - **Data Preservation Philosophy**: Keep SSN and Name in silver for data lineage, exclude from gold for ML
- **Extensibility**: Easy to add new data sources or modify validation rules per processor

### Technical Architecture Box
```
BaseSilverProcessor (ABC)
├── ValidationMixin (data quality methods)
├── LoanDailySilverProcessor (strict financial validation)
├── FinancialsSilverProcessor (conservative + quarantine outliers)
├── AttributesSilverProcessor (moderate validation)
└── ClickstreamSilverProcessor (lenient defaults)
```

### Visual Needs
- Class inheritance diagram
- Processor-specific validation rules matrix

### Key Messages
- Clean, extensible architecture
- Data-source-specific validation
- Code reusability

---

## Slide 7: Gold Layer: ML-Ready Feature & Label Stores

### Content
- **Feature Store Design**: Time-series features joined on Customer_ID with mob=0 (application time)
- **Label Store Logic**: Binary classification (1=default at 30+ DPD at mob=6, 0=performing)
- **Feature Selection Strategy**:
  - Exclude non-predictive identifiers (SSN, Name, valid_ssn, valid_credit_mix, Credit_History_Age, Type_of_Loan) from gold layer
  - Keep raw Age (continuous) for tree-based models - simpler for baseline model training
  - Age bucketing deferred to v2 based on feature importance analysis (if important but non-linear then bucket)
  - Credit_History_Age parsing: Convert "10 Years and 9 Months" format to Credit_History_Age_Months (integer) in silver layer for ML compatibility - only the integer version flows to gold
  - Payment_Behavior feature engineering: Extract spending_level (binary) and value_size (ordinal 0-2) to reduce cardinality while preserving information
  - Payment_of_Min_Amount one-hot encoding: Convert YES/NO/NM into payment_min_yes (binary) and payment_min_nm (binary) with NO as reference category - avoids ordinal assumption since no clear ordering between YES and NM
  - Credit_Mix one-hot encoding: Convert GOOD/STANDARD/BAD/missing into credit_mix_good, credit_mix_standard, credit_mix_bad (all binary) with missing (~20% of data) as reference category - treats missing as legitimate category with potential predictive signal
  - Type_of_Loan multi-label binarization: Convert comma-separated loan types (e.g., "Payday Loan, Mortgage Loan") into binary indicators (has_payday_loan, has_mortgage_loan, etc.) - automatically extracts all unique loan types from data, ignores duplicates, and creates has_any_loan summary feature
- **ML Compatibility**:
  - Point-in-time correctness (no future leakage)
  - Consistent schema across time periods
  - Null handling strategy for missing features
- **Production Scalability**: Incremental processing with date-based partitioning

### Visual Needs
- Feature store schema diagram
- Label distribution chart
- Temporal data flow diagram

### Key Messages
- ML-ready data outputs
- Data leakage prevention
- Production scalability

---

## Slide 8: Pipeline Orchestration & Scalability

### Content
- **Orchestration Strategy**: `main.py` with layer-based execution control (`--layer`, `--timestamp` flags)
- **Scalability Features**:
  - PySpark for distributed processing
  - Date-based partitioning for incremental updates
  - Docker containerization for consistent environments
- **Operational Features**: Resumability, selective layer execution, data lineage tracking

### Visual Needs
- Pipeline execution flow diagram
- Docker deployment architecture

### Key Messages
- Production-ready orchestration
- Operational flexibility
- Environment consistency

---

## Slide 9: Technical Implementation Results

### Content
- **Modular Architecture Success**: 4 specialized processors handling different data quality requirements
- **Conservative Data Retention**: X% of records preserved with quality flags vs. Y% dropped in traditional approaches
- **Technical Metrics**:
  - Processing time: X minutes for 24 months of data
  - Data retention rate by source (Financial: 98%, Clickstream: 99.5%, etc.)
  - Validation flag distribution showing transparent quality tracking
- **Code Quality**: Extensible factory pattern enables easy addition of new data sources

### Visual Needs
- Processing performance metrics
- Data retention comparison chart
- Validation flag distribution

### Key Messages
- Quantified success metrics
- Superior data preservation
- Technical excellence

---

## Slide 10: Next Steps & Production Readiness

### Content
- **Immediate Outcomes**: ML-ready feature and label stores for model development
- **Production Considerations**:
  - Model training pipeline integration
  - Real-time feature serving architecture
  - Data monitoring and drift detection
- **Future Enhancements**: Additional data sources, advanced feature engineering, automated retraining

### Visual Needs
- Production ML architecture diagram showing how data pipeline integrates with model training/serving

### Key Messages
- Clear path to production
- Foundation for ML operations
- Extensibility for future needs

---

## Key Technical Differentiators to Emphasize

1. **Modular Validation Strategy**: Unlike monolithic data cleaning, your factory pattern allows different validation rules per data type while maintaining consistent interfaces

2. **Conservative Data Philosophy**: "Flag but preserve" approach maximizes ML training data while maintaining quality transparency - crucial for financial ML where edge cases might be important signals

3. **Extensible Architecture**: Easy to onboard new data sources or modify validation rules without changing core pipeline logic

4. **Production-Ready Design**: Docker containerization + PySpark + incremental processing enables seamless scaling

## Recommended Slide Flow for Mixed Audience

- **Slides 1-4**: Business context and architecture decisions (accessible to all)
- **Slides 5-6**: Deep dive on technical innovations (technical focus)
- **Slides 7-8**: ML-ready outputs and production scalability (mixed audience)
- **Slides 9-10**: Results and next steps (business value focus)

## Required Diagrams/Screenshots Summary

1. **Slide 1**: High-level data flow architecture
2. **Slide 2**: Detailed Medallion Architecture with data volumes
3. **Slide 3**: Loan lifecycle timeline + default rate stabilization chart
4. **Slide 4**: Data schema and join diagram
5. **Slide 5**: Validation strategy decision tree + data retention statistics
6. **Slide 6**: Class inheritance diagram + validation rules matrix
7. **Slide 7**: Feature/label store schema + label distribution
8. **Slide 8**: Pipeline orchestration flow + Docker architecture
9. **Slide 9**: Processing performance + data quality metrics
10. **Slide 10**: End-to-end ML production architecture

## Areas Needing Specific Data/Metrics

- Slide 3: Your default rate stabilization screenshot/chart
- Slide 9: Actual processing metrics, data retention rates, validation flag distributions
- Slide 4: Actual record counts from your data sources
- Slide 7: Actual label distribution from your gold tables