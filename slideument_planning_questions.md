# Slideument Planning Questions

## Core Business Questions

### 1. Prediction Use Case Definition

- **What exactly are we predicting?**
  - Current state detection at month 6? ("Is this customer currently in distress at month 6?")
  - Future risk prediction? ("Will this customer default in the next X months?")
  - Application-time screening? ("Should we approve this loan application?")

- **When is the prediction made?**
  - At loan application (month 0)?
  - At month 6 when features stabilize?
  - Continuously throughout loan lifecycle?

- **What action will be taken based on the prediction?**
  - Loan approval/rejection
  - Early intervention/collection
  - Risk monitoring and portfolio management

### 2. Target Variable Design

- **Why did we choose 30 DPD as the default threshold?**
- **Why did we choose month 6 as the evaluation point?**
- **How does this align with regulatory requirements (if any)?**
- **What is the business impact of false positives vs false negatives?**

## Technical Architecture Questions

### 3. Medallion Architecture Implementation

- **Why did we choose Bronze → Silver → Gold layers?**
- **What specific value does each layer add?**
- **How does our implementation prevent data quality issues?**

### 4. Data Processing Decisions

- **Why PySpark instead of pandas or other tools?**
- **Why Parquet format for silver/gold layers?**
- **How do we handle schema evolution over time?**
- **What is our data retention strategy?**

### 5. Feature Engineering Strategy

- **Why do we consider features "stable" at month 6?**
- **What makes month 6 special vs month 3 or month 9?**
- **How do we handle customers who don't reach month 6?**
- **What external data sources could enhance our features?**

## Data Quality & Validation Questions

### 6. Data Leakage Prevention

- **How do we ensure no future information leaks into training?**
- **What validation checks prevent temporal leakage?**
- **How do we handle missing data without creating bias?**

### 7. Data Quality Strategy

- **What is our strategy for handling invalid data (quarantine vs imputation vs dropping)?**
- **How do we validate data consistency across sources?**
- **What monitoring do we have for data drift?**

## Model Development Questions

### 8. Training Strategy

- **What time periods will we use for training vs validation vs testing?**
- **How do we handle class imbalance in default rates?**
- **What baseline models will we compare against?**

### 9. Evaluation Metrics

- **What metrics matter most for this business case?**
- **How do we measure model performance over time?**
- **What constitutes acceptable model performance?**

## Operational Questions

### 10. Production Deployment

- **How often will the model be retrained?**
- **How will we detect model drift?**
- **What is the rollback strategy if model performance degrades?**

### 11. Monitoring & Governance

- **What data lineage tracking do we need?**
- **How do we ensure model explainability for regulatory requirements?**
- **What audit trails are required?**

## Business Value Questions

### 12. ROI and Impact

- **What is the expected business value from this model?**
- **How much can we reduce default rates?**
- **What is the cost of false positives (rejected good customers)?**
- **How does this compare to current manual processes?**

### 13. Stakeholder Alignment

- **Who are the primary users of this model?**
- **How does this fit into the broader risk management framework?**
- **What training do end users need?**

## Risk and Compliance Questions

### 14. Model Risk Management

- **What are the key model risks and how do we mitigate them?**
- **How do we ensure fair lending compliance?**
- **What documentation is required for audit purposes?**

### 15. Ethical Considerations

- **Are there any protected classes we need to monitor for bias?**
- **How do we ensure model fairness across different customer segments?**
- **What transparency requirements exist for our predictions?**

---

## Questions Specific to Current Implementation

### 16. Current Pipeline Review

- **Is `bronze_label_store.py` needed or should it be removed?**
- **Are we correctly implementing the mob=6 filtering strategy?**
- **Do our validation strategies in silver layer make business sense?**

### 17. Feature Store Design

- **How do we handle customers with missing feature data?**
- **Should we create separate feature stores for different prediction horizons?**
- **How do we version control our feature definitions?**

### 18. Label Store Design

- **Should we create multiple label definitions (different DPD thresholds, different time horizons)?**
- **How do we handle customers who close their loans early?**
- **What about customers who make late payments but catch up?**

---

*Note: These questions should be answered before finalizing the slideument to ensure we present a coherent, well-reasoned solution that addresses real business needs.*
