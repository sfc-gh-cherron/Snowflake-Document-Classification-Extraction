# Document Classification & Extraction Schema

**Last Updated:** September 30, 2025  
**Total Document Types:** 9 classifications  
**Total Extraction Attributes:** 79 attributes

---

## Overview

This document processing pipeline uses Snowflake Cortex AI functions to automatically classify and extract structured data from business documents. The system supports 9 different document types with 4-10 targeted attributes per type.

### Architecture Reference

- Built on [AWS document processing pipeline patterns](https://aws.amazon.com/blogs/architecture/building-a-scalable-document-pre-processing-pipeline/)
- Implements [intelligent governance for regulated industries](https://aws.amazon.com/blogs/machine-learning/intelligent-governance-of-document-processing-pipelines-for-regulated-industries/)
- Uses modern [LLM-powered document conversion stack](https://medium.com/@pankaj_pandey/streamlining-document-conversion-the-modern-document-processing-stack-for-llm-powered-applications-90e3374b95ee)

---

## Document Classifications & Attributes

### 1. W2 Tax Form (10 attributes)
**Classification:** `w2`  
**Sample Location:** `demo_docs/w2s/`

| Attribute Name | Description |
|----------------|-------------|
| `employee_name` | Employee's full legal name |
| `employer_name` | Employer company name |
| `employer_ein` | Employer Identification Number |
| `tax_year` | Tax year for the W-2 |
| `wages_tips_compensation` | Box 1 - Wages, tips, other compensation |
| `federal_income_tax_withheld` | Box 2 - Federal income tax withheld |
| `social_security_wages` | Box 3 - Social security wages |
| `social_security_tax_withheld` | Box 4 - Social security tax withheld |
| `medicare_wages` | Box 5 - Medicare wages and tips |
| `state` | State for income tax reporting |

**Business Value:** Automated payroll processing, tax compliance, employee record management

---

### 2. Vendor/Service Contract (10 attributes)
**Classification:** `vendor_contract`  
**Sample Location:** `demo_docs/vendor_contracts/`

| Attribute Name | Description |
|----------------|-------------|
| `vendor_name` | Name of vendor/service provider |
| `client_name` | Name of client/customer company |
| `contract_effective_date` | Contract effective date |
| `contract_expiration_date` | Contract expiration date |
| `contract_term_months` | Length of contract in months/years |
| `total_contract_value` | Total contract value (with currency) |
| `payment_terms` | Payment schedule or terms |
| `services_description` | Brief description of services provided |
| `auto_renewal_clause` | Whether contract auto-renews (Yes/No) |
| `termination_notice_period` | Notice period required for termination |

**Business Value:** Contract lifecycle management, spend analytics, vendor compliance tracking, renewal alerts

---

### 3. Sales Performance Report (8 attributes)
**Classification:** `sales_report`  
**Sample Location:** `demo_docs/sales/`

| Attribute Name | Description |
|----------------|-------------|
| `reporting_period` | Period covered (Q4 2024, FY2025, etc.) |
| `total_revenue` | Total revenue for reporting period |
| `revenue_growth_percent` | Revenue growth percentage (YoY or PoP) |
| `top_performing_region` | Best performing sales region/territory |
| `new_customers_count` | Number of new customers acquired |
| `average_deal_size` | Average deal/contract value |
| `sales_pipeline_value` | Total value in sales pipeline |
| `quota_attainment_percent` | Percentage of sales quota achieved |

**Business Value:** Sales performance tracking, forecasting, territory analysis, quota management

---

### 4. Marketing Campaign Report (9 attributes)
**Classification:** `marketing_report`  
**Sample Location:** `demo_docs/marketing/`

| Attribute Name | Description |
|----------------|-------------|
| `campaign_name` | Name of marketing campaign |
| `reporting_period` | Time period covered |
| `campaign_budget` | Total budget allocated (with currency) |
| `total_impressions` | Total ad impressions |
| `total_clicks` | Total clicks received |
| `click_through_rate` | CTR as a percentage |
| `conversion_rate` | Conversion rate as a percentage |
| `cost_per_acquisition` | CPA or cost per lead |
| `roi_percent` | Return on investment as a percentage |

**Business Value:** Marketing ROI analysis, campaign optimization, budget allocation, performance benchmarking

---

### 5. HR Policy/Handbook (7 attributes)
**Classification:** `hr_policy`  
**Sample Location:** `demo_docs/hr/`

| Attribute Name | Description |
|----------------|-------------|
| `document_title` | Official title of HR policy/handbook |
| `effective_date` | Policy effective date or last update |
| `version_number` | Version or revision number |
| `department` | Responsible department (HR, Legal, etc.) |
| `policy_type` | Type (Handbook, Performance Review, etc.) |
| `approval_authority` | Who approved the policy |
| `last_review_date` | When policy was last reviewed |

**Business Value:** Policy compliance, version control, audit trail, employee onboarding

---

### 6. Corporate Policy (8 attributes)
**Classification:** `corporate_policy`  
**Sample Location:** `demo_docs/policies/`

| Attribute Name | Description |
|----------------|-------------|
| `policy_name` | Name of corporate policy |
| `policy_category` | Category (Expense, Travel, Vendor Mgmt) |
| `effective_date` | When policy takes effect |
| `approval_date` | When policy was approved |
| `policy_owner` | Department/role that owns policy |
| `approval_levels_required` | Required approval levels/authorities |
| `spending_limits` | Key spending limits or thresholds |
| `review_frequency` | How often policy is reviewed |

**Business Value:** Governance, compliance management, spending control, audit preparedness

---

### 7. Financial Infographic/Earnings Report (10 attributes)
**Classification:** `financial_infographic`  
**Sample Location:** `demo_docs/financial filings/`

| Attribute Name | Description |
|----------------|-------------|
| `quarter` | Fiscal quarter (Q1, Q2, Q3, Q4) |
| `fiscal_year` | Fiscal year |
| `revenue` | Total revenue (with currency and units) |
| `revenue_growth_percent` | YoY revenue growth percentage |
| `operating_margin` | Operating margin as a percentage |
| `net_income` | Net income/profit for the period |
| `customers_count` | Total number of customers |
| `customers_over_1m_count` | Enterprise customers (>$1M revenue) |
| `net_revenue_retention_rate` | NRR as a percentage |
| `gross_margin` | Gross margin as a percentage |

**Business Value:** Financial analysis, investor relations, board reporting, competitive benchmarking

---

### 8. Case Study/Customer Success Story (7 attributes)
**Classification:** `case_study`  
**Sample Location:** `demo_docs/sales/`

| Attribute Name | Description |
|----------------|-------------|
| `customer_name` | Name of featured customer/company |
| `industry` | Customer's industry |
| `use_case` | Primary use case or problem solved |
| `business_impact` | Key business impact or result achieved |
| `metrics_improved` | Specific metrics improved |
| `implementation_duration` | How long implementation took |
| `testimonial_quote` | Key testimonial or customer quote |

**Business Value:** Sales enablement, marketing content, customer validation, competitive positioning

---

### 9. Strategy Document (8 attributes)
**Classification:** `strategy_document`  
**Sample Location:** `demo_docs/sales/`, `demo_docs/marketing/`

| Attribute Name | Description |
|----------------|-------------|
| `document_title` | Title of strategy document |
| `planning_period` | Time period covered (2025, FY2025, etc.) |
| `department` | Department/business unit (Marketing, Sales) |
| `strategic_goals` | Main strategic goals or objectives |
| `key_initiatives` | Major initiatives or programs planned |
| `budget_allocation` | Total budget or key allocations |
| `success_metrics` | KPIs or success metrics |
| `document_date` | When strategy document was created |

**Business Value:** Strategic planning, resource allocation, OKR tracking, executive alignment

---

### 10. Other/Fallback (2 attributes)
**Classification:** `other`  
**Sample Location:** Any unrecognized document

| Attribute Name | Description |
|----------------|-------------|
| `document_title` | Title of document |
| `document_date` | Document date or most relevant date |

**Business Value:** Ensures all documents are processed with minimal metadata

---

## Pipeline Architecture

### Processing Flow

```
1. UPLOAD → S3 Bucket (External Stage)
2. PARSE → AI_PARSE_DOCUMENT (Extract text from PDFs, images, Office docs)
3. CLASSIFY → AI_CLASSIFY (Categorize into 9 document types)
4. EXTRACT → AI_EXTRACT (Pull 4-10 structured attributes per type)
5. CHUNK → Split documents into searchable chunks
6. SEARCH → CORTEX_SEARCH (Semantic search across all documents)
```

### Key Snowflake Cortex AI Functions

- **AI_PARSE_DOCUMENT**: OCR and text extraction from various formats
- **AI_CLASSIFY**: Multi-class document classification
- **AI_EXTRACT**: Structured data extraction using natural language prompts
- **CORTEX_SEARCH**: Semantic search and retrieval

---

## Database Schema

### Core Tables

1. **`parsed_documents`** - Raw parsed document content
2. **`document_classifications`** - Document type classifications
3. **`document_extractions`** - Extracted structured attributes
4. **`extraction_prompts`** - Question templates for each document type
5. **`document_chunks`** - Searchable document segments
6. **`cortex_search_service`** - Semantic search index

### Flattened View

**`document_processing_summary`** - Combines all data into flattened attribute-value pairs for easy querying and analytics

---

## Analytics Capabilities

### What You Can Query

1. **Contract Analytics**
   - Total contract value by vendor
   - Expiring contracts in next 90 days
   - Auto-renewal contract list
   - Payment terms distribution

2. **Sales Analytics**
   - Revenue trends by quarter
   - Top performing regions
   - Average deal size trends
   - Quota attainment by period

3. **Marketing Analytics**
   - Campaign ROI comparison
   - Cost per acquisition trends
   - Conversion rate by campaign type
   - Budget utilization

4. **HR/Policy Analytics**
   - Policy review dates tracking
   - Version control audit trail
   - Approval authority mapping
   - Policy effective date timeline

5. **Financial Analytics**
   - Revenue growth trajectories
   - Margin analysis over time
   - Customer count trends
   - NRR performance tracking

---

## Getting Started

### 1. Set Up Pipeline
```sql
-- Run setup scripts in order:
USE ROLE ACCOUNTADMIN;
@01_s3_integration_setup.sql
@02_document_pipeline_setup.sql
```

### 2. Upload Documents
Place documents in your S3 bucket under the configured path

### 3. Run Pipeline
```sql
CALL document_db.s3_documents.parse_new_documents();
CALL document_db.s3_documents.classify_parsed_documents();
CALL document_db.s3_documents.extract_attributes_for_classified_documents();
CALL document_db.s3_documents.chunk_classified_documents();
```

### 4. Query Results
```sql
-- View all extracted data
SELECT * FROM document_db.s3_documents.document_processing_summary;

-- Search documents semantically
SELECT CORTEX_SEARCH('document_db.s3_documents.cortex_search_service', 
  'What are our contract renewal deadlines?');
```

---

## Cost Optimization

### Estimated Costs (Per 1,000 Documents)

- **AI_PARSE_DOCUMENT**: ~$10-15 (varies by doc size)
- **AI_CLASSIFY**: ~$2-3
- **AI_EXTRACT**: ~$5-8 (varies by attributes)
- **CORTEX_SEARCH**: ~$1-2 (storage + queries)

**Total**: ~$18-28 per 1,000 documents

### Cost Reduction Tips

1. Batch process documents instead of real-time
2. Use document type filters to skip unnecessary parsing
3. Optimize extraction prompts for conciseness
4. Set up chunking parameters based on actual search needs
5. Monitor usage via cost dashboard in Streamlit app

---

## References

- [Snowflake Cortex AI Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex)
- [AWS Document Processing Patterns](https://aws.amazon.com/blogs/architecture/building-a-scalable-document-pre-processing-pipeline/)
- [Document Governance Best Practices](https://aws.amazon.com/blogs/machine-learning/intelligent-governance-of-document-processing-pipelines-for-regulated-industries/)

---

**Built with ❄️ Snowflake Cortex AI**
