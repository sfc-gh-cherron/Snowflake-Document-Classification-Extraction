-- =============================
-- DOCUMENT PROCESSING PIPELINE SETUP
-- =============================
-- This file creates all tables, procedures, tasks, and services for the document AI pipeline
-- Run this file after completing the S3 integration setup

-- =============================
-- TABLES
-- =============================
-- parsed_documents
CREATE OR REPLACE TABLE document_db.s3_documents.parsed_documents (
  document_id VARCHAR(100) PRIMARY KEY,
  file_name VARCHAR(500) NOT NULL,
  file_path VARCHAR(1000) NOT NULL,
  file_size NUMBER,
  file_url VARCHAR(1000),
  document_type VARCHAR(50),
  parsed_content VARIANT,
  content_text STRING,
  parse_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  status VARCHAR(50) DEFAULT 'parsed'
)
COMMENT = 'Intermediate table storing parsed document content before classification';

-- document_classifications (final classification results)
CREATE OR REPLACE TABLE document_db.s3_documents.document_classifications (
  document_id VARCHAR(100) PRIMARY KEY,
  file_name VARCHAR(500) NOT NULL,
  file_path VARCHAR(1000) NOT NULL,
  file_size NUMBER,
  file_url VARCHAR(1000),
  document_type VARCHAR(50),
  parsed_content VARIANT,
  document_class VARCHAR(100),
  classification_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- document_extractions
CREATE OR REPLACE TABLE document_db.s3_documents.document_extractions (
  document_id VARCHAR(100),
  file_name VARCHAR(500),
  file_path VARCHAR(1000),
  document_class VARCHAR(100),
  attribute_name VARCHAR(200),
  attribute_value STRING,
  extraction_json VARIANT,
  extraction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- extraction_prompts
CREATE OR REPLACE TABLE document_db.s3_documents.extraction_prompts (
  document_class VARCHAR(100),
  attribute_name VARCHAR(200),
  question_text STRING,
  PRIMARY KEY (document_class, attribute_name)
);

-- Seed all extraction prompts (79 total attributes across 9 document classes)
-- Updated to match demo_docs folder structure with comprehensive business-relevant attributes
INSERT INTO document_db.s3_documents.extraction_prompts

-- ======================
-- W2 TAX FORM (10 attributes)
-- ======================
SELECT 'w2' AS document_class, 'employee_name' AS attribute_name, 'What is the employee''s full legal name as shown on the W-2?' AS question_text UNION ALL
SELECT 'w2', 'employer_name', 'What is the employer company name on the W-2?' UNION ALL
SELECT 'w2', 'employer_ein', 'What is the Employer Identification Number (EIN)?' UNION ALL
SELECT 'w2', 'tax_year', 'What tax year is this W-2 for?' UNION ALL
SELECT 'w2', 'wages_tips_compensation', 'What is the amount in Box 1 - Wages, tips, other compensation?' UNION ALL
SELECT 'w2', 'federal_income_tax_withheld', 'What is the amount in Box 2 - Federal income tax withheld?' UNION ALL
SELECT 'w2', 'social_security_wages', 'What is the amount in Box 3 - Social security wages?' UNION ALL
SELECT 'w2', 'social_security_tax_withheld', 'What is the amount in Box 4 - Social security tax withheld?' UNION ALL
SELECT 'w2', 'medicare_wages', 'What is the amount in Box 5 - Medicare wages and tips?' UNION ALL
SELECT 'w2', 'state', 'What state is listed for state income tax on the W-2?' UNION ALL

-- ======================
-- VENDOR/SERVICE CONTRACT (10 attributes)
-- ======================
SELECT 'vendor_contract', 'vendor_name', 'What is the name of the vendor or service provider?' UNION ALL
SELECT 'vendor_contract', 'client_name', 'What is the name of the client or customer company?' UNION ALL
SELECT 'vendor_contract', 'contract_effective_date', 'When does the contract become effective?' UNION ALL
SELECT 'vendor_contract', 'contract_expiration_date', 'When does the contract expire or end?' UNION ALL
SELECT 'vendor_contract', 'contract_term_months', 'What is the length of the contract in months or years?' UNION ALL
SELECT 'vendor_contract', 'total_contract_value', 'What is the total value of the contract? Include currency.' UNION ALL
SELECT 'vendor_contract', 'payment_terms', 'What are the payment terms or payment schedule (e.g., Net 30, monthly, quarterly)?' UNION ALL
SELECT 'vendor_contract', 'services_description', 'What services or products are being provided? Provide a brief description.' UNION ALL
SELECT 'vendor_contract', 'auto_renewal_clause', 'Does the contract have an automatic renewal clause? (Yes/No)' UNION ALL
SELECT 'vendor_contract', 'termination_notice_period', 'How many days or months notice is required for termination?' UNION ALL

-- ======================
-- SALES REPORT/PERFORMANCE (8 attributes)
-- ======================
SELECT 'sales_report', 'reporting_period', 'What reporting period does this cover (e.g., Q4 2024, FY2025, December 2024)?' UNION ALL
SELECT 'sales_report', 'total_revenue', 'What is the total revenue for the reporting period? Include currency.' UNION ALL
SELECT 'sales_report', 'revenue_growth_percent', 'What is the revenue growth percentage (year-over-year or period-over-period)?' UNION ALL
SELECT 'sales_report', 'top_performing_region', 'What is the top performing sales region or territory?' UNION ALL
SELECT 'sales_report', 'new_customers_count', 'How many new customers were acquired during this period?' UNION ALL
SELECT 'sales_report', 'average_deal_size', 'What is the average deal size or average contract value?' UNION ALL
SELECT 'sales_report', 'sales_pipeline_value', 'What is the total value of the sales pipeline?' UNION ALL
SELECT 'sales_report', 'quota_attainment_percent', 'What percentage of sales quota was achieved?' UNION ALL

-- ======================
-- MARKETING CAMPAIGN REPORT (9 attributes)
-- ======================
SELECT 'marketing_report', 'campaign_name', 'What is the name of the marketing campaign?' UNION ALL
SELECT 'marketing_report', 'reporting_period', 'What time period does this campaign report cover?' UNION ALL
SELECT 'marketing_report', 'campaign_budget', 'What was the total budget allocated for this campaign? Include currency.' UNION ALL
SELECT 'marketing_report', 'total_impressions', 'How many total impressions or views were generated?' UNION ALL
SELECT 'marketing_report', 'total_clicks', 'How many total clicks were received?' UNION ALL
SELECT 'marketing_report', 'click_through_rate', 'What is the click-through rate (CTR) as a percentage?' UNION ALL
SELECT 'marketing_report', 'conversion_rate', 'What is the conversion rate as a percentage?' UNION ALL
SELECT 'marketing_report', 'cost_per_acquisition', 'What is the cost per acquisition (CPA) or cost per lead?' UNION ALL
SELECT 'marketing_report', 'roi_percent', 'What is the return on investment (ROI) as a percentage?' UNION ALL

-- ======================
-- HR POLICY/HANDBOOK (7 attributes)
-- ======================
SELECT 'hr_policy', 'document_title', 'What is the official title of this HR policy or handbook?' UNION ALL
SELECT 'hr_policy', 'effective_date', 'When does this policy become effective or was it last updated?' UNION ALL
SELECT 'hr_policy', 'version_number', 'What is the version number or revision number of this document?' UNION ALL
SELECT 'hr_policy', 'department', 'Which department is responsible for this policy (e.g., HR, Legal, Compliance)?' UNION ALL
SELECT 'hr_policy', 'policy_type', 'What type of policy is this (e.g., Employee Handbook, Performance Review Guidelines, Code of Conduct)?' UNION ALL
SELECT 'hr_policy', 'approval_authority', 'Who approved this policy (e.g., CEO, Board, HR Director)?' UNION ALL
SELECT 'hr_policy', 'last_review_date', 'When was this policy last reviewed?' UNION ALL

-- ======================
-- CORPORATE POLICY (8 attributes)
-- ======================
SELECT 'corporate_policy', 'policy_name', 'What is the name of this corporate policy?' UNION ALL
SELECT 'corporate_policy', 'policy_category', 'What category does this policy fall under (e.g., Expense Policy, Travel Policy, Vendor Management)?' UNION ALL
SELECT 'corporate_policy', 'effective_date', 'When does this policy take effect?' UNION ALL
SELECT 'corporate_policy', 'approval_date', 'When was this policy approved?' UNION ALL
SELECT 'corporate_policy', 'policy_owner', 'Which department or role owns this policy?' UNION ALL
SELECT 'corporate_policy', 'approval_levels_required', 'What approval levels or authorities are required under this policy?' UNION ALL
SELECT 'corporate_policy', 'spending_limits', 'What are the key spending limits or monetary thresholds mentioned?' UNION ALL
SELECT 'corporate_policy', 'review_frequency', 'How often is this policy reviewed (e.g., annually, quarterly)?' UNION ALL

-- ======================
-- FINANCIAL INFOGRAPHIC/EARNINGS REPORT (10 attributes)
-- ======================
SELECT 'financial_infographic', 'quarter', 'Which fiscal quarter is reported (e.g., Q1, Q2, Q3, Q4)?' UNION ALL
SELECT 'financial_infographic', 'fiscal_year', 'Which fiscal year is reported?' UNION ALL
SELECT 'financial_infographic', 'revenue', 'What is the total revenue for the period? Include currency and units.' UNION ALL
SELECT 'financial_infographic', 'revenue_growth_percent', 'What is the year-over-year revenue growth percentage?' UNION ALL
SELECT 'financial_infographic', 'operating_margin', 'What is the operating margin as a percentage?' UNION ALL
SELECT 'financial_infographic', 'net_income', 'What is the net income or profit for the period?' UNION ALL
SELECT 'financial_infographic', 'customers_count', 'How many total customers are reported?' UNION ALL
SELECT 'financial_infographic', 'customers_over_1m_count', 'How many customers have trailing 12-month revenue over $1M (enterprise customers)?' UNION ALL
SELECT 'financial_infographic', 'net_revenue_retention_rate', 'What is the net revenue retention rate (NRR) as a percentage?' UNION ALL
SELECT 'financial_infographic', 'gross_margin', 'What is the gross margin as a percentage?' UNION ALL

-- ======================
-- CASE STUDY/CUSTOMER SUCCESS STORY (7 attributes)
-- ======================
SELECT 'case_study', 'customer_name', 'What is the name of the featured customer or company?' UNION ALL
SELECT 'case_study', 'industry', 'What industry does the customer operate in?' UNION ALL
SELECT 'case_study', 'use_case', 'What was the primary use case or business problem being solved?' UNION ALL
SELECT 'case_study', 'business_impact', 'What was the key business impact or result achieved?' UNION ALL
SELECT 'case_study', 'metrics_improved', 'What specific metrics improved (e.g., 50% cost reduction, 2x faster processing)?' UNION ALL
SELECT 'case_study', 'implementation_duration', 'How long did the implementation take?' UNION ALL
SELECT 'case_study', 'testimonial_quote', 'What is the key testimonial or quote from the customer?' UNION ALL

-- ======================
-- STRATEGY DOCUMENT (8 attributes)
-- ======================
SELECT 'strategy_document', 'document_title', 'What is the title of this strategy document?' UNION ALL
SELECT 'strategy_document', 'planning_period', 'What time period does this strategy cover (e.g., 2025, FY2025, Q1-Q4 2025)?' UNION ALL
SELECT 'strategy_document', 'department', 'Which department or business unit does this strategy belong to (e.g., Marketing, Sales, Corporate)?' UNION ALL
SELECT 'strategy_document', 'strategic_goals', 'What are the main strategic goals or objectives?' UNION ALL
SELECT 'strategy_document', 'key_initiatives', 'What are the major initiatives or programs planned?' UNION ALL
SELECT 'strategy_document', 'budget_allocation', 'What is the total budget or key budget allocations mentioned?' UNION ALL
SELECT 'strategy_document', 'success_metrics', 'What are the key performance indicators (KPIs) or success metrics?' UNION ALL
SELECT 'strategy_document', 'document_date', 'When was this strategy document created or published?' UNION ALL

-- ======================
-- OTHER/FALLBACK (2 attributes - minimal extraction for unclassified docs)
-- ======================
SELECT 'other', 'document_title', 'What is the title of this document?' UNION ALL
SELECT 'other', 'document_date', 'What is the document''s date or most relevant date?';


-- =============================
-- PROCEDURES
-- =============================
-- Step 1: Parse new documents using AI_PARSE_DOCUMENT
-- Processes files from the stream and extracts text content using LAYOUT mode (fallback to OCR)
CREATE OR REPLACE PROCEDURE document_db.s3_documents.parse_new_documents()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  -- Cursor to get new documents from stream (only supported file types)
  doc_cursor CURSOR FOR
    SELECT 
      relative_path,
      size,
      file_url
    FROM document_db.s3_documents.new_documents_stream
    WHERE METADATA$ACTION = 'INSERT'
      AND (
        UPPER(relative_path) LIKE '%.PDF' 
        OR UPPER(relative_path) LIKE '%.DOCX'
        OR UPPER(relative_path) LIKE '%.PPTX'
        OR UPPER(relative_path) LIKE '%.JPEG'
        OR UPPER(relative_path) LIKE '%.JPG' 
        OR UPPER(relative_path) LIKE '%.PNG'
        OR UPPER(relative_path) LIKE '%.TIFF'
        OR UPPER(relative_path) LIKE '%.TIF'
        OR UPPER(relative_path) LIKE '%.HTML'
        OR UPPER(relative_path) LIKE '%.TXT'
      );
  
  -- Variables for processing each document
  file_path STRING;
  parsed_content VARIANT;
  content_text STRING;
  processed_count INTEGER := 0;
  file_name STRING;
  file_size NUMBER;
  file_url STRING;
  file_extension STRING;
  document_type STRING;
BEGIN
  
  FOR doc_record IN doc_cursor DO
    BEGIN
      -- Skip empty file paths
      IF (doc_record.relative_path IS NULL OR doc_record.relative_path = '') THEN
        CONTINUE;
      END IF;
      
      -- Extract file metadata
      file_path := doc_record.relative_path;
      file_name := REGEXP_SUBSTR(file_path, '[^/]+$');
      file_size := doc_record.size;
      file_url := doc_record.file_url;
      file_extension := UPPER(REGEXP_SUBSTR(file_name, '\.[^.]+$'));
      document_type := CASE 
        WHEN file_extension = '.PDF' THEN 'pdf'
        WHEN file_extension = '.DOCX' THEN 'docx'
        WHEN file_extension = '.PPTX' THEN 'pptx'
        WHEN file_extension IN ('.JPG', '.JPEG') THEN 'jpeg'
        WHEN file_extension = '.PNG' THEN 'png'
        WHEN file_extension IN ('.TIFF', '.TIF') THEN 'tiff'
        WHEN file_extension = '.HTML' THEN 'html'
        WHEN file_extension = '.TXT' THEN 'txt'
        ELSE 'unknown'
      END;
      
      -- Try parsing with LAYOUT mode first (best for structured documents)
      parsed_content := AI_PARSE_DOCUMENT(
        TO_FILE('@document_db.s3_documents.document_stage', file_path),
        PARSE_JSON('{"mode": "LAYOUT"}')
      );
      content_text := parsed_content:content::STRING;
      
      -- Store parsed document with generated unique ID
      INSERT INTO document_db.s3_documents.parsed_documents 
      (document_id, file_name, file_path, file_size, file_url, document_type, parsed_content, content_text)
      SELECT
        CONCAT('DOC_', REPLACE(REPLACE(CURRENT_TIMESTAMP()::STRING,' ', '_'), ':',''), '_', ABS(HASH(:file_path))),
        :file_name,
        :file_path,
        :file_size,
        :file_url,
        :document_type,
        :parsed_content,
        :content_text;
      
      processed_count := processed_count + 1;
      
    EXCEPTION
      WHEN OTHER THEN
        BEGIN
          -- Fallback: Try OCR mode if LAYOUT mode fails
          parsed_content := AI_PARSE_DOCUMENT(
            TO_FILE('@document_db.s3_documents.document_stage', :file_path),
            PARSE_JSON('{"mode": "OCR"}')
          );
          content_text := parsed_content:content::STRING;
          
          INSERT INTO document_db.s3_documents.parsed_documents 
          (document_id, file_name, file_path, file_size, file_url, document_type, parsed_content, content_text)
          SELECT
            CONCAT('DOC_', REPLACE(REPLACE(CURRENT_TIMESTAMP()::STRING,' ', '_'), ':',''), '_', ABS(HASH(:file_path))),
            :file_name,
            :file_path,
            :file_size,
            :file_url,
            :document_type,
            :parsed_content,
            :content_text;
          
          processed_count := processed_count + 1;
        EXCEPTION
          WHEN OTHER THEN
            -- Final fallback: Log parsing failure but continue processing
            INSERT INTO document_db.s3_documents.parsed_documents 
            (document_id, file_name, file_path, file_size, file_url, document_type, parsed_content, content_text)
            SELECT
              CONCAT('ERR_DOC_', REPLACE(REPLACE(CURRENT_TIMESTAMP()::STRING,' ', '_'), ':',''), '_', ABS(HASH(COALESCE(:file_path,'UNKNOWN')))),
              COALESCE(:file_name, COALESCE(:file_path,'UNKNOWN')),
              COALESCE(:file_path,'UNKNOWN'),
              :file_size,
              COALESCE(:file_url, ''),
              COALESCE(:document_type, 'error'),
              PARSE_JSON('{"error":"Document parsing failed in both modes","timestamp":"' || CURRENT_TIMESTAMP()::STRING || '"}'),
              'parsing_failed';
        END;
    END;
  END FOR;
  
  -- Advance the stream offset so files are not reprocessed.
  -- Consuming the stream requires a DML statement that references the stream.
  -- Creating and dropping a temporary table fulfills this and moves the offset.
  CREATE OR REPLACE TEMPORARY TABLE temp_stream_consume AS 
  SELECT * FROM document_db.s3_documents.new_documents_stream;
  DROP TABLE temp_stream_consume;

  RETURN 'SUCCESS: Processed ' || processed_count || ' files';
END;
$$;

-- Step 2: Classify parsed documents using AI_CLASSIFY
-- Categorizes documents into 9 business document types: w2, vendor_contract, sales_report, 
-- marketing_report, hr_policy, corporate_policy, financial_infographic, case_study, strategy_document, or other
CREATE OR REPLACE PROCEDURE document_db.s3_documents.classify_parsed_documents()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  -- Cursor to get successfully parsed documents ready for classification
  doc_cursor CURSOR FOR 
    SELECT 
      document_id,
      file_name,
      file_path,
      file_size,
      file_url,
      document_type,
      parsed_content,
      content_text
    FROM document_db.s3_documents.parsed_documents
    WHERE status = 'parsed'
      AND content_text IS NOT NULL
      AND LENGTH(TRIM(content_text)) > 0;
  
  -- Variables for processing each document
  v_document_id STRING;
  v_file_name STRING;
  v_file_path STRING;
  v_file_size NUMBER;
  v_file_url STRING;
  v_document_type STRING;
  v_parsed_content VARIANT;
  v_content_text STRING;
  doc_class STRING;
  processed_count INTEGER := 0;
  error_count INTEGER := 0;
BEGIN
  
  FOR doc_record IN doc_cursor DO
    BEGIN
      -- Extract document data from cursor
      v_document_id := doc_record.document_id;
      v_file_name := doc_record.file_name;
      v_file_path := doc_record.file_path;
      v_file_size := doc_record.file_size;
      v_file_url := doc_record.file_url;
      v_document_type := doc_record.document_type;
      v_parsed_content := doc_record.parsed_content;
      v_content_text := doc_record.content_text;

      -- Classify document into one of 9 business categories using AI
      -- Categories aligned with demo_docs folder structure
      doc_class := AI_CLASSIFY(
        :v_content_text,
        ['w2', 'vendor_contract', 'sales_report', 'marketing_report', 'hr_policy', 
         'corporate_policy', 'financial_infographic', 'case_study', 'strategy_document', 'other']
      );
      
      -- Store classification result
      INSERT INTO document_db.s3_documents.document_classifications 
      (document_id, file_name, file_path, file_size, file_url, document_type, 
       parsed_content, document_class)
      SELECT :v_document_id, :v_file_name, :v_file_path, :v_file_size, :v_file_url, :v_document_type,
             :v_parsed_content, :doc_class;
      
      -- Mark document as classified to prevent reprocessing
      UPDATE document_db.s3_documents.parsed_documents 
      SET status = 'classified' 
      WHERE document_id = :v_document_id;
      
      processed_count := processed_count + 1;
      
    EXCEPTION
      WHEN OTHER THEN
        -- Handle classification errors gracefully
        error_count := error_count + 1;
        INSERT INTO document_db.s3_documents.document_classifications 
        (document_id, file_name, file_path, file_size, file_url, document_type, 
         parsed_content, document_class)
        SELECT CONCAT('ERR_CLASS_', REPLACE(REPLACE(CURRENT_TIMESTAMP()::STRING,' ', '_'), ':',''), '_', ABS(HASH(COALESCE(:v_file_path,'UNKNOWN')))),
               COALESCE(:v_file_name, 'classification_error'),
               COALESCE(:v_file_path, 'error_during_classification'),
               :v_file_size,
               COALESCE(:v_file_url, ''),
               COALESCE(:v_document_type, 'error'),
               PARSE_JSON('{"error": "Document classification failed", "timestamp": "' || CURRENT_TIMESTAMP()::STRING || '"}'),
               'classification_error';
        
        UPDATE document_db.s3_documents.parsed_documents 
        SET status = 'classification_error' 
        WHERE document_id = :v_document_id;
    END;
  END FOR;
  
  RETURN 'Classification completed. Processed: ' || processed_count || ', Errors: ' || error_count;
END;
$$;

-- Step 3: Extract specific attributes using AI_EXTRACT
-- Uses document class to lookup relevant prompts and extract structured data
CREATE OR REPLACE PROCEDURE document_db.s3_documents.extract_attributes_for_classified_documents()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  -- Cursor to get classified documents and normalize class labels
  doc_cursor CURSOR FOR
    SELECT 
      dc.document_id, 
      dc.file_name, 
      dc.file_path, 
      dc.document_class,
      CASE 
        WHEN TRY_PARSE_JSON(dc.document_class) IS NOT NULL THEN 
          TRY_PARSE_JSON(dc.document_class):labels[0]::STRING
        ELSE dc.document_class
      END AS document_class_norm
    FROM document_db.s3_documents.document_classifications dc;

  -- Variables for processing each document
  v_document_id STRING;
  v_file_name STRING;
  v_file_path STRING;
  v_document_class STRING;
  v_document_class_norm STRING;
  v_prompt_obj VARIANT;  -- JSON object containing attribute->question mappings
  v_result VARIANT;      -- AI_EXTRACT response with extracted values
  processed_count INTEGER := 0;
  error_count INTEGER := 0;
BEGIN
  FOR doc_record IN doc_cursor DO
    BEGIN
      -- Extract document data from cursor
      v_document_id := doc_record.document_id;
      v_file_name := doc_record.file_name;
      v_file_path := doc_record.file_path;
      v_document_class := doc_record.document_class;
      v_document_class_norm := doc_record.document_class_norm;

      -- Skip documents with missing file paths
      IF (v_file_path IS NULL OR v_file_path = '') THEN
        CONTINUE;
      END IF;

      -- Build prompt object from extraction_prompts table for this document class
      SELECT OBJECT_AGG(attribute_name, TO_VARIANT(question_text))
      INTO v_prompt_obj
      FROM document_db.s3_documents.extraction_prompts
      WHERE LOWER(REPLACE(REPLACE(TRIM(document_class),' ','_'),'-','_')) = 
            LOWER(REPLACE(REPLACE(TRIM(:v_document_class_norm),' ','_'),'-','_'));

      -- Skip if no prompts found for this document class
      IF (v_prompt_obj IS NULL) THEN
        CONTINUE;
      END IF;

      -- Extract structured data using AI with class-specific prompts
      v_result := AI_EXTRACT(
        file => TO_FILE('@document_db.s3_documents.document_stage', :v_file_path),
        responseFormat => :v_prompt_obj
      );

      -- Store extracted attributes using MERGE for idempotent updates
      MERGE INTO document_db.s3_documents.document_extractions t
      USING (
        SELECT :v_document_id AS document_id,
               :v_file_name AS file_name,
               :v_file_path AS file_path,
               :v_document_class AS document_class,
               f.key::STRING AS attribute_name,
               f.value::STRING AS attribute_value,
               :v_result AS extraction_json
        FROM LATERAL FLATTEN(INPUT => :v_result) f  -- Flatten JSON response into rows
      ) s
      ON t.document_id = s.document_id AND t.attribute_name = s.attribute_name
      WHEN MATCHED THEN UPDATE SET
        t.attribute_value = s.attribute_value,
        t.extraction_json = s.extraction_json,
        t.extraction_timestamp = CURRENT_TIMESTAMP()
      WHEN NOT MATCHED THEN INSERT (document_id, file_name, file_path, document_class, attribute_name, attribute_value, extraction_json)
      VALUES (s.document_id, s.file_name, s.file_path, s.document_class, s.attribute_name, s.attribute_value, s.extraction_json);

      processed_count := processed_count + 1;
    EXCEPTION
      WHEN OTHER THEN
        -- Handle extraction errors gracefully and continue processing
        error_count := error_count + 1;
    END;
  END FOR;

  RETURN 'Extraction completed. Processed: ' || processed_count || ', Errors: ' || error_count;
END;
$$;

-- =============================
-- DOCUMENT CHUNKING AND CORTEX SEARCH
-- =============================
-- Table to store chunked document content for semantic search
CREATE OR REPLACE TABLE document_db.s3_documents.document_chunks (
  chunk_id VARCHAR(150) PRIMARY KEY,
  document_id VARCHAR(100) NOT NULL,
  file_name VARCHAR(500),
  file_path VARCHAR(1000),
  document_class VARCHAR(100),
  chunk_index INTEGER,
  chunk_text STRING,
  chunk_size INTEGER,
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  FOREIGN KEY (document_id) REFERENCES document_db.s3_documents.document_classifications(document_id)
)
COMMENT = 'Table storing chunked document content for semantic search and RAG applications';

-- Step 3.5: Chunk documents using SPLIT_TEXT_RECURSIVE_CHARACTER
-- Creates searchable chunks from classified documents for semantic search
CREATE OR REPLACE PROCEDURE document_db.s3_documents.chunk_classified_documents()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  -- Cursor to get classified documents with content
  doc_cursor CURSOR FOR
    SELECT 
      dc.document_id,
      dc.file_name,
      dc.file_path,
      dc.document_class,
      pd.content_text
    FROM document_db.s3_documents.document_classifications dc
    JOIN document_db.s3_documents.parsed_documents pd 
      ON dc.document_id = pd.document_id
    WHERE pd.content_text IS NOT NULL 
      AND LENGTH(TRIM(pd.content_text)) > 100  -- Only chunk documents with substantial content
      AND dc.document_class NOT LIKE 'ERR_%'   -- Skip error records
      AND dc.document_class != 'classification_error';

  -- Variables for processing each document
  v_document_id STRING;
  v_file_name STRING;
  v_file_path STRING;
  v_document_class STRING;
  v_content_text STRING;
  v_chunks VARIANT;
  v_chunk_text STRING;
  v_chunk_index INTEGER;
  processed_count INTEGER := 0;
  chunk_count INTEGER := 0;
  error_count INTEGER := 0;
BEGIN
  
  FOR doc_record IN doc_cursor DO
    BEGIN
      -- Extract document data from cursor
      v_document_id := doc_record.document_id;
      v_file_name := doc_record.file_name;
      v_file_path := doc_record.file_path;
      v_document_class := doc_record.document_class;
      v_content_text := doc_record.content_text;

      -- Skip if document already chunked (avoid duplicates)
      LET chunk_exists INTEGER := (SELECT COUNT(*) FROM document_db.s3_documents.document_chunks WHERE document_id = :v_document_id);
      IF (chunk_exists > 0) THEN
        CONTINUE;
      END IF;

      -- Try Cortex chunking first, fallback to manual chunking if it fails
      BEGIN
        -- Split text into chunks using Snowflake Cortex function
        v_chunks := SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
          :v_content_text,
          1000,  -- chunk_size
          200    -- chunk_overlap
        );

        -- Process each chunk and insert into chunks table
        v_chunk_index := 0;
        FOR chunk_record IN (SELECT value FROM LATERAL FLATTEN(INPUT => :v_chunks)) DO
          v_chunk_text := chunk_record.value::STRING;
          
          -- Only store chunks with meaningful content
          LET chunk_length INTEGER := LENGTH(TRIM(:v_chunk_text));
          IF (chunk_length > 50) THEN
            INSERT INTO document_db.s3_documents.document_chunks
            (chunk_id, document_id, file_name, file_path, document_class, chunk_index, chunk_text, chunk_size)
            SELECT
              CONCAT(:v_document_id, '_CHUNK_', :v_chunk_index),
              :v_document_id,
              :v_file_name,
              :v_file_path,
              :v_document_class,
              :v_chunk_index,
              :v_chunk_text,
              LENGTH(:v_chunk_text);
            
            v_chunk_index := v_chunk_index + 1;
            chunk_count := chunk_count + 1;
          END IF;
        END FOR;
        
      EXCEPTION
        WHEN OTHER THEN
          -- Fallback: Manual chunking if Cortex function fails
          v_chunk_index := 0;
          LET text_length INTEGER := LENGTH(:v_content_text);
          LET chunk_size INTEGER := 1000;
          LET start_pos INTEGER := 1;
          
          WHILE (start_pos <= text_length) DO
            -- Extract chunk of text
            v_chunk_text := SUBSTR(:v_content_text, start_pos, chunk_size);
            
            -- Only store chunks with meaningful content
            LET chunk_length INTEGER := LENGTH(TRIM(:v_chunk_text));
            IF (chunk_length > 50) THEN
              INSERT INTO document_db.s3_documents.document_chunks
              (chunk_id, document_id, file_name, file_path, document_class, chunk_index, chunk_text, chunk_size)
              SELECT
                CONCAT(:v_document_id, '_CHUNK_', :v_chunk_index),
                :v_document_id,
                :v_file_name,
                :v_file_path,
                :v_document_class,
                :v_chunk_index,
                :v_chunk_text,
                LENGTH(:v_chunk_text);
              
              chunk_count := chunk_count + 1;
            END IF;
            
            v_chunk_index := v_chunk_index + 1;
            start_pos := start_pos + chunk_size;
          END WHILE;
      END;

      processed_count := processed_count + 1;
      
    EXCEPTION
      WHEN OTHER THEN
        -- Handle chunking errors gracefully - log the error for debugging
        error_count := error_count + 1;
        -- You can uncomment the next line to see specific errors during debugging
        -- RAISE;
    END;
  END FOR;
  
  RETURN 'Chunking completed. Documents: ' || processed_count || ', Chunks: ' || chunk_count || ', Errors: ' || error_count;
END;
$$;

-- Create Cortex Search Service for semantic search on document chunks
CREATE OR REPLACE CORTEX SEARCH SERVICE document_db.s3_documents.document_search_service
ON chunk_text
ATTRIBUTES document_id, file_name, file_path, document_class, chunk_index
WAREHOUSE = COMPUTE_WH
TARGET_LAG = '1 hour'
AS (
  SELECT 
    chunk_id,
    chunk_text,
    document_id,
    file_name,
    file_path,
    document_class,
    chunk_index
  FROM document_db.s3_documents.document_chunks
);


-- =============================
-- TASKS - Automated pipeline execution
-- =============================
-- Task 1: Triggered when new files appear in the stream
CREATE OR REPLACE TASK document_db.s3_documents.parse_documents_task
  TARGET_COMPLETION_INTERVAL = '10 MINUTES'
  COMMENT = 'Parse new documents from S3 stage using AI_PARSE_DOCUMENT'
WHEN SYSTEM$STREAM_HAS_DATA('document_db.s3_documents.new_documents_stream')
AS
  CALL document_db.s3_documents.parse_new_documents();

-- Task 2: Runs after parsing completes
CREATE OR REPLACE TASK document_db.s3_documents.classify_documents_task
  COMMENT = 'Classify parsed documents using AI_CLASSIFY'
  AFTER document_db.s3_documents.parse_documents_task
AS
  CALL document_db.s3_documents.classify_parsed_documents();

-- Task 3: Runs after classification completes
CREATE OR REPLACE TASK document_db.s3_documents.extract_documents_task
  COMMENT = 'Extract attributes using AI_EXTRACT after classification'
  AFTER document_db.s3_documents.classify_documents_task
AS
  CALL document_db.s3_documents.extract_attributes_for_classified_documents();

-- Task 4: Chunk documents for semantic search after extraction
CREATE OR REPLACE TASK document_db.s3_documents.chunk_documents_task
  COMMENT = 'Chunk classified documents for Cortex Search using SPLIT_TEXT_RECURSIVE_CHARACTER'
  AFTER document_db.s3_documents.extract_documents_task
AS
  CALL document_db.s3_documents.chunk_classified_documents();

-- =============================
-- FLATTENED DOCUMENT PROCESSING VIEW
-- =============================
-- Create a comprehensive flattened view that combines all document processing results
-- This view shows document_id, file_name, document_class, attribute_name, and attribute_value
-- Each row represents one document-attribute pair (e.g., customer_count: 12,062)

CREATE OR REPLACE VIEW document_db.s3_documents.document_processing_summary AS
WITH flattened_extractions AS (
    -- Handle regular attributes (non-JSON)
    SELECT 
        document_id,
        file_name,
        file_path,
        document_class,
        attribute_name,
        attribute_value,
        extraction_timestamp
    FROM document_db.s3_documents.document_extractions
    WHERE TRY_PARSE_JSON(attribute_value) IS NULL
    
    UNION ALL
    
    -- Handle JSON attributes - flatten them
    SELECT 
        de.document_id,
        de.file_name,
        de.file_path,
        de.document_class,
        f.key::STRING AS attribute_name,
        f.value::STRING AS attribute_value,
        de.extraction_timestamp
    FROM document_db.s3_documents.document_extractions de,
         LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(de.attribute_value)) f
    WHERE TRY_PARSE_JSON(de.attribute_value) IS NOT NULL
)
SELECT 
    dc.document_id,
    dc.file_name,
    dc.file_path,
    dc.document_type,
    -- Clean document classification (remove JSON formatting if present)
    CASE 
        WHEN TRY_PARSE_JSON(dc.document_class) IS NOT NULL THEN 
            TRY_PARSE_JSON(dc.document_class):labels[0]::STRING
        ELSE dc.document_class
    END as document_classification,
    fe.attribute_name,
    fe.attribute_value,
    dc.classification_timestamp,
    fe.extraction_timestamp
FROM document_db.s3_documents.document_classifications dc
LEFT JOIN flattened_extractions fe 
    ON dc.document_id = fe.document_id
ORDER BY dc.document_id, fe.attribute_name;


-- =============================
-- START TASKS AND VALIDATION QUERIES
-- =============================
-- No manual refresh needed - auto-refresh handles this automatically!
ALTER TASK document_db.s3_documents.parse_documents_task RESUME;

-- Validation queries to check pipeline status
SELECT * FROM document_db.s3_documents.new_documents_stream;
SELECT * FROM document_db.s3_documents.parsed_documents;
SELECT * FROM document_db.s3_documents.document_classifications;
SELECT * FROM document_db.s3_documents.document_extractions;
SELECT * FROM document_db.s3_documents.document_chunks;

-- Validation query for flattened document processing results
-- This shows each document with individual attribute-value pairs (one row per attribute)
SELECT * FROM document_db.s3_documents.document_processing_summary LIMIT 20;

-- Debug: Test the main view with w2_3 specifically
SELECT * FROM document_db.s3_documents.document_processing_summary 
WHERE file_name LIKE '%w2_3%' 
ORDER BY document_id, attribute_name;

-- Summary statistics for the flattened view
SELECT 
    COUNT(DISTINCT document_id) as total_documents,
    COUNT(DISTINCT document_classification) as unique_classifications,
    COUNT(DISTINCT attribute_name) as unique_attributes,
    COUNT(*) as total_rows_including_nulls
FROM document_db.s3_documents.document_processing_summary;

-- Debug: Check what's in the extractions table
SELECT 
    document_id, 
    file_name, 
    document_class, 
    attribute_name, 
    attribute_value,
    LENGTH(attribute_value) as value_length
FROM document_db.s3_documents.document_extractions 
ORDER BY document_id, attribute_name 
LIMIT 10;

-- Debug: Check if extractions exist at all
SELECT COUNT(*) as total_extractions FROM document_db.s3_documents.document_extractions;

-- Debug: Check extraction JSON structure
SELECT 
    document_id,
    file_name,
    attribute_name,
    attribute_value,
    extraction_json,
    TRY_PARSE_JSON(extraction_json) as parsed_json
FROM document_db.s3_documents.document_extractions 
WHERE extraction_json IS NOT NULL
LIMIT 5;

-- Debug: Check if attribute_value contains JSON that should be flattened
SELECT 
    document_id,
    file_name,
    attribute_name,
    attribute_value,
    TRY_PARSE_JSON(attribute_value) as parsed_attribute_value,
    CASE 
        WHEN TRY_PARSE_JSON(attribute_value) IS NOT NULL THEN 'JSON_IN_ATTRIBUTE_VALUE'
        ELSE 'PLAIN_TEXT'
    END as value_type
FROM document_db.s3_documents.document_extractions 
WHERE attribute_value IS NOT NULL
ORDER BY document_id, attribute_name;

-- Example: Show all attributes for a specific document (like infographic_1)
-- SELECT * FROM document_db.s3_documents.document_processing_summary 
-- WHERE file_name LIKE '%infographic%' 
-- ORDER BY document_id, attribute_name;

-- Debug: Test the main view with JSON flattening
SELECT * FROM document_db.s3_documents.document_processing_summary 
ORDER BY document_id, attribute_name 
LIMIT 20;

-- Debug: Test with w2_3 specifically
SELECT * FROM document_db.s3_documents.document_processing_summary 
WHERE file_name LIKE '%w2_3%' 
ORDER BY attribute_name;

-- Debug: Check if attribute names match between prompts and extractions
SELECT 
    'PROMPTS' as source,
    document_class,
    attribute_name
FROM document_db.s3_documents.extraction_prompts
WHERE document_class = 'w2'
UNION ALL
SELECT 
    'EXTRACTIONS' as source,
    document_class,
    attribute_name
FROM document_db.s3_documents.document_extractions
WHERE document_class = 'w2'
ORDER BY attribute_name, source;

-- Debug: Check specific w2_3 document extractions
SELECT 
    document_id,
    file_name,
    document_class,
    attribute_name,
    attribute_value
FROM document_db.s3_documents.document_extractions
WHERE file_name LIKE '%w2_3%'
ORDER BY attribute_name;

-- Debug: Check document classification for w2_3
SELECT 
    document_id,
    file_name,
    document_class,
    CASE 
        WHEN TRY_PARSE_JSON(document_class) IS NOT NULL THEN 
            TRY_PARSE_JSON(document_class):labels[0]::STRING
        ELSE document_class
    END as clean_document_class
FROM document_db.s3_documents.document_classifications
WHERE file_name LIKE '%w2_3%';

-- Debug: Test direct JOIN between classifications and extractions for w2_3
SELECT 
    dc.document_id,
    dc.file_name,
    CASE 
        WHEN TRY_PARSE_JSON(dc.document_class) IS NOT NULL THEN 
            TRY_PARSE_JSON(dc.document_class):labels[0]::STRING
        ELSE dc.document_class
    END as clean_document_class,
    de.attribute_name,
    de.attribute_value,
    de.document_class as extraction_document_class
FROM document_db.s3_documents.document_classifications dc
LEFT JOIN document_db.s3_documents.document_extractions de 
    ON dc.document_id = de.document_id
WHERE dc.file_name LIKE '%w2_3%'
ORDER BY de.attribute_name;

-- Debug: Compare expected vs actual attribute names for w2_3
WITH w2_doc AS (
    SELECT document_id, file_name,
           CASE 
               WHEN TRY_PARSE_JSON(document_class) IS NOT NULL THEN 
                   TRY_PARSE_JSON(document_class):labels[0]::STRING
               ELSE document_class
           END as clean_document_class
    FROM document_db.s3_documents.document_classifications
    WHERE file_name LIKE '%w2_3%'
),
expected_attrs AS (
    SELECT wd.document_id, wd.file_name, wd.clean_document_class, 
           ep.attribute_name as expected_attribute
    FROM w2_doc wd
    LEFT JOIN document_db.s3_documents.extraction_prompts ep 
        ON ep.document_class = wd.clean_document_class
),
actual_attrs AS (
    SELECT document_id, attribute_name as actual_attribute, attribute_value
    FROM document_db.s3_documents.document_extractions
    WHERE document_id IN (SELECT document_id FROM w2_doc)
)
SELECT 
    ea.document_id,
    ea.file_name,
    ea.clean_document_class,
    ea.expected_attribute,
    aa.actual_attribute,
    aa.attribute_value,
    CASE 
        WHEN ea.expected_attribute = aa.actual_attribute THEN 'MATCH'
        WHEN ea.expected_attribute IS NULL THEN 'NO_EXPECTED'
        WHEN aa.actual_attribute IS NULL THEN 'NO_ACTUAL'
        ELSE 'MISMATCH'
    END as match_status
FROM expected_attrs ea
FULL OUTER JOIN actual_attrs aa 
    ON ea.document_id = aa.document_id 
    AND ea.expected_attribute = aa.actual_attribute
ORDER BY ea.expected_attribute, aa.actual_attribute;

-- Test the JSON flattening in the view - should show individual attributes
SELECT 
    document_id,
    file_name,
    attribute_name,
    attribute_value
FROM document_db.s3_documents.document_processing_summary
WHERE file_name LIKE '%infographic%'
ORDER BY document_id, attribute_name;

-- =============================
-- AUTO-REFRESH TESTING & MONITORING
-- =============================

-- Test auto-refresh functionality
-- After uploading a file to S3, these should automatically update:

-- 1. Directory table should show new files (within seconds)
SELECT relative_path, size, last_modified 
FROM DIRECTORY(@document_db.s3_documents.document_stage) 
ORDER BY last_modified DESC 
LIMIT 5;

-- 2. Stream should capture the new files (within seconds)
SELECT COUNT(*) as new_files FROM document_db.s3_documents.new_documents_stream;

-- 3. Tasks should automatically trigger
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME LIKE '%document%'
ORDER BY SCHEDULED_TIME DESC 
LIMIT 10;

-- =============================
-- MONITORING AND TROUBLESHOOTING
-- =============================

-- Check auto-refresh status
-- DESC STAGE document_db.s3_documents.document_stage;

-- Monitor auto-refresh activity
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.AUTO_REFRESH_REGISTRATION_HISTORY(
--   DATE_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
-- ));

-- View refresh history and any errors
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.STAGE_DIRECTORY_FILE_REGISTRATION_HISTORY(
--   'document_db.s3_documents.document_stage'
-- ));

-- Monitor costs (auto-refresh appears as Snowpipe usage)
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
--   DATE_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP())
-- ));