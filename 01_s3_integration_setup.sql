-- =============================
-- S3 INTEGRATION SETUP
-- =============================
-- This file sets up the Snowflake S3 integration, database, schema, stage, and stream
-- Run this file first to establish the foundation for document processing

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- Create database and schema
CREATE OR REPLACE DATABASE document_db
  COMMENT = 'Database for storing documents from S3 bucket';
CREATE OR REPLACE SCHEMA document_db.s3_documents
  COMMENT = 'Schema for document tables and stages from S3';

-- Create S3 storage integration
CREATE OR REPLACE STORAGE INTEGRATION s3_document_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR-AWS-ACCOUNT-ID:role/YOUR-SNOWFLAKE-ROLE'  -- Replace with your IAM role ARN
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('*');  -- Replace with your S3 bucket name

DESCRIBE INTEGRATION s3_document_integration;

-- Create external stage with auto-refresh enabled
CREATE OR REPLACE STAGE document_db.s3_documents.document_stage
  STORAGE_INTEGRATION = s3_document_integration
  URL = 's3://your-bucket-name/'  -- Replace with your S3 bucket name
  DIRECTORY = (
    ENABLE = TRUE
    AUTO_REFRESH = TRUE
  )
  COMMENT = 'Stage for loading documents from private S3 bucket with auto-refresh';

-- Create stream to monitor new files (automatically updated by S3 events)
CREATE OR REPLACE STREAM document_db.s3_documents.new_documents_stream
ON DIRECTORY(@document_db.s3_documents.document_stage)
COMMENT = 'Stream to capture new documents uploaded to S3 bucket via auto-refresh';

-- Get SQS queue ARN for S3 event notification setup
-- Look for DIRECTORY_NOTIFICATION_CHANNEL in the output
DESC STAGE document_db.s3_documents.document_stage;

-- =============================
-- S3 EVENT NOTIFICATION SETUP
-- =============================
/*
STEP-BY-STEP: Configure S3 Event Notifications for Auto-Refresh

1. COPY THE SQS QUEUE ARN:
   From the DESC STAGE output above, copy the DIRECTORY_NOTIFICATION_CHANNEL value.
   It will look like: arn:aws:sqs:us-east-1:123456789012:sf-snowflake-AIDACKCEVSQ6C2EXAMPLE...

2. IN AWS S3 CONSOLE:
   a) Navigate to your S3 bucket: s3://your-bucket-name/
   b) Go to Properties tab → Event notifications → Create event notification
   
3. CONFIGURE THE NOTIFICATION:
   - Name: "snowflake-auto-refresh-notification"
   - Prefix: Leave blank (or specify folder if needed)
   - Suffix: Leave blank (to capture all file types)
   
4. SELECT EVENT TYPES:
   ✅ s3:ObjectCreated:* (All object create events)
   ✅ s3:ObjectRemoved:* (All object remove events)
   
5. SET DESTINATION:
   - Type: SQS Queue
   - SQS Queue: [Paste the DIRECTORY_NOTIFICATION_CHANNEL ARN from step 1]
   - Note: This is Snowflake's managed SQS queue for directory table auto-refresh
   
6. SAVE CHANGES
*/

-- =============================
-- VERIFICATION AND TESTING
-- =============================

-- Verify stage configuration (should show AUTO_REFRESH = TRUE)
DESC STAGE document_db.s3_documents.document_stage;

-- Test current stage contents
SELECT * FROM DIRECTORY(@document_db.s3_documents.document_stage);

-- Check stream status
SELECT COUNT(*) as stream_records FROM document_db.s3_documents.new_documents_stream;

-- =============================
-- TESTING AFTER S3 EVENT NOTIFICATION SETUP
-- =============================

-- 1. Check directory table (should show new files within seconds)
-- SELECT relative_path, size, last_modified 
-- FROM DIRECTORY(@document_db.s3_documents.document_stage) 
-- ORDER BY last_modified DESC LIMIT 5;

-- 2. Verify stream captured new files
-- SELECT COUNT(*) as new_files FROM document_db.s3_documents.new_documents_stream;

-- 3. Monitor auto-refresh activity
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.AUTO_REFRESH_REGISTRATION_HISTORY(
--   DATE_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
-- ));

-- 4. Check task execution
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
-- WHERE NAME LIKE '%document%'
-- ORDER BY SCHEDULED_TIME DESC LIMIT 10;
