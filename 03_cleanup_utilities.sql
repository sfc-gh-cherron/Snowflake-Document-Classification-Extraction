-- =============================
-- CLEANUP UTILITIES
-- =============================
-- This file contains utilities for cleaning up and resetting the document processing pipeline
-- Use these commands when you need to restart the pipeline from scratch

-- =============================
-- CLEANUP UTILITIES - Use when restarting pipeline
-- =============================
-- Clear all files from stage and reset stream
-- NOTE: With auto-refresh enabled, the stage will automatically sync with S3
REMOVE @document_db.s3_documents.document_stage;
-- No manual refresh needed - auto-refresh will handle this automatically
CREATE OR REPLACE TEMPORARY TABLE temp_stream_clear AS 
SELECT * FROM document_db.s3_documents.new_documents_stream;
DROP TABLE temp_stream_clear;

-- Verify stage is empty and clear all processing tables
LIST @document_db.s3_documents.document_stage;
TRUNCATE TABLE document_db.s3_documents.document_chunks;        -- Clear chunks first (has FK dependency)
TRUNCATE TABLE document_db.s3_documents.document_extractions;
TRUNCATE TABLE document_db.s3_documents.document_classifications;
TRUNCATE TABLE document_db.s3_documents.parsed_documents;

-- Validation queries to confirm clean state
SELECT COUNT(*) as stream_record_count FROM document_db.s3_documents.new_documents_stream;
SELECT COUNT(*) as parsed_record_count FROM document_db.s3_documents.parsed_documents;
SELECT COUNT(*) as classification_record_count FROM document_db.s3_documents.document_classifications;
SELECT COUNT(*) as extraction_record_count FROM document_db.s3_documents.document_extractions;
SELECT COUNT(*) as chunk_record_count FROM document_db.s3_documents.document_chunks;

-- =============================
-- COMPLETE PIPELINE RESET (ADVANCED)
-- =============================
-- Use this section only if you need to completely rebuild the pipeline
/*
-- Drop all tasks (in reverse dependency order)
DROP TASK IF EXISTS document_db.s3_documents.extract_documents_task;
DROP TASK IF EXISTS document_db.s3_documents.chunk_documents_task;
DROP TASK IF EXISTS document_db.s3_documents.classify_documents_task;
DROP TASK IF EXISTS document_db.s3_documents.parse_documents_task;

-- Drop all tables
DROP TABLE IF EXISTS document_db.s3_documents.document_chunks;
DROP TABLE IF EXISTS document_db.s3_documents.document_extractions;
DROP TABLE IF EXISTS document_db.s3_documents.document_classifications;
DROP TABLE IF EXISTS document_db.s3_documents.parsed_documents;

-- Drop stream
DROP STREAM IF EXISTS document_db.s3_documents.new_documents_stream;

-- Drop stage (this will also clean up the auto-refresh SQS queue)
DROP STAGE IF EXISTS document_db.s3_documents.document_stage;

-- Drop schema and database
DROP SCHEMA IF EXISTS document_db.s3_documents;
DROP DATABASE IF EXISTS document_db;

-- Drop storage integration
DROP INTEGRATION IF EXISTS s3_document_integration;
*/

-- =============================
-- AUTO-REFRESH MANAGEMENT
-- =============================
-- Check auto-refresh status
-- DESC STAGE document_db.s3_documents.document_stage;

-- Disable auto-refresh temporarily (if needed for maintenance)
-- ALTER STAGE document_db.s3_documents.document_stage SET DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = FALSE);

-- Re-enable auto-refresh
-- ALTER STAGE document_db.s3_documents.document_stage SET DIRECTORY = (ENABLE = TRUE AUTO_REFRESH = TRUE);