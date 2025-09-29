# Snowflake Document AI Pipeline

[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io/)
[![AWS S3](https://img.shields.io/badge/AWS%20S3-569A31?style=for-the-badge&logo=amazon-s3&logoColor=white)](https://aws.amazon.com/s3/)

An intelligent, automated document processing pipeline that transforms unstructured documents into structured, searchable data using Snowflake's AI capabilities.

## Key Highlights

- **Real-time & Event-driven Processing**: Using Auto-Refresh of Snowflake Directory Tables and Streams/Triggered Tasks, Documents are auto-processed in Snowflake within seconds of S3 upload
- **Scalable: Serverless Tasks allows Snowflake to predict, assign, and scale compute resources as necessary based on Document demands 
- **Cortex AI-Powered**: Leverages Snowflake Cortex AI functions for parsing (AI_PARSE_DOCUMENT), classification (AI_CLASSIFY), and extraction (AI_EXTRACT)
- **Interactive Dashboard**: Streamlit app for visualization, monitoring, and document chat
- **Semantic Search**: RAG-enabled document chatbot with Cortex Search
- **Multi-Format Support**: Handles PDF, DOCX, PPTX, JPEG, JPG, PNG, TIFF, TIF, HTML, and TXT files
- **Cost-Effective**: Documents live in S3/blob storage. No need to pay for loading documents into Snowflake. 

## Architecture

![Architecture Diagram](architecture_diagram.png)

## Supported File Formats

The pipeline supports all file formats compatible with Snowflake's AI_PARSE_DOCUMENT function:

| Format Category | File Extensions | Processing Notes |
|-----------------|-----------------|------------------|
| **Documents** | PDF, DOCX, PPTX | Full layout preservation with LAYOUT mode |
| **Images** | JPEG, JPG, PNG, TIFF, TIF | OCR and layout analysis |
| **Web/Text** | HTML, TXT | Automatic chunking for large files |

**Processing Details:**
- **PDF/DOCX/PPTX**: Each page billed as 970 tokens
- **Images**: Each file billed as 1 page (970 tokens)  
- **HTML/TXT**: Each 3,000 characters billed as 1 page (970 tokens)
- **Maximum**: 500 pages per document, 100 MB file size limit

## Document Classification Types

| Document Type | Extracted Fields | Use Cases |
|---------------|------------------|-----------|
| **W2 Forms** | Employee name, employer, wages, taxes, state | Payroll processing, tax preparation |
| **Snowflake Infographics** | Revenue, growth, customers, margins | Financial analysis, reporting |
| **Sustainability Reports** | Emissions, energy usage, ESG ratings | Environmental compliance |
| **Other Documents** | Title, date, author, summary, entities | General document management |

## Quick Start

### Prerequisites

- Snowflake account with Cortex AI enabled
- AWS S3 bucket with appropriate IAM permissions
- `COMPUTE_WH` warehouse (or equivalent)
- `ACCOUNTADMIN` role access for initial setup

### Setup Steps

1. **Configure S3 Integration** - Update `01_s3_integration_setup.sql`:
   ```sql
   STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR-ACCOUNT:role/YOUR-SNOWFLAKE-ROLE'
   URL = 's3://your-bucket-name/'
   ```

2. **Deploy Pipeline**:
   ```bash
   snowsql -f 01_s3_integration_setup.sql
   snowsql -f 02_document_pipeline_setup.sql
   ```

3. **Configure S3 Event Notifications**:
   - Copy the `DIRECTORY_NOTIFICATION_CHANNEL` ARN from step 2 output
   - In AWS S3 Console: Properties → Event notifications → Create event notification
   - Configure: All object create/remove events → SQS Queue → Paste ARN
   - Result: Real-time automatic pipeline triggering within seconds

4. **Launch Dashboard**:
   ```sql
   CREATE STREAMLIT document_db.s3_documents.document_ai_dashboard
     FROM 'snowflake_document_ai_dashboard.py'
     MAIN_FILE = 'snowflake_document_ai_dashboard.py';
   ```

## Usage

### Basic Document Processing

1. Upload documents to your S3 bucket
2. Monitor processing:
   ```sql
   SELECT * FROM document_db.s3_documents.parsed_documents;
   SELECT * FROM document_db.s3_documents.document_classifications;
   ```
3. View extracted data:
   ```sql
   SELECT file_name, document_class, attribute_name, attribute_value
   FROM document_db.s3_documents.document_extractions
   ORDER BY extraction_timestamp DESC;
   ```

### Dashboard Features

- **Pipeline Overview**: Real-time processing metrics and status
- **Document Explorer**: Browse individual documents and extracted data
- **AI Assistant**: Chat with your documents using semantic search
- **Pipeline Control**: Manual processing triggers and task management
- **Analytics**: Processing trends and performance insights

## Core Components

### Tables
- `parsed_documents` - Raw parsed content
- `document_classifications` - Classification results
- `document_extractions` - Structured extracted data
- `document_chunks` - Searchable text chunks
- `extraction_prompts` - Classification prompts

### Stored Procedures
- `parse_new_documents()` - Parse documents using AI_PARSE_DOCUMENT
- `classify_parsed_documents()` - Classify documents using AI_CLASSIFY
- `extract_attributes_for_classified_documents()` - Extract data using AI_EXTRACT
- `chunk_classified_documents()` - Create searchable chunks

### Automated Tasks
- Stream-triggered parsing → Classification → Extraction → Chunking

## Troubleshooting

### Common Issues

**Documents not processing:**
- Check tasks are running: `SHOW TASKS IN SCHEMA document_db.s3_documents;`
- Verify stream has data: `SELECT COUNT(*) FROM document_db.s3_documents.new_documents_stream;`

**Classification errors:**
- Test AI functions manually with sample documents
- Verify document formats are supported (PDF, DOCX, PPTX, JPEG, JPG, PNG, TIFF, TIF, HTML, TXT)

**No extracted attributes:**
- Check extraction prompts exist for document classes
- Verify file paths are accessible

### Manual Operations

**Reset pipeline:**
```bash
snowsql -f 03_cleanup_utilities.sql
```

**Reprocess documents:**
```sql
UPDATE document_db.s3_documents.parsed_documents 
SET status = 'parsed' 
WHERE file_name = 'your_document.pdf';

CALL document_db.s3_documents.classify_parsed_documents();
```

## Semantic Search

Search documents using Cortex Search:
```sql
SELECT * FROM TABLE(
  document_db.s3_documents.document_search_service.SEARCH(
    'your search query here', 5
  )
);
```

---

Built with Snowflake Cortex AI