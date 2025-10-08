import io, os, json, time, re
import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
from snowflake.core import Root  # requires snowflake>=0.8.0
import pypdfium2 as pdfium
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# App config & session
st.set_page_config(
    page_title="Document Classification & Extraction Dashboard", 
    layout="wide",
    initial_sidebar_state="expanded"
)
session = get_active_session()
root = Root(session)

# Custom CSS for Snowflake branding
def load_custom_css():
    st.markdown("""
    <style>
    /* Snowflake Brand Colors */
    :root {
        --snowflake-blue: #29B5E8;
        --mid-blue: #11567F;
        --midnight: #000000;
        --star-blue: #71D3DC;
        --valencia-orange: #FF9F36;
        --purple-moon: #7D44CF;
        --first-light: #D45B90;
        --windy-city: #8A999E;
    }
    
    /* Main title styling */
    .main .block-container h1 {
        color: var(--mid-blue);
        font-weight: 700;
        font-size: 2.5rem;
        margin-bottom: 2rem;
        text-align: center;
    }
    
    /* Subheader styling */
    .main .block-container h2 {
        color: var(--mid-blue);
        font-weight: 600;
        font-size: 1.8rem;
        margin-top: 2rem;
        margin-bottom: 1rem;
        border-bottom: 2px solid var(--snowflake-blue);
        padding-bottom: 0.5rem;
    }
    
    .main .block-container h3 {
        color: var(--mid-blue);
        font-weight: 500;
        font-size: 1.3rem;
        margin-bottom: 0.8rem;
    }
    
    /* Button styling */
    .stButton > button {
        background-color: var(--snowflake-blue);
        color: white;
        border: none;
        border-radius: 20px;
        padding: 0.5rem 1.5rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .stButton > button:hover {
        background-color: var(--mid-blue);
        color: white;
    }
    
    /* Slide title styling */
    .slide-title {
        background: linear-gradient(90deg, var(--snowflake-blue), var(--star-blue));
        padding: 1rem;
        border-radius: 8px;
        margin: 2rem 0;
    }
    
    .slide-title h2 {
        color: white;
        margin: 0;
        font-weight: 600;
    }
    
    /* Metadata styling with Snowflake branding */
    .metadata-item {
        background: linear-gradient(135deg, #f8fafc 0%, #e0f2fe 100%);
        border: 1px solid var(--star-blue);
        border-radius: 8px;
        padding: 1rem;
        margin: 0.5rem 0;
        font-size: 0.9rem;
        color: var(--mid-blue);
        box-shadow: 0 2px 4px rgba(41, 181, 232, 0.1);
    }
    
    .metadata-item strong {
        color: var(--mid-blue);
        font-weight: 600;
    }
    
    /* Professional header cards */
    .header-card {
        background: linear-gradient(135deg, var(--snowflake-blue) 0%, var(--star-blue) 100%);
        padding: 2rem;
        border-radius: 12px;
        margin-bottom: 2rem;
        color: white;
        box-shadow: 0 4px 12px rgba(41, 181, 232, 0.3);
    }
    
    .header-card h1 {
        margin: 0;
        font-size: 2.5rem;
        font-weight: 700;
        color: white;
        text-align: center;
    }
    
    .header-card p {
        margin: 0.5rem 0 0 0;
        font-size: 1.1rem;
        opacity: 0.9;
        text-align: center;
    }
    
    /* Sidebar styling */
    .css-1d391kg {
        background: linear-gradient(180deg, var(--mid-blue) 0%, var(--midnight) 100%);
    }
    
    .sidebar .sidebar-content {
        background: linear-gradient(180deg, var(--mid-blue) 0%, var(--midnight) 100%);
        color: white;
    }
    
    /* Section headers */
    .section-header-spaced {
        color: white;
        font-weight: 600;
        margin: 1.5rem 0 1rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid var(--snowflake-blue);
    }
    
    /* Metric cards with Snowflake styling */
    .metric-container {
        background: linear-gradient(135deg, #ffffff 0%, #f0f9ff 100%);
        border: 2px solid var(--star-blue);
        border-radius: 12px;
        padding: 1.5rem;
        box-shadow: 0 2px 8px rgba(41, 181, 232, 0.15);
        transition: all 0.3s ease;
    }
    
    .metric-container:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 16px rgba(41, 181, 232, 0.25);
        border-color: var(--snowflake-blue);
    }
    
    /* Status indicators with Snowflake colors */
    .status-success {
        color: var(--mid-blue);
        background: linear-gradient(135deg, #f0f9ff 0%, #e0f2fe 100%);
        border: 1px solid var(--star-blue);
        padding: 0.4rem 1rem;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: 600;
    }
    
    .status-error {
        color: var(--first-light);
        background: linear-gradient(135deg, #fdf2f8 0%, #fce7f3 100%);
        border: 1px solid var(--first-light);
        padding: 0.4rem 1rem;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: 600;
    }
    
    /* Table styling with Snowflake theme */
    .dataframe {
        border: 2px solid var(--star-blue);
        border-radius: 12px;
        overflow: hidden;
        box-shadow: 0 2px 8px rgba(41, 181, 232, 0.1);
    }
    
    .dataframe th {
        background: linear-gradient(90deg, var(--snowflake-blue), var(--star-blue));
        color: white;
        font-weight: 600;
        padding: 1rem;
        text-align: center;
    }
    
    .dataframe td {
        padding: 0.8rem;
        border-bottom: 1px solid var(--star-blue);
        color: var(--mid-blue);
    }
    
    /* Expander styling */
    .streamlit-expanderHeader {
        background: linear-gradient(135deg, var(--snowflake-blue) 0%, var(--star-blue) 100%);
        border: none;
        border-radius: 8px;
        font-weight: 600;
        color: white;
    }
    
    /* Success/Error message styling with Snowflake branding */
    .stSuccess {
        background: linear-gradient(135deg, #f0f9ff 0%, #e0f2fe 100%);
        border: 1px solid var(--star-blue);
        border-radius: 8px;
        color: var(--mid-blue);
    }
    
    .stError {
        background: linear-gradient(135deg, #fdf2f8 0%, #fce7f3 100%);
        border: 1px solid var(--first-light);
        border-radius: 8px;
        color: var(--first-light);
    }
    
    .stWarning {
        background: linear-gradient(135deg, #fffbeb 0%, #fef3c7 100%);
        border: 1px solid var(--valencia-orange);
        border-radius: 8px;
        color: #92400e;
    }
    
    .stInfo {
        background: linear-gradient(135deg, #f0f9ff 0%, #e0f2fe 100%);
        border: 1px solid var(--snowflake-blue);
        border-radius: 8px;
        color: var(--mid-blue);
    }
    
    /* Section dividers with Snowflake styling */
    hr {
        border: none;
        height: 2px;
        background: linear-gradient(90deg, var(--star-blue) 0%, var(--snowflake-blue) 50%, var(--star-blue) 100%);
        margin: 2rem 0;
    }
    
    /* Sidebar navigation selectbox styling */
    section[data-testid="stSidebar"] .stSelectbox > label {
        font-weight: 600;
        color: var(--mid-blue);
        font-size: 0.9rem;
        margin-bottom: 0.5rem;
    }
    
    section[data-testid="stSidebar"] .stSelectbox > div > div {
        border-radius: 8px;
        border: 2px solid var(--snowflake-blue);
    }
    
    /* Sidebar element spacing */
    section[data-testid="stSidebar"] .element-container {
        margin-bottom: 0.5rem !important;
    }
    </style>
    """, unsafe_allow_html=True)

# Load custom CSS
load_custom_css()

# ─────────────────────────────────────────────────────────────
# Utility Functions
# ─────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=30)
def get_pipeline_stats():
    """Get overall pipeline statistics"""
    try:
        stats = session.sql("""
            SELECT 
                COUNT(*) as total_documents,
                COUNT(CASE WHEN status = 'parsed' THEN 1 END) as parsed_count,
                COUNT(CASE WHEN status = 'classified' THEN 1 END) as classified_count,
                COUNT(CASE WHEN status = 'classification_error' THEN 1 END) as error_count
            FROM document_db.s3_documents.parsed_documents
        """).to_pandas().iloc[0]
        
        # Get extraction stats
        extraction_stats = session.sql("""
            SELECT COUNT(DISTINCT document_id) as extracted_count
            FROM document_db.s3_documents.document_extractions
        """).to_pandas().iloc[0]
        
        # Get chunk stats
        chunk_stats = session.sql("""
            SELECT 
                COUNT(*) as total_chunks,
                COUNT(DISTINCT document_id) as chunked_documents
            FROM document_db.s3_documents.document_chunks
        """).to_pandas().iloc[0]
        
        return {
            'total_documents': int(stats['TOTAL_DOCUMENTS']),
            'parsed_count': int(stats['PARSED_COUNT']),
            'classified_count': int(stats['CLASSIFIED_COUNT']),
            'error_count': int(stats['ERROR_COUNT']),
            'extracted_count': int(extraction_stats['EXTRACTED_COUNT']),
            'total_chunks': int(chunk_stats['TOTAL_CHUNKS']),
            'chunked_documents': int(chunk_stats['CHUNKED_DOCUMENTS'])
        }
    except Exception as e:
        st.error(f"Error fetching pipeline stats: {e}")
        return {}

@st.cache_data(show_spinner=False, ttl=60)
def get_document_classifications():
    """Get document classification breakdown"""
    try:
        df = session.sql("""
            SELECT 
                CASE 
                    WHEN TRY_PARSE_JSON(document_class) IS NOT NULL THEN 
                        TRY_PARSE_JSON(document_class):labels[0]::STRING
                    ELSE document_class
                END as document_class_clean,
                COUNT(*) as count
            FROM document_db.s3_documents.document_classifications
            WHERE document_class NOT LIKE 'ERR_%' 
              AND document_class != 'classification_error'
            GROUP BY document_class_clean
            ORDER BY count DESC
        """).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error fetching classifications: {e}")
        return pd.DataFrame()

@st.cache_data(show_spinner=False, ttl=30)
def get_recent_documents(limit=10):
    """Get recently processed documents"""
    try:
        df = session.sql(f"""
            SELECT 
                dc.document_id,
                dc.file_name,
                dc.file_path,
                CASE 
                    WHEN TRY_PARSE_JSON(dc.document_class) IS NOT NULL THEN 
                        TRY_PARSE_JSON(dc.document_class):labels[0]::STRING
                    ELSE dc.document_class
                END as document_class,
                dc.classification_timestamp,
                pd.status,
                pd.document_type
            FROM document_db.s3_documents.document_classifications dc
            JOIN document_db.s3_documents.parsed_documents pd 
                ON dc.document_id = pd.document_id
            ORDER BY dc.classification_timestamp DESC
            LIMIT {limit}
        """).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error fetching recent documents: {e}")
        return pd.DataFrame()

def get_document_details(document_id):
    """Get detailed information for a specific document"""
    try:
        # Get basic document info
        doc_info = session.sql(f"""
            SELECT 
                dc.document_id,
                dc.file_name,
                dc.file_path,
                dc.file_size,
                dc.document_type,
                CASE 
                    WHEN TRY_PARSE_JSON(dc.document_class) IS NOT NULL THEN 
                        TRY_PARSE_JSON(dc.document_class):labels[0]::STRING
                    ELSE dc.document_class
                END as document_class,
                dc.classification_timestamp,
                pd.content_text,
                pd.status
            FROM document_db.s3_documents.document_classifications dc
            JOIN document_db.s3_documents.parsed_documents pd 
                ON dc.document_id = pd.document_id
            WHERE dc.document_id = '{document_id}'
        """).to_pandas()
        
        # Get extracted fields
        extracted_fields = session.sql(f"""
            SELECT 
                attribute_name,
                attribute_value,
                extraction_timestamp
            FROM document_db.s3_documents.document_extractions
            WHERE document_id = '{document_id}'
            ORDER BY attribute_name
        """).to_pandas()
        
        # Get chunks
        chunks = session.sql(f"""
            SELECT 
                chunk_index,
                chunk_text,
                chunk_size
            FROM document_db.s3_documents.document_chunks
            WHERE document_id = '{document_id}'
            ORDER BY chunk_index
        """).to_pandas()
        
        return doc_info, extracted_fields, chunks
    except Exception as e:
        st.error(f"Error fetching document details: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

def render_document_preview(file_path, document_type):
    """Render document preview using Snowflake's unstructured data capabilities"""
    try:
        # Extract relative path from full file path for stage access
        if file_path.startswith('s3://'):
            # For S3 paths, extract the relative path after the bucket
            relative_path = '/'.join(file_path.split('/')[3:])  # Remove s3://bucket-name/
        else:
            relative_path = file_path
        
        if document_type.lower() in ['png', 'jpg', 'jpeg', 'tiff', 'tif']:
            # Generate a presigned URL for image preview
            try:
                presigned_url_result = session.sql(f"""
                    SELECT GET_PRESIGNED_URL('@document_db.s3_documents.document_stage', '{relative_path}', 3600) as presigned_url
                """).collect()
                
                if presigned_url_result and presigned_url_result[0]['PRESIGNED_URL']:
                    presigned_url = presigned_url_result[0]['PRESIGNED_URL']
                    st.image(presigned_url, caption=f"Preview: {file_path.split('/')[-1]}", use_container_width=True)
                else:
                    st.info("🖼️ Image preview not available - unable to generate access URL")
            except Exception as e:
                st.warning(f"🖼️ Image preview not available: {str(e)}")
                
        elif document_type.lower() == 'pdf':
            # For PDF files, show download option
            try:
                presigned_url_result = session.sql(f"""
                    SELECT GET_PRESIGNED_URL('@document_db.s3_documents.document_stage', '{relative_path}', 3600) as presigned_url
                """).collect()
                
                if presigned_url_result and presigned_url_result[0]['PRESIGNED_URL']:
                    presigned_url = presigned_url_result[0]['PRESIGNED_URL']
                    st.markdown(f"""
                    📄 **PDF Document**
                    
                    [📥 Download PDF]({presigned_url})
                    
                    *Click the link above to download and view the PDF document*
                    """)
                else:
                    st.info("📄 PDF preview not available - unable to generate access URL")
            except Exception as e:
                st.warning(f"📄 PDF preview not available: {str(e)}")
                
        elif document_type.lower() in ['docx', 'pptx']:
            # For Office documents, show download option
            try:
                presigned_url_result = session.sql(f"""
                    SELECT GET_PRESIGNED_URL('@document_db.s3_documents.document_stage', '{relative_path}', 3600) as presigned_url
                """).collect()
                
                if presigned_url_result and presigned_url_result[0]['PRESIGNED_URL']:
                    presigned_url = presigned_url_result[0]['PRESIGNED_URL']
                    doc_icon = "📊" if document_type.lower() == 'pptx' else "📝"
                    doc_name = "PowerPoint Presentation" if document_type.lower() == 'pptx' else "Word Document"
                    st.markdown(f"""
                    {doc_icon} **{doc_name}**
                    
                    [📥 Download {document_type.upper()}]({presigned_url})
                    
                    *Click the link above to download and view the {doc_name.lower()}*
                    """)
                else:
                    st.info(f"📄 {document_type.upper()} preview not available - unable to generate access URL")
            except Exception as e:
                st.warning(f"📄 {document_type.upper()} preview not available: {str(e)}")
                
        elif document_type.lower() in ['html', 'txt']:
            # For text-based files, show download option and potentially preview content
            try:
                presigned_url_result = session.sql(f"""
                    SELECT GET_PRESIGNED_URL('@document_db.s3_documents.document_stage', '{relative_path}', 3600) as presigned_url
                """).collect()
                
                if presigned_url_result and presigned_url_result[0]['PRESIGNED_URL']:
                    presigned_url = presigned_url_result[0]['PRESIGNED_URL']
                    doc_icon = "🌐" if document_type.lower() == 'html' else "📄"
                    doc_name = "HTML Document" if document_type.lower() == 'html' else "Text File"
                    st.markdown(f"""
                    {doc_icon} **{doc_name}**
                    
                    [📥 Download {document_type.upper()}]({presigned_url})
                    
                    *Click the link above to download and view the {doc_name.lower()}*
                    """)
                else:
                    st.info(f"📄 {document_type.upper()} preview not available - unable to generate access URL")
            except Exception as e:
                st.warning(f"📄 {document_type.upper()} preview not available: {str(e)}")
                
        else:
            # For other file types, show file info and download option if possible
            try:
                presigned_url_result = session.sql(f"""
                    SELECT GET_PRESIGNED_URL('@document_db.s3_documents.document_stage', '{relative_path}', 3600) as presigned_url
                """).collect()
                
                if presigned_url_result and presigned_url_result[0]['PRESIGNED_URL']:
                    presigned_url = presigned_url_result[0]['PRESIGNED_URL']
                    st.markdown(f"""
                    📁 **{document_type.upper()} File**
                    
                    [📥 Download File]({presigned_url})
                    
                    *Preview not available for this file type*
                    """)
                else:
                    st.info(f"📁 Preview not available for {document_type.upper()} files")
            except Exception as e:
                st.info(f"📁 Preview not available for {document_type.upper()} files")
                
    except Exception as e:
        st.error(f"Error rendering preview: {e}")



# ─────────────────────────────────────────────────────────────
# UI Components
# ─────────────────────────────────────────────────────────────
def render_pipeline_metrics():
    """Render pipeline status metrics"""
    stats = get_pipeline_stats()
    if not stats:
        return
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Documents", 
            stats.get('total_documents', 0),
            help="Total documents in the pipeline"
        )
    
    with col2:
        st.metric(
            "Classified", 
            stats.get('classified_count', 0),
            help="Documents successfully classified"
        )
    
    with col3:
        st.metric(
            "Extracted", 
            stats.get('extracted_count', 0),
            help="Documents with extracted attributes"
        )
    
    with col4:
        st.metric(
            "Total Chunks", 
            stats.get('total_chunks', 0),
            help="Total text chunks for search"
        )

def render_classification_chart():
    """Render document classification breakdown chart"""
    df = get_document_classifications()
    if df.empty:
        st.info("No classification data available")
        return
    
    fig = px.pie(
        df, 
        values='COUNT', 
        names='DOCUMENT_CLASS_CLEAN',
        title="Document Classification Breakdown",
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    fig.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(fig, use_container_width=True)

def render_processing_status_chart():
    """Render processing status breakdown"""
    stats = get_pipeline_stats()
    if not stats:
        return
    
    # Create status breakdown
    status_data = {
        'Status': ['Parsed', 'Classified', 'Extracted', 'Chunked', 'Errors'],
        'Count': [
            stats.get('parsed_count', 0),
            stats.get('classified_count', 0),
            stats.get('extracted_count', 0),
            stats.get('chunked_documents', 0),
            stats.get('error_count', 0)
        ],
        'Color': ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
    }
    
    fig = go.Figure(data=[
        go.Bar(
            x=status_data['Status'],
            y=status_data['Count'],
            marker_color=status_data['Color'],
            text=status_data['Count'],
            textposition='auto'
        )
    ])
    
    fig.update_layout(
        title="Pipeline Processing Status",
        xaxis_title="Processing Stage",
        yaxis_title="Document Count",
        yaxis=dict(dtick=1),  # Force whole numbers on y-axis
        showlegend=False
    )
    
    st.plotly_chart(fig, use_container_width=True)

def render_recent_documents():
    """Render table of recently processed documents"""
    df = get_recent_documents()
    if df.empty:
        st.info("No recent documents found")
        return
    
    # Format the dataframe for display
    display_df = df.copy()
    display_df['CLASSIFICATION_TIMESTAMP'] = pd.to_datetime(display_df['CLASSIFICATION_TIMESTAMP']).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    st.dataframe(
        display_df,
        use_container_width=True,
        column_config={
            "DOCUMENT_ID": st.column_config.TextColumn("Document ID", width="medium"),
            "FILE_NAME": st.column_config.TextColumn("File Name", width="large"),
            "DOCUMENT_CLASS": st.column_config.TextColumn("Class", width="small"),
            "STATUS": st.column_config.TextColumn("Status", width="small"),
            "CLASSIFICATION_TIMESTAMP": st.column_config.TextColumn("Processed", width="medium")
        },
        hide_index=True
    )

# ─────────────────────────────────────────────────────────────
# Main App Navigation
# ─────────────────────────────────────────────────────────────
if "nav" not in st.session_state:
    st.session_state.nav = "dashboard"

# Modern Navigation Pane
st.sidebar.markdown("""
<div style="text-align: center; padding: 0.5rem 0; margin-bottom: 1rem;">
    <div style="height: 3px; background: linear-gradient(90deg, #29B5E8, #71D3DC); margin: 0.5rem 0;"></div>
</div>
""", unsafe_allow_html=True)

# Navigation options with icons and descriptions
nav_options = [
    {"label": "🏠 Dashboard", "key": "dashboard", "desc": "Overview & Metrics"},
    {"label": "📁 Document Review & Explore", "key": "explorer", "desc": "Review & Browse Documents"}, 
    {"label": "💬 Document Assistant", "key": "search", "desc": "AI Chat & Search"},
    {"label": "⚙️ Pipeline Control", "key": "control", "desc": "Manage Processing"},
    {"label": "📈 Analytics", "key": "analytics", "desc": "Reports & Insights"},
    {"label": "💰 Cost Monitoring", "key": "costs", "desc": "Pipeline Expenses"}
]

# Create navigation with modern styling using radio buttons
nav_labels = [f"{item['label']} - {item['desc']}" for item in nav_options]
nav_keys = [item['key'] for item in nav_options]

# Find current selection index
try:
    current_index = nav_keys.index(st.session_state.nav)
except ValueError:
    current_index = 0

# Custom styled navigation using selectbox
selected_nav = st.sidebar.selectbox(
    "Navigate to:",
    options=nav_keys,
    format_func=lambda x: next(item['label'] for item in nav_options if item['key'] == x),
    index=current_index,
    key="nav_selector"
)

# Update session state if selection changed
if selected_nav != st.session_state.nav:
    st.session_state.nav = selected_nav
    st.rerun()

# Display current page info with modern styling
current_nav = next(item for item in nav_options if item['key'] == st.session_state.nav)
st.sidebar.markdown(f"""
<div style="
    background: linear-gradient(135deg, #29B5E8, #71D3DC);
    border-radius: 12px;
    padding: 1rem;
    margin: 1rem 0;
    text-align: center;
">
    <div style="font-weight: 600; color: white; font-size: 1rem;">
        {current_nav['label']}
    </div>
    <div style="font-size: 0.8rem; color: rgba(255,255,255,0.9); margin-top: 0.3rem;">
        {current_nav['desc']}
    </div>
</div>
""", unsafe_allow_html=True)

# Footer
st.sidebar.markdown("""
<div style="margin-top: 2rem; padding-top: 1rem; border-top: 1px solid #e2e8f0; text-align: center;">
    <div style="color: #64748b; font-size: 0.8rem;">
        Powered by <strong style="color: #29B5E8;">Snowflake Cortex AI</strong>
    </div>
</div>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────
# Page Routing
# ─────────────────────────────────────────────────────────────

# ========================= DASHBOARD =========================
if st.session_state.nav == "dashboard":
    # Professional header
    st.markdown("""
    <div class="header-card">
        <h1>Document Classification & Extraction Dashboard</h1>
        <p>Real-time overview of your intelligent document processing pipeline powered by Snowflake Cortex AI</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Refresh button
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("Refresh Data"):
            st.cache_data.clear()
            st.rerun()
    
    st.markdown("---")
    
    # Pipeline Metrics
    st.subheader("Pipeline Status")
    render_pipeline_metrics()
    
    st.markdown("---")
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        render_classification_chart()
    
    with col2:
        render_processing_status_chart()
    
    st.markdown("---")
    
    # Recent Documents
    st.subheader("Recently Processed Documents")
    render_recent_documents()

# ========================= DOCUMENT REVIEW & EXPLORE =========================
elif st.session_state.nav == "explorer":
    # Professional header for Document Review & Explore
    st.markdown("""
    <div class="header-card">
        <h1>Document Review & Explore</h1>
        <p>Review extraction quality and explore individual documents and their extracted data in detail</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Confidence threshold slider at the top
    st.subheader("⚙️ Quality Review Settings")
    col1, col2 = st.columns([2, 1])
    with col1:
        confidence_threshold = st.slider(
            "Confidence Score Threshold (highlight extractions below this value)",
            min_value=0.0,
            max_value=1.0,
            value=0.5,
            step=0.05,
            help="Extractions with confidence scores below this threshold will be highlighted for review"
        )
    with col2:
        auto_refresh = st.checkbox(
            "Auto-refresh on action",
            value=True,
            help="Automatically refresh after approving/denying values"
        )
    
    st.markdown("---")
    
    # Get list of documents
    recent_docs = get_recent_documents(50)  # Get more for selection
    
    if recent_docs.empty:
        st.warning("No documents found in the pipeline")
        st.stop()
    
    # Document selector
    doc_options = {}
    for _, row in recent_docs.iterrows():
        label = f"{row['FILE_NAME']}"
        doc_options[label] = row['DOCUMENT_ID']
    
    selected_doc_label = st.selectbox(
        "Select a document to explore:",
        options=list(doc_options.keys()),
        help="Choose a document to view its details and extracted data"
    )
    
    if selected_doc_label:
        document_id = doc_options[selected_doc_label]
        
        # Get document details with confidence scores
        @st.cache_data(ttl=60)
        def get_document_details_with_confidence(doc_id):
            """Get document details including confidence scores"""
            doc_query = f"""
            SELECT 
                dc.*,
                pd.content_text
            FROM document_db.s3_documents.document_classifications dc
            LEFT JOIN document_db.s3_documents.parsed_documents pd
                ON dc.document_id = pd.document_id
            WHERE dc.document_id = '{doc_id}'
            """
            
            fields_query = f"""
            SELECT 
                attribute_name,
                attribute_value,
                confidence_score,
                extraction_timestamp
            FROM document_db.s3_documents.document_extractions
            WHERE document_id = '{doc_id}'
            ORDER BY attribute_name
            """
            
            chunks_query = f"""
            SELECT chunk_text, chunk_index
            FROM document_db.s3_documents.document_chunks
            WHERE document_id = '{doc_id}'
            ORDER BY chunk_index
            LIMIT 3
            """
            
            doc_info = session.sql(doc_query).to_pandas()
            extracted_fields = session.sql(fields_query).to_pandas()
            chunks = session.sql(chunks_query).to_pandas()
            
            return doc_info, extracted_fields, chunks
        
        doc_info, extracted_fields, chunks = get_document_details_with_confidence(document_id)
        
        if not doc_info.empty:
            doc = doc_info.iloc[0]
            
            # Document Info
            st.subheader("📄 Document Information")
            
            # Clean document class display
            doc_class_display = doc['DOCUMENT_CLASS']
            if pd.notna(doc_class_display):
                try:
                    # Try to parse as JSON and extract the label
                    import json
                    parsed = json.loads(doc_class_display)
                    if isinstance(parsed, dict) and 'labels' in parsed:
                        doc_class_display = parsed['labels'][0] if parsed['labels'] else doc_class_display
                except:
                    # If parsing fails, use as-is
                    pass
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Document Class", doc_class_display)
            with col2:
                st.metric("File Type", doc['DOCUMENT_TYPE'].upper())
            
            # Document metadata (professional card layout)
            st.subheader("Document Metadata")
            meta_col1, meta_col2, meta_col3, meta_col4 = st.columns(4)
            with meta_col1:
                st.markdown(f"""
                <div class="metadata-item">
                    <strong>File Name</strong><br>
                    {doc['FILE_NAME']}
                </div>
                """, unsafe_allow_html=True)
            with meta_col2:
                file_size = f"{doc['FILE_SIZE']:,} bytes" if pd.notna(doc['FILE_SIZE']) else "N/A"
                st.markdown(f"""
                <div class="metadata-item">
                    <strong>File Size</strong><br>
                    {file_size}
                </div>
                """, unsafe_allow_html=True)
            with meta_col3:
                st.markdown(f"""
                <div class="metadata-item">
                    <strong>Processed</strong><br>
                    {doc['CLASSIFICATION_TIMESTAMP']}
                </div>
                """, unsafe_allow_html=True)
            with meta_col4:
                # Truncate long paths for display
                display_path = doc['FILE_PATH']
                if len(display_path) > 30:
                    display_path = "..." + display_path[-27:]
                st.markdown(f"""
                <div class="metadata-item">
                    <strong>File Path</strong><br>
                    <span title="{doc['FILE_PATH']}">{display_path}</span>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown("---")
            
            # Document preview (full width, larger)
            st.subheader("Document Preview")
            render_document_preview(doc['FILE_PATH'], doc['DOCUMENT_TYPE'])
            
            st.markdown("---")
            
            # Extracted Fields with Quality Review
            st.subheader("Extracted Fields")
            if not extracted_fields.empty:
                # Calculate quality metrics
                low_conf_count = len(extracted_fields[extracted_fields['CONFIDENCE_SCORE'] < confidence_threshold])
                if low_conf_count > 0:
                    st.warning(f"⚠️ {low_conf_count} extraction(s) below {confidence_threshold:.0%} confidence threshold")
                
                # Display extracted fields in a more compact grid layout with confidence scores
                field_cols = st.columns(2)  # Two columns for extracted fields
                for idx, (_, field) in enumerate(extracted_fields.iterrows()):
                    with field_cols[idx % 2]:
                        # Determine confidence score styling
                        conf_score = field['CONFIDENCE_SCORE']
                        is_low_confidence = pd.notna(conf_score) and conf_score < confidence_threshold
                        
                        # Color coding for confidence
                        if pd.isna(conf_score):
                            conf_color = "#64748b"  # Gray for N/A
                            conf_bg = "#f1f5f9"
                            conf_text = "N/A"
                        elif conf_score < 0.3:
                            conf_color = "#dc2626"  # Red
                            conf_bg = "#fef2f2"
                            conf_text = f"{conf_score:.1%}"
                        elif conf_score < 0.4:
                            conf_color = "#ea580c"  # Orange
                            conf_bg = "#fff7ed"
                            conf_text = f"{conf_score:.1%}"
                        elif conf_score < confidence_threshold:
                            conf_color = "#ca8a04"  # Yellow
                            conf_bg = "#fefce8"
                            conf_text = f"{conf_score:.1%}"
                        else:
                            conf_color = "#16a34a"  # Green
                            conf_bg = "#f0fdf4"
                            conf_text = f"{conf_score:.1%}"
                        
                        # Create expandable tile with confidence badge
                        tile_border = "2px solid #fca5a5" if is_low_confidence else "1px solid #e2e8f0"
                        
                        with st.expander(
                            f"{field['ATTRIBUTE_NAME']}", 
                            expanded=is_low_confidence
                        ):
                            # Confidence badge
                            st.markdown(f"""
                            <div style="
                                display: inline-block;
                                background: {conf_bg};
                                border: 1px solid {conf_color};
                                border-radius: 6px;
                                padding: 0.3rem 0.6rem;
                                font-weight: 600;
                                color: {conf_color};
                                font-size: 0.85rem;
                                margin-bottom: 0.5rem;
                            ">
                                Confidence: {conf_text}
                            </div>
                            """, unsafe_allow_html=True)
                            
                            # Value display and editing for low confidence items
                            if is_low_confidence:
                                unique_key = f"{document_id}_{field['ATTRIBUTE_NAME']}"
                                
                                # Editable value
                                edited_value = st.text_input(
                                    "Value",
                                    value=field['ATTRIBUTE_VALUE'] if pd.notna(field['ATTRIBUTE_VALUE']) else "",
                                    key=f"value_{unique_key}",
                                    label_visibility="collapsed"
                                )
                                
                                # Approve/Deny buttons
                                col_approve, col_deny = st.columns(2)
                                
                                with col_approve:
                                    if st.button(
                                        "✅ Approve",
                                        key=f"approve_{unique_key}",
                                        use_container_width=True,
                                        type="primary"
                                    ):
                                        try:
                                            update_query = f"""
                                            UPDATE document_db.s3_documents.document_extractions
                                            SET attribute_value = '{edited_value.replace("'", "''")}',
                                                confidence_score = 1.0
                                            WHERE document_id = '{document_id}'
                                                AND attribute_name = '{field['ATTRIBUTE_NAME']}'
                                            """
                                            session.sql(update_query).collect()
                                            st.success(f"✅ Approved!")
                                            
                                            if auto_refresh:
                                                get_document_details_with_confidence.clear()
                                                time.sleep(0.3)
                                                st.rerun()
                                        except Exception as e:
                                            st.error(f"Error: {str(e)}")
                                
                                with col_deny:
                                    if st.button(
                                        "❌ Deny",
                                        key=f"deny_{unique_key}",
                                        use_container_width=True
                                    ):
                                        try:
                                            update_query = f"""
                                            UPDATE document_db.s3_documents.document_extractions
                                            SET attribute_value = NULL,
                                                confidence_score = 0.0
                                            WHERE document_id = '{document_id}'
                                                AND attribute_name = '{field['ATTRIBUTE_NAME']}'
                                            """
                                            session.sql(update_query).collect()
                                            st.warning(f"❌ Denied")
                                            
                                            if auto_refresh:
                                                get_document_details_with_confidence.clear()
                                                time.sleep(0.3)
                                                st.rerun()
                                        except Exception as e:
                                            st.error(f"Error: {str(e)}")
                            else:
                                # Just display the value for high confidence items
                                st.write(field['ATTRIBUTE_VALUE'])
                
                # Bulk actions for low confidence items
                low_conf_fields = extracted_fields[extracted_fields['CONFIDENCE_SCORE'] < confidence_threshold]
                if not low_conf_fields.empty:
                    st.markdown("---")
                    st.subheader("🔧 Bulk Actions")
                    
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        if st.button("✅ Approve All Low-Confidence", type="secondary", use_container_width=True):
                            try:
                                update_query = f"""
                                UPDATE document_db.s3_documents.document_extractions
                                SET confidence_score = 1.0
                                WHERE document_id = '{document_id}'
                                    AND confidence_score < {confidence_threshold}
                                """
                                session.sql(update_query).collect()
                                st.success(f"✅ Approved {len(low_conf_fields)} extractions")
                                
                                if auto_refresh:
                                    get_document_details_with_confidence.clear()
                                    time.sleep(0.3)
                                    st.rerun()
                            except Exception as e:
                                st.error(f"Error: {str(e)}")
                    
                    with col2:
                        if st.button("❌ Deny All Low-Confidence", type="secondary", use_container_width=True):
                            try:
                                update_query = f"""
                                UPDATE document_db.s3_documents.document_extractions
                                SET attribute_value = NULL,
                                    confidence_score = 0.0
                                WHERE document_id = '{document_id}'
                                    AND confidence_score < {confidence_threshold}
                                """
                                session.sql(update_query).collect()
                                st.warning(f"❌ Denied {len(low_conf_fields)} extractions")
                                
                                if auto_refresh:
                                    get_document_details_with_confidence.clear()
                                    time.sleep(0.3)
                                    st.rerun()
                            except Exception as e:
                                st.error(f"Error: {str(e)}")
                    
                    with col3:
                        if st.button("🔄 Refresh", use_container_width=True):
                            get_document_details_with_confidence.clear()
                            st.rerun()
            else:
                st.info("No extracted fields found for this document")
            
            # Raw content
            if pd.notna(doc['CONTENT_TEXT']) and doc['CONTENT_TEXT']:
                with st.expander("Full Document Text"):
                    st.text_area(
                        "Raw extracted text",
                        doc['CONTENT_TEXT'],
                        height=300,
                        disabled=True
                    )

# ========================= DOCUMENT CHATBOT =========================
elif st.session_state.nav == "search":
    # Professional header for Document Assistant
    st.markdown("""
    <div class="header-card">
        <h1>Document AI Assistant</h1>
        <p>Ask questions about your documents and get AI-powered answers using Snowflake Cortex AI</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Constants
    CHAT_MEMORY = 10
    
    # Reset chat conversation
    def reset_conversation():
        st.session_state.doc_messages = [
            {
                "role": "assistant",
                "content": "Hello! I'm your Document AI Assistant. Ask me anything about your processed documents and I'll search through them to provide you with relevant answers.",
            }
        ]
    
    # Settings
    with st.expander("⚙️ Settings"):
        col1, col2 = st.columns(2)
        
        with col1:
            # Get available document classes for filtering
            class_df = get_document_classifications()
            class_options = ["All"] + class_df['DOCUMENT_CLASS_CLEAN'].tolist() if not class_df.empty else ["All"]
            
            doc_class_filter = st.selectbox(
                "Filter by document class:",
                options=class_options,
                help="Filter search results by document type"
            )
        
        with col2:
            result_limit = st.number_input(
                "Max search results:",
                min_value=1,
                max_value=20,
                value=5,
                help="Maximum number of document chunks to use for context"
            )
        
        st.button("Reset Chat", on_click=reset_conversation)
    
    # Helper functions for document search and AI response
    def find_relevant_documents(query, doc_filter=None, limit=5):
        """Search for relevant document chunks using Cortex Search"""
        try:
            # Use the specific database and schema where the service was created
            db = "document_db"
            schema = "s3_documents"
            
            # Access the Cortex Search service using the Root object
            cortex_search_service = (
                root.databases[db]
                .schemas[schema]
                .cortex_search_services["document_search_service"]
            )
            
            # Build search parameters
            search_columns = ["chunk_id", "document_id", "file_name", "file_path", "document_class", "chunk_index", "chunk_text"]
            
            # Perform the search with optional filter
            if doc_filter and doc_filter != "All":
                search_response = cortex_search_service.search(
                    query=query,
                    columns=search_columns,
                    filter={"@eq": {"document_class": doc_filter}},
                    limit=limit
                )
            else:
                search_response = cortex_search_service.search(
                    query=query,
                    columns=search_columns,
                    limit=limit
                )
            
            # Parse the search results
            results = []
            if search_response and hasattr(search_response, 'results'):
                for result in search_response.results:
                    results.append({
                        'chunk_id': result.get('chunk_id', ''),
                        'document_id': result.get('document_id', ''),
                        'file_name': result.get('file_name', ''),
                        'document_class': result.get('document_class', ''),
                        'chunk_text': result.get('chunk_text', ''),
                        'relevance_score': 1.0  # Cortex Search doesn't return explicit scores
                    })
                
                # Show which documents were found
                if results:
                    doc_names = list(set([doc['file_name'] for doc in results if doc['file_name']]))
                    st.info(f"📄 Found relevant content in: {', '.join(doc_names[:3])}{'...' if len(doc_names) > 3 else ''}")
                
                return results
            else:
                st.warning("No relevant documents found for your query.")
                return []
                
        except Exception as e:
            error_msg = str(e)
            if "does not exist" in error_msg or "404" in error_msg:
                st.error(f"""
                🚨 **Cortex Search Service Not Found**
                
                The search service `document_db.s3_documents.document_search_service` doesn't exist or isn't accessible.
                
                **Possible solutions:**
                1. **Check if the service exists:** Run `SHOW CORTEX SEARCH SERVICES;` in SQL
                2. **Create the service:** Execute the pipeline setup SQL to create the search service
                3. **Check permissions:** Ensure your role has access to the `document_db.s3_documents` schema
                4. **Verify data:** Make sure document chunks exist in `document_db.s3_documents.document_chunks`
                
                **Error details:** {error_msg}
                """)
            else:
                st.error(f"Error searching documents: {error_msg}")
            return []
    
    def get_ai_response(question, context_docs):
        """Generate AI response using Snowflake Cortex"""
        try:
            # Prepare context from documents
            context_text = "\n\n".join([
                f"Document: {doc['file_name']} (Class: {doc['document_class']})\nContent: {doc['chunk_text']}"
                for doc in context_docs
            ])
            
            # Create prompt for AI
            prompt = f"""You are a helpful document analysis assistant. Answer the user's question based on the provided document context.
            
            Question: {question}
            
            Document Context:
            {context_text}
            
            Instructions:
            - Provide a clear, concise answer based on the document content
            - If the answer isn't in the documents, say so clearly
            - Reference specific documents when relevant
            - Be helpful and informative
            
            Answer:"""
            
            # Use Snowflake Cortex Complete function
            response = session.sql(f"""
                SELECT SNOWFLAKE.CORTEX.COMPLETE(
                    'mixtral-8x7b',
                    '{prompt.replace("'", "''")}'
                ) as response
            """).collect()[0][0]
            
            return response
            
        except Exception as e:
            st.error(f"Error generating AI response: {e}")
            return "I apologize, but I encountered an error while processing your question. Please try again."
    
    # Initialize chat messages
    if "doc_messages" not in st.session_state:
        reset_conversation()
    
    # Chat input
    if user_message := st.chat_input("Ask me about your documents..."):
        st.session_state.doc_messages.append({"role": "user", "content": user_message})
    
    # Display chat messages
    for message in st.session_state.doc_messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
    
    # Generate response if last message was from user
    if st.session_state.doc_messages[-1]["role"] != "assistant":
        user_question = st.session_state.doc_messages[-1]["content"]
        
        with st.chat_message("assistant"):
            with st.status("🔍 Searching documents and generating response...", expanded=True) as status:
                st.write("🔎 Searching through your documents...")
                
                # Find relevant documents
                relevant_docs = find_relevant_documents(
                    user_question, 
                    doc_class_filter if doc_class_filter != "All" else None,
                    result_limit
                )
                
                if not relevant_docs:
                    response = "I couldn't find any relevant documents to answer your question. Please try rephrasing your query or check if documents have been processed."
                    st.markdown(response)
                else:
                    st.write("🤖 Generating AI response based on document content...")
                    
                    # Generate AI response
                    response = get_ai_response(user_question, relevant_docs)
                    
                    status.update(label="✅ Complete!", state="complete", expanded=False)
            
            # Display the response
            st.markdown("### 💬 Answer:")
            st.markdown(response)
            
            # Show source documents used
            if relevant_docs:
                st.markdown("### 📚 Sources Used:")
                for i, doc in enumerate(relevant_docs):
                    with st.expander(f"📄 {doc['file_name']} (Relevance: {doc['relevance_score']:.3f})"):
                        st.markdown(f"**Document Class:** {doc['document_class']}")
                        st.markdown(f"**Content:**")
                        st.text_area(
                            "Document content",
                            doc['chunk_text'],
                            height=150,
                            disabled=True,
                            key=f"doc_context_{i}"
                        )
        
        # Add assistant response to chat history
        st.session_state.doc_messages.append({"role": "assistant", "content": response})

# ========================= PIPELINE CONTROL =========================
elif st.session_state.nav == "control":
    # Professional header for Pipeline Control
    st.markdown("""
    <div class="header-card">
        <h1>Pipeline Control</h1>
        <p>Monitor and control your document processing pipeline powered by Snowflake</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Pipeline procedures
    st.subheader("Manual Pipeline Execution")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("Parse Documents", use_container_width=True):
            with st.spinner("Running parse procedure..."):
                try:
                    result = session.sql("CALL document_db.s3_documents.parse_new_documents()").collect()
                    st.success(f"Parse completed: {result[0][0]}")
                except Exception as e:
                    st.error(f"Parse failed: {e}")
    
    with col2:
        if st.button("Classify Documents", use_container_width=True):
            with st.spinner("Running classification procedure..."):
                try:
                    result = session.sql("CALL document_db.s3_documents.classify_parsed_documents()").collect()
                    st.success(f"Classification completed: {result[0][0]}")
                except Exception as e:
                    st.error(f"Classification failed: {e}")
    
    with col3:
        if st.button("Extract Attributes", use_container_width=True):
            with st.spinner("Running extraction procedure..."):
                try:
                    result = session.sql("CALL document_db.s3_documents.extract_attributes_for_classified_documents()").collect()
                    st.success(f"Extraction completed: {result[0][0]}")
                except Exception as e:
                    st.error(f"Extraction failed: {e}")
    
    with col4:
        if st.button("Chunk Documents", use_container_width=True):
            with st.spinner("Running chunking procedure..."):
                try:
                    result = session.sql("CALL document_db.s3_documents.chunk_classified_documents()").collect()
                    st.success(f"Chunking completed: {result[0][0]}")
                except Exception as e:
                    st.error(f"Chunking failed: {e}")
    
    # Additional pipeline operations
    st.markdown("**Additional Operations:**")
    col5, col6, col7, col8 = st.columns(4)
    
    with col6:
        if st.button("Run Full Pipeline", use_container_width=True):
            with st.spinner("Running complete pipeline..."):
                try:
                    # Run all procedures in sequence
                    parse_result = session.sql("CALL document_db.s3_documents.parse_new_documents()").collect()
                    classify_result = session.sql("CALL document_db.s3_documents.classify_parsed_documents()").collect()
                    extract_result = session.sql("CALL document_db.s3_documents.extract_attributes_for_classified_documents()").collect()
                    chunk_result = session.sql("CALL document_db.s3_documents.chunk_classified_documents()").collect()
                    
                    st.success("✅ Full pipeline completed successfully!")
                    st.info(f"Parse: {parse_result[0][0]}")
                    st.info(f"Classify: {classify_result[0][0]}")
                    st.info(f"Extract: {extract_result[0][0]}")
                    st.info(f"Chunk: {chunk_result[0][0]}")
                    st.info("📋 Flattened view automatically updated with new extractions")
                except Exception as e:
                    st.error(f"Pipeline execution failed: {e}")
    
    st.markdown("---")
    
    # Task status
    st.subheader("Task Status")
    try:
        # Use SHOW TASKS instead of INFORMATION_SCHEMA.TASK_HISTORY() to avoid session context issues
        task_status = session.sql("""
            SHOW TASKS LIKE '%document%' IN SCHEMA document_db.s3_documents
        """).to_pandas()
        
        if not task_status.empty:
            # Select relevant columns for display
            display_columns = ['name', 'state', 'condition', 'schedule', 'warehouse']
            available_columns = [col for col in display_columns if col in task_status.columns]
            
            if available_columns:
                st.dataframe(
                    task_status[available_columns], 
                    use_container_width=True,
                    column_config={
                        "name": st.column_config.TextColumn("Task Name", width="large"),
                        "state": st.column_config.TextColumn("State", width="small"),
                        "condition": st.column_config.TextColumn("Condition", width="medium"),
                        "schedule": st.column_config.TextColumn("Schedule", width="medium"),
                        "warehouse": st.column_config.TextColumn("Warehouse", width="small")
                    }
                )
            else:
                st.dataframe(task_status, use_container_width=True)
        else:
            st.info("No document-related tasks found")
    except Exception as e:
        st.error(f"Error fetching task status: {e}")
        # Fallback: Try to show all tasks in the schema
        try:
            st.info("Attempting to show all tasks in the schema...")
            all_tasks = session.sql("SHOW TASKS IN SCHEMA document_db.s3_documents").to_pandas()
            if not all_tasks.empty:
                st.dataframe(all_tasks, use_container_width=True)
            else:
                st.info("No tasks found in document_db.s3_documents schema")
        except Exception as fallback_error:
            st.error(f"Fallback query also failed: {fallback_error}")
    
    st.markdown("---")
    
    # Stream status
    st.subheader("Stream Status")
    try:
        stream_info = session.sql("""
            SELECT COUNT(*) as pending_files
            FROM document_db.s3_documents.new_documents_stream
        """).to_pandas().iloc[0]
        
        st.metric("Pending Files in Stream", int(stream_info['PENDING_FILES']))
    except Exception as e:
        st.error(f"Error fetching stream status: {e}")

# ========================= ANALYTICS =========================
elif st.session_state.nav == "analytics":
    # Professional header for Analytics
    st.markdown("""
    <div class="header-card">
        <h1>Analytics</h1>
        <p>Detailed analytics and insights from your document pipeline using Snowflake data</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Processing timeline
    st.subheader("Processing Timeline")
    try:
        timeline_data = session.sql("""
            SELECT 
                DATE(classification_timestamp) as process_date,
                COUNT(*) as documents_processed
            FROM document_db.s3_documents.document_classifications
            WHERE classification_timestamp >= CURRENT_DATE - 30
            GROUP BY DATE(classification_timestamp)
            ORDER BY process_date
        """).to_pandas()
        
        if not timeline_data.empty:
            fig = px.line(
                timeline_data,
                x='PROCESS_DATE',
                y='DOCUMENTS_PROCESSED',
                title="Documents Processed Over Time (Last 30 Days)",
                markers=True
            )
            fig.update_layout(
                xaxis_title="Date",
                yaxis_title="Documents Processed"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No timeline data available")
    except Exception as e:
        st.error(f"Error generating timeline: {e}")
    
    # Document type analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Document Types")
        try:
            type_data = session.sql("""
                SELECT 
                    document_type,
                    COUNT(*) as count
                FROM document_db.s3_documents.parsed_documents
                GROUP BY document_type
                ORDER BY count DESC
            """).to_pandas()
            
            if not type_data.empty:
                fig = px.bar(
                    type_data,
                    x='DOCUMENT_TYPE',
                    y='COUNT',
                    title="Documents by File Type"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No document type data available")
        except Exception as e:
            st.error(f"Error generating type analysis: {e}")
    
    with col2:
        st.subheader("Processing Success Rate")
        stats = get_pipeline_stats()
        if stats:
            total = stats.get('total_documents', 0)
            success = stats.get('classified_count', 0)
            errors = stats.get('error_count', 0)
            
            if total > 0:
                success_rate = (success / total) * 100
                error_rate = (errors / total) * 100
                
                fig = go.Figure(data=[
                    go.Pie(
                        labels=['Success', 'Errors', 'Pending'],
                        values=[success, errors, total - success - errors],
                        hole=0.4,
                        marker_colors=['#2ca02c', '#d62728', '#ff7f0e']
                    )
                ])
                fig.update_layout(title="Processing Success Rate")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No processing data available")
    
    # Extraction statistics
    st.subheader("Extraction Statistics")
    try:
        extraction_stats = session.sql("""
            SELECT 
                attribute_name,
                COUNT(*) as extraction_count,
                COUNT(DISTINCT document_id) as unique_documents
            FROM document_db.s3_documents.document_extractions
            GROUP BY attribute_name
            ORDER BY extraction_count DESC
            LIMIT 20
        """).to_pandas()
        
        if not extraction_stats.empty:
            fig = px.bar(
                extraction_stats,
                x='ATTRIBUTE_NAME',
                y='EXTRACTION_COUNT',
                title="Most Extracted Attributes",
                hover_data=['UNIQUE_DOCUMENTS']
            )
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No extraction statistics available")
    except Exception as e:
        st.error(f"Error generating extraction stats: {e}")
    
    # Flattened Document Processing Summary
    st.markdown("---")
    st.subheader("📋 Complete Document Processing Summary")
    st.markdown("*Each row represents one document-attribute pair (e.g., customer_count: 12,062, fiscal_year: 2025). JSON automatically flattened.*")
    
    try:
        # Get flattened document processing data
        flattened_query = """
        SELECT 
            document_id,
            file_name,
            document_type,
            document_classification,
            attribute_name,
            attribute_value,
            classification_timestamp,
            extraction_timestamp
        FROM document_db.s3_documents.document_processing_summary
        ORDER BY document_id, attribute_name
        LIMIT 500
        """
        flattened_df = session.sql(flattened_query).to_pandas()
        
        if not flattened_df.empty:
            # Add download button for the flattened data
            col1, col2 = st.columns([1, 4])
            with col1:
                csv_data = flattened_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download CSV",
                    data=csv_data,
                    file_name="document_processing_summary.csv",
                    mime="text/csv"
                )
            
            # Display summary statistics
            st.markdown("**Summary Statistics:**")
            stats_col1, stats_col2, stats_col3, stats_col4 = st.columns(4)
            
            with stats_col1:
                st.metric("Total Documents", flattened_df['DOCUMENT_ID'].nunique())
            with stats_col2:
                st.metric("Document Types", flattened_df['DOCUMENT_CLASSIFICATION'].nunique())
            with stats_col3:
                st.metric("Unique Attributes", flattened_df['ATTRIBUTE_NAME'].nunique())
            with stats_col4:
                st.metric("Total Extractions", len(flattened_df))
            
            # Display the flattened table
            st.markdown("**Complete Processing Results (JSON automatically flattened into individual attributes):**")
            st.dataframe(
                flattened_df,
                use_container_width=True,
                column_config={
                    "DOCUMENT_ID": st.column_config.TextColumn("Document ID", width="medium"),
                    "FILE_NAME": st.column_config.TextColumn("File Name", width="large"),
                    "DOCUMENT_TYPE": st.column_config.TextColumn("File Type", width="small"),
                    "DOCUMENT_CLASSIFICATION": st.column_config.TextColumn("Classification", width="medium"),
                    "ATTRIBUTE_NAME": st.column_config.TextColumn("Attribute", width="medium"),
                    "ATTRIBUTE_VALUE": st.column_config.TextColumn("Value", width="large"),
                    "CLASSIFICATION_TIMESTAMP": st.column_config.DatetimeColumn("Classified", width="medium"),
                    "EXTRACTION_TIMESTAMP": st.column_config.DatetimeColumn("Extracted", width="medium")
                },
                hide_index=True
            )
            
            # Show attribute distribution
            if 'ATTRIBUTE_NAME' in flattened_df.columns:
                st.markdown("**Attribute Distribution:**")
                attr_counts = flattened_df['ATTRIBUTE_NAME'].value_counts().head(10)
                fig = px.bar(
                    x=attr_counts.values,
                    y=attr_counts.index,
                    orientation='h',
                    title="Top 10 Most Extracted Attributes",
                    labels={'x': 'Count', 'y': 'Attribute Name'}
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No flattened document processing data available")
    except Exception as e:
        st.error(f"Error fetching flattened document data: {e}")

# ========================= COST MONITORING =========================
elif st.session_state.nav == "costs":
    st.markdown("""
    <div class="header-card">
        <h1>Cost Monitoring Dashboard</h1>
        <p>Track and analyze Cortex AI services and serverless task costs</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Date Range Filter
    st.subheader("Date Range Filter")
    col1, col2, col3 = st.columns([2, 2, 1])
    
    with col1:
        start_date = st.date_input(
            "Start Date",
            value=datetime.now() - timedelta(days=30),
            max_value=datetime.now().date(),
            help="Select the start date for cost analysis"
        )
    
    with col2:
        end_date = st.date_input(
            "End Date",
            value=datetime.now().date(),
            max_value=datetime.now().date(),
            help="Select the end date for cost analysis"
        )
    
    with col3:
        st.write("")  # Spacing
        st.write("")  # Spacing
        if st.button("Refresh Cost Data", type="primary"):
            st.rerun()
    
    # Validate date range
    if start_date > end_date:
        st.error("Error: Start date must be before or equal to end date")
        st.stop()
    
    st.markdown("---")
    
    # Summary Metrics Section
    st.subheader("Cost Summary")
    
    try:
        # Get total serverless compute and AI services costs for document pipeline only
        total_cost_query = f"""
        SELECT 
            SUM(CASE 
                WHEN SERVICE_TYPE = 'SERVERLESS_TASK' 
                    AND NAME IS NOT NULL
                    AND (UPPER(NAME) LIKE '%PARSE_DOCUMENTS_TASK%' 
                        OR UPPER(NAME) LIKE '%CLASSIFY_DOCUMENTS_TASK%' 
                        OR UPPER(NAME) LIKE '%EXTRACT_DOCUMENTS_TASK%'
                        OR UPPER(NAME) LIKE '%CHUNK_DOCUMENTS_TASK%')
                THEN CREDITS_USED ELSE 0 END) as serverless_credits,
            SUM(CASE 
                WHEN SERVICE_TYPE = 'AI_SERVICES'
                THEN CREDITS_USED ELSE 0 END) as ai_services_credits
        FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
        WHERE DATE(START_TIME) >= '{start_date}'
            AND DATE(START_TIME) <= '{end_date}'
            AND SERVICE_TYPE IN ('SERVERLESS_TASK', 'AI_SERVICES')
        """
        total_cost_df = session.sql(total_cost_query).to_pandas()
        
        if not total_cost_df.empty:
            serverless_credits = total_cost_df['SERVERLESS_CREDITS'].iloc[0] or 0
            ai_services_credits = total_cost_df['AI_SERVICES_CREDITS'].iloc[0] or 0
            total_credits = serverless_credits + ai_services_credits
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric(
                    "Total Serverless Compute Cost", 
                    f"{serverless_credits:.2f} credits",
                    help="Total credits for document pipeline serverless tasks only"
                )
            with col2:
                st.metric(
                    "Total AI Services Cost", 
                    f"{ai_services_credits:.2f} credits",
                    help="Total credits for all AI Services including Cortex AI functions"
                )
            with col3:
                st.metric(
                    "Combined Total Cost", 
                    f"{total_credits:.2f} credits",
                    help="Sum of document pipeline serverless tasks + AI functions costs"
                )
        else:
            st.info("No cost data available for the selected date range")
            
    except Exception as e:
        st.error(f"Error fetching cost summary: {str(e)}")
        st.info("Note: METERING_DAILY_HISTORY requires ACCOUNTADMIN privileges or proper grants")
    
    st.markdown("---")
    
    # 1. Serverless Task Costs (Document Pipeline Tasks Only)
    st.subheader("Serverless Task Costs (Document Pipeline)")
    
    try:
        serverless_query = f"""
        SELECT 
            NAME as task_name,
            DATE(START_TIME) as usage_date,
            SUM(CREDITS_USED) as credits_used
        FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
        WHERE SERVICE_TYPE = 'SERVERLESS_TASK'
            AND DATE(START_TIME) >= '{start_date}'
            AND DATE(START_TIME) <= '{end_date}'
            AND (UPPER(NAME) LIKE '%PARSE_DOCUMENTS_TASK%' 
                OR UPPER(NAME) LIKE '%CLASSIFY_DOCUMENTS_TASK%'
                OR UPPER(NAME) LIKE '%EXTRACT_DOCUMENTS_TASK%'
                OR UPPER(NAME) LIKE '%CHUNK_DOCUMENTS_TASK%')
        GROUP BY NAME, DATE(START_TIME)
        ORDER BY usage_date DESC, credits_used DESC
        """
        serverless_df = session.sql(serverless_query).to_pandas()
        
        if not serverless_df.empty:
            # Summary metrics
            total_task_credits = serverless_df['CREDITS_USED'].sum()
            unique_tasks = serverless_df['TASK_NAME'].nunique()
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Serverless Task Credits", f"{total_task_credits:.2f}")
            with col2:
                st.metric("Unique Tasks", f"{unique_tasks}")
            
            # Detailed table
            st.dataframe(
                serverless_df,
                column_config={
                    'TASK_NAME': 'Task Name',
                    'USAGE_DATE': st.column_config.DateColumn('Date'),
                    'CREDITS_USED': st.column_config.NumberColumn('Credits Used', format="%.2f")
                },
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No serverless task cost data available for the selected date range")
    except Exception as e:
        st.error(f"Error fetching serverless task costs: {str(e)}")
    
    st.markdown("---")
    
    # 2. Cortex Function Usage from CORTEX_FUNCTIONS_USAGE_HISTORY  
    st.subheader("Cortex Function Token Credits (AI_PARSE_DOCUMENT, AI_CLASSIFY, AI_EXTRACT)")
    
    try:
        cortex_functions_query = f"""
        SELECT 
            FUNCTION_NAME,
            DATE(START_TIME) as usage_date,
            SUM(TOKEN_CREDITS) as token_credits,
            COUNT(*) as call_count
        FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_USAGE_HISTORY
        WHERE DATE(START_TIME) >= '{start_date}'
            AND DATE(START_TIME) <= '{end_date}'
            AND (UPPER(FUNCTION_NAME) LIKE '%PARSE_DOCUMENT%' 
                OR UPPER(FUNCTION_NAME) LIKE '%CLASSIFY%' 
                OR UPPER(FUNCTION_NAME) LIKE '%EXTRACT%')
        GROUP BY FUNCTION_NAME, DATE(START_TIME)
        ORDER BY usage_date DESC, token_credits DESC
        """
        cortex_functions_df = session.sql(cortex_functions_query).to_pandas()
        
        if not cortex_functions_df.empty:
            # Overall Summary metrics
            total_token_credits = cortex_functions_df['TOKEN_CREDITS'].sum()
            total_calls = cortex_functions_df['CALL_COUNT'].sum()
            
            # Function-specific subtotals
            parse_credits = cortex_functions_df[cortex_functions_df['FUNCTION_NAME'].str.upper().str.contains('PARSE_DOCUMENT')]['TOKEN_CREDITS'].sum()
            classify_credits = cortex_functions_df[cortex_functions_df['FUNCTION_NAME'].str.upper().str.contains('CLASSIFY')]['TOKEN_CREDITS'].sum()
            extract_credits = cortex_functions_df[cortex_functions_df['FUNCTION_NAME'].str.upper().str.contains('EXTRACT')]['TOKEN_CREDITS'].sum()
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Function Calls", f"{total_calls:,.0f}")
            with col2:
                st.metric("Total Token Credits", f"{total_token_credits:.2f}")
            
            # Subtotals by function
            st.markdown("**Subtotals by Function:**")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("AI_PARSE_DOCUMENT", f"{parse_credits:.2f}")
            with col2:
                st.metric("AI_CLASSIFY", f"{classify_credits:.2f}")
            with col3:
                st.metric("AI_EXTRACT", f"{extract_credits:.2f}")
            
            # Detailed table
            st.dataframe(
                cortex_functions_df,
                column_config={
                    'FUNCTION_NAME': 'Function',
                    'USAGE_DATE': st.column_config.DateColumn('Date'),
                    'TOKEN_CREDITS': st.column_config.NumberColumn('Token Credits', format="%.2f"),
                    'CALL_COUNT': st.column_config.NumberColumn('Calls', format="%d")
                },
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No Cortex function token usage data available for the selected date range")
    except Exception as e:
        st.error(f"Error fetching Cortex function usage data: {str(e)}")
    
    st.markdown("---")
    
    # 3. Cortex Search Costs (Document Pipeline)
    st.subheader("Cortex Search Service Costs (Document Pipeline)")
    
    try:
        search_costs_query = f"""
        SELECT 
            SERVICE_NAME,
            USAGE_DATE,
            SUM(TOKENS) as total_tokens,
            SUM(CREDITS) as total_credits
        FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_SEARCH_DAILY_USAGE_HISTORY
        WHERE USAGE_DATE >= '{start_date}'
            AND USAGE_DATE <= '{end_date}'
            AND UPPER(SERVICE_NAME) LIKE '%DOCUMENT_SEARCH_SERVICE%'
        GROUP BY SERVICE_NAME, USAGE_DATE
        ORDER BY USAGE_DATE DESC, total_credits DESC
        """
        search_df = session.sql(search_costs_query).to_pandas()
        
        if not search_df.empty:
            # Summary
            total_tokens = search_df['TOTAL_TOKENS'].sum()
            total_credits = search_df['TOTAL_CREDITS'].sum()
            unique_services = search_df['SERVICE_NAME'].nunique()
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Tokens", f"{total_tokens:,.0f}")
            with col2:
                st.metric("Total Credits", f"{total_credits:.2f}")
            with col3:
                st.metric("Unique Services", f"{unique_services}")
            
            # Detailed table
            st.dataframe(
                search_df,
                column_config={
                    'SERVICE_NAME': 'Service Name',
                    'USAGE_DATE': st.column_config.DateColumn('Date'),
                    'TOTAL_TOKENS': st.column_config.NumberColumn('Tokens', format="%d"),
                    'TOTAL_CREDITS': st.column_config.NumberColumn('Credits', format="%.2f")
                },
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No Cortex Search cost data available for the selected date range")
    except Exception as e:
        st.error(f"Error fetching Cortex Search costs: {str(e)}")
    
    st.markdown("---")
    
    # 5. Cost Trends Over Time
    st.subheader("Cost Trends Over Time")
    
    # Serverless Task Costs Over Time (Document Pipeline)
    try:
        serverless_trend_query = f"""
        SELECT 
            DATE(START_TIME) as usage_date,
            SUM(CREDITS_USED) as total_credits
        FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
        WHERE SERVICE_TYPE = 'SERVERLESS_TASK'
            AND DATE(START_TIME) >= '{start_date}'
            AND DATE(START_TIME) <= '{end_date}'
            AND NAME IS NOT NULL
            AND (UPPER(NAME) LIKE '%PARSE_DOCUMENTS_TASK%' 
                OR UPPER(NAME) LIKE '%CLASSIFY_DOCUMENTS_TASK%' 
                OR UPPER(NAME) LIKE '%EXTRACT_DOCUMENTS_TASK%'
                OR UPPER(NAME) LIKE '%CHUNK_DOCUMENTS_TASK%')
        GROUP BY DATE(START_TIME)
        ORDER BY usage_date ASC
        """
        serverless_trend_df = session.sql(serverless_trend_query).to_pandas()
        
        if not serverless_trend_df.empty:
            fig1 = px.line(
                serverless_trend_df,
                x='USAGE_DATE',
                y='TOTAL_CREDITS',
                title="Serverless Task Credits Over Time (Document Pipeline)",
                labels={'TOTAL_CREDITS': 'Credits', 'USAGE_DATE': 'Date'}
            )
            st.plotly_chart(fig1, use_container_width=True)
    except Exception as e:
        st.error(f"Error generating serverless trend chart: {str(e)}")
    
    # AI Services Costs Over Time (Document Pipeline)
    try:
        ai_trend_query = f"""
        SELECT 
            DATE(START_TIME) as usage_date,
            SUM(CREDITS_USED) as total_credits
        FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
        WHERE SERVICE_TYPE = 'AI_SERVICES'
            AND DATE(START_TIME) >= '{start_date}'
            AND DATE(START_TIME) <= '{end_date}'
        GROUP BY DATE(START_TIME)
        ORDER BY usage_date ASC
        """
        ai_trend_df = session.sql(ai_trend_query).to_pandas()
        
        if not ai_trend_df.empty:
            fig2 = px.area(
                ai_trend_df,
                x='USAGE_DATE',
                y='TOTAL_CREDITS',
                title="AI Services Credits Over Time",
                labels={'TOTAL_CREDITS': 'Credits', 'USAGE_DATE': 'Date'}
            )
            st.plotly_chart(fig2, use_container_width=True)
    except Exception as e:
        st.error(f"Error generating AI services trend chart: {str(e)}")
    
    # Combined Services Cost Comparison (Document Pipeline)
    try:
        combined_trend_query = f"""
        SELECT 
            DATE(START_TIME) as usage_date,
            SERVICE_TYPE,
            SUM(CREDITS_USED) as total_credits
        FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY
        WHERE DATE(START_TIME) >= '{start_date}'
            AND DATE(START_TIME) <= '{end_date}'
            AND (
                (SERVICE_TYPE = 'SERVERLESS_TASK' 
                    AND NAME IS NOT NULL
                    AND (UPPER(NAME) LIKE '%PARSE_DOCUMENTS_TASK%' 
                        OR UPPER(NAME) LIKE '%CLASSIFY_DOCUMENTS_TASK%' 
                        OR UPPER(NAME) LIKE '%EXTRACT_DOCUMENTS_TASK%'
                        OR UPPER(NAME) LIKE '%CHUNK_DOCUMENTS_TASK%'))
                OR
                (SERVICE_TYPE = 'AI_SERVICES')
            )
        GROUP BY DATE(START_TIME), SERVICE_TYPE
        ORDER BY usage_date ASC
        """
        combined_trend_df = session.sql(combined_trend_query).to_pandas()
        
        if not combined_trend_df.empty:
            fig3 = px.bar(
                combined_trend_df,
                x='USAGE_DATE',
                y='TOTAL_CREDITS',
                color='SERVICE_TYPE',
                title="Cost Comparison: Serverless Tasks vs AI Services (Document Pipeline)",
                labels={'TOTAL_CREDITS': 'Credits', 'USAGE_DATE': 'Date', 'SERVICE_TYPE': 'Service Type'},
                barmode='group'
            )
            st.plotly_chart(fig3, use_container_width=True)
    except Exception as e:
        st.error(f"Error generating combined trend chart: {str(e)}")

# Footer with Snowflake branding
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: var(--mid-blue); font-size: 14px; font-weight: 500; padding: 1rem 0;'>
        <strong>Document Classification & Extraction App</strong> | 
        Powered by <span style='color: var(--snowflake-blue);'>Snowflake Cortex AI</span>
    </div>
    """,
    unsafe_allow_html=True
)