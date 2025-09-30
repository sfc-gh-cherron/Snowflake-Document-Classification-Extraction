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
    {"label": "📁 Document Explorer", "key": "explorer", "desc": "Browse Documents"}, 
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

# ========================= DOCUMENT EXPLORER =========================
elif st.session_state.nav == "explorer":
    # Professional header for Document Explorer
    st.markdown("""
    <div class="header-card">
        <h1>Document Explorer</h1>
        <p>Explore individual documents and their extracted data in detail using Snowflake AI</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Get list of documents
    recent_docs = get_recent_documents(50)  # Get more for selection
    
    if recent_docs.empty:
        st.warning("No documents found in the pipeline")
        st.stop()
    
    # Document selector
    doc_options = {}
    for _, row in recent_docs.iterrows():
        label = f"{row['FILE_NAME']} ({row['DOCUMENT_CLASS']}) - {row['CLASSIFICATION_TIMESTAMP']}"
        doc_options[label] = row['DOCUMENT_ID']
    
    selected_doc_label = st.selectbox(
        "Select a document to explore:",
        options=list(doc_options.keys()),
        help="Choose a document to view its details and extracted data"
    )
    
    if selected_doc_label:
        document_id = doc_options[selected_doc_label]
        
        # Get document details
        doc_info, extracted_fields, chunks = get_document_details(document_id)
        
        if not doc_info.empty:
            doc = doc_info.iloc[0]
            
            # Document Info
            st.subheader("📄 Document Information")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Document Class", doc['DOCUMENT_CLASS'])
            with col2:
                st.metric("File Type", doc['DOCUMENT_TYPE'].upper())
            with col3:
                st.metric("Status", doc['STATUS'])
            
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
            
            # Extracted Fields (full width, below preview)
            st.subheader("Extracted Fields")
            if not extracted_fields.empty:
                # Display extracted fields in a more compact grid layout
                field_cols = st.columns(2)  # Two columns for extracted fields
                for idx, (_, field) in enumerate(extracted_fields.iterrows()):
                    with field_cols[idx % 2]:
                        with st.expander(f"{field['ATTRIBUTE_NAME']}", expanded=True):
                            st.write(field['ATTRIBUTE_VALUE'])
                            st.caption(f"Extracted: {field['EXTRACTION_TIMESTAMP']}")
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
        <h1>💰 Cost Monitoring</h1>
        <p>Track and analyze pipeline expenses across AI functions, compute, and storage</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Refresh button
    if st.button("🔄 Refresh Cost Data", type="primary"):
        st.rerun()
    
    st.markdown("---")
    
    # Cost Overview Metrics
    col1, col2, col3, col4, col5 = st.columns(5)
    
    # Initialize variables for broader scope
    total_calls = 0
    parse_calls = 0
    classify_calls = 0
    extract_calls = 0
    search_calls = 0
    estimated_parse_cost = 0
    estimated_classify_cost = 0
    estimated_extract_cost = 0
    estimated_search_cost = 0
    total_estimated_cost = 0
    ai_usage_df = None
    
    try:
        # Get AI function usage costs (estimated)
        ai_usage_query = """
        SELECT 
            COUNT(*) as total_ai_calls,
            COUNT(CASE WHEN pd.parsed_content IS NOT NULL THEN 1 END) as parse_calls,
            COUNT(CASE WHEN dc.document_class IS NOT NULL THEN 1 END) as classify_calls,
            COUNT(CASE WHEN de.attribute_name IS NOT NULL THEN 1 END) as extract_calls
        FROM document_db.s3_documents.parsed_documents pd
        LEFT JOIN document_db.s3_documents.document_classifications dc ON pd.document_id = dc.document_id
        LEFT JOIN document_db.s3_documents.document_extractions de ON pd.document_id = de.document_id
        WHERE pd.parse_timestamp >= CURRENT_DATE - 30
        """
        ai_usage_df = session.sql(ai_usage_query).to_pandas()
        
        if not ai_usage_df.empty:
            total_calls = ai_usage_df['TOTAL_AI_CALLS'].iloc[0]
            parse_calls = ai_usage_df['PARSE_CALLS'].iloc[0]
            classify_calls = ai_usage_df['CLASSIFY_CALLS'].iloc[0]
            extract_calls = ai_usage_df['EXTRACT_CALLS'].iloc[0]
            
            # Get Cortex Search usage
            try:
                search_usage_query = """
                SELECT COUNT(*) as search_calls
                FROM document_db.s3_documents.document_chunks
                WHERE created_timestamp >= CURRENT_DATE - 30
                """
                search_df = session.sql(search_usage_query).to_pandas()
                search_calls = search_df['SEARCH_CALLS'].iloc[0] if not search_df.empty else 0
            except:
                search_calls = 0
            
            # Estimated costs (based on Snowflake pricing)
            # AI_PARSE_DOCUMENT: ~$0.002 per page, AI_CLASSIFY: ~$0.001 per call
            # AI_EXTRACT: ~$0.0015 per call, Cortex Search: ~$0.0005 per search
            estimated_parse_cost = parse_calls * 0.002
            estimated_classify_cost = classify_calls * 0.001
            estimated_extract_cost = extract_calls * 0.0015
            estimated_search_cost = search_calls * 0.0005
            total_estimated_cost = estimated_parse_cost + estimated_classify_cost + estimated_extract_cost + estimated_search_cost
            
            with col1:
                st.metric("Parse Calls", f"{parse_calls:,}", help="AI_PARSE_DOCUMENT function calls")
            with col2:
                st.metric("Classify Calls", f"{classify_calls:,}", help="AI_CLASSIFY function calls")
            with col3:
                st.metric("Extract Calls", f"{extract_calls:,}", help="AI_EXTRACT function calls")
            with col4:
                st.metric("Search Calls", f"{search_calls:,}", help="Cortex Search service calls")
            with col5:
                st.metric("Total AI Cost", f"${total_estimated_cost:.2f}", help="Estimated total cost for all AI services")
        
    except Exception as e:
        st.error(f"Error fetching AI usage data: {e}")
        with col1:
            st.metric("Parse Calls", "N/A")
        with col2:
            st.metric("Classify Calls", "N/A")
        with col3:
            st.metric("Extract Calls", "N/A")
        with col4:
            st.metric("Search Calls", "N/A")
        with col5:
            st.metric("Total AI Cost", "N/A")
    
    st.markdown("---")
    
    # Cost Breakdown Charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Daily AI Function Usage")
        try:
            daily_usage_query = """
            SELECT 
                DATE(pd.parse_timestamp) as usage_date,
                COUNT(*) as total_calls,
                COUNT(CASE WHEN pd.parsed_content IS NOT NULL THEN 1 END) as parse_calls,
                COUNT(CASE WHEN dc.document_class IS NOT NULL THEN 1 END) as classify_calls,
                COUNT(CASE WHEN de.attribute_name IS NOT NULL THEN 1 END) as extract_calls
            FROM document_db.s3_documents.parsed_documents pd
            LEFT JOIN document_db.s3_documents.document_classifications dc ON pd.document_id = dc.document_id
            LEFT JOIN document_db.s3_documents.document_extractions de ON pd.document_id = de.document_id
            WHERE pd.parse_timestamp >= CURRENT_DATE - 30
            GROUP BY DATE(pd.parse_timestamp)
            ORDER BY usage_date DESC
            LIMIT 30
            """
            daily_df = session.sql(daily_usage_query).to_pandas()
            
            if not daily_df.empty:
                fig = px.line(
                    daily_df, 
                    x='USAGE_DATE', 
                    y=['PARSE_CALLS', 'CLASSIFY_CALLS', 'EXTRACT_CALLS'],
                    title="AI Function Calls Over Time",
                    labels={'value': 'Number of Calls', 'USAGE_DATE': 'Date'}
                )
                fig.update_layout(yaxis=dict(dtick=1))  # Force whole numbers
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No usage data available for the last 30 days")
        except Exception as e:
            st.error(f"Error generating usage chart: {e}")
    
    with col2:
        st.subheader("💵 Estimated Cost Breakdown")
        try:
            if ai_usage_df is not None and not ai_usage_df.empty and total_calls > 0:
                cost_data = {
                    'Function': ['AI_PARSE_DOCUMENT', 'AI_CLASSIFY', 'AI_EXTRACT', 'CORTEX_SEARCH'],
                    'Calls': [parse_calls, classify_calls, extract_calls, search_calls],
                    'Est_Cost': [estimated_parse_cost, estimated_classify_cost, estimated_extract_cost, estimated_search_cost]
                }
                cost_df = pd.DataFrame(cost_data)
                cost_df = cost_df[cost_df['Calls'] > 0]  # Only show functions with usage
                
                fig = px.pie(
                    cost_df,
                    values='Est_Cost',
                    names='Function',
                    title="Cost Distribution by AI Function"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No cost data available")
        except Exception as e:
            st.error(f"Error generating cost breakdown: {e}")
    
    st.markdown("---")
    
    # Detailed Cost Analysis
    st.subheader("📋 Detailed Cost Analysis")
    
    # Cost per document type
    try:
        cost_by_type_query = """
        SELECT 
            COALESCE(dc.document_class, 'unclassified') as doc_type,
            COUNT(*) as document_count,
            COUNT(CASE WHEN pd.parsed_content IS NOT NULL THEN 1 END) as parsed_count,
            COUNT(CASE WHEN de.attribute_name IS NOT NULL THEN 1 END) as extract_count,
            COUNT(CASE WHEN ch.chunk_text IS NOT NULL THEN 1 END) as search_count,
            AVG(pd.file_size) as avg_file_size
        FROM document_db.s3_documents.parsed_documents pd
        LEFT JOIN document_db.s3_documents.document_classifications dc ON pd.document_id = dc.document_id
        LEFT JOIN document_db.s3_documents.document_extractions de ON pd.document_id = de.document_id
        LEFT JOIN document_db.s3_documents.document_chunks ch ON pd.document_id = ch.document_id
        WHERE pd.parse_timestamp >= CURRENT_DATE - 30
        GROUP BY dc.document_class
        ORDER BY document_count DESC
        """
        cost_type_df = session.sql(cost_by_type_query).to_pandas()
        
        if not cost_type_df.empty:
            # Calculate estimated costs per document type
            cost_type_df['ESTIMATED_PARSE_COST'] = cost_type_df['PARSED_COUNT'] * 0.002
            cost_type_df['ESTIMATED_CLASSIFY_COST'] = cost_type_df['DOCUMENT_COUNT'] * 0.001
            cost_type_df['ESTIMATED_EXTRACT_COST'] = cost_type_df['EXTRACT_COUNT'] * 0.0015
            cost_type_df['ESTIMATED_SEARCH_COST'] = cost_type_df['SEARCH_COUNT'] * 0.0005
            cost_type_df['ESTIMATED_TOTAL_COST'] = (cost_type_df['ESTIMATED_PARSE_COST'] + 
                                                   cost_type_df['ESTIMATED_CLASSIFY_COST'] + 
                                                   cost_type_df['ESTIMATED_EXTRACT_COST'] + 
                                                   cost_type_df['ESTIMATED_SEARCH_COST'])
            
            st.dataframe(
                cost_type_df[['DOC_TYPE', 'DOCUMENT_COUNT', 'PARSED_COUNT', 'EXTRACT_COUNT', 'SEARCH_COUNT', 'AVG_FILE_SIZE', 'ESTIMATED_TOTAL_COST']],
                column_config={
                    'DOC_TYPE': 'Document Type',
                    'DOCUMENT_COUNT': 'Total Documents',
                    'PARSED_COUNT': 'Parsed Documents',
                    'EXTRACT_COUNT': 'Extracted Documents',
                    'SEARCH_COUNT': 'Searchable Documents',
                    'AVG_FILE_SIZE': st.column_config.NumberColumn('Avg File Size (bytes)', format="%.0f"),
                    'ESTIMATED_TOTAL_COST': st.column_config.NumberColumn('Estimated Cost ($)', format="$%.3f")
                },
                use_container_width=True
            )
        else:
            st.info("No document processing data available")
    except Exception as e:
        st.error(f"Error generating cost analysis: {e}")
    
    # Additional Cost Metrics
    st.markdown("---")
    st.subheader("📊 Cost Breakdown by Service")
    
    # Show individual service costs
    if total_estimated_cost > 0:
        service_col1, service_col2, service_col3, service_col4 = st.columns(4)
        
        with service_col1:
            st.metric(
                "AI Parse Cost", 
                f"${estimated_parse_cost:.3f}",
                help="Cost for AI_PARSE_DOCUMENT function calls"
            )
        with service_col2:
            st.metric(
                "AI Classify Cost", 
                f"${estimated_classify_cost:.3f}",
                help="Cost for AI_CLASSIFY function calls"
            )
        with service_col3:
            st.metric(
                "AI Extract Cost", 
                f"${estimated_extract_cost:.3f}",
                help="Cost for AI_EXTRACT function calls"
            )
        with service_col4:
            st.metric(
                "Search Cost", 
                f"${estimated_search_cost:.3f}",
                help="Cost for Cortex Search service usage"
            )

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