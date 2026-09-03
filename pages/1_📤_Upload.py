"""
Upload page for InsightHub
"""
import streamlit as st
import pandas as pd
import sys
sys.path.append('src')

from database.connection import get_db_manager
from utils.validators import validate_ticket_data
from etl.transform import transform_tickets, prepare_for_database
from etl.loader import (
    create_upload_record, 
    load_tickets_to_db, 
    mark_upload_processed,
    get_all_uploads
)

st.set_page_config(
    page_title="Upload Data",
    page_icon="📤",
    layout="wide"
)

st.title("📤 Upload Ticket Data")
st.markdown("Upload your customer support tickets in CSV or Excel format")

# Initialize session state
if 'uploaded_data' not in st.session_state:
    st.session_state.uploaded_data = None
if 'upload_id' not in st.session_state:
    st.session_state.upload_id = None

# Sidebar info
with st.sidebar:
    st.header("📋 Required Columns")
    st.markdown("""
    **Required:**
    - `created_at` - Ticket creation date/time
    - `text` - Ticket text content
    
    **Optional (recommended):**
    - `ticket_id`
    - `customer_id`
    - `category`
    - `channel`
    - `priority`
    - `customer_tier`
    - `product`
    """)
    
    st.divider()
    
    st.header("📊 Sample Data")
    if st.button("Load Sample Dataset"):
        try:
            sample_df = pd.read_csv('data/samples/tickets_sample.csv')
            st.session_state.uploaded_data = sample_df
            st.success(f"✅ Loaded {len(sample_df)} sample tickets!")
            st.rerun()
        except Exception as e:
            st.error(f"Error loading sample: {e}")

# Main upload section
st.subheader("1️⃣ Upload File")

uploaded_file = st.file_uploader(
    "Choose a CSV or Excel file",
    type=['csv', 'xlsx'],
    help="Upload your ticket data file"
)

if uploaded_file is not None:
    try:
        # Read file
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)
        
        st.session_state.uploaded_data = df
        st.success(f"✅ File loaded: **{uploaded_file.name}** ({len(df)} rows)")
        
    except Exception as e:
        st.error(f"❌ Error reading file: {e}")

# If data is loaded, show validation and preview
if st.session_state.uploaded_data is not None:
    df = st.session_state.uploaded_data
    
    st.divider()
    st.subheader("2️⃣ Validate Data")
    
    # Validate button
    if st.button("🔍 Validate Data", type="primary"):
        with st.spinner("Validating..."):
            is_valid, report = validate_ticket_data(df)
            
            if is_valid:
                st.success("✅ Validation passed!")
            else:
                st.error("❌ Validation failed - please fix errors before uploading")
            
            # Show report
            st.text(report)
    
    st.divider()
    st.subheader("3️⃣ Preview Data")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Rows", len(df))
    with col2:
        st.metric("Total Columns", len(df.columns))
    with col3:
        if 'created_at' in df.columns:
            date_range = pd.to_datetime(df['created_at'])
            days = (date_range.max() - date_range.min()).days
            st.metric("Date Range", f"{days} days")
    
    # Show data preview
    st.dataframe(df.head(10), use_container_width=True)
    
    # Column info
    with st.expander("📋 Column Information"):
        col_info = pd.DataFrame({
            'Column': df.columns,
            'Type': df.dtypes.values,
            'Missing': df.isna().sum().values,
            'Missing %': (df.isna().sum() / len(df) * 100).round(2).values
        })
        st.dataframe(col_info, use_container_width=True)
    
    st.divider()
    st.subheader("4️⃣ Upload to Database")
    
    user_notes = st.text_area(
        "Notes (optional)",
        placeholder="Add any notes about this upload..."
    )
    
    if st.button("🚀 Upload to Database", type="primary"):
        # Validate first
        is_valid, report = validate_ticket_data(df)
        
        if not is_valid:
            st.error("❌ Please fix validation errors before uploading")
            st.text(report)
        else:
            try:
                with st.spinner("Uploading to database..."):
                    # Get database connection
                    db = get_db_manager()
                    
                    # Create upload record
                    filename = uploaded_file.name if uploaded_file else "sample_data.csv"
                    upload_id = create_upload_record(
                        db, 
                        filename, 
                        len(df), 
                        user_notes
                    )
                    
                    # Transform data
                    transformed_df = transform_tickets(df)
                    
                    # Prepare for database
                    db_df = prepare_for_database(transformed_df, upload_id)
                    
                    # Load to database
                    load_tickets_to_db(db, db_df, upload_id)
                    
                    # Mark as processed
                    mark_upload_processed(db, upload_id)
                    
                    st.session_state.upload_id = upload_id
                    
                st.success(f"✅ Successfully uploaded {len(df)} tickets!")
                st.info(f"Upload ID: {upload_id}")
                
                # Show next steps
                st.markdown("""
                ### ✨ Next Steps:
                1. Go to **🎯 Themes** page to discover themes
                2. View **⚡ Severity & Priority** for analysis
                3. Export results from **💾 Export** page
                """)
                
            except Exception as e:
                st.error(f"❌ Upload failed: {e}")
                import traceback
                st.code(traceback.format_exc())

# Show upload history
st.divider()
st.subheader("📚 Upload History")

try:
    db = get_db_manager()
    uploads = get_all_uploads(db)
    
    if uploads:
        upload_df = pd.DataFrame(uploads)
        upload_df['uploaded_at'] = pd.to_datetime(upload_df['uploaded_at'])
        
        st.dataframe(
            upload_df,
            use_container_width=True,
            column_config={
                "upload_id": "ID",
                "filename": "Filename",
                "row_count": st.column_config.NumberColumn("Rows", format="%d"),
                "uploaded_at": st.column_config.DatetimeColumn("Uploaded At"),
                "processed": st.column_config.CheckboxColumn("Processed")
            }
        )
    else:
        st.info("No uploads yet. Upload your first dataset above!")
        
except Exception as e:
    st.error(f"Error loading upload history: {e}")