"""
Customer Support Insight - Main Streamlit App
"""
import streamlit as st

st.set_page_config(
    page_title="InsightHub",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Home page
st.title("🔍 InsightHub ")
st.markdown("### AI-Powered Customer Support Analytics")

st.markdown("""
Welcome to **InsightHub** - your intelligent platform for analyzing customer support data!

#### 🚀 Features:
- **📤 Upload Data**: Import tickets from CSV/Excel files
- **🎯 Theme Discovery**: Automatically categorize tickets using NLP
- **⚡ Severity & Priority**: Identify critical issues
- **📈 Business Impact**: Analyze trends and patterns
- **💾 Export Results**: Download enriched data and reports

#### 📋 Getting Started:
1. **Upload your data** using the sidebar navigation → 📤 Upload
2. Wait for analysis to complete
3. Explore insights in the dashboard pages

#### 💡 Don't have data?
Click the "Load Sample Dataset" button in the Upload page to try it out!
""")

# Quick stats (if data exists)
try:
    import sys
    sys.path.append('src')
    from database.connection import get_db_manager
    from sqlalchemy import text
    
    db = get_db_manager()
    
    with db.get_connection() as conn:
        # Get ticket count
        result = conn.execute(text("SELECT COUNT(*) FROM tickets"))
        ticket_count = result.fetchone()[0]
        
        # Get upload count
        result = conn.execute(text("SELECT COUNT(*) FROM uploads"))
        upload_count = result.fetchone()[0]
    
    if ticket_count > 0:
        st.divider()
        st.subheader("📊 Quick Stats")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Tickets", f"{ticket_count:,}")
        with col2:
            st.metric("Total Uploads", upload_count)
        with col3:
            st.metric("Status", "✅ Database Connected")

except:
    pass  # Database not set up yet or no data

# Footer
st.divider()
st.markdown("""
<div style='text-align: center; color: gray;'>
    Built with Python, PostgreSQL, Streamlit & ❤️
</div>
""", unsafe_allow_html=True)