"""
Database schema creation and management
"""
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)


# SQL Schema
SCHEMA_SQL = """
-- ============================================================================
-- CX INSIGHTS LAB - POSTGRESQL SCHEMA
-- ============================================================================

-- Table 1: uploads
CREATE TABLE IF NOT EXISTS uploads (
    upload_id SERIAL PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    file_size_bytes BIGINT,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    uploaded_by VARCHAR(100) DEFAULT 'default_user',
    row_count INTEGER,
    processed BOOLEAN DEFAULT FALSE,
    user_notes TEXT,
    config_params JSONB
);

CREATE INDEX IF NOT EXISTS idx_uploads_timestamp ON uploads(uploaded_at);

-- Table 2: tickets
CREATE TABLE IF NOT EXISTS tickets (
    ticket_id VARCHAR(100) PRIMARY KEY,
    upload_id INTEGER REFERENCES uploads(upload_id) ON DELETE CASCADE,
    
    created_at TIMESTAMP NOT NULL,
    text_content TEXT NOT NULL,
    product VARCHAR(100),
    channel VARCHAR(50),
    original_priority VARCHAR(20),
    customer_tier VARCHAR(50),
    customer_id VARCHAR(100),
    
    assigned_theme_id INTEGER,
    assigned_theme_name VARCHAR(200),
    theme_confidence FLOAT,
    severity_score INTEGER CHECK (severity_score BETWEEN 1 AND 5),
    severity_label VARCHAR(20),
    priority_rank INTEGER,
    
    text_length INTEGER,
    created_date DATE,
    created_month VARCHAR(7),
    
    processed_at TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tickets_upload ON tickets(upload_id);
CREATE INDEX IF NOT EXISTS idx_tickets_created_at ON tickets(created_at);
CREATE INDEX IF NOT EXISTS idx_tickets_theme ON tickets(assigned_theme_name);
CREATE INDEX IF NOT EXISTS idx_tickets_severity ON tickets(severity_label);

-- Table 3: themes
CREATE TABLE IF NOT EXISTS themes (
    theme_id SERIAL PRIMARY KEY,
    upload_id INTEGER REFERENCES uploads(upload_id) ON DELETE CASCADE,
    theme_number INTEGER NOT NULL,
    theme_name VARCHAR(200) NOT NULL,
    theme_description TEXT,
    keywords TEXT[],
    
    ticket_count INTEGER DEFAULT 0,
    percentage_of_total FLOAT,
    avg_severity FLOAT,
    growth_rate FLOAT,
    
    discovery_method VARCHAR(50),
    model_params JSONB,
    discovery_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(upload_id, theme_number)
);

CREATE INDEX IF NOT EXISTS idx_themes_upload ON themes(upload_id);
CREATE INDEX IF NOT EXISTS idx_themes_name ON themes(theme_name);

-- Table 4: analysis_cache
CREATE TABLE IF NOT EXISTS analysis_cache (
    cache_key VARCHAR(200) PRIMARY KEY,
    upload_id INTEGER REFERENCES uploads(upload_id) ON DELETE CASCADE,
    result_type VARCHAR(50),
    result_data JSONB NOT NULL,
    parameters JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP,
    access_count INTEGER DEFAULT 0,
    last_accessed TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_cache_upload ON analysis_cache(upload_id);
CREATE INDEX IF NOT EXISTS idx_cache_type ON analysis_cache(result_type);
"""


def create_schema(db_manager):
    """Create all database tables"""
    try:
        with db_manager.get_connection() as conn:
            conn.execute(text(SCHEMA_SQL))
            conn.commit()
            logger.info("✅ Database schema created successfully!")
            return True
    except Exception as e:
        logger.error(f"❌ Schema creation failed: {e}")
        raise


def drop_all_tables(db_manager):
    """Drop all tables (use with caution!)"""
    drop_sql = """
    DROP TABLE IF EXISTS analysis_cache CASCADE;
    DROP TABLE IF EXISTS themes CASCADE;
    DROP TABLE IF EXISTS tickets CASCADE;
    DROP TABLE IF EXISTS uploads CASCADE;
    """
    
    try:
        with db_manager.get_connection() as conn:
            conn.execute(text(drop_sql))
            conn.commit()
            logger.info("All tables dropped")
            return True
    except Exception as e:
        logger.error(f"Failed to drop tables: {e}")
        raise


def get_table_info(db_manager):
    """Get information about existing tables"""
    query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public'
    ORDER BY table_name;
    """
    
    try:
        with db_manager.get_connection() as conn:
            result = conn.execute(text(query))
            tables = [row[0] for row in result]
            logger.info(f"Existing tables: {tables}")
            return tables
    except Exception as e:
        logger.error(f"Failed to get table info: {e}")
        raise
