"""
Data loading functions to PostgreSQL
"""
import pandas as pd
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

def create_upload_record(db_manager, filename, row_count, user_notes=""):
    """
    Create a record in uploads table
    Returns: upload_id
    """
    query = """
    INSERT INTO uploads (filename, row_count, user_notes, processed)
    VALUES (:filename, :row_count, :user_notes, FALSE)
    RETURNING upload_id
    """
    
    try:
        with db_manager.get_connection() as conn:
            result = conn.execute(
                text(query),
                {
                    'filename': filename,
                    'row_count': row_count,
                    'user_notes': user_notes
                }
            )
            conn.commit()
            upload_id = result.fetchone()[0]
            logger.info(f"Created upload record: {upload_id}")
            return upload_id
    except Exception as e:
        logger.error(f"Failed to create upload record: {e}")
        raise

def load_tickets_to_db(db_manager, df, upload_id):
    """
    Load tickets DataFrame to database
    """
    try:
        # Use pandas to_sql for bulk insert
        df.to_sql(
            'tickets',
            db_manager.engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=100
        )
        
        logger.info(f"Loaded {len(df)} tickets to database")
        return True
    except Exception as e:
        logger.error(f"Failed to load tickets: {e}")
        raise

def mark_upload_processed(db_manager, upload_id):
    """Mark upload as processed"""
    query = """
    UPDATE uploads
    SET processed = TRUE
    WHERE upload_id = :upload_id
    """
    
    try:
        with db_manager.get_connection() as conn:
            conn.execute(text(query), {'upload_id': upload_id})
            conn.commit()
            logger.info(f"Marked upload {upload_id} as processed")
    except Exception as e:
        logger.error(f"Failed to mark upload as processed: {e}")
        raise

def get_upload_info(db_manager, upload_id):
    """Get information about an upload"""
    query = """
    SELECT upload_id, filename, row_count, uploaded_at, processed
    FROM uploads
    WHERE upload_id = :upload_id
    """
    
    try:
        with db_manager.get_connection() as conn:
            result = conn.execute(text(query), {'upload_id': upload_id})
            row = result.fetchone()
            if row:
                return {
                    'upload_id': row[0],
                    'filename': row[1],
                    'row_count': row[2],
                    'uploaded_at': row[3],
                    'processed': row[4]
                }
            return None
    except Exception as e:
        logger.error(f"Failed to get upload info: {e}")
        raise

def get_all_uploads(db_manager):
    """Get all uploads"""
    query = """
    SELECT upload_id, filename, row_count, uploaded_at, processed
    FROM uploads
    ORDER BY uploaded_at DESC
    """
    
    try:
        with db_manager.get_connection() as conn:
            result = conn.execute(text(query))
            uploads = []
            for row in result:
                uploads.append({
                    'upload_id': row[0],
                    'filename': row[1],
                    'row_count': row[2],
                    'uploaded_at': row[3],
                    'processed': row[4]
                })
            return uploads
    except Exception as e:
        logger.error(f"Failed to get uploads: {e}")
        raise