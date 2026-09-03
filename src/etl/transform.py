"""
Data transformation functions
"""
import pandas as pd
#for regex patterns
import re
from datetime import datetime

def clean_text(text):
    """Clean and normalize text"""
    if pd.isna(text):
        return ""
    
    # convert to string
    text = str(text)
    
    # remove extra whitespace
    text = ' '.join(text.split())
    
    # remove special characters but keep basic punctuation
    text = re.sub(r'[^\w\s.,!?-]', '', text)
    
    return text.strip()

def transform_tickets(df):
    """
    Transform raw ticket data for database storage
    Returns: cleaned DataFrame
    """
    df = df.copy()
    
    # 1. Parse dates
    df['created_at'] = pd.to_datetime(df['created_at'])
    
    # 2. Clean text
    df['text'] = df['text'].apply(clean_text)
    
    # 3. Generate ticket_id if missing
    if 'ticket_id' not in df.columns or df['ticket_id'].isna().any():
        # Generate IDs for missing ones
        max_id = 100000
        for idx in df[df['ticket_id'].isna()].index:
            df.loc[idx, 'ticket_id'] = f'TKT-{max_id}'
            max_id += 1
    
    # 4. Add computed fields
    df['text_length'] = df['text'].str.len()
    df['created_date'] = df['created_at'].dt.date
    df['created_month'] = df['created_at'].dt.strftime('%Y-%m')
    
    # 5. Standardize column names (map to our schema)
    column_mapping = {
        'text': 'text_content',
        'priority': 'original_priority'
    }
    df = df.rename(columns=column_mapping)
    
    # 6. Fill missing optional fields
    optional_fills = {
        'product': 'Unknown',
        'channel': 'Unknown',
        'original_priority': 'Medium',
        'customer_tier': 'Unknown',
        'customer_id': 'Unknown'
    }
    
    for col, fill_value in optional_fills.items():
        if col in df.columns:
            df[col] = df[col].fillna(fill_value)
    
    # 7. Add metadata
    df['last_updated'] = datetime.now()
    
    return df

def prepare_for_database(df, upload_id):
    """
    Prepare DataFrame for database insertion
    """
    df = df.copy()
    
    # Add upload_id
    df['upload_id'] = upload_id
    
    # Select only columns that exist in database schema
    db_columns = [
        'ticket_id', 'upload_id', 'created_at', 'text_content',
        'product', 'channel', 'original_priority', 'customer_tier',
        'customer_id', 'text_length', 'created_date', 'created_month',
        'last_updated'
    ]
    
    # Keep only columns that exist in both df and db_columns
    final_columns = [col for col in db_columns if col in df.columns]
    df = df[final_columns]
    
    return df