"""
Test database connection and schema creation
"""
import sys
sys.path.append('src')

from database.connection import get_db_manager
from database.schema import create_schema, get_table_info

def main():
    print("=" * 60)
    print("TESTING DATABASE CONNECTION")
    print("=" * 60)
    
    # Initialize database manager
    print("\n1. Initializing database connection...")
    db = get_db_manager()
    
    # Test connection
    print("\n2. Testing connection...")
    if db.test_connection():
        print("✅ Connection successful!")
    else:
        print("❌ Connection failed!")
        return
    
    # Create schema
    print("\n3. Creating database schema...")
    try:
        create_schema(db)
        print("✅ Schema created!")
    except Exception as e:
        print(f"❌ Schema creation failed: {e}")
        return
    
    # Get table info
    print("\n4. Checking created tables...")
    tables = get_table_info(db)
    print(f"Created tables: {tables}")
    
    # Test a simple query
    print("\n5. Testing a simple query...")
    result = db.execute_query("SELECT COUNT(*) FROM uploads")
    print(f"Number of uploads: {result[0][0]}")
    
    print("\n" + "=" * 60)
    print("✅ ALL TESTS PASSED!")
    print("=" * 60)

if __name__ == "__main__":
    main()