from access_db import DBAccess
from sqlalchemy import text

def apply_schema():
    db = DBAccess()
    with open("update_schema_etfs.sql", "r") as f:
        sql = f.read()
    
    # Split by GO or just execute if it's simple T-SQL without GO
    # The file content I wrote doesn't have GO, so it should be fine to execute as one block or split by statement if needed.
    # SQLAlchemy might not like multiple statements in one go depending on the driver.
    # Let's try executing it.
    
    try:
        with db.engine.begin() as conn:
            conn.execute(text(sql))
        print("Schema updated successfully.")
    except Exception as e:
        print(f"Error updating schema: {e}")
    finally:
        db.close_connection()

if __name__ == "__main__":
    apply_schema()
