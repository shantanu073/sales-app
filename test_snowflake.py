import os
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

cursor = conn.cursor()

cursor.execute("""
    SELECT *
    FROM V_SALES_KPIS
    LIMIT 10
""")

rows = cursor.fetchall()

print("Rows returned:", len(rows))

for row in rows:
    print(row)

cursor.close()
conn.close()