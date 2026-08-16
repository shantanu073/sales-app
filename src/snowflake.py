import os

import snowflake.connector
import pandas as pd
from dotenv import load_dotenv

load_dotenv()


def get_sales_kpis():

    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

    try:

        query = """
            SELECT
                REGION,
                PRODUCT_CATEGORY,
                TOTAL_REVENUE,
                TOTAL_UNITS_SOLD,
                TOTAL_PROFIT,
                AVG_DISCOUNT,
                ORDER_COUNT,
                LAST_UPDATED_AT
            FROM V_SALES_KPIS
            ORDER BY TOTAL_REVENUE DESC
        """

        df = pd.read_sql(query, conn)

        return df

    finally:
        conn.close()