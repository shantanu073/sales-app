"""
Two independent pipeline branches, same as your reference:

  1. upload_to_blob_storage()  -> Azure Blob Storage (triggers Snowpipe externally)
  2. process_operational_etl() -> Azure PostgreSQL (operational store)

Differences from the reference version:
  - Column set matches your actual SALES_RAW schema (product_id, sale_date,
    sales_rep, region, sales_amount, quantity_sold, ...), not
    transaction_id/customer_id/amount.
  - process_operational_etl() no longer silently swallows every Postgres
    error with a bare except+print. A connection-refused (e.g. port 5432
    blocked on your local network) is expected and handled gracefully, but
    a real error - bad data, wrong password, table doesn't exist - now
    surfaces to the caller so the Flask route can tell the user honestly
    instead of reporting "success" either way.
"""

import logging
import os
import urllib.parse

import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

load_dotenv(override=True)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("sales_etl")

# --------------------------------------------------------------------------
# Environment / config
# --------------------------------------------------------------------------
AZURE_CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = os.getenv("CONTAINER_NAME", "salesfiles")

PG_HOST = os.getenv("POSTGRES_HOST")
PG_DB = os.getenv("POSTGRES_DB", "salesdb")
PG_USER = os.getenv("POSTGRES_USER")
PG_PASS = os.getenv("POSTGRES_PASSWORD")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_TABLE = os.getenv("POSTGRES_TABLE", "sales_table")

REQUIRED_COLUMNS = [
    "product_id", "sale_date", "sales_rep", "region", "sales_amount",
    "quantity_sold", "product_category", "unit_cost", "unit_price",
    "customer_type", "discount", "payment_method", "sales_channel",
]


def get_postgres_engine():
    """Create a SQLAlchemy engine with a short connect timeout so a
    blocked/unreachable DB fails fast instead of hanging the request."""
    encoded_pass = urllib.parse.quote_plus(PG_PASS) if PG_PASS else ""
    db_url = f"postgresql://{PG_USER}:{encoded_pass}@{PG_HOST}:{PG_PORT}/{PG_DB}?sslmode=require"
    return create_engine(db_url, connect_args={"connect_timeout": 5})


def upload_to_blob_storage(file_path: str, filename: str) -> None:
    """Upload the raw CSV to Blob Storage. This alone is what triggers
    Snowpipe (via the event grid -> queue -> notification integration)."""
    if not AZURE_CONN_STR:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING is not set.")

    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONN_STR)
    blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=filename)

    with open(file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    logger.info("Uploaded %s to container '%s'", filename, CONTAINER_NAME)


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required column(s): {missing}")

    for col in ("sales_rep", "region", "product_category", "customer_type",
                "payment_method", "sales_channel"):
        df[col] = df[col].astype(str).str.strip().str.upper()

    df["sales_amount"] = pd.to_numeric(df["sales_amount"], errors="coerce")
    df["unit_cost"] = pd.to_numeric(df["unit_cost"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["discount"] = pd.to_numeric(df["discount"], errors="coerce").fillna(0.0)
    df["quantity_sold"] = pd.to_numeric(df["quantity_sold"], errors="coerce")
    # Source dates are M/D/YYYY (e.g. 2/3/2023) - pin the format explicitly
    # rather than let pandas guess, since M/D/YYYY vs D/M/YYYY is ambiguous
    # for any date where day <= 12.
    # df["sale_date"] = pd.to_datetime(df["sale_date"], format="%m/%d/%Y", errors="coerce").dt.date
    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce").dt.date
    # Always recompute rather than trust whatever came in the file
    df["region_and_sales_rep"] = df["region"] + "-" + df["sales_rep"]

    before = len(df)
    df = df.dropna(subset=["product_id", "sale_date", "sales_amount", "quantity_sold"])
    df = df[(df["sales_amount"] >= 0) & (df["quantity_sold"] > 0)]
    dropped = before - len(df)
    if dropped:
        logger.warning("Dropped %d invalid row(s) during cleaning", dropped)

    return df


def process_operational_etl(file_path: str) -> dict:
    """
    Clean the file and load it into Postgres.

    Returns a status dict rather than silently swallowing failures, so the
    Flask route can report accurately:
        {"loaded": True,  "rows": 10}
        {"loaded": False, "reason": "unreachable", "detail": "..."}
        {"loaded": False, "reason": "error", "detail": "..."}
    """
    df = pd.read_csv(file_path)
    df = _clean(df)

    if df.empty:
        return {"loaded": False, "reason": "no_valid_rows", "detail": "No valid rows after cleaning."}

    df["source_file"] = os.path.basename(file_path)

    try:
        engine = get_postgres_engine()
        df[REQUIRED_COLUMNS + ["region_and_sales_rep", "source_file"]].to_sql(
            PG_TABLE, con=engine, if_exists="append", index=False
        )
        logger.info("Inserted %d record(s) into %s", len(df), PG_TABLE)
        return {"loaded": True, "rows": len(df)}

    except OperationalError as exc:
        # Typically: can't reach the host at all (e.g. port 5432 blocked on
        # your local network - expected locally, should work once deployed
        # to Azure App Service, which sits inside the same VNet).
        logger.warning("Postgres unreachable, skipping operational load: %s", exc)
        return {"loaded": False, "reason": "unreachable", "detail": str(exc)}

    except Exception as exc:
        # A real problem - bad credentials, missing table, data type
        # mismatch - should NOT be silently swallowed.
        logger.exception("Operational ETL failed")
        return {"loaded": False, "reason": "error", "detail": str(exc)}
