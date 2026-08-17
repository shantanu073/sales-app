# Sales Data Pipeline — Azure + Snowflake + PostgreSQL

End-to-end sales data pipeline: a single CSV upload triggers two independent
downstream flows — an operational store (PostgreSQL) and an analytics
warehouse (Snowflake, medallion architecture) feeding a live dashboard.

**Live app:** http://sales-dashboard-snagwekar.azurewebsites.net/dashboard
**Repo:** https://github.com/shantanu073/sales-app

## Architecture

```
GitHub → GitHub Actions → Azure App Service (Flask)
                                  |
                          Upload Sales CSV
                                  v
                       Azure Blob Storage
                                  |
                 +----------------+----------------+
                 v                                 v
        Operational ETL                     Analytics ETL
        (pandas, synchronous)                (Snowpipe, event-driven)
                 |                                 |
                 v                                 v
        Azure PostgreSQL                Snowflake External Stage
                                                    |
                                                    v
                                          Bronze — SALES_RAW
                                                    |
                                  Stream + Task (LOAD_SALES_SILVER)
                                                    v
                                          Silver — SALES_CLEAN
                                                    |
                                  Stream + Task (LOAD_SALES_GOLD,
                                     chained AFTER the Silver task)
                                                    v
                                    Gold — SALES_KPIS_BY_REGION_CATEGORY
                                                    |
                                                    v
                                          Dashboard (auto-refreshing)
```

## Why two branches

- **Operational (PostgreSQL):** fast, single-row reads/writes for whatever
  application layer needs current-state data.
- **Analytical (Snowflake):** columnar warehouse built for aggregation
  across the full historical dataset, feeding KPI reporting.

The two branches are **deliberately independent** — a failure in one
(e.g. Postgres unreachable) never blocks or is reported as a failure in
the other. The Flask app reads the uploaded file into memory once and
reuses it for both branches.

## Tech stack

| Layer | Technology |
|---|---|
| Ingestion | Flask, deployed on Azure App Service |
| Storage | Azure Blob Storage |
| Operational DB | Azure Database for PostgreSQL (Flexible Server) |
| Event trigger | Azure Storage Queue + Event Grid → Snowflake Notification Integration |
| Analytics warehouse | Snowflake (Storage Integration, Snowpipe, Streams, Tasks) |
| Transformation | pandas (operational branch), SQL (Snowflake Silver/Gold tasks) |
| CI/CD | GitHub Actions → Azure App Service |
| Dashboard | Flask/Streamlit-served view over the Gold layer |

## Data model

Source CSV columns: `Product_ID, Sale_Date, Sales_Rep, Region, Sales_Amount,
Quantity_Sold, Product_Category, Unit_Cost, Unit_Price, Customer_Type,
Discount, Payment_Method, Sales_Channel, Region_and_Sales_Rep`

**Design note:** `Product_ID` increments per row rather than identifying a
distinct product, so it is *not* used as a dedup/merge key. Bronze → Silver
is append-only (`INSERT`, not `MERGE`) to avoid silently overwriting sales
records. Gold aggregates incrementally via `MERGE` keyed on
`(Region, Product_Category)`.

## Setup

1. **Snowflake:** run `snowflake_fresh_setup.sql` — creates the storage/
   notification integrations, Bronze/Silver/Gold schemas, Snowpipe, streams,
   and chained tasks.
2. **Azure IAM:** grant the Snowflake service principal `Storage Blob Data
   Reader` on the storage account and `Storage Queue Data Contributor` on
   the queue (via the `AZURE_CONSENT_URL` from `DESC STORAGE INTEGRATION`).
3. **PostgreSQL:** create an Azure Database for PostgreSQL Flexible Server,
   then run `postgres_setup.sql` to create `sales_table`.
4. **Flask app:** copy `.env.example` → `.env`, fill in Blob + Postgres
   credentials, `pip install -r requirements.txt`, run locally or deploy to
   App Service.

## Known operational notes

- Source files have used inconsistent date formats and discount scales
  across test runs — both the Snowflake file format (`DATE_FORMAT = 'AUTO'`)
  and the pandas ETL (`pd.to_datetime(..., errors="coerce")`, discount
  normalized to a 0–1 fraction) are defensive against this.
- Testing the PostgreSQL branch from a local machine may report
  "unreachable" if the local network blocks outbound port 5432 — this
  resolves once the app is deployed to Azure App Service.
