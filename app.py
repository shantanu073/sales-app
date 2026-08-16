import os

from flask import Flask, render_template, request
from werkzeug.utils import secure_filename

from src.etl import process_operational_etl, upload_to_blob_storage
from src.snowflake import get_sales_kpis

app = Flask(__name__)

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload_file():
    if "file" not in request.files or request.files["file"].filename == "":
        return render_template("index.html", message="No file selected.", alert_type="danger")

    file = request.files["file"]
    if not file.filename.lower().endswith(".csv"):
        return render_template("index.html", message="Only .csv files are supported.", alert_type="warning")

    filename = secure_filename(file.filename)
    file_path = os.path.join(app.config["UPLOAD_FOLDER"], filename)
    file.save(file_path)

    blob_ok = False
    blob_error = None
    pg_result = None

    # --- Branch 1: Blob Storage (triggers Snowpipe externally) --------------
    try:
        upload_to_blob_storage(file_path, filename)
        blob_ok = True
    except Exception as exc:
        blob_error = str(exc)

    # --- Branch 2: Operational ETL -> PostgreSQL -----------------------------
    # Independent of branch 1 - a Postgres failure should never be reported
    # as a Blob Storage failure, and shouldn't block the Snowflake pipeline.
    try:
        pg_result = process_operational_etl(file_path)
    except ValueError as exc:
        # Bad/missing columns - this IS worth surfacing clearly, it means
        # the file itself doesn't match the expected schema.
        pg_result = {"loaded": False, "reason": "validation", "detail": str(exc)}
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

    # --- Build one honest status message combining both branches ------------
    parts = []
    alert_type = "success"

    if blob_ok:
        parts.append(f"Uploaded '{filename}' to Blob Storage — Snowflake pipeline will pick it up shortly.")
    else:
        parts.append(f"Blob Storage upload failed: {blob_error}")
        alert_type = "danger"

    if pg_result and pg_result.get("loaded"):
        parts.append(f"PostgreSQL: inserted {pg_result['rows']} row(s).")
    elif pg_result and pg_result.get("reason") == "unreachable":
        parts.append("PostgreSQL: skipped (unreachable — expected locally, should work once deployed).")
        if alert_type == "success":
            alert_type = "warning"
    elif pg_result:
        parts.append(f"PostgreSQL: failed — {pg_result.get('detail')}")
        alert_type = "danger"

    return render_template("index.html", message=" ".join(parts), alert_type=alert_type)

@app.route("/dashboard")
def dashboard():

    try:

        df = get_sales_kpis()

        if df.empty:
            return render_template(
                "dashboard.html",
                error="No sales data available."
            )

        total_revenue = df["TOTAL_REVENUE"].sum()
        total_profit = df["TOTAL_PROFIT"].sum()
        total_units = df["TOTAL_UNITS_SOLD"].sum()
        total_orders = df["ORDER_COUNT"].sum()

        # Convert DataFrame to JSON-compatible records
        data = df.to_dict(orient="records")

        return render_template(
            "dashboard.html",

            total_revenue=total_revenue,
            total_profit=total_profit,
            total_units=total_units,
            total_orders=total_orders,

            data=data
        )

    except Exception as e:

        return render_template(
            "dashboard.html",
            error=str(e)
        )
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
