import os
from pathlib import Path
from typing import Dict, Optional

from flask import Flask, Response, jsonify, redirect, render_template, request, send_from_directory, url_for

from config import LOG_FILE, OUTPUT_DIR, REPORT_DIR, SUPPORTED_DATABASES, OUTPUT_FORMATS
from database.postgres import PostgresConnector
from database.sqlserver import SQLServerConnector
from etl.extract import Extractor
from etl.transform import Transformer
from etl.validate import ValidationSummary, Validator
from etl.writer import DataWriter
from etl.logger import logger as app_logger

JDBC_DRIVER_WARNING = (
    "Place JDBC driver jars in the drivers/ directory before running ETL. "
    "PostgreSQL jar name should match postgresql*.jar and SQL Server jar name should match mssql-jdbc*.jar."
)

DRIVER_MISSING_HINT = (
    "If ETL fails with ClassNotFoundException, verify the driver jar file exists and uses the correct JDBC driver class."
)

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "edpe-local-secret")

REPORT_DIRECTORY = Path(REPORT_DIR)
OUTPUT_DIRECTORY = Path(OUTPUT_DIR)


def get_connector(form: Dict[str, str]):
    db_type = form.get("database_type")
    if db_type == "sqlserver":
        return SQLServerConnector(
            host=form.get("host", "localhost"),
            port=form.get("port", 1433),
            database=form.get("database", ""),
            username=form.get("username", ""),
            password=form.get("password", ""),
        )
    if db_type == "postgres":
        return PostgresConnector(
            host=form.get("host", "localhost"),
            port=form.get("port", 5432),
            database=form.get("database", ""),
            username=form.get("username", ""),
            password=form.get("password", ""),
        )
    raise ValueError("Unsupported database type")


@app.route("/", methods=["GET"])
def connection_page():
    return render_template(
        "connection.html",
        database_types=SUPPORTED_DATABASES,
        output_formats=OUTPUT_FORMATS,
        driver_warning=JDBC_DRIVER_WARNING,
    )


@app.route("/test_connection", methods=["POST"])
def test_connection():
    form = request.get_json(force=True)
    try:
        connector = get_connector(form)
        connector.test_connection()
        return jsonify(success=True, message="Connection successful")
    except Exception as exc:
        app_logger.error("Connection test failed: {}", exc)
        return jsonify(success=False, message=str(exc)), 400


@app.route("/run_etl", methods=["POST"])
def run_etl():
    form = request.form
    table_name = form.get("table_name", "")
    output_format = form.get("output_format", "csv")
    partition_by = form.get("partition_by") or None
    job_title = f"edpe_{table_name}"

    try:
        connector = get_connector(form)
        connector.test_connection()

        extractor = Extractor(connector, table_name)
        dataframe = extractor.read_table()

        validator = Validator()
        valid_df, invalid_df, summary = validator.validate(dataframe)

        transformer = Transformer()
        transformed_df = transformer.transform(valid_df)

        writer = DataWriter()
        output_path = writer.write_data(transformed_df, output_format, job_title, partition_by)
        error_report = writer.write_error_report(invalid_df, job_title)
        execution_report = writer.write_execution_report(
            {
                "Connection Status": "Success",
                "Table": table_name,
                "Total Records Read": summary.total_records,
                "Valid Records": summary.valid_records,
                "Invalid Records": summary.invalid_records,
                "Duplicates Removed": summary.duplicates_removed,
                "Output Format": output_format.upper(),
                "Output Folder": str(output_path),
            },
            job_title,
        )

        return render_template(
            "result.html",
            connection_status="Success",
            total_records=summary.total_records,
            valid_records=summary.valid_records,
            invalid_records=summary.invalid_records,
            duplicates_removed=summary.duplicates_removed,
            execution_time="N/A",
            memory_used="N/A",
            output_format=output_format.upper(),
            output_folder=str(output_path),
            error_report=error_report.name,
            execution_report=execution_report.name,
            job_status="Completed",
            warning_message=None,
        )
    except Exception as exc:
        app_logger.exception("ETL job failed")
        friendly_warning = None
        message = str(exc)
        if isinstance(exc, FileNotFoundError) or "ClassNotFoundException" in message:
            friendly_warning = JDBC_DRIVER_WARNING + " " + DRIVER_MISSING_HINT
        return render_template(
            "result.html",
            connection_status="Failed",
            total_records=0,
            valid_records=0,
            invalid_records=0,
            duplicates_removed=0,
            execution_time="N/A",
            memory_used="N/A",
            output_format=output_format.upper(),
            output_folder="",
            error_report="",
            execution_report="",
            job_status=f"Failed: {exc}",
            warning_message=friendly_warning,
        )


@app.route("/download_report")
def download_report():
    filename = request.args.get("name")
    if not filename or ".." in filename or filename.startswith("/"):
        return "Invalid file name", 400
    return send_from_directory(REPORT_DIRECTORY, filename, as_attachment=True)


@app.route("/logs", methods=["GET"])
def logs_page():
    return render_template("logs.html")


@app.route("/logs_data", methods=["GET"])
def logs_data():
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as log_file:
            content = log_file.read()
        return Response(content, mimetype="text/plain")
    except FileNotFoundError:
        return Response("No logs available yet.", mimetype="text/plain")


@app.route("/favicon.ico")
def favicon():
    return redirect(url_for("static", filename="favicon.ico"))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
