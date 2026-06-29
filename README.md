# Enterprise Data Processing Engine (EDPE)

## Overview

EDPE is a local enterprise-grade ETL proof-of-concept built with Python, Flask, and PySpark. It connects to SQL Server or PostgreSQL, reads large datasets using Spark, validates and transforms data, and exports optimized analytics-ready output.

## Features

- SQL Server and PostgreSQL connectors
- PySpark-based extraction, validation, transformation, and writing
- Support for CSV, Parquet, and Avro exports
- Error and execution reporting
- Live logs page with auto-refresh
- Modular design for future connector expansion

## Requirements

- Python 3.11+
- Java 11+ (required by PySpark)
- Local SQL Server or PostgreSQL instance
- ODBC driver for SQL Server
- JDBC driver jars for Spark:
  - PostgreSQL: place `postgresql.jar` in `drivers/`
  - SQL Server: place `mssql-jdbc.jar` in `drivers/`

## Setup

1. Create a Python virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2. Install dependencies:

```powershell
pip install -r requirements.txt
```

3. Start the application:

```powershell
python app.py
```

4. Open the browser:

```text
http://localhost:5000
```

## Application Workflow

1. Enter database credentials and target table.
2. Test the connection.
3. Run the ETL job.
4. Review the result summary and download reports.
5. Monitor the log stream on the Logs page.

## Project Structure

- `app.py` - Flask web application and ETL orchestration
- `config.py` - Application configuration and path constants
- `etl/` - Extraction, validation, transformation, writing, and logging
- `database/` - Database connector abstraction and implementations
- `templates/` - HTML pages
- `static/` - CSS and JavaScript assets
- `logs/` - Persistent runtime logs
- `output/` - Generated data exports
- `reports/` - Validation and execution reports

## Notes

- No Docker, AWS, Kubernetes, or authentication is included.
- The application is designed to be extended with new connector types in the future.
