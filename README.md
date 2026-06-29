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

