from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote_plus

from loguru import logger
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

from config import TEST_QUERY


class DatabaseConnector(ABC):
    def __init__(self, host: str, port: int, database: str, username: str, password: str):
        self.host = host
        self.port = int(port)
        self.database = database
        self.username = username
        self.password = password

    @abstractmethod
    def sqlalchemy_url(self) -> str:
        pass

    @abstractmethod
    def jdbc_url(self) -> str:
        pass

    @abstractmethod
    def jdbc_driver(self) -> str:
        pass

    @abstractmethod
    def dialect(self) -> str:
        pass

    def connect_args(self) -> Dict[str, Any]:
        return {}

    def create_engine(self) -> Engine:
        url = self.sqlalchemy_url()
        engine = create_engine(url, connect_args=self.connect_args(), pool_pre_ping=True)
        return engine

    def test_connection(self) -> bool:
        logger.info("Testing connection to {}", self.dialect())
        engine = self.create_engine()
        with engine.connect() as connection:
            connection.execute(text(TEST_QUERY))
        logger.info("Connection test succeeded for {}", self.dialect())
        return True

    def jdbc_options(self, table_name: str) -> Dict[str, Any]:
        # Return only the minimal set of JDBC options required by Spark JDBC
        return {
            "url": self.jdbc_url(),
            "dbtable": table_name,
            "user": self.username,
            "password": self.password,
            "driver": self.jdbc_driver(),
        }

    def get_partition_info(self, table_name: str) -> Optional[Tuple[str, int, int]]:
        try:
            engine = self.create_engine()
            inspector = inspect(engine)
            columns = inspector.get_columns(table_name)
            numeric_columns = [c for c in columns if hasattr(c["type"], "python_type") and c["type"].python_type in (int,)]
            if not numeric_columns:
                return None
            candidate = next((c for c in numeric_columns if c["name"].lower() in ("id", "row_id", "pk", "identity")), numeric_columns[0])
            partition_column = candidate["name"]
            with engine.connect() as connection:
                result = connection.execute(text(f"SELECT MIN({partition_column}), MAX({partition_column}) FROM {table_name}"))
                row = result.fetchone()
                if row is None:
                    return None
                min_value, max_value = row[0], row[1]
            if min_value is None or max_value is None:
                return None
            return partition_column, int(min_value), int(max_value)
        except Exception:
            logger.debug("Unable to detect partition information for table {}", table_name, exc_info=True)
            return None

    # No local jar discovery required; Spark will fetch JDBC drivers via
    # `spark.jars.packages` when the session is created.
    def driver_jar(self) -> Optional[str]:
        return None

    @staticmethod
    def escape_value(value: str) -> str:
        return quote_plus(value)
