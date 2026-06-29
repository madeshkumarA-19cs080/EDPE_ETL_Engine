from typing import Optional
from urllib.parse import quote_plus

from .connector import DatabaseConnector


class SQLServerConnector(DatabaseConnector):
    def sqlalchemy_url(self) -> str:
        driver = quote_plus("ODBC Driver 18 for SQL Server")
        username = quote_plus(self.username)
        password = quote_plus(self.password)
        return (
            f"mssql+pyodbc://{username}:{password}@{self.host}:{self.port}/{self.database}"
            f"?driver={driver}&TrustServerCertificate=yes"
        )

    def jdbc_url(self) -> str:
        return (
            f"jdbc:sqlserver://{self.host}:{self.port};"
            f"databaseName={self.database};encrypt=false;trustServerCertificate=true"
        )

    def jdbc_driver(self) -> str:
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver"

    def dialect(self) -> str:
        return "sqlserver"

    def driver_jar(self) -> Optional[str]:
        # Local jar lookup removed; Spark will load driver via packages if required.
        return None
