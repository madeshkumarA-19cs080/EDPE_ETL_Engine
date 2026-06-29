from typing import Optional
from urllib.parse import quote_plus

from .connector import DatabaseConnector


class PostgresConnector(DatabaseConnector):
    def sqlalchemy_url(self) -> str:
        username = quote_plus(self.username)
        password = quote_plus(self.password)
        return f"postgresql+psycopg2://{username}:{password}@{self.host}:{self.port}/{self.database}"

    def connect_args(self):
        return {"connect_timeout": 10}

    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    def jdbc_driver(self) -> str:
        return "org.postgresql.Driver"

    def dialect(self) -> str:
        return "postgresql"

    def driver_jar(self) -> Optional[str]:
        # Local jar lookup removed; Spark will load driver via packages.
        return None
