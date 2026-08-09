"""Runtime configuration, sourced from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass

# Bounding box covering the Japanese archipelago together with the offshore
# subduction zones that generate most of its seismicity: from the
# Nansei-shoto islands in the south-west up to the Kuril trench off Hokkaido.
JAPAN_BBOX = {
    "minlatitude": 24.0,
    "maxlatitude": 46.5,
    "minlongitude": 122.0,
    "maxlongitude": 154.0,
}

USGS_ENDPOINT = "https://earthquake.usgs.gov/fdsnws/event/1/query"

# USGS truncates a single response at 20 000 features. Japan sees far fewer
# events than that per day, but paging keeps wide backfill windows safe.
PAGE_LIMIT = 20_000

REQUEST_TIMEOUT_SECONDS = 30


@dataclass(frozen=True)
class DatabaseConfig:
    """Connection details for the warehouse Postgres instance."""

    host: str
    port: int
    user: str
    password: str
    database: str

    @property
    def dsn(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    @classmethod
    def from_env(cls) -> DatabaseConfig:
        return cls(
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=int(os.environ.get("POSTGRES_PORT", "5432")),
            user=os.environ.get("POSTGRES_USER", "seismic"),
            password=os.environ.get("POSTGRES_PASSWORD", "seismic"),
            database=os.environ.get("POSTGRES_DB", "seismic"),
        )
