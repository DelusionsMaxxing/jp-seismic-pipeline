from __future__ import annotations

import os
import tempfile
from pathlib import Path

# Airflow reads these at import time, before any fixture can run. Left alone it
# writes a sqlite database, a config file and a logs directory into the
# developer's home directory the moment a test imports it.
os.environ.setdefault(
    "AIRFLOW_HOME", str(Path(tempfile.gettempdir()) / "jp_seismic_airflow_home")
)
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
