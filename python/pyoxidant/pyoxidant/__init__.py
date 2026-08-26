"""pyoxidant — start a Oxidant Spark Connect server from Python.

Usage::

    from pyoxidant import SparkConnectServer
    server = SparkConnectServer(port=50051)
    server.start()              # spawns the `oxidant` binary

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()
    spark.sql("SELECT 1").show()

This package only manages the server process; query execution happens in the Rust engine.
Use the stock ``pyspark-client`` as the client (``pip install 'pyoxidant[client]'``).
"""
from __future__ import annotations

import shutil
import subprocess
import sys

__all__ = ["SparkConnectServer", "__version__"]
__version__ = "0.0.0"


class SparkConnectServer:
    """Thin wrapper around the ``oxidant spark server`` process."""

    def __init__(self, port: int = 50051) -> None:
        self.port = port
        self._proc: subprocess.Popen | None = None

    def start(self) -> "SparkConnectServer":
        binary = shutil.which("oxidant")
        if binary is None:
            raise RuntimeError(
                "the `oxidant` binary was not found on PATH; build it with "
                "`cargo build --release --bin oxidant`"
            )
        self._proc = subprocess.Popen(
            # --foreground: this class is the supervisor — it holds the Popen handle and
            # terminates it in `stop()`. The daemon path (`oxidant start`) would fork away
            # and hand back a pid that exits at once.
            [binary, "spark", "server", "--port", str(self.port), "--foreground"]
        )
        return self

    def stop(self) -> None:
        if self._proc is not None:
            self._proc.terminate()
            self._proc = None


def _main() -> int:
    SparkConnectServer().start()
    print("oxidant server started; connect via sc://localhost:50051", file=sys.stderr)
    return 0
