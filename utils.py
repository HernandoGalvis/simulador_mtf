# simulator/utils.py
# Utilidad simple para logging de debug a archivo

import os
from datetime import datetime

LOG_FILE = os.environ.get("SIMULADOR_LOG_FILE", "simulador_debug.log")

def log_debug(msg: str):
    """Escribe un mensaje de debug a un archivo de log con timestamp."""
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"[{ts}] {msg}\n")
