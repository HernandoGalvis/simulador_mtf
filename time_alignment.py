# simulator/time_alignment.py
# Utilidades para alinear base_datetime y rango de simulación a partir de datos reales
# v1.0.0

import psycopg2
from datetime import datetime
from parmspg import build_dsn

def get_signal_time_bounds() -> tuple[datetime, datetime]:
    sql = "SELECT MIN(timestamp_senal), MAX(timestamp_senal) FROM senales_generadas;"
    with psycopg2.connect(build_dsn()) as conn, conn.cursor() as cur:
        cur.execute(sql)
        mn, mx = cur.fetchone()
        if not mn or not mx:
            raise ValueError("No hay señales en senales_generadas.")
        return mn, mx

def truncate_to_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)

def compute_minute_span(start: datetime, end: datetime) -> int:
    delta = end - start
    return int(delta.total_seconds() // 60)

def prepare_time_alignment():
    """
    Retorna (base_datetime, ts_inicio, ts_fin)
    base_datetime: datetime truncado al minuto de la primera señal
    ts_inicio: 0
    ts_fin: minutos entre base y última señal
    """
    mn, mx = get_signal_time_bounds()
    base = truncate_to_minute(mn)
    ts_fin = compute_minute_span(base, truncate_to_minute(mx))
    return base, 0, ts_fin