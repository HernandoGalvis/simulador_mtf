# simulator/utils_time.py
# Utilidades de timeline y conversión (minutos -> timestamps base)
# v1.0.0

from datetime import datetime, timedelta, timezone

def generar_timeline(ts_inicio: int, ts_fin: int):
    return range(ts_inicio, ts_fin + 1)

def minute_to_datetime(base: datetime, minute_offset: int) -> datetime:
    return (base + timedelta(minutes=minute_offset)).replace(tzinfo=timezone.utc)