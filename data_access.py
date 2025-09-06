# simulator/data_access.py
# Acceso a datos: señales y precios desde tablas reales (casteo a float de numerics)
# v1.3.0
#
# Cambios v1.3.0:
# - Persistencia de conexión a PostgreSQL en SignalProviderDB y PriceProviderDB (una conexión por proveedor).
# - Eliminado psycopg2.connect en cada método; ahora se reutiliza self.conn y cursor por llamada.
# - Manejo de rollback en caso de error en SELECT para limpiar el estado de la transacción.
# - Contadores de queries: query_count en cada proveedor (observabilidad).
# - Método close() en cada proveedor para cerrar la conexión al finalizar la simulación.
#
# Notas:
# - Esto no reduce el número de queries, pero elimina el overhead de abrir/cerrar conexión por cada llamada.
# - Los siguientes pasos (2–4) atacarán la reducción de lecturas duplicadas y ruidos (cache intra-minuto, etc.).

import psycopg2
from datetime import datetime
from typing import List, Optional
from simulator.models import StrategyParams
from parmspg import build_dsn

# Nombre de la columna id de la tabla de velas (ajusta si tu DDL usa otro nombre)
OHLCV_ID_COL = "id"

def _to_float(x):
    if x is None:
        return 0.0
    return float(x)

class SignalRecord:
    """
    Representa una señal cruda proveniente de senales_generadas.
    Los campos target_profit_price, stop_loss_price, precio_senal se castean a float.
    """
    def __init__(self, row):
        (self.id_senal,
         self.id_estrategia_fk,
         self.ticker_fk,
         self.timestamp_senal,
         self.tipo_senal,
         target_profit_price,
         stop_loss_price,
         apalancamiento_calculado,
         precio_senal) = row
        self.target_profit_price = _to_float(target_profit_price)
        self.stop_loss_price = _to_float(stop_loss_price)
        self.apalancamiento_calculado = int(apalancamiento_calculado) if apalancamiento_calculado is not None else 1
        self.precio_senal = _to_float(precio_senal)

class PriceRecord:
    """
    Vela 1m para un (ticker, timestamp).
    Incluye id_vela para registrar en operaciones.
    """
    def __init__(self, row):
        (self.id_vela,
         self.ticker,
         self.timestamp,
         o,
         h,
         l,
         c) = row
        self.open = _to_float(o)
        self.high = _to_float(h)
        self.low = _to_float(l)
        self.close = _to_float(c)

class StrategyLoader:
    """
    Carga parámetros de estrategia desde tabla estrategias.
    """
    def __init__(self):
        self.dsn = build_dsn()

    def load_strategy_params(self, id_estrategia: int) -> StrategyParams:
        sql = """
        SELECT avance_minimo_pct,
               porc_limite_retro,
               porc_retroceso_liquidacion_sl,
               porc_liquidacion_parcial_sl,
               porc_limite_retro_entrada
          FROM estrategias
         WHERE id_estrategia = %s
        """
        with psycopg2.connect(self.dsn) as conn, conn.cursor() as cur:
            cur.execute(sql, (id_estrategia,))
            r = cur.fetchone()
            if not r:
                raise ValueError(f"Estrategia no encontrada: {id_estrategia}")
            return StrategyParams(
                avance_minimo_pct=float(r[0] or 0),
                porc_limite_retro=float(r[1] or 0),
                porc_retroceso_liquidacion_sl=float(r[2] or 0),
                porc_liquidacion_parcial_sl=float(r[3] or 0),
                porc_limite_retro_entrada=float(r[4] or 0)
            )

class SignalProviderDB:
    """
    Proveedor de señales minuto a minuto desde la BD (conexión persistente).
    """
    def __init__(self):
        self.dsn = build_dsn()
        self.conn = psycopg2.connect(self.dsn)
        # Autocommit False: SELECT no requiere commit; en caso de error hacemos rollback.
        self.conn.autocommit = False
        self.query_count = 0
        self.strategy_loader: StrategyLoader | None = None  # asignada externamente

    def _ensure_conn(self):
        if self.conn is None or getattr(self.conn, "closed", 0):
            self.conn = psycopg2.connect(self.dsn)
            self.conn.autocommit = False

    def get_signals_by_minute(self, dt: datetime) -> List[SignalRecord]:
        self._ensure_conn()
        sql = """
        SELECT id_senal,
               id_estrategia_fk,
               ticker_fk,
               timestamp_senal,
               tipo_senal,
               target_profit_price,
               stop_loss_price,
               apalancamiento_calculado,
               precio_senal
          FROM senales_generadas
         WHERE timestamp_senal = %s
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql, (dt.replace(tzinfo=None),))
                self.query_count += 1
                return [SignalRecord(r) for r in cur.fetchall()]
        except Exception:
            # Limpia estado de transacción abortada para siguientes consultas
            try:
                self.conn.rollback()
            except:
                pass
            raise

    def close(self):
        try:
            self.conn.close()
        except:
            pass

class PriceProviderDB:
    """
    Proveedor de precios (velas 1m) con conexión persistente.
    """
    def __init__(self):
        self.dsn = build_dsn()
        self.conn = psycopg2.connect(self.dsn)
        self.conn.autocommit = False
        self.query_count = 0

    def _ensure_conn(self):
        if self.conn is None or getattr(self.conn, "closed", 0):
            self.conn = psycopg2.connect(self.dsn)
            self.conn.autocommit = False

    def get_price(self, ticker: str, dt: datetime) -> Optional[PriceRecord]:
        self._ensure_conn()
        sql = f"""
        SELECT {OHLCV_ID_COL}, ticker, "timestamp", "open", high, low, "close"
          FROM ohlcv_raw_1m
         WHERE ticker = %s AND "timestamp" = %s
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql, (ticker, dt.replace(tzinfo=None)))
                self.query_count += 1
                r = cur.fetchone()
                if not r:
                    return None
                return PriceRecord(r)
        except Exception:
            try:
                self.conn.rollback()
            except:
                pass
            raise

    def close(self):
        try:
            self.conn.close()
        except:
            pass