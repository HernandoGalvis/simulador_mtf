# run_simulacion.py
# Ejecución de simulación usando rango manual UTC dentro del propio archivo
# v1.5.0
#
# Cambios v1.5.0:
# - Procesa TODOS los inversionistas activos (inversionistas.activo = true) en un bucle.
# - Reutiliza conexiones persistentes existentes (SignalProviderDB, PriceProviderDB, PersistenceAdapter).
# - No se crean conexiones efímeras adicionales para leer inversionistas; se usa la conexión de SignalProviderDB.
# - Mantiene la lógica de simulación intacta por inversionista; NO cierra operaciones abiertas al finalizar cada inversionista.
# - Actualiza capital_actual y PYG no realizado para abiertas al finalizar cada inversionista (mediante sim.finalizar()).
# - Consolida eventos de todos los inversionistas para un resumen global al final.
#
# Nota: No se cambiaron otros aspectos fuera del correctivo solicitado.

FECHA_INICIO_UTC = "2025-01-01T00:05:00Z"
FECHA_FIN_UTC    = "2025-02-01T23:59:00Z"

from datetime import datetime, timezone
import time
from collections import Counter
from typing import List, Dict, Any

from simulator.models import Investor, RiskConfig
from simulator.strategy_cache import StrategyCache
from simulator.persistence import PersistenceAdapter
from simulator.logger import EventLogger
from simulator.logger_persist_callback import build_persist_callback
from simulator.simulator_core import SimulatorCore
from simulator.data_access import SignalProviderDB, PriceProviderDB, StrategyLoader
from parmspg import build_dsn

def parse_datetime_utc(s: str) -> datetime:
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.replace(second=0, microsecond=0)

def compute_ts_fin(base: datetime, end: datetime) -> int:
    delta_min = int((end - base).total_seconds() // 60)
    if delta_min < 0:
        raise ValueError("FECHA_FIN_UTC es anterior a FECHA_INICIO_UTC")
    return delta_min

def load_active_investors(conn) -> List[Dict[str, Any]]:
    """
    Lee todos los inversionistas activos desde la BD usando la conexión persistente provista.
    Retorna una lista de diccionarios con los campos necesarios para Investor y RiskConfig.
    """
    sql = """
    SELECT id_inversionista,
           capital_aportado,
           capital_actual,
           usar_parametros_senal,
           apalancamiento_inversionista,
           apalancamiento_max,
           drawdown_max_pct,
           riesgo_max_operacion_pct,
           tamano_min_operacion,
           tamano_max_operacion
      FROM inversionistas
     WHERE activo = true
     ORDER BY id_inversionista
    """
    rows: List[Dict[str, Any]] = []
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            for r in cur.fetchall():
                rows.append({
                    "id_inversionista": r[0],
                    "capital_aportado": float(r[1]),
                    "capital_actual": float(r[2]),
                    "usar_parametros_senal": bool(r[3]),
                    "apalancamiento_inversionista": r[4],
                    "apalancamiento_max": r[5],
                    "drawdown_max_pct": float(r[6] or 0.0),
                    "riesgo_max_operacion_pct": float(r[7]),
                    "tamano_min_operacion": float(r[8]),
                    "tamano_max_operacion": float(r[9]),
                })
    except Exception:
        # Si ocurre error en una transacción, limpiar estado para siguientes consultas
        try:
            conn.rollback()
        except:
            pass
        raise
    return rows

def main():
    base_datetime = parse_datetime_utc(FECHA_INICIO_UTC)
    end_datetime = parse_datetime_utc(FECHA_FIN_UTC)
    ts_inicio = 0
    ts_fin = compute_ts_fin(base_datetime, end_datetime)

    # Conexiones persistentes (se mantienen durante todo el proceso)
    strategy_cache = StrategyCache()
    signal_provider = SignalProviderDB()
    signal_provider.strategy_loader = StrategyLoader()
    price_provider = PriceProviderDB()

    dsn = build_dsn()
    persistence = PersistenceAdapter(dsn=dsn, base_datetime=base_datetime)

    # Cargar inversionistas activos usando la conexión YA abierta del proveedor de señales
    active_investors = load_active_investors(signal_provider.conn)
    if not active_investors:
        print("[INFO] No hay inversionistas activos para simular.")
        # Cierre ordenado de recursos
        try:
            persistence.close()
        finally:
            try: signal_provider.close()
            except: pass
            try: price_provider.close()
            except: pass
        return

    print(f"[INICIO] Simulación UTC: {base_datetime} -> {end_datetime} (minutos inclusivos: {ts_inicio}-{ts_fin})")
    print(f"[INFO] Inversionistas activos: {[inv['id_inversionista'] for inv in active_investors]}")

    eventos_globales = []
    t0_total = time.time()

    # Bucle por inversionista activo
    for inv_row in active_investors:
        investor = Investor(
            id_inversionista=inv_row["id_inversionista"],
            capital_inicial=inv_row["capital_aportado"],
            capital_actual=inv_row["capital_actual"],
            usar_parametros_senal=inv_row["usar_parametros_senal"],
            apalancamiento_inversionista=inv_row["apalancamiento_inversionista"],
            apalancamiento_max=inv_row["apalancamiento_max"],
            drawdown_max_pct=inv_row["drawdown_max_pct"]
        )
        risk = RiskConfig(
            riesgo_max_pct=inv_row["riesgo_max_operacion_pct"],
            tamano_min=inv_row["tamano_min_operacion"],
            tamano_max=inv_row["tamano_max_operacion"]
        )

        # Logger y persistencia por inversionista (callback parametrizado)
        logger = EventLogger(persist_callback=build_persist_callback(persistence, investor))

        sim = SimulatorCore(
            investor=investor,
            risk=risk,
            strategy_cache=strategy_cache,
            signal_provider=signal_provider,
            price_provider=price_provider,
            logger=logger,
            persistence=persistence,
            base_datetime=base_datetime
        )

        print(f"[INV] Iniciando simulación para inversionista {investor.id_inversionista}")
        t0 = time.time()
        sim.run(ts_inicio=ts_inicio, ts_fin=ts_fin)
        sim.finalizar(precios_close_final={})
        elapsed = time.time() - t0
        eventos_globales.extend(logger.eventos)
        print(f"[INV] Finalizó inversionista {investor.id_inversionista} | Duración: {elapsed:.2f}s | Eventos: {len(logger.eventos)}")

    elapsed_total = time.time() - t0_total

    # Cierre de recursos globales
    try:
        persistence.close()
    finally:
        try: signal_provider.close()
        except: pass
        try: price_provider.close()
        except: pass

    # Resumen global de eventos
    counts = Counter(e["tipo"] for e in eventos_globales)
    print("--------------------------------------------------")
    print("Resumen eventos (global):")
    for k, v in sorted(counts.items()):
        print(f"  {k}: {v}")
    print("--------------------------------------------------")
    print(f"Duración total: {elapsed_total:.2f}s | Minutos simulados: {ts_fin - ts_inicio + 1} | Eventos totales: {len(eventos_globales)}")
    # Observabilidad: contadores de queries en proveedores (acumulados)
    print(f"Consultas señales: {getattr(signal_provider, 'query_count', 'N/A')} | Consultas precios: {getattr(price_provider, 'query_count', 'N/A')}")

if __name__ == "__main__":
    main()