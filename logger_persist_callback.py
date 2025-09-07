# simulator/logger_persist_callback.py
# Callback que traduce eventos internos a inserciones en log_operaciones_simuladas
# v1.1.0
#
# Cambios v1.1.0:
# - Se agrega el traspaso de id_vela_1m_apertura (si existe en el evento).
# - Asegura el envío de precio_senal (close de la vela usada en apertura o el precio relevante del evento).
# - Mantiene compatibilidad con clave 'detalle' (dict) que se serializa como JSON en persistence.
#
# Notas:
# - El timestamp del evento se toma en el momento de persistir (ahora) salvo que el evento ya traiga ts_evento.
# - Si en el futuro deseas reutilizar ts del simulador, pasa 'ts_evento' en evt y aquí se respeta.
#
# Campos esperados en evt (opcionales según tipo):
#   tipo, id_op, id_senal_fk, ticker, detalle, capital_antes, capital_despues,
#   motivo_no_operacion, motivo_cierre, resultado, precio_cierre,
#   precio_max_alcanzado, precio_min_alcanzado, id_op_padre,
#   id_estrategia_fk, cantidad, sl, tp, precio_senal, id_vela_1m_apertura

from datetime import datetime, timezone
from simulator.persistence import PersistenceAdapter
from simulator.models import Investor

def build_persist_callback(persistence: PersistenceAdapter, investor: Investor):
    def callback(evt: dict):
        detalle_dict = evt.get("detalle", {})
        persistence.insert_log_evento({
            "ts_evento": evt.get("ts_evento") or datetime.now(timezone.utc),
            "id_op": evt.get("id_op"),
            "id_senal_fk": evt.get("id_senal_fk"),
            "ticker": evt.get("ticker"),
            "tipo": evt.get("tipo"),
            "detalle_json": detalle_dict,
            "capital_antes": evt.get("capital_antes"),
            "capital_despues": evt.get("capital_despues"),
            "motivo_no_operacion": evt.get("motivo_no_operacion"),
            "motivo_cierre": evt.get("motivo_cierre"),
            "resultado": evt.get("resultado"),
            "precio_cierre": evt.get("precio_cierre"),
            "precio_max_alcanzado": evt.get("precio_max_alcanzado"),
            "precio_min_alcanzado": evt.get("precio_min_alcanzado"),
            "id_op_padre": evt.get("id_op_padre"),
            "id_estrategia_fk": evt.get("id_estrategia_fk"),
            "cantidad": evt.get("cantidad"),
            "sl": evt.get("sl"),
            "tp": evt.get("tp"),
            "precio_senal": evt.get("precio_senal"),
            "id_vela_1m_apertura": evt.get("id_vela_1m_apertura")
        }, investor)
    return callback
