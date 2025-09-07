# simulator/persistence.py
# Adaptador de persistencia a PostgreSQL
# v1.4.0
#
# Cambios v1.4.0:
# - INSERT de operaciones ahora persiste mult_sl_asignado y mult_tp_asignado (desde Operation).
#   * En apertura normal y en aperturas hijas por cierre parcial (porque insert_operacion es común).
#
# Cambios v1.3.0:
# - porc_sl y porc_tp: cálculo y persistencia en INSERT y en UPDATE de exposición (DCA).

import psycopg2
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, Callable, Tuple
from simulator.models import Operation, Investor
import psycopg2.extras

def default_ts_to_datetime(base_dt: datetime, ts_minute: int) -> datetime:
    return (base_dt + timedelta(minutes=ts_minute)).replace(tzinfo=timezone.utc)

class PersistenceAdapter:
    def __init__(self, dsn: str, base_datetime: datetime,
                 ts_to_datetime_fn: Optional[Callable[[int], datetime]] = None,
                 error_callback: Optional[Callable[[Exception,str], None]] = None):
        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = False
        self.base_datetime = base_datetime.replace(tzinfo=timezone.utc)
        self.ts_to_datetime_fn = ts_to_datetime_fn or (lambda ts: default_ts_to_datetime(self.base_datetime, ts))
        self.error_callback = error_callback

    def close(self):
        try:
            self.conn.close()
        except:
            pass

    def _dt(self, ts: Optional[int]) -> Optional[datetime]:
        if ts is None:
            return None
        return self.ts_to_datetime_fn(ts)

    def _exec(self, sql: str, params: dict, fetch: bool=False):
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall() if fetch else None
            self.conn.commit()
            return rows
        except Exception as e:
            self.conn.rollback()
            if self.error_callback:
                self.error_callback(e, sql)
            raise

    # -------------------
    # Helpers de cálculo
    # -------------------
    def _calc_porcentajes(self, op: Operation) -> Tuple[float, float]:
        pe = float(op.precio_entrada or 0.0)
        sl = float(op.stop_loss or 0.0)
        tp = float(op.take_profit or 0.0)
        if pe <= 0:
            return (0.0, 0.0)
        if (op.tipo == "LONG"):
            porc_sl = round(((pe - sl) / pe) * 100.0, 2) if sl > 0 else 0.0
            porc_tp = round(((tp - pe) / pe) * 100.0, 2) if tp > 0 else 0.0
        else:
            porc_sl = round(((sl - pe) / pe) * 100.0, 2) if sl > 0 else 0.0
            porc_tp = round(((pe - tp) / pe) * 100.0, 2) if tp > 0 else 0.0
        return (porc_sl, porc_tp)

    # -------------
    # Operaciones
    # -------------
    def insert_operacion(self, op: Operation, inv_capital_total: float, inv_capital_disp: float) -> int:
        porc_sl, porc_tp = self._calc_porcentajes(op)
        sql = """
        INSERT INTO operaciones_simuladas (
            id_inversionista_fk,
            id_estrategia_fk,
            id_senal_fk,
            ticker_fk,
            timestamp_apertura,
            precio_entrada,
            cantidad,
            apalancamiento,
            tipo_operacion,
            capital_riesgo_usado,
            capital_bloqueado,
            stop_loss_price,
            take_profit_price,
            estado,
            valor_total_exposicion,
            capital_total_inversionista,
            capital_disponible_inversionista,
            id_operacion_padre,
            precio_max_alcanzado,
            precio_min_alcanzado,
            id_vela_1m_apertura,
            cnt_operaciones,
            porc_sl,
            porc_tp,
            mult_sl_asignado,
            mult_tp_asignado
        ) VALUES (
            %(id_inversionista_fk)s,
            %(id_estrategia_fk)s,
            %(id_senal_fk)s,
            %(ticker)s,
            %(ts_apertura)s,
            %(precio_entrada)s,
            %(cantidad)s,
            %(apalancamiento)s,
            %(tipo)s,
            %(capital_invertido)s,
            %(capital_bloqueado)s,
            %(stop_loss)s,
            %(take_profit)s,
            %(estado)s,
            %(valor_total_exposicion)s,
            %(capital_total)s,
            %(capital_disponible)s,
            %(id_operacion_padre)s,
            %(precio_max)s,
            %(precio_min)s,
            %(id_vela_1m_apertura)s,
            %(cnt_operaciones)s,
            %(porc_sl)s,
            %(porc_tp)s,
            %(mult_sl_asignado)s,
            %(mult_tp_asignado)s
        )
        RETURNING id_operacion;
        """
        data = dict(
            id_inversionista_fk=op.id_inversionista_fk,
            id_estrategia_fk=op.id_estrategia_fk,
            id_senal_fk=op.id_senal_fk,
            ticker=op.ticker,
            ts_apertura=self._dt(op.timestamp_apertura),
            precio_entrada=op.precio_entrada,
            cantidad=op.cantidad,
            apalancamiento=op.apalancamiento,
            tipo=op.tipo,
            capital_invertido=op.capital_invertido,
            capital_bloqueado=op.capital_bloqueado,
            stop_loss=op.stop_loss,
            take_profit=op.take_profit,
            estado=op.estado,
            valor_total_exposicion=op.valor_total_exposicion,
            capital_total=inv_capital_total,
            capital_disponible=inv_capital_disp,
            id_operacion_padre=op.id_operacion_padre,
            precio_max=op.precio_max if op.precio_max != float("-inf") else op.precio_entrada,
            precio_min=op.precio_min if op.precio_min != float("inf") else op.precio_entrada,
            id_vela_1m_apertura=op.id_vela_1m_apertura,
            cnt_operaciones=1 if getattr(op, "id_operacion_padre", None) is None else getattr(op, "cnt_operaciones", 1),
            porc_sl=porc_sl,
            porc_tp=porc_tp,
            mult_sl_asignado=getattr(op, "mult_sl_asignado", None),
            mult_tp_asignado=getattr(op, "mult_tp_asignado", None)
        )
        rows = self._exec(sql, data, fetch=True)
        return rows[0][0]

    def update_operacion_cierre_total(self, op: Operation, motivo: str, id_vela_1m_cierre: Optional[int]):
        sql = """
        UPDATE operaciones_simuladas
           SET estado='cerrada_total',
               timestamp_cierre=%(ts_cierre)s,
               precio_cierre=%(precio_cierre)s,
               resultado=%(resultado)s,
               motivo_cierre=%(motivo)s,
               valor_total_exposicion=0,
               precio_max_alcanzado=%(pmax)s,
               precio_min_alcanzado=%(pmin)s,
               duracion_operacion = EXTRACT(EPOCH FROM (%(ts_cierre)s - timestamp_apertura)) / 60.0,
               id_vela_1m_cierre = %(id_vela_1m_cierre)s
         WHERE id_operacion=%(id)s;
        """
        params = dict(
            ts_cierre=self._dt(op.timestamp_cierre),
            precio_cierre=op.ultimo_precio_exec_cierre,
            resultado=op.pnl_realizado,
            motivo=motivo,
            pmax=None if op.precio_max == float("-inf") else op.precio_max,
            pmin=None if op.precio_min == float("inf") else op.precio_min,
            id_vela_1m_cierre=id_vela_1m_cierre,
            id=op.id_operacion
        )
        self._exec(sql, params)

    def update_operacion_cierre_parcial(self, op: Operation, id_vela_1m_cierre: Optional[int]):
        sql = """
        UPDATE operaciones_simuladas
           SET estado='cerrada_parcial',
               timestamp_cierre=%(ts_cierre)s,
               resultado=COALESCE(resultado,0)+%(pnl)s,
               precio_max_alcanzado=%(pmax)s,
               precio_min_alcanzado=%(pmin)s,
               duracion_operacion = EXTRACT(EPOCH FROM (%(ts_cierre)s - timestamp_apertura)) / 60.0,
               id_vela_1m_cierre = %(id_vela_1m_cierre)s
         WHERE id_operacion=%(id)s;
        """
        params = dict(
            ts_cierre=self._dt(op.timestamp_cierre),
            pnl=op.pnl_realizado,
            pmax=None if op.precio_max == float("-inf") else op.precio_max,
            pmin=None if op.precio_min == float("inf") else op.precio_min,
            id_vela_1m_cierre=id_vela_1m_cierre,
            id=op.id_operacion
        )
        self._exec(sql, params)

    def update_operacion_exposicion(self, op: Operation):
        porc_sl, porc_tp = self._calc_porcentajes(op)
        sql = """
        UPDATE operaciones_simuladas
           SET precio_entrada=%(precio_entrada)s,
               cantidad=%(cantidad)s,
               capital_riesgo_usado=%(capital_invertido)s,
               capital_bloqueado=%(capital_bloqueado)s,
               valor_total_exposicion=%(valor_total_exposicion)s,
               cnt_operaciones = COALESCE(cnt_operaciones, 0) + 1,
               porc_sl=%(porc_sl)s,
               porc_tp=%(porc_tp)s
         WHERE id_operacion=%(id)s;
        """
        params = dict(
            precio_entrada=op.precio_entrada,
            cantidad=op.cantidad,
            capital_invertido=op.capital_invertido,
            capital_bloqueado=op.capital_bloqueado,
            valor_total_exposicion=op.valor_total_exposicion,
            porc_sl=porc_sl,
            porc_tp=porc_tp,
            id=op.id_operacion
        )
        self._exec(sql, params)

    def update_pyg_no_realizado(self, op: Operation, pyg: float):
        sql = "UPDATE operaciones_simuladas SET pyg_no_realizado=%(pyg)s WHERE id_operacion=%(id)s;"
        self._exec(sql, dict(pyg=pyg, id=op.id_operacion))

    # -------------------
    # Logs
    # -------------------
    def insert_log_evento(self, evento: Dict[str, Any], investor: Investor):
        sql = """
        INSERT INTO log_operaciones_simuladas (
            timestamp_evento,
            id_inversionista_fk,
            id_senal_fk,
            id_operacion_fk,
            ticker,
            tipo_evento,
            detalle,
            capital_antes,
            capital_despues,
            motivo_no_operacion,
            resultado,
            motivo_cierre,
            precio_cierre,
            id_estrategia_fk,
            cantidad,
            sl,
            tp,
            id_operacion_padre,
            precio_max_alcanzado,
            precio_min_alcanzado,
            id_vela_1m_apertura,
            precio_senal
        ) VALUES (
            %(ts_evento)s,
            %(id_inv)s,
            %(id_senal_fk)s,
            %(id_op)s,
            %(ticker)s,
            %(tipo)s,
            %(detalle)s,
            %(capital_antes)s,
            %(capital_despues)s,
            %(motivo_no)s,
            %(resultado)s,
            %(motivo_cierre)s,
            %(precio_cierre)s,
            %(id_estrategia_fk)s,
            %(cantidad)s,
            %(sl)s,
            %(tp)s,
            %(id_op_padre)s,
            %(pmax)s,
            %(pmin)s,
            %(id_vela_1m_apertura)s,
            %(precio_senal)s
        );
        """
        from psycopg2.extras import Json
        detalle_crudo = evento.get("detalle_json")
        if detalle_crudo is None:
            detalle_crudo = evento.get("detalle", {})
        params = dict(
            ts_evento=evento.get("ts_evento") or datetime.now(timezone.utc),
            id_inv=investor.id_inversionista,
            id_senal_fk=evento.get("id_senal_fk"),
            id_op=evento.get("id_op"),
            ticker=evento.get("ticker"),
            tipo=evento.get("tipo"),
            detalle=Json(detalle_crudo or {}),
            capital_antes=evento.get("capital_antes"),
            capital_despues=evento.get("capital_despues"),
            motivo_no=evento.get("motivo_no_operacion"),
            resultado=evento.get("resultado"),
            motivo_cierre=evento.get("motivo_cierre"),
            precio_cierre=evento.get("precio_cierre"),
            id_estrategia_fk=evento.get("id_estrategia_fk"),
            cantidad=evento.get("cantidad"),
            sl=evento.get("sl"),
            tp=evento.get("tp"),
            id_op_padre=evento.get("id_op_padre"),
            pmax=evento.get("precio_max_alcanzado"),
            pmin=evento.get("precio_min_alcanzado"),
            id_vela_1m_apertura=evento.get("id_vela_1m_apertura"),
            precio_senal=evento.get("precio_senal")
        )
        self._exec(sql, params)

    # -------------------
    # Capital
    # -------------------
    def update_capital_inversionista(self, inv: Investor):
        sql = "UPDATE inversionistas SET capital_actual=%(cap)s WHERE id_inversionista=%(id)s;"
        self._exec(sql, dict(cap=inv.capital_actual, id=inv.id_inversionista))
