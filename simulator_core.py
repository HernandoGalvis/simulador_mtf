# simulator/simulator_core.py
# Núcleo del simulador
# v1.4.0
#
# Cambios v1.4.0:
# - Si mult_sl_asignado o mult_tp_asignado vienen NULL o 0 en la señal:
#   * No se procesa la señal (ni apertura ni DCA).
#   * Se loguea "rechazo_apertura" o "rechazo_dca" con detalle y ts_evento del minuto simulado.
# - Persistimos mult_sl_asignado y mult_tp_asignado en la Operation en la apertura.
# - En apertura de hija por cierre parcial, la hija hereda los multiplicadores del padre.
#
# Cambios v1.3.1:
# - Fix: _log_evento no setea clave "tipo" (evita conflicto con logger.log)
#
# Cambios v1.3.0:
# - Logs con ts_evento = minuto simulado (no hora del sistema).
# - En DCA y rechazo_dca, id_senal_fk y precio_senal de la señal evaluada.
# - En cierres/apertura_hija_parcial, id_senal_fk y precio_senal = NULL.

from typing import Dict, Optional, Callable
from datetime import datetime, timezone
from simulator.models import Investor, Operation, StrategyParams, RiskConfig
from simulator.strategy_cache import StrategyCache
from simulator.logger import EventLogger
from simulator.persistence import PersistenceAdapter
from simulator.capital import calcular_monto_operacion, debitar_capital
from simulator.validations import (
    validar_limites_inversionista,
    validar_max_abiertas,
    validar_capital_disponible,
    validar_riesgo_monto
)
from simulator.fees import calcular_comision
from simulator.dca import aplicar_dca
from simulator.closures import evaluar_cierres_reglas
from simulator.finalization import finalizar_simulacion
from simulator.utils_time import minute_to_datetime

class SimulatorCore:
    def __init__(
        self,
        investor: Investor,
        risk: RiskConfig,
        strategy_cache: StrategyCache,
        signal_provider,
        price_provider,
        logger: EventLogger,
        persistence: PersistenceAdapter,
        base_datetime: datetime,
        confirmar_pendientes_fn: Optional[Callable] = None
    ):
        self.investor = investor
        self.risk = risk
        self.strategy_cache = strategy_cache
        self.signal_provider = signal_provider
        self.price_provider = price_provider
        self.logger = logger
        self.persistence = persistence
        self.base_datetime = base_datetime
        self.confirmar_pendientes_fn = confirmar_pendientes_fn
        self.operaciones: Dict[int, Operation] = {}
        self.map_ticker_dir: Dict[str, int] = {}

    def _marcar_error_persistencia(self, exc: Exception, contexto: str):
        self.investor.desincronizado = True
        self.investor.halted = True
        self.logger.log("error_persistencia",
                        contexto=contexto,
                        error=str(exc),
                        capital_antes=self.investor.capital_actual,
                        capital_despues=self.investor.capital_actual)

    def _seleccionar_apalancamiento(self, signal) -> int:
        if self.investor.usar_parametros_senal:
            lev = getattr(signal, "apalancamiento_calculado", None)
            if lev is None or lev < 1:
                return 0
            return int(lev)
        base = self.investor.apalancamiento_inversionista
        if (base is None or base < 1) and self.investor.apalancamiento_max:
            base = self.investor.apalancamiento_max
        if base is None or base < 1:
            base = 1
        return int(base)

    def _log_evento(self, tipo: str, op: Operation = None, **extra):
        ev = {}
        if op:
            ev.update({
                "id_op": op.id_operacion,
                "ticker": op.ticker,
                "id_estrategia_fk": op.id_estrategia_fk,
                "id_senal_fk": op.id_senal_fk,  # puede ser sobrescrito con kwargs
                "cantidad": op.cantidad,
                "sl": op.stop_loss,
                "tp": op.take_profit,
                "precio_max_alcanzado": None if op.precio_max == float("-inf") else op.precio_max,
                "precio_min_alcanzado": None if op.precio_min == float("inf") else op.precio_min,
                "id_vela_1m_apertura": op.id_vela_1m_apertura,
                "precio_senal": op.precio_entrada  # puede ser sobrescrito con kwargs
            })
        ev.update(extra)
        self.logger.log(tipo, **ev)

    def _dt_utc_from_ts(self, ts: int) -> datetime:
        dt = minute_to_datetime(self.base_datetime, ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def _rechazo_apertura(self, s, motivo: str, contexto: dict, ts: Optional[int] = None, dt: Optional[datetime] = None):
        dt_utc = None
        if dt is not None:
            dt_utc = dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
        elif ts is not None:
            dt_utc = self._dt_utc_from_ts(ts)

        detalle = {
            "motivo": motivo,
            "id_senal": s.id_senal,
            "id_estrategia_fk": s.id_estrategia_fk,
            "ticker": s.ticker_fk,
            "tipo_senal": s.tipo_senal,
            "precio_senal": getattr(s, "precio_senal", None),
            "contexto": contexto
        }
        self.logger.log("rechazo_apertura",
                        ts_evento=dt_utc,
                        id_senal_fk=s.id_senal,
                        id_estrategia_fk=s.id_estrategia_fk,
                        ticker=s.ticker_fk,
                        precio_senal=getattr(s, "precio_senal", None),
                        detalle=detalle)

    def _abrir_operacion(self, s, price_record, ts: int):
        # Validaciones previas de estado/inversor
        if self.investor.drawdown_activo or self.investor.halted:
            self._rechazo_apertura(s, "investor_halted_drawdown", {
                "drawdown_activo": self.investor.drawdown_activo,
                "halted": self.investor.halted
            }, ts=ts)
            return
        if not validar_limites_inversionista(self.investor):
            self._rechazo_apertura(s, "limites_inversionista", {
                "operaciones_hoy": self.investor.operaciones_hoy,
                "max_operaciones_diarias": self.investor.max_operaciones_diarias
            }, ts=ts)
            return
        abiertas = len([o for o in self.operaciones.values() if o.abierta])
        if not validar_max_abiertas(self.investor, abiertas):
            self._rechazo_apertura(s, "max_abiertas", {
                "abiertas": abiertas,
                "max_permitidas": self.investor.max_operaciones_abiertas
            }, ts=ts)
            return

        # Selección de apalancamiento
        apalancamiento = self._seleccionar_apalancamiento(s)
        if apalancamiento == 0:
            self._rechazo_apertura(s, "apalancamiento_cero", {
                "apalancamiento_calculado": getattr(s, "apalancamiento_calculado", None)
            }, ts=ts)
            return

        # Monto y riesgo
        monto = calcular_monto_operacion(self.investor, self.risk)
        if not validar_riesgo_monto(self.risk, monto):
            self._rechazo_apertura(s, "monto_fuera_riesgo", {
                "monto": monto,
                "riesgo_max_pct": self.risk.riesgo_max_pct
            }, ts=ts)
            return

        # Precio ejecución = close de la vela 1m
        precio_exec = price_record.close
        cantidad = (monto * apalancamiento) / max(precio_exec, 1e-12)
        comision = calcular_comision(precio_exec, cantidad, self.investor.commission_pct)
        total_debitar = monto + comision
        if not validar_capital_disponible(self.investor, total_debitar):
            self._rechazo_apertura(s, "capital_insuficiente", {
                "capital_actual": self.investor.capital_actual,
                "total_debitar": total_debitar
            }, ts=ts)
            return

        # Estrategia
        try:
            sp: StrategyParams = self.strategy_cache.get(s.id_estrategia_fk)
        except KeyError:
            sp = self.strategy_cache.set(
                s.id_estrategia_fk,
                self.signal_provider.strategy_loader.load_strategy_params(s.id_estrategia_fk)
            )

        # Construcción de operación
        op = Operation(
            id_operacion=None,
            id_inversionista_fk=self.investor.id_inversionista,
            id_estrategia_fk=s.id_estrategia_fk,
            id_senal_fk=s.id_senal,
            ticker=s.ticker_fk,
            tipo=s.tipo_senal,
            precio_entrada=precio_exec,
            take_profit=s.target_profit_price or 0.0,
            stop_loss=s.stop_loss_price or 0.0,
            cantidad=cantidad,
            strategy=sp,
            capital_invertido=monto,
            apalancamiento=apalancamiento,
            capital_bloqueado=monto,
            comisiones_acumuladas=comision,
            timestamp_apertura=ts,
            id_vela_1m_apertura=price_record.id_vela
        )
        # Guardar multiplicadores de la señal en la operación (aunque Operation no los tipifique)
        setattr(op, "mult_sl_asignado", getattr(s, "mult_sl_asignado", None))
        setattr(op, "mult_tp_asignado", getattr(s, "mult_tp_asignado", None))
        op.init_extremos()

        capital_antes = self.investor.capital_actual
        try:
            new_id = self.persistence.insert_operacion(
                op,
                inv_capital_total=self.investor.capital_actual,
                inv_capital_disp=self.investor.capital_actual
            )
            op.id_operacion = new_id
        except Exception as e:
            self._marcar_error_persistencia(e, "insert_operacion")
            return

        debitar_capital(self.investor, total_debitar)
        self.investor.operaciones_hoy += 1
        self.operaciones[new_id] = op
        self.map_ticker_dir[f"{op.ticker}:{op.tipo}"] = new_id

        dt_utc = self._dt_utc_from_ts(ts)
        self._log_evento(
            "apertura",
            op,
            ts_evento=dt_utc,
            capital_antes=capital_antes,
            capital_despues=self.investor.capital_actual,
            detalle={
                "precio_exec": precio_exec,
                "cantidad": cantidad,
                "monto_margen": monto,
                "comision": comision,
                "apalancamiento": apalancamiento,
                "mult_sl_asignado": getattr(s, "mult_sl_asignado", None),
                "mult_tp_asignado": getattr(s, "mult_tp_asignado", None)
            }
        )

    def _aplicar_dca(self, op: Operation, price_record, ts: int, s):
        capital_antes = self.investor.capital_actual
        monto_base = calcular_monto_operacion(self.investor, self.risk)
        res = aplicar_dca(op, price_record.close, monto_base, self.investor, self.risk)
        dt_utc = self._dt_utc_from_ts(ts)
        if not res:
            return
        precio_senal_ev = getattr(s, "precio_senal", price_record.close)
        if "rechazo_dca" in res:
            detalle = {
                "motivo": res["rechazo_dca"],
                "capital_actual": self.investor.capital_actual,
                "monto_base": monto_base,
                "precio_close": price_record.close,
                "cantidad_actual": op.cantidad,
                "apalancamiento": op.apalancamiento
            }
            self._log_evento("rechazo_dca", op,
                             ts_evento=dt_utc,
                             id_senal_fk=s.id_senal,
                             precio_senal=precio_senal_ev,
                             capital_antes=capital_antes,
                             capital_despues=self.investor.capital_actual,
                             detalle=detalle)
            return
        try:
            self.persistence.update_operacion_exposicion(op)
        except Exception as e:
            self._marcar_error_persistencia(e, "update_operacion_exposicion")
            return
        self._log_evento("dca", op,
                         ts_evento=dt_utc,
                         id_senal_fk=s.id_senal,
                         precio_senal=precio_senal_ev,
                         capital_antes=capital_antes,
                         capital_despues=self.investor.capital_actual,
                         detalle=res)

    def _procesar_cierres(self, ts: int):
        for op in list(self.operaciones.values()):
            if not op.abierta:
                continue
            dt = minute_to_datetime(self.base_datetime, ts)
            dt_utc = dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
            price_record = self.price_provider.get_price(op.ticker, dt)
            if not price_record:
                continue
            op.actualizar_extremos(price_record.high, price_record.low)
            capital_antes = self.investor.capital_actual

            eventos = evaluar_cierres_reglas(op,
                                             high=price_record.high,
                                             low=price_record.low,
                                             close=price_record.close,
                                             inv=self.investor,
                                             ts=ts)
            if not eventos:
                continue

            for ev in eventos:
                if ev["tipo_evento"] == "cierre_total":
                    try:
                        self.persistence.update_operacion_cierre_total(ev["op"], ev["motivo"], price_record.id_vela)
                    except Exception as e:
                        self._marcar_error_persistencia(e, "update_operacion_cierre_total")
                        return
                    self._log_evento(
                        "cierre_total",
                        ev["op"],
                        ts_evento=dt_utc,
                        id_senal_fk=None,
                        precio_senal=None,
                        capital_antes=capital_antes,
                        capital_despues=self.investor.capital_actual,
                        motivo_cierre=ev["motivo"],
                        resultado=ev["pnl_net"],
                        precio_cierre=ev["precio_exec"]
                    )
                elif ev["tipo_evento"] == "cierre_parcial":
                    try:
                        self.persistence.update_operacion_cierre_parcial(ev["op_padre"], price_record.id_vela)
                    except Exception as e:
                        self._marcar_error_persistencia(e, "update_operacion_cierre_parcial")
                        return
                    hija = ev["hija"]
                    # Heredar multiplicadores a la hija
                    setattr(hija, "mult_sl_asignado", getattr(ev["op_padre"], "mult_sl_asignado", None))
                    setattr(hija, "mult_tp_asignado", getattr(ev["op_padre"], "mult_tp_asignado", None))
                    try:
                        new_id = self.persistence.insert_operacion(
                            hija,
                            inv_capital_total=self.investor.capital_actual,
                            inv_capital_disp=self.investor.capital_actual
                        )
                        hija.id_operacion = new_id
                        self.operaciones[new_id] = hija
                        self.map_ticker_dir[f"{hija.ticker}:{hija.tipo}"] = new_id
                    except Exception as e:
                        self._marcar_error_persistencia(e, "insert_operacion_hija")
                        return
                    self._log_evento(
                        "cierre_parcial",
                        ev["op_padre"],
                        ts_evento=dt_utc,
                        id_senal_fk=None,
                        precio_senal=None,
                        capital_antes=capital_antes,
                        capital_despues=self.investor.capital_actual,
                        motivo_cierre=ev["motivo"],
                        resultado=ev["pnl_parcial_net"],
                        detalle={
                            "qty_liq": ev["qty_liq"],
                            "capital_liq": ev["capital_liq"],
                            "precio_exec": ev["precio_exec"]
                        }
                    )
                    self._log_evento(
                        "apertura_hija_parcial",
                        hija,
                        ts_evento=dt_utc,
                        id_senal_fk=None,
                        precio_senal=None,
                        capital_antes=self.investor.capital_actual,
                        capital_despues=self.investor.capital_actual,
                        id_op_padre=ev["op_padre"].id_operacion
                    )
            if self.investor.drawdown_activo and not self.investor.halted:
                self.investor.halted = True
                return
            if self.investor.desincronizado:
                return

    def run(self, ts_inicio: int, ts_fin: int):
        for ts in range(ts_inicio, ts_fin + 1):
            if self.investor.halted or self.investor.desincronizado:
                break
            dia = ts // 1440
            self.investor.reset_diario_si_cambia_dia(dia)

            self._procesar_cierres(ts)
            if self.investor.halted or self.investor.desincronizado:
                break

            dt = minute_to_datetime(self.base_datetime, ts)
            dt_utc = dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
            signals = self.signal_provider.get_signals_by_minute(dt)

            for s in signals:
                # 1) Validar multiplicadores de la señal: si son inválidos, no procesar
                ms = getattr(s, "mult_sl_asignado", None)
                mt = getattr(s, "mult_tp_asignado", None)
                if (ms is None or mt is None or ms <= 0 or mt <= 0):
                    key = f"{s.ticker_fk}:{s.tipo_senal}"
                    op_id = self.map_ticker_dir.get(key)
                    if op_id:
                        op = self.operaciones.get(op_id)
                        if op and op.abierta:
                            self._log_evento(
                                "rechazo_dca",
                                op,
                                ts_evento=dt_utc,
                                id_senal_fk=s.id_senal,
                                precio_senal=getattr(s, "precio_senal", None),
                                capital_antes=self.investor.capital_actual,
                                capital_despues=self.investor.capital_actual,
                                detalle={
                                    "motivo": "multiplicadores_invalidos",
                                    "mult_sl_asignado": ms,
                                    "mult_tp_asignado": mt
                                }
                            )
                            continue
                    # No hay operación abierta: rechazo de apertura
                    self._rechazo_apertura(
                        s,
                        "multiplicadores_invalidos",
                        {"mult_sl_asignado": ms, "mult_tp_asignado": mt},
                        ts=ts, dt=dt
                    )
                    continue

                # 2) Con multiplicadores válidos, procesar normalmente
                price_record = self.price_provider.get_price(s.ticker_fk, dt)
                if not price_record:
                    self._rechazo_apertura(s, "sin_precio_minuto", {"timestamp": dt.isoformat()}, ts=ts, dt=dt)
                    continue

                key = f"{s.ticker_fk}:{s.tipo_senal}"
                op_id = self.map_ticker_dir.get(key)
                if op_id:
                    op = self.operaciones.get(op_id)
                    if op and op.abierta:
                        self._aplicar_dca(op, price_record, ts, s)
                        if self.investor.desincronizado:
                            return
                        continue
                self._abrir_operacion(s, price_record, ts)
                if self.investor.desincronizado:
                    return

    def finalizar(self, precios_close_final: Dict[str, float]):
        if self.investor.desincronizado:
            return None
        if not self.investor.halted:
            pyg = finalizar_simulacion(self.operaciones, precios_close_final, self.investor, self.logger)
            self.logger.log("finalizacion_inversionista",
                            capital_antes=self.investor.capital_actual,
                            capital_despues=self.investor.capital_actual,
                            pnl_realizado_acumulado=self.investor.pnl_realizado_acumulado,
                            pyg_no_realizado_total=pyg,
                            drawdown_activo=self.investor.drawdown_activo)
            try:
                self.persistence.update_capital_inversionista(self.investor)
                for op in self.operaciones.values():
                    if op.abierta:
                        price = precios_close_final.get(op.ticker)
                        if price:
                            self.persistence.update_pyg_no_realizado(op, op.pnl_no_realizado(price))
            except Exception as e:
                self._marcar_error_persistencia(e, "finalizar_snapshot")
            return pyg
        return None
