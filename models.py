# simulator/models.py
# Modelos de dominio: Investor, StrategyParams, Operation, RiskConfig
# v1.1.0
#
# Cambios v1.1.0:
# - Se agrega campo id_vela_1m_apertura a Operation para registrar la vela 1m utilizada
#   en la apertura (requerido para trazabilidad y logging).
# - Se propaga el campo al crear operaciones hijas en cierres parciales (se hereda el mismo id
#   de la operación original).
#
# Nota: No se modifica la lógica existente de extremos ni PnL.

from dataclasses import dataclass, field
from typing import Optional, Literal
import math

TipoOperacion = Literal["LONG", "SHORT"]

@dataclass
class StrategyParams:
    avance_minimo_pct: float
    porc_limite_retro: float
    porc_retroceso_liquidacion_sl: float
    porc_liquidacion_parcial_sl: float
    porc_limite_retro_entrada: float
    max_parciales: int = 1
    habilitar_proteccion_ganancias: bool = True
    habilitar_parcial: bool = True
    habilitar_retroceso_sin_avance: bool = True

    def to_fracciones(self):
        return {
            "avance_minimo": self.avance_minimo_pct / 100.0,
            "limite_retro_proteccion": self.porc_limite_retro / 100.0,
            "retroceso_parcial": self.porc_retroceso_liquidacion_sl / 100.0,
            "porc_liq_parcial": self.porc_liquidacion_parcial_sl / 100.0,
            "retroceso_sin_avance": self.porc_limite_retro_entrada / 100.0
        }

    def limite_retroceso_liq_parcial_SL(self):
        return self.porc_liquidacion_parcial_sl / 100.0

    def limite_liq_retroceso_entrada(self):
        return self.porc_limite_retro_entrada / 100.0

    def limite_retroceso_max(self):
        return self.porc_limite_retro / 100.0

    @property
    def umbral_avance_minimo(self):
        return self.avance_minimo_pct / 100.0
    
@dataclass
class Investor:
    id_inversionista: int
    capital_inicial: float
    capital_actual: float
    operaciones_hoy: int = 0
    max_operaciones_diarias: int = 50
    max_operaciones_abiertas: int = 20
    dia_actual: Optional[int] = None
    slippage_open_pct: float = 0.0
    slippage_close_pct: float = 0.0
    commission_pct: float = 0.0
    drawdown_max_pct: float = 0.0
    drawdown_activo: bool = False
    pnl_realizado_acumulado: float = 0.0
    halted: bool = False
    usar_parametros_senal: bool = False
    apalancamiento_inversionista: Optional[int] = None
    apalancamiento_max: Optional[int] = None
    desincronizado: bool = False

    def reset_diario_si_cambia_dia(self, dia: int):
        if self.dia_actual is None or self.dia_actual != dia:
            self.dia_actual = dia
            self.operaciones_hoy = 0

    def registrar_pnl_realizado(self, pnl_net: float):
        self.pnl_realizado_acumulado += pnl_net

    def verificar_drawdown(self):
        if self.drawdown_max_pct <= 0:
            return
        limite_perdida = self.capital_inicial * (self.drawdown_max_pct / 100.0)
        if -self.pnl_realizado_acumulado >= limite_perdida:
            self.drawdown_activo = True

@dataclass
class RiskConfig:
    riesgo_max_pct: float
    tamano_min: float
    tamano_max: float

@dataclass
class Operation:
    id_operacion: Optional[int]
    id_inversionista_fk: int
    id_estrategia_fk: int
    id_senal_fk: int
    ticker: str
    tipo: TipoOperacion
    precio_entrada: float
    take_profit: float
    stop_loss: float
    cantidad: float
    strategy: StrategyParams
    capital_invertido: float
    apalancamiento: int
    capital_bloqueado: float
    abierta: bool = True
    estado: str = "abierta"
    precio_max: float = field(default=-math.inf)
    precio_min: float = field(default= math.inf)
    parciales_realizados: int = 0
    pnl_realizado: float = 0.0
    es_hija: bool = False
    id_operacion_padre: Optional[int] = None
    comisiones_acumuladas: float = 0.0
    permite_parcial: bool = True
    timestamp_apertura: Optional[int] = None
    timestamp_cierre: Optional[int] = None
    ultimo_precio_exec_cierre: Optional[float] = None
    # NUEVO: id de la vela 1m empleada en la apertura (close usado como precio_entrada)
    id_vela_1m_apertura: Optional[int] = None

    def init_extremos(self):
        self.precio_max = self.precio_entrada
        self.precio_min = self.precio_entrada

    def actualizar_extremos(self, high: float, low: float):
        if high > self.precio_max:
            self.precio_max = high
        if low < self.precio_min:
            self.precio_min = low

    def avance_minimo_alcanzado(self) -> bool:
        fr = self.strategy.to_fracciones()
        if self.tipo == "LONG":
            return self.precio_max >= self.precio_entrada * (1 + fr["avance_minimo"])
        else:
            return self.precio_min <= self.precio_entrada * (1 - fr["avance_minimo"])

    def hubo_algun_avance(self) -> bool:
        if self.tipo == "LONG":
            return self.precio_max > self.precio_entrada
        else:
            return self.precio_min < self.precio_entrada

    def sin_avance(self) -> bool:
        return not self.hubo_algun_avance()

    def retroceso_desde_entrada(self, low: float = None, high: float = None) -> float:
        if self.tipo == "LONG":
            return (self.precio_entrada - (low if low is not None else self.precio_min)) / self.precio_entrada
        else:
            return ((high if high is not None else self.precio_max) - self.precio_entrada) / self.precio_entrada

    def ratio_retroceso_proteccion(self, low: float = None, high: float = None) -> float:
        if self.tipo == "LONG":
            if self.precio_max <= self.precio_entrada:
                return 0.0
            retro = self.precio_max - (low if low is not None else self.precio_min)
            total = self.precio_max - self.precio_entrada
            return retro / total
        else:
            if self.precio_min >= self.precio_entrada:
                return 0.0
            retro = (high if high is not None else self.precio_max) - self.precio_min
            total = self.precio_entrada - self.precio_min
            return retro / total

    def _pnl_gross(self, precio_salida: float, cantidad: float) -> float:
        if self.tipo == "LONG":
            return (precio_salida - self.precio_entrada) * cantidad
        return (self.precio_entrada - precio_salida) * cantidad

    def cerrar_total(self, precio_exec: float, comision_salida: float, ts: int) -> float:
        if not self.abierta:
            return 0.0
        gross = self._pnl_gross(precio_exec, self.cantidad)
        pnl_net = gross - comision_salida
        self.pnl_realizado += pnl_net
        self.comisiones_acumuladas += comision_salida
        self.cantidad = 0.0
        self.abierta = False
        self.estado = "cerrada_total"
        self.timestamp_cierre = ts
        self.ultimo_precio_exec_cierre = precio_exec
        return pnl_net

    def cerrar_parcial_creando_hija(self, precio_exec: float, comision_salida_parcial: float, ts: int):
        fr = self.strategy.to_fracciones()
        porc_liq = fr["porc_liq_parcial"]
        qty_before = self.cantidad
        qty_liq = qty_before * porc_liq
        if qty_liq <= 0:
            return None
        gross = self._pnl_gross(precio_exec, qty_liq)
        pnl_parcial_net = gross - comision_salida_parcial
        self.comisiones_acumuladas += comision_salida_parcial
        proporcion_liq = qty_liq / qty_before
        capital_liq = self.capital_invertido * proporcion_liq
        capital_rem = self.capital_invertido - capital_liq
        self.pnl_realizado += pnl_parcial_net
        self.cantidad = 0.0
        self.abierta = False
        self.estado = "cerrada_parcial"
        self.parciales_realizados += 1
        self.timestamp_cierre = ts
        self.ultimo_precio_exec_cierre = precio_exec
        hija = Operation(
            id_operacion=None,
            id_inversionista_fk=self.id_inversionista_fk,
            id_estrategia_fk=self.id_estrategia_fk,
            id_senal_fk=self.id_senal_fk,
            ticker=self.ticker,
            tipo=self.tipo,
            precio_entrada=self.precio_entrada,
            take_profit=self.take_profit,
            stop_loss=self.stop_loss,
            cantidad=qty_before - qty_liq,
            strategy=self.strategy,
            capital_invertido=capital_rem,
            apalancamiento=self.apalancamiento,
            capital_bloqueado=capital_rem,
            es_hija=True,
            id_operacion_padre=self.id_operacion,
            permite_parcial=False,
            timestamp_apertura=ts,
            id_vela_1m_apertura=self.id_vela_1m_apertura  # hereda la vela original
        )
        hija.init_extremos()
        hija.precio_max = self.precio_max
        hija.precio_min = self.precio_min
        return {
            "qty_liq": qty_liq,
            "pnl_parcial_net": pnl_parcial_net,
            "capital_liq": capital_liq,
            "hija": hija
        }

    def pnl_no_realizado(self, precio_actual: float) -> float:
        if not self.abierta or self.cantidad <= 0:
            return 0.0
        return self._pnl_gross(precio_actual, self.cantidad)

    @property
    def valor_total_exposicion(self) -> float:
        return self.cantidad * self.precio_entrada
    
