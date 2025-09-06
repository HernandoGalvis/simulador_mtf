# simulator/capital.py
# Funciones de capital (debitar / acreditar / sizing)
# v1.0.0

from simulator.models import Investor, RiskConfig

def calcular_monto_operacion(inv: Investor, risk: RiskConfig) -> float:
    monto = inv.capital_actual * (risk.riesgo_max_pct / 100.0)
    if monto < risk.tamano_min:
        monto = risk.tamano_min
    if monto > risk.tamano_max:
        monto = risk.tamano_max
    if monto > inv.capital_actual:
        monto = inv.capital_actual
    return monto

def debitar_capital(inv: Investor, monto: float):
    inv.capital_actual -= monto
    if inv.capital_actual < 0:
        inv.capital_actual = 0

def acreditar_capital(inv: Investor, monto: float):
    inv.capital_actual += monto