# simulator/validations.py
# Validaciones de límites, riesgo y capital
# v1.0.0

from simulator.models import Investor, RiskConfig, Operation

def validar_limites_inversionista(inv: Investor) -> bool:
    if inv.max_operaciones_diarias and inv.operaciones_hoy >= inv.max_operaciones_diarias:
        return False
    return True

def validar_max_abiertas(inv: Investor, abiertas_actuales: int) -> bool:
    if inv.max_operaciones_abiertas and abiertas_actuales >= inv.max_operaciones_abiertas:
        return False
    return True

def validar_riesgo_monto(risk: RiskConfig, monto: float) -> bool:
    if monto < risk.tamano_min:
        return False
    if monto > risk.tamano_max:
        return False
    return True

def validar_capital_disponible(inv: Investor, requerido: float) -> bool:
    return inv.capital_actual >= requerido

def validar_dca_limite_operacion(op: Operation, risk: RiskConfig, monto: float) -> bool:
    return (op.capital_invertido + monto) <= risk.tamano_max