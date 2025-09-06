# simulator/fees.py
# Cálculo de slippage y comisiones
# v1.0.0

def aplicar_slippage(precio: float, tipo_operacion: str, slippage_pct: float, side: str) -> float:
    if slippage_pct <= 0:
        return precio
    factor = slippage_pct / 100.0
    if side == "entry":
        if tipo_operacion == "LONG":
            return precio * (1 + factor)
        else:
            return precio * (1 - factor)
    else:  # exit
        if tipo_operacion == "LONG":
            return precio * (1 - factor)
        else:
            return precio * (1 + factor)

def calcular_comision(precio: float, cantidad: float, commission_pct: float) -> float:
    if commission_pct <= 0:
        return 0.0
    notional = precio * cantidad
    return notional * (commission_pct / 100.0)