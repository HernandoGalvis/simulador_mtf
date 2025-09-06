# simulator/dca.py
# Lógica de DCA (agregar tamaño a operación existente)
# v1.0.0

from simulator.models import Operation, Investor, RiskConfig
from simulator.capital import debitar_capital
from simulator.validations import validar_dca_limite_operacion
from simulator.fees import aplicar_slippage, calcular_comision

def aplicar_dca(op: Operation, precio_base: float, monto: float,
                inv: Investor, risk: RiskConfig):
    if not validar_dca_limite_operacion(op, risk, monto):
        return {"rechazo_dca": "limite_tamano_operacion"}
    precio_exec = aplicar_slippage(precio_base, op.tipo, inv.slippage_open_pct, side="entry")
    qty_extra = (monto * op.apalancamiento) / precio_exec
    if inv.capital_actual < monto:
        return {"rechazo_dca": "sin_capital"}
    comision = calcular_comision(precio_exec, qty_extra, inv.commission_pct)
    total_debitar = monto + comision
    if inv.capital_actual < total_debitar:
        return {"rechazo_dca": "sin_capital_comision"}
    nuevo_prom = (op.precio_entrada * op.cantidad + precio_exec * qty_extra) / (op.cantidad + qty_extra)
    op.precio_entrada = nuevo_prom
    op.cantidad += qty_extra
    op.capital_invertido += monto
    op.capital_bloqueado += monto
    op.comisiones_acumuladas += comision
    debitar_capital(inv, total_debitar)
    return {
        "tipo_evento": "dca",
        "precio_exec": precio_exec,
        "precio_base": precio_base,
        "monto_margen": monto,
        "qty_add": qty_extra,
        "nuevo_prom": nuevo_prom,
        "apalancamiento": op.apalancamiento,
        "comision": comision
    }