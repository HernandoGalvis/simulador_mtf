# simulator/finalization.py
# Cálculo de PnL no realizado al final / drawdown
# v1.0.0

from typing import Dict
from simulator.models import Operation, Investor
from simulator.logger import EventLogger

def finalizar_simulacion(operaciones: Dict[int, Operation], precios_close: Dict[str, float],
                         inv: Investor, logger: EventLogger):
    pyg_no_realizado_total = 0.0
    for op in operaciones.values():
        if op.abierta and op.cantidad > 0:
            price = precios_close.get(op.ticker)
            if price:
                nr = op.pnl_no_realizado(price)
                pyg_no_realizado_total += nr
                logger.log("pnl_no_realizado",
                           id_op=op.id_operacion,
                           ticker=op.ticker,
                           close=price,
                           pnl_flotante=nr,
                           capital_antes=inv.capital_actual,
                           capital_despues=inv.capital_actual)
    return pyg_no_realizado_total