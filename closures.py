# simulator/closures.py
# Reglas de cierres: TP, SL, Protección, Retroceso sin avance, Parcial
# v1.0.2 (Refactor: prioriza liquidación parcial SL antes de total SL + logging detallado a archivo)

from simulator.models import Operation, Investor
from simulator.capital import acreditar_capital
from simulator.fees import aplicar_slippage, calcular_comision
from simulator.utils import log_debug  # <-- Utilidad para logging a archivo

def evaluar_cierres_reglas(op: Operation, high: float, low: float, close: float,
                           inv: Investor, ts: int):
    if not op.abierta or inv.halted:
        return []
    eventos = []
    fr = op.strategy.to_fracciones()

    # Take Profit (TP)
    if (op.tipo == "LONG" and high >= op.take_profit) or \
       (op.tipo == "SHORT" and low <= op.take_profit):
        precio_exec = aplicar_slippage(op.take_profit, op.tipo, inv.slippage_close_pct, side="exit")
        comision = calcular_comision(precio_exec, op.cantidad, inv.commission_pct)
        pnl_net = op.cerrar_total(precio_exec, comision, ts)
        acreditar_capital(inv, op.capital_invertido + pnl_net)
        inv.registrar_pnl_realizado(pnl_net)
        inv.verificar_drawdown()
        eventos.append({
            "tipo_evento": "cierre_total",
            "motivo": "Take Profit",
            "precio_exec": precio_exec,
            "comision": comision,
            "pnl_net": pnl_net,
            "op": op
        })
        return eventos

    avance_minimo = op.avance_minimo_alcanzado()
    hubo_avance = op.hubo_algun_avance()
    sin_avance = op.sin_avance()

    # 1. Liquidación PARCIAL por SL (antes que SL total)
    log_debug(
        f"Evaluando parcial SL: avance_minimo={avance_minimo}, hubo_avance={hubo_avance}, "
        f"habilitar_parcial={op.strategy.habilitar_parcial}, permite_parcial={op.permite_parcial}"
    )
    if (not avance_minimo) and hubo_avance and op.strategy.habilitar_parcial and op.permite_parcial:
        retro = op.retroceso_desde_entrada(low=low, high=high)
        log_debug(
            f"  -> retro={retro}, retroceso_parcial={fr['retroceso_parcial']}, "
            f"parciales_realizados={op.parciales_realizados}, max_parciales={op.strategy.max_parciales}"
        )
        if retro >= fr["retroceso_parcial"] and op.parciales_realizados < op.strategy.max_parciales:
            qty_liq_estimada = op.cantidad * fr["porc_liq_parcial"]
            log_debug(
                f"    -> qty_liq_estimada={qty_liq_estimada}, "
                f"porc_liq_parcial={fr['porc_liq_parcial']}, cantidad={op.cantidad}"
            )
            precio_exec = aplicar_slippage(close, op.tipo, inv.slippage_close_pct, side="exit")
            comision_parcial = calcular_comision(precio_exec, qty_liq_estimada, inv.commission_pct)
            log_debug(
                f"    -> precio_exec={precio_exec}, comision_parcial={comision_parcial}"
            )
            info = op.cerrar_parcial_creando_hija(precio_exec, comision_parcial, ts)
            log_debug(
                f"    -> info={info}"
            )
            if info:
                acreditar_capital(inv, info["capital_liq"] + info["pnl_parcial_net"])
                inv.registrar_pnl_realizado(info["pnl_parcial_net"])
                inv.verificar_drawdown()
                log_debug(
                    f"    -> Parcial ejecutado, se acredita capital_liq + pnl_parcial_net = "
                    f"{info['capital_liq']} + {info['pnl_parcial_net']}"
                )
                eventos.append({
                    "tipo_evento": "cierre_parcial",
                    "motivo": "Liquidación parcial por SL",
                    "precio_exec": precio_exec,
                    "comision": comision_parcial,
                    "retro": retro,
                    "qty_liq": info["qty_liq"],
                    "pnl_parcial_net": info["pnl_parcial_net"],
                    "capital_liq": info["capital_liq"],
                    "op_padre": op,
                    "hija": info["hija"]
                })
                return eventos

    # 2. Stop Loss (SL) - después del parcial
    if (op.tipo == "LONG" and low <= op.stop_loss) or \
       (op.tipo == "SHORT" and high >= op.stop_loss):
        precio_exec = aplicar_slippage(op.stop_loss, op.tipo, inv.slippage_close_pct, side="exit")
        comision = calcular_comision(precio_exec, op.cantidad, inv.commission_pct)
        pnl_net = op.cerrar_total(precio_exec, comision, ts)
        acreditar_capital(inv, op.capital_invertido + pnl_net)
        inv.registrar_pnl_realizado(pnl_net)
        inv.verificar_drawdown()
        eventos.append({
            "tipo_evento": "cierre_total",
            "motivo": "Stop Loss",
            "precio_exec": precio_exec,
            "comision": comision,
            "pnl_net": pnl_net,
            "op": op
        })
        return eventos

    # Protección (retroceso desde el extremo tras avance mínimo)
    if avance_minimo and op.strategy.habilitar_proteccion_ganancias:
        ratio_retro = op.ratio_retroceso_proteccion(low=low, high=high)
        if ratio_retro >= fr["limite_retro_proteccion"]:
            precio_exec = aplicar_slippage(close, op.tipo, inv.slippage_close_pct, side="exit")
            comision = calcular_comision(precio_exec, op.cantidad, inv.commission_pct)
            pnl_net = op.cerrar_total(precio_exec, comision, ts)
            acreditar_capital(inv, op.capital_invertido + pnl_net)
            inv.registrar_pnl_realizado(pnl_net)
            inv.verificar_drawdown()
            motivo = "Retroceso desde máximo" if op.tipo == "LONG" else "Retroceso desde mínimo"
            eventos.append({
                "tipo_evento": "cierre_total",
                "motivo": motivo,
                "precio_exec": precio_exec,
                "comision": comision,
                "pnl_net": pnl_net,
                "ratio_retro": ratio_retro,
                "op": op
            })
            return eventos

    # Retroceso sin avance
    if sin_avance and op.strategy.habilitar_retroceso_sin_avance and op.permite_parcial:
        retro = op.retroceso_desde_entrada(low=low, high=high)
        if retro >= fr["retroceso_sin_avance"]:
            precio_exec = aplicar_slippage(close, op.tipo, inv.slippage_close_pct, side="exit")
            comision = calcular_comision(precio_exec, op.cantidad, inv.commission_pct)
            pnl_net = op.cerrar_total(precio_exec, comision, ts)
            acreditar_capital(inv, op.capital_invertido + pnl_net)
            inv.registrar_pnl_realizado(pnl_net)
            inv.verificar_drawdown()
            eventos.append({
                "tipo_evento": "cierre_total",
                "motivo": "Retroceso desde entrada (sin avance)",
                "precio_exec": precio_exec,
                "comision": comision,
                "pnl_net": pnl_net,
                "retro": retro,
                "op": op
            })
            return eventos

    return eventos
