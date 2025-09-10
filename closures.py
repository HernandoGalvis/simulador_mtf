from simulator.fees import aplicar_slippage, calcular_comision
from simulator.capital import acreditar_capital
import logging

# Configuración del logger para archivo y pantalla
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("simulador_debug.log"),
        logging.StreamHandler()
    ]
)
def log_debug(msg: str):
    logging.info(msg)

def cerrar_operacion(op, precios, inv, ts, eventos, logger):
    """
    Lógica de cierres y liquidaciones para operaciones.
    Cada bloque es independiente y el primero que cumple su condición ejecuta y retorna.
    precios: dict con keys 'open', 'high', 'low', 'close'
    """
    high = precios['high']
    low = precios['low']
    close = precios['close']
    entrada = op.precio_entrada
    tp = op.take_profit
    sl = op.stop_loss

    # Detectar si la operación es una HIJA (NO permitir liquidación parcial)
    es_hija = hasattr(op, "id_operacion_padre") and op.id_operacion_padre not in [None, 0]

    # --- b1: Cierre total por TP ---
    if (op.tipo == "LONG" and high >= tp) or (op.tipo == "SHORT" and low <= tp):
        precio_exec = aplicar_slippage(tp, op.tipo, inv.slippage_close_pct, side="exit")
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
        if logger:
            logger.log("cierre_total", motivo="Take Profit", op=op.id_operacion, precio=precio_exec)
        return eventos

    # --- b2: Cierre parcial por SL (retroceso sin avance mínimo, SOLO para posición madre y SOLO una vez) ---
    if not es_hija and getattr(op, "permite_parcial", False) and not getattr(op, "liq_parcial_previa", False):
        avance_minimo = (tp - entrada) * op.strategy.avance_minimo_pct / 100.0
        porc_retroceso_liq_parcial = op.strategy.porc_retroceso_liquidacion_sl
        porc_liq_parcial = op.strategy.porc_liquidacion_parcial_sl

        if op.tipo == "LONG":
            hubo_peq_avance = op.precio_max > entrada
            no_avance_min = op.precio_max < (entrada + avance_minimo)
            bajo_de_entrada = low < entrada
            # El límite es ENTRADA - % de la distancia a SL
            limite_parcial = entrada - ((entrada - sl) * porc_retroceso_liq_parcial / 100.0)
            bajo_de_limite = low <= limite_parcial
            retroceso_parcial = hubo_peq_avance and no_avance_min and bajo_de_entrada and bajo_de_limite
            if low < entrada:
                log_debug(f"[DEBUG] LIQ PARCIAL LONG - id_op={op.id_operacion}, low={low}, sl={sl}, limite_parcial={limite_parcial}, hubo_peq_avance={hubo_peq_avance}, no_avance_min={no_avance_min}, bajo_de_entrada={bajo_de_entrada}, bajo_de_limite={bajo_de_limite}, retroceso_parcial={retroceso_parcial}, precio_max={op.precio_max}, entrada={entrada}, avance_minimo={avance_minimo}, porc_liq_parcial={porc_liq_parcial}")

            if retroceso_parcial:
                precio_exec = aplicar_slippage(low, op.tipo, inv.slippage_close_pct, side="exit")
                comision = calcular_comision(precio_exec, op.cantidad * porc_liq_parcial / 100.0, inv.commission_pct)
                cerrado = op.cerrar_parcial_creando_hija(precio_exec, comision, ts)
                if cerrado is not None:
                    pnl_net = cerrado["pnl_parcial_net"]
                    cantidad_parcial = cerrado["qty_liq"]
                    hija = cerrado["hija"]
                    # Lógica de acreditación y registro
                    acreditar_capital(inv, (op.capital_invertido / (op.capital_invertido + hija.capital_invertido)) * cantidad_parcial + pnl_net)
                    inv.registrar_pnl_realizado(pnl_net)
                    eventos.append({
                        "tipo_evento": "cierre_parcial",
                        "motivo": "Parcial SL",
                        "precio_exec": precio_exec,
                        "comision": comision,
                        "pnl_net": pnl_net,
                        "cantidad": cantidad_parcial,
                        "op": op,
                        "hija": hija
                    })
                    if logger:
                        logger.log("cierre_parcial", motivo="Parcial SL", op=op.id_operacion, precio=precio_exec)
                    op.liq_parcial_previa = True
                    return eventos

        else:  # SHORT
            hubo_peq_avance = op.precio_min < entrada
            no_avance_min = op.precio_min > (entrada - avance_minimo)
            sobre_de_entrada = high > entrada
            # El límite es ENTRADA + % de la distancia a SL (al alza)
            limite_parcial = entrada + ((sl - entrada) * porc_retroceso_liq_parcial / 100.0)
            sobre_limite = high >= limite_parcial
            retroceso_parcial = hubo_peq_avance and no_avance_min and sobre_de_entrada and sobre_limite

            log_debug(f"[DEBUG] LIQ PARCIAL SHORT - id_op={op.id_operacion}, high={high}, sl={sl}, limite_parcial={limite_parcial}, hubo_peq_avance={hubo_peq_avance}, no_avance_min={no_avance_min}, sobre_de_entrada={sobre_de_entrada}, sobre_limite={sobre_limite}, retroceso_parcial={retroceso_parcial}, precio_min={op.precio_min}, entrada={entrada}, avance_minimo={avance_minimo}, porc_liq_parcial={porc_liq_parcial}")

            if retroceso_parcial:
                precio_exec = aplicar_slippage(high, op.tipo, inv.slippage_close_pct, side="exit")
                comision = calcular_comision(precio_exec, op.cantidad * porc_liq_parcial / 100.0, inv.commission_pct)
                cerrado = op.cerrar_parcial_creando_hija(precio_exec, comision, ts)
                if cerrado is not None:
                    pnl_net = cerrado["pnl_parcial_net"]
                    cantidad_parcial = cerrado["qty_liq"]
                    hija = cerrado["hija"]
                    # Lógica de acreditación y registro
                    acreditar_capital(inv, (op.capital_invertido / (op.capital_invertido + hija.capital_invertido)) * cantidad_parcial + pnl_net)
                    inv.registrar_pnl_realizado(pnl_net)
                    eventos.append({
                        "tipo_evento": "cierre_parcial",
                        "motivo": "Parcial SL",
                        "precio_exec": precio_exec,
                        "comision": comision,
                        "pnl_net": pnl_net,
                        "cantidad": cantidad_parcial,
                        "op": op,
                        "hija": hija
                    })
                    if logger:
                        logger.log("cierre_parcial", motivo="Parcial SL", op=op.id_operacion, precio=precio_exec)
                    op.liq_parcial_previa = True
                    return eventos

    # --- b3: Cierre total por SL ---
    if (op.tipo == "LONG" and low <= sl) or (op.tipo == "SHORT" and high >= sl):
        precio_exec = aplicar_slippage(sl, op.tipo, inv.slippage_close_pct, side="exit")
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
        if logger:
            logger.log("cierre_total", motivo="Stop Loss", op=op.id_operacion, precio=precio_exec)
        return eventos

    # --- b4: Cierre total por retroceso desde entrada (sin avance significativo) ---
    if op.tipo == "LONG":
        if (low < entrada and op.precio_max <= entrada and low > sl and
                low <= op.strategy.limite_liq_retroceso_entrada()):
            precio_exec = aplicar_slippage(low, op.tipo, inv.slippage_close_pct, side="exit")
            comision = calcular_comision(precio_exec, op.cantidad, inv.commission_pct)
            pnl_net = op.cerrar_total(precio_exec, comision, ts)
            acreditar_capital(inv, op.capital_invertido + pnl_net)
            inv.registrar_pnl_realizado(pnl_net)
            eventos.append({
                "tipo_evento": "cierre_total",
                "motivo": "Retroceso desde entrada",
                "precio_exec": precio_exec,
                "comision": comision,
                "pnl_net": pnl_net,
                "op": op
            })
            if logger:
                logger.log("cierre_total", motivo="Retroceso desde entrada", op=op.id_operacion, precio=precio_exec)
            return eventos
    else:  # SHORT
        if (high > entrada and op.precio_min >= entrada and high < sl and
                high >= op.strategy.limite_liq_retroceso_entrada()):
            precio_exec = aplicar_slippage(high, op.tipo, inv.slippage_close_pct, side="exit")
            comision = calcular_comision(precio_exec, op.cantidad, inv.commission_pct)
            pnl_net = op.cerrar_total(precio_exec, comision, ts)
            acreditar_capital(inv, op.capital_invertido + pnl_net)
            inv.registrar_pnl_realizado(pnl_net)
            eventos.append({
                "tipo_evento": "cierre_total",
                "motivo": "Retroceso desde entrada",
                "precio_exec": precio_exec,
                "comision": comision,
                "pnl_net": pnl_net,
                "op": op
            })
            if logger:
                logger.log("cierre_total", motivo="Retroceso desde entrada", op=op.id_operacion, precio=precio_exec)
            return eventos

    # --- b5: Cierre total por retroceso desde máximo alcanzado (tras avance mínimo) ---
    if op.tipo == "LONG":
        avance_minimo = (tp - entrada) * op.strategy.avance_minimo_pct / 100.0
        limite_retroceso_precio = op.precio_max - ((op.precio_max - entrada) * op.strategy.limite_retroceso_max())
        if (high > entrada and op.precio_max >= entrada + avance_minimo and
                low < op.precio_max and low <= limite_retroceso_precio):
            precio_exec = aplicar_slippage(low, op.tipo, inv.slippage_close_pct, side="exit")
            comision = calcular_comision(precio_exec, op.cantidad, inv.commission_pct)
            pnl_net = op.cerrar_total(precio_exec, comision, ts)
            acreditar_capital(inv, op.capital_invertido + pnl_net)
            inv.registrar_pnl_realizado(pnl_net)
            eventos.append({
                "tipo_evento": "cierre_total",
                "motivo": "Retroceso desde máximo",
                "precio_exec": precio_exec,
                "comision": comision,
                "pnl_net": pnl_net,
                "op": op
            })
            if logger:
                logger.log("cierre_total", motivo="Retroceso desde máximo", op=op.id_operacion, precio=precio_exec)
            return eventos
    else:  # SHORT
        avance_minimo = (entrada - tp) * op.strategy.avance_minimo_pct / 100.0
        if (low < entrada and op.precio_min <= entrada - avance_minimo and
                high > op.precio_min and high >= op.strategy.limite_retroceso_max()):
            precio_exec = aplicar_slippage(high, op.tipo, inv.slippage_close_pct, side="exit")
            comision = calcular_comision(precio_exec, op.cantidad, inv.commission_pct)
            pnl_net = op.cerrar_total(precio_exec, comision, ts)
            acreditar_capital(inv, op.capital_invertido + pnl_net)
            inv.registrar_pnl_realizado(pnl_net)
            eventos.append({
                "tipo_evento": "cierre_total",
                "motivo": "Retroceso desde máximo",
                "precio_exec": precio_exec,
                "comision": comision,
                "pnl_net": pnl_net,
                "op": op
            })
            if logger:
                logger.log("cierre_total", motivo="Retroceso desde máximo", op=op.id_operacion, precio=precio_exec)
            return eventos

    # Si ninguna condición se cumple, no se cierra nada
    return eventos
