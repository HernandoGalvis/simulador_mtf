# simulator/strategy_cache.py
# Cache en memoria de parámetros de estrategias activas
# v1.1.0

from typing import Dict, Any, Iterable, Mapping
from simulator.models import StrategyParams

class StrategyCache:
    """
    Mantiene en memoria las estrategias activas:
      _cache[id_estrategia] = StrategyParams
    """

    def __init__(self):
        self._cache: Dict[int, StrategyParams] = {}

    def load_from_rows(self, rows: Iterable[Mapping[str, Any]]):
        """
        rows: iterable de dicts/rows con columnas:
          id_estrategia,
          avance_minimo_pct,
          porc_limite_retro,
          porc_retroceso_liquidacion_sl,
          porc_liquidacion_parcial_sl,
          porc_limite_retro_entrada,
          activa
        Solo guarda las activas (activa = True o NULL).
        """
        for r in rows:
            if not (r.get("activa", True)):
                continue
            sp = StrategyParams(
                avance_minimo_pct=float(r.get("avance_minimo_pct") or 0),
                porc_limite_retro=float(r.get("porc_limite_retro") or 0),
                porc_retroceso_liquidacion_sl=float(r.get("porc_retroceso_liquidacion_sl") or 0),
                porc_liquidacion_parcial_sl=float(r.get("porc_liquidacion_parcial_sl") or 0),
                porc_limite_retro_entrada=float(r.get("porc_limite_retro_entrada") or 0),
                max_parciales=1,
                habilitar_proteccion_ganancias=True,
                habilitar_parcial=True,
                habilitar_retroceso_sin_avance=True
            )
            self._cache[int(r["id_estrategia"])] = sp

    def set(self, id_estrategia: int, params: StrategyParams) -> StrategyParams:
        self._cache[id_estrategia] = params
        return params

    def get(self, id_estrategia: int) -> StrategyParams:
        return self._cache[id_estrategia]

    def exists(self, id_estrategia: int) -> bool:
        return id_estrategia in self._cache