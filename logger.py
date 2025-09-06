# simulator/logger.py
# Logger de eventos en memoria con callback a persistencia
# v1.0.0

from typing import List, Dict, Any, Optional, Callable
from dataclasses import dataclass, field

PersistFn = Callable[[Dict[str, Any]], None]

@dataclass
class EventLogger:
    eventos: List[Dict[str, Any]] = field(default_factory=list)
    persist_callback: Optional[PersistFn] = None

    def log(self, tipo: str, **data):
        evt = {"tipo": tipo, **data}
        self.eventos.append(evt)
        if self.persist_callback:
            try:
                self.persist_callback(evt)
            except Exception:
                pass