# English-only comments
from typing import Optional
from pivots.pivot_buffer import PivotBufferRegistry

_pivot_registry: Optional[PivotBufferRegistry] = None

def get_pivot_registry() -> PivotBufferRegistry:
    """
    Returns a shared PivotBufferRegistry singleton for the whole process.
    """
    global _pivot_registry
    if _pivot_registry is None:
        _pivot_registry = PivotBufferRegistry()
    return _pivot_registry
