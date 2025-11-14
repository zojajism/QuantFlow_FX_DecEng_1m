# file: buffers/tick_registry_provider.py
# English-only comments

from __future__ import annotations
from typing import Optional

from buffers.tick_registry import TickRegistry

_TICK_REGISTRY: Optional[TickRegistry] = None


def init_tick_registry(capacity: int = 5) -> None:
    """
    Optional explicit initializer.
    If not called, get_tick_registry() will lazy-init with default capacity.
    """
    global _TICK_REGISTRY
    if _TICK_REGISTRY is None:
        _TICK_REGISTRY = TickRegistry(capacity=capacity)


def get_tick_registry() -> TickRegistry:
    """
    Global accessor for the singleton TickRegistry.
    Lazily initializes the registry if needed.
    """
    global _TICK_REGISTRY
    if _TICK_REGISTRY is None:
        _TICK_REGISTRY = TickRegistry(capacity=5)
    return _TICK_REGISTRY
