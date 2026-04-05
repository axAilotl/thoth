"""Pipeline stage registry centralizing stage metadata and configuration checks."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, Optional, Sequence, Tuple, TYPE_CHECKING

from .config import config

if TYPE_CHECKING:  # pragma: no cover - used for type hints only
    from .config import Config

logger = logging.getLogger(__name__)


Predicate = Callable[["Config"], bool]


@dataclass(frozen=True)
class PipelineStage:
    """Metadata describing a pipeline stage and its configuration dependencies."""

    name: str
    config_path: str
    description: str = ""
    processor: Optional[str] = None
    capabilities: Tuple[str, ...] = field(default_factory=tuple)
    required_config: Tuple[str, ...] = field(default_factory=tuple)
    config_keys: Tuple[str, ...] = field(default_factory=tuple)
    predicate: Optional[Predicate] = None


class PipelineRegistry:
    """Registry storing pipeline stage declarations provided by processors."""

    def __init__(self) -> None:
        self._stages: Dict[str, PipelineStage] = {}

    def register_stage(self, stage: PipelineStage) -> PipelineStage:
        """Register a pipeline stage definition if it has not already been declared."""
        existing = self._stages.get(stage.name)
        if existing:
            if existing != stage:
                logger.warning(
                    "Conflicting pipeline stage registration for %s; keeping first declaration", stage.name
                )
            return existing
        self._stages[stage.name] = stage
        logger.debug("Registered pipeline stage: %s", stage.name)
        return stage

    def register_many(self, stages: Sequence[PipelineStage]) -> None:
        """Register multiple stages in one call."""
        for stage in stages:
            self.register_stage(stage)

    def is_enabled(self, stage_name: str) -> bool:
        """Determine whether a stage is enabled per configuration and predicates."""
        stage = self._stages.get(stage_name)
        config_path = stage.config_path if stage else stage_name
        if not config.is_pipeline_stage_enabled(config_path):
            return False

        if not stage:
            return True

        for cfg_path in stage.required_config:
            if not self._is_truthy(cfg_path):
                return False

        if stage.predicate and not stage.predicate(config):
            return False

        return True

    def any_enabled(self, stage_names: Iterable[str]) -> bool:
        """Return True if any of the provided stages are enabled."""
        return any(self.is_enabled(name) for name in stage_names)

    def get_stage(self, stage_name: str) -> Optional[PipelineStage]:
        """Retrieve a registered stage by name."""
        return self._stages.get(stage_name)

    def all_stages(self) -> Tuple[PipelineStage, ...]:
        """Return a tuple of all registered stages."""
        return tuple(self._stages.values())

    def stages_for_processor(self, processor_name: str) -> Tuple[PipelineStage, ...]:
        """Return stages declared by a specific processor."""
        return tuple(stage for stage in self._stages.values() if stage.processor == processor_name)

    def _is_truthy(self, cfg_path: str) -> bool:
        """Evaluate a configuration path as truthy."""
        value = config.get(cfg_path)
        return bool(value)


pipeline_registry = PipelineRegistry()


def register_pipeline_stage(stage: PipelineStage) -> PipelineStage:
    """Convenience helper mirroring :meth:`PipelineRegistry.register_stage`."""
    return pipeline_registry.register_stage(stage)


def register_pipeline_stages(*stages: PipelineStage) -> None:
    """Register multiple pipeline stages."""
    pipeline_registry.register_many(stages)
