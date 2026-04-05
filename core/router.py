"""
Capability Router - Maps capabilities to processors for artifact dispatch.

Routes artifacts to appropriate processors based on declared capabilities.
Integrates with pipeline_registry for backward compatibility.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple, TYPE_CHECKING, Any, Callable
from enum import Enum

from .pipeline_registry import PipelineStage, pipeline_registry

if TYPE_CHECKING:  # pragma: no cover
    from .pipeline_registry import PipelineRegistry

logger = logging.getLogger(__name__)


class Capability(str, Enum):
    """Standard capabilities that processors can declare."""

    # Content extraction
    URL_EXPANSION = "url_expansion"
    MEDIA_DOWNLOAD = "media_download"

    # Document processing
    ARXIV_PAPERS = "arxiv_papers"
    PDF_DOCUMENTS = "pdf_documents"
    GITHUB_READMES = "github_readmes"
    HUGGINGFACE_READMES = "huggingface_readmes"

    # Video/audio processing
    YOUTUBE_METADATA = "youtube_metadata"
    YOUTUBE_TRANSCRIPTS = "youtube_transcripts"
    TWITTER_VIDEO_TRANSCRIPTS = "twitter_video_transcripts"

    # LLM processing
    LLM_TAGS = "llm_tags"
    LLM_SUMMARIES = "llm_summaries"
    LLM_ALT_TEXT = "llm_alt_text"
    LLM_README_SUMMARIES = "llm_readme_summaries"

    # Output generation
    TWEET_MARKDOWN = "tweet_markdown"
    THREAD_MARKDOWN = "thread_markdown"


@dataclass
class RouteResult:
    """Result of routing an artifact to processors."""

    capability: str
    processor_name: Optional[str] = None
    stage_name: Optional[str] = None
    handler: Optional[Callable] = None
    enabled: bool = False
    reason: str = ""


@dataclass
class ProcessorInfo:
    """Information about a registered processor."""

    name: str
    capabilities: Tuple[str, ...]
    stages: Tuple[PipelineStage, ...]
    handler: Optional[Callable] = None


class CapabilityRouter:
    """
    Routes artifacts to processors based on declared capabilities.

    Uses pipeline_registry as the source of truth for capability declarations.
    Provides both capability-based and stage-based routing for backward
    compatibility.
    """

    def __init__(self, registry: Optional[PipelineRegistry] = None):
        """Initialize router with optional registry override."""
        self._registry = registry or pipeline_registry
        self._capability_map: Dict[str, List[ProcessorInfo]] = {}
        self._processor_handlers: Dict[str, Callable] = {}
        self._built = False

    def register_handler(self, processor_name: str, handler: Callable) -> None:
        """
        Register a handler function for a processor.

        Handlers are called when route() dispatches to that processor.

        Args:
            processor_name: Name of the processor (e.g., "media_processor")
            handler: Async or sync callable that processes artifacts
        """
        self._processor_handlers[processor_name] = handler
        logger.debug("Registered handler for processor: %s", processor_name)

    def build(self) -> None:
        """
        Build capability map from registered pipeline stages.

        Scans all stages in the registry and indexes them by capability.
        Must be called after all processors have registered their stages.
        """
        self._capability_map.clear()

        # Group stages by processor
        processor_stages: Dict[str, List[PipelineStage]] = {}
        for stage in self._registry.all_stages():
            processor_name = stage.processor or "unknown"
            if processor_name not in processor_stages:
                processor_stages[processor_name] = []
            processor_stages[processor_name].append(stage)

        # Build capability map
        for processor_name, stages in processor_stages.items():
            all_caps: Set[str] = set()
            for stage in stages:
                all_caps.update(stage.capabilities)

            if not all_caps:
                continue

            info = ProcessorInfo(
                name=processor_name,
                capabilities=tuple(all_caps),
                stages=tuple(stages),
                handler=self._processor_handlers.get(processor_name),
            )

            for cap in all_caps:
                if cap not in self._capability_map:
                    self._capability_map[cap] = []
                self._capability_map[cap].append(info)

        self._built = True
        logger.debug(
            "Built capability map: %d capabilities, %d processors",
            len(self._capability_map),
            len(processor_stages),
        )

    def route(self, capability: str) -> RouteResult:
        """
        Route an artifact to the appropriate processor for a capability.

        Args:
            capability: The capability needed (e.g., "media_download")

        Returns:
            RouteResult with processor info and handler if found
        """
        if not self._built:
            self.build()

        processors = self._capability_map.get(capability, [])

        if not processors:
            return RouteResult(
                capability=capability,
                enabled=False,
                reason=f"No processor registered for capability: {capability}",
            )

        # Find first enabled processor for this capability
        for proc_info in processors:
            # Check if any stage for this processor+capability is enabled
            for stage in proc_info.stages:
                if capability in stage.capabilities:
                    stage_enabled = self._registry.is_enabled(stage.name)
                    if stage_enabled:
                        return RouteResult(
                            capability=capability,
                            processor_name=proc_info.name,
                            stage_name=stage.name,
                            handler=proc_info.handler,
                            enabled=True,
                            reason="",
                        )

        # Capability exists but all stages disabled
        return RouteResult(
            capability=capability,
            enabled=False,
            reason=f"All stages for capability '{capability}' are disabled",
        )

    def route_all(self, capabilities: List[str]) -> List[RouteResult]:
        """
        Route multiple capabilities and return all results.

        Args:
            capabilities: List of capabilities to route

        Returns:
            List of RouteResult objects in same order as input
        """
        return [self.route(cap) for cap in capabilities]

    def get_enabled_capabilities(self) -> Set[str]:
        """
        Get all capabilities that have at least one enabled processor.

        Returns:
            Set of capability names with enabled processors
        """
        if not self._built:
            self.build()

        enabled: Set[str] = set()
        for cap, processors in self._capability_map.items():
            for proc_info in processors:
                for stage in proc_info.stages:
                    if cap in stage.capabilities and self._registry.is_enabled(
                        stage.name
                    ):
                        enabled.add(cap)
                        break
        return enabled

    def get_disabled_capabilities(self) -> Set[str]:
        """
        Get all capabilities that have no enabled processors.

        Returns:
            Set of capability names with no enabled processors
        """
        if not self._built:
            self.build()

        all_caps = set(self._capability_map.keys())
        return all_caps - self.get_enabled_capabilities()

    def get_processors_for_capability(self, capability: str) -> List[ProcessorInfo]:
        """
        Get all processors that declare a capability.

        Args:
            capability: The capability to look up

        Returns:
            List of ProcessorInfo objects (may be empty)
        """
        if not self._built:
            self.build()
        return self._capability_map.get(capability, []).copy()

    def get_capabilities_for_processor(self, processor_name: str) -> Tuple[str, ...]:
        """
        Get all capabilities declared by a processor.

        Args:
            processor_name: Name of the processor

        Returns:
            Tuple of capability names
        """
        stages = self._registry.stages_for_processor(processor_name)
        caps: Set[str] = set()
        for stage in stages:
            caps.update(stage.capabilities)
        return tuple(caps)

    def dispatch(
        self,
        capability: str,
        artifact: Any,
        **kwargs,
    ) -> Any:
        """
        Route and immediately dispatch an artifact to a processor.

        Convenience method that combines route() and handler invocation.

        Args:
            capability: The capability needed
            artifact: The artifact to process
            **kwargs: Additional arguments passed to the handler

        Returns:
            Result from the handler, or None if no handler/disabled

        Raises:
            ValueError: If no handler registered for routed processor
        """
        result = self.route(capability)

        if not result.enabled:
            logger.debug(
                "Skipping dispatch for disabled capability: %s (%s)",
                capability,
                result.reason,
            )
            return None

        if not result.handler:
            raise ValueError(
                f"No handler registered for processor: {result.processor_name}"
            )

        return result.handler(artifact, **kwargs)

    async def dispatch_async(
        self,
        capability: str,
        artifact: Any,
        **kwargs,
    ) -> Any:
        """
        Route and asynchronously dispatch an artifact to a processor.

        Async version of dispatch() for use with async handlers.

        Args:
            capability: The capability needed
            artifact: The artifact to process
            **kwargs: Additional arguments passed to the handler

        Returns:
            Result from the handler, or None if no handler/disabled

        Raises:
            ValueError: If no handler registered for routed processor
        """
        result = self.route(capability)

        if not result.enabled:
            logger.debug(
                "Skipping async dispatch for disabled capability: %s (%s)",
                capability,
                result.reason,
            )
            return None

        if not result.handler:
            raise ValueError(
                f"No handler registered for processor: {result.processor_name}"
            )

        import asyncio
        import inspect

        if inspect.iscoroutinefunction(result.handler):
            return await result.handler(artifact, **kwargs)
        else:
            # Run sync handler in thread pool
            return await asyncio.to_thread(result.handler, artifact, **kwargs)


# Singleton instance for convenience
capability_router = CapabilityRouter()


def get_capability_router() -> CapabilityRouter:
    """Get the global capability router instance."""
    return capability_router


def register_processor_handler(processor_name: str, handler: Callable) -> None:
    """Convenience function to register a handler on the global router."""
    capability_router.register_handler(processor_name, handler)


def route_capability(capability: str) -> RouteResult:
    """Convenience function to route using the global router."""
    return capability_router.route(capability)


def dispatch_capability(capability: str, artifact: Any, **kwargs) -> Any:
    """Convenience function to dispatch using the global router."""
    return capability_router.dispatch(capability, artifact, **kwargs)


async def dispatch_capability_async(capability: str, artifact: Any, **kwargs) -> Any:
    """Convenience function to async dispatch using the global router."""
    return await capability_router.dispatch_async(capability, artifact, **kwargs)
