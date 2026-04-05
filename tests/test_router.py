"""
Tests for CapabilityRouter in core/router.py.
"""

import pytest
from unittest.mock import MagicMock, patch
from core.router import CapabilityRouter, Capability, RouteResult, ProcessorInfo
from core.pipeline_registry import PipelineStage, PipelineRegistry


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def mock_registry():
    registry = PipelineRegistry()
    
    # Register some dummy stages with capabilities
    registry.register_stage(PipelineStage(
        name="media_download",
        config_path="pipeline.stages.media_download",
        processor="media_processor",
        capabilities=(Capability.MEDIA_DOWNLOAD,)
    ))
    
    registry.register_stage(PipelineStage(
        name="url_expansion",
        config_path="pipeline.stages.url_expansion",
        processor="url_processor",
        capabilities=(Capability.URL_EXPANSION,)
    ))
    
    registry.register_stage(PipelineStage(
        name="llm_tags",
        config_path="pipeline.stages.llm_processing.tweet_tags",
        processor="llm_processor",
        capabilities=(Capability.LLM_TAGS, Capability.LLM_SUMMARIES)
    ))
    
    return registry


def test_router_build(mock_registry):
    router = CapabilityRouter(registry=mock_registry)
    router.build()
    
    # Check if capabilities are mapped correctly
    assert Capability.MEDIA_DOWNLOAD in router._capability_map
    assert Capability.URL_EXPANSION in router._capability_map
    assert Capability.LLM_TAGS in router._capability_map
    assert Capability.LLM_SUMMARIES in router._capability_map
    
    # Check processor info
    media_procs = router.get_processors_for_capability(Capability.MEDIA_DOWNLOAD)
    assert len(media_procs) == 1
    assert media_procs[0].name == "media_processor"
    
    llm_procs = router.get_processors_for_capability(Capability.LLM_SUMMARIES)
    assert len(llm_procs) == 1
    assert llm_procs[0].name == "llm_processor"


def test_router_route(mock_registry):
    router = CapabilityRouter(registry=mock_registry)
    
    # Mock registry.is_enabled to always return True for this test
    mock_registry.is_enabled = MagicMock(return_value=True)
    
    result = router.route(Capability.MEDIA_DOWNLOAD)
    assert result.enabled is True
    assert result.processor_name == "media_processor"
    assert result.stage_name == "media_download"
    
    # Test disabled capability
    mock_registry.is_enabled.return_value = False
    result = router.route(Capability.MEDIA_DOWNLOAD)
    assert result.enabled is False
    assert "disabled" in result.reason


def test_router_dispatch(mock_registry):
    router = CapabilityRouter(registry=mock_registry)
    mock_registry.is_enabled = MagicMock(return_value=True)
    
    # Register a handler
    handler = MagicMock(return_value="processed")
    router.register_handler("media_processor", handler)
    
    artifact = MagicMock()
    result = router.dispatch(Capability.MEDIA_DOWNLOAD, artifact, some_arg="value")
    
    assert result == "processed"
    handler.assert_called_once_with(artifact, some_arg="value")


@pytest.mark.anyio
async def test_router_dispatch_async(mock_registry):
    router = CapabilityRouter(registry=mock_registry)
    mock_registry.is_enabled = MagicMock(return_value=True)
    
    # Register an async handler
    async def async_handler(art, **kwargs):
        return f"async_processed_{kwargs.get('val')}"
        
    router.register_handler("llm_processor", async_handler)
    
    artifact = MagicMock()
    result = await router.dispatch_async(Capability.LLM_TAGS, artifact, val="test")
    
    assert result == "async_processed_test"


def test_router_unregistered_capability(mock_registry):
    router = CapabilityRouter(registry=mock_registry)
    result = router.route("unknown_capability")
    assert result.enabled is False
    assert "No processor registered" in result.reason
