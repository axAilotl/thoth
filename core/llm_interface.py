"""
LLM Interface - Abstract interface for different LLM providers
Supports OpenAI, OpenRouter, Anthropic, and local models
"""

import os
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import asyncio

logger = logging.getLogger(__name__)


@dataclass
class LLMResponse:
    """Response from LLM"""
    content: str
    model: str
    provider: str
    tokens_used: Optional[int] = None
    cost: Optional[float] = None
    error: Optional[str] = None


class BaseLLMProvider(ABC):
    """Abstract base class for LLM providers"""
    
    def __init__(self, api_key: str = None, base_url: str = None, model: str = None):
        self.api_key = api_key
        self.base_url = base_url
        self.model = model
    
    @abstractmethod
    async def generate(self, prompt: str, system_prompt: str = None, **kwargs) -> LLMResponse:
        """Generate text from prompt"""
        pass
    
    @abstractmethod
    def get_provider_name(self) -> str:
        """Get provider name"""
        pass


class OpenAIProvider(BaseLLMProvider):
    """OpenAI provider"""
    
    def __init__(self, api_key: str = None, model: str = "gpt-4o-mini"):
        super().__init__(api_key=api_key, model=model)
        try:
            logger.debug(f"🤖 [LLM] CALL provider=openai task=generic model={self.model}")
            import openai
            self.client = openai.AsyncOpenAI(api_key=self.api_key)
        except ImportError:
            raise ImportError("openai package required for OpenAI provider")
    
    def _supports_temperature(self, model: str) -> bool:
        """Check if model supports temperature parameter"""
        # o1/o3/gpt-5 reasoning models only support temperature=1
        model_lower = model.lower()
        no_temp_prefixes = ('o1', 'o3', 'o4', 'gpt-5')
        return not any(model_lower.startswith(p) or f'/{p}' in model_lower for p in no_temp_prefixes)

    async def generate(self, prompt: str, system_prompt: str = None, **kwargs) -> LLMResponse:
        try:
            logger.debug(f"🤖 [LLM] CALL provider=openai task=generic model={self.model}")
            messages = []
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            messages.append({"role": "user", "content": prompt})

            # Use max_completion_tokens (newer API) for OpenAI direct calls
            # This is required for o1/o3/gpt-5+ models and supported by recent gpt-4o models
            create_kwargs = {
                "model": self.model,
                "messages": messages,
                "max_completion_tokens": kwargs.get('max_tokens', 1000),
            }

            # Only set temperature for models that support it (not o1/o3/gpt-5)
            if self._supports_temperature(self.model):
                create_kwargs["temperature"] = kwargs.get('temperature', 0.7)

            response = await self.client.chat.completions.create(**create_kwargs)

            # Safe access with bounds checking
            if not response.choices:
                return LLMResponse(
                    content="",
                    model=self.model,
                    provider="openai",
                    error="Empty response from API"
                )

            content = response.choices[0].message.content or ""
            tokens = response.usage.total_tokens if response.usage else None

            return LLMResponse(
                content=content,
                model=self.model,
                provider="openai",
                tokens_used=tokens
            )
        except Exception as e:
            logger.error(f"OpenAI generation error: {e}")
            return LLMResponse(
                content="",
                model=self.model,
                provider="openai",
                error=str(e)
            )

    def get_provider_name(self) -> str:
        return "openai"


class OpenRouterProvider(BaseLLMProvider):
    """OpenRouter provider using OpenAI client with configurable base URL"""

    def __init__(self, api_key: str = None, base_url: str = "https://openrouter.ai/api/v1", model: str = "anthropic/claude-3-haiku"):
        super().__init__(api_key=api_key, model=model, base_url=base_url)
        try:
            import openai
            self.client = openai.AsyncOpenAI(
                api_key=self.api_key,
                base_url=self.base_url
            )
        except ImportError:
            raise ImportError("openai package required for OpenRouter provider")

    async def generate(self, prompt: str, system_prompt: str = None, **kwargs) -> LLMResponse:
        try:
            logger.debug(f"🤖 [LLM] CALL provider=openrouter task=generic model={self.model}")
            messages = []
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            messages.append({"role": "user", "content": prompt})

            response = await self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                max_tokens=kwargs.get('max_tokens', 1000),
                temperature=kwargs.get('temperature', 0.7)
            )

            # Safe access with bounds checking
            if not response.choices:
                return LLMResponse(
                    content="",
                    model=self.model,
                    provider="openrouter",
                    error="Empty response from API"
                )

            content = response.choices[0].message.content or ""
            tokens = response.usage.total_tokens if response.usage else None
            
            return LLMResponse(
                content=content,
                model=self.model,
                provider="openrouter",
                tokens_used=tokens
            )
        except Exception as e:
            logger.error(f"OpenRouter generation error: {e}")
            return LLMResponse(
                content="",
                model=self.model,
                provider="openrouter",
                error=str(e)
            )
    
    def get_provider_name(self) -> str:
        return "openrouter"


class AnthropicProvider(BaseLLMProvider):
    """Anthropic provider"""
    
    def __init__(self, api_key: str = None, model: str = "claude-3-haiku-20240307"):
        super().__init__(api_key=api_key, model=model)
        try:
            import anthropic
            self.client = anthropic.AsyncAnthropic(api_key=self.api_key)
        except ImportError:
            raise ImportError("anthropic package required for Anthropic provider")
    
    async def generate(self, prompt: str, system_prompt: str = None, **kwargs) -> LLMResponse:
        try:
            logger.debug(f"🤖 [LLM] CALL provider=anthropic task=generic model={self.model}")
            message_kwargs = {
                "model": self.model,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": kwargs.get('max_tokens', 1000),
                "temperature": kwargs.get('temperature', 0.7)
            }

            if system_prompt:
                message_kwargs["system"] = system_prompt

            response = await self.client.messages.create(**message_kwargs)

            # Safe access with bounds checking
            if not response.content:
                return LLMResponse(
                    content="",
                    model=self.model,
                    provider="anthropic",
                    error="Empty response from API"
                )

            content = response.content[0].text if response.content else ""
            tokens = (response.usage.input_tokens + response.usage.output_tokens) if response.usage else None

            return LLMResponse(
                content=content,
                model=self.model,
                provider="anthropic",
                tokens_used=tokens
            )
        except Exception as e:
            logger.error(f"Anthropic generation error: {e}")
            return LLMResponse(
                content="",
                model=self.model,
                provider="anthropic",
                error=str(e)
            )

    def get_provider_name(self) -> str:
        return "anthropic"


class LocalProvider(BaseLLMProvider):
    """Local LLM provider using OpenAI-compatible endpoint"""

    def __init__(self, base_url: str = "http://localhost:11434/v1", model: str = "llama3.2"):
        super().__init__(base_url=base_url, model=model)
        try:
            import openai
            self.client = openai.AsyncOpenAI(
                api_key="ollama",  # Ollama doesn't require real API key
                base_url=self.base_url
            )
        except ImportError:
            raise ImportError("openai package required for Local provider")

    async def generate(self, prompt: str, system_prompt: str = None, **kwargs) -> LLMResponse:
        try:
            logger.debug(f"🤖 [LLM] CALL provider=local task=generic model={self.model}")
            messages = []
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            messages.append({"role": "user", "content": prompt})

            response = await self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                max_tokens=kwargs.get('max_tokens', 1000),
                temperature=kwargs.get('temperature', 0.7)
            )

            # Safe access with bounds checking
            if not response.choices:
                return LLMResponse(
                    content="",
                    model=self.model,
                    provider="local",
                    error="Empty response from API"
                )

            content = response.choices[0].message.content or ""
            tokens = response.usage.total_tokens if response.usage else None

            return LLMResponse(
                content=content,
                model=self.model,
                provider="local",
                tokens_used=tokens
            )
        except Exception as e:
            logger.error(f"Local LLM generation error: {e}")
            return LLMResponse(
                content="",
                model=self.model,
                provider="local",
                error=str(e)
            )
    
    def get_provider_name(self) -> str:
        return "local"


class LLMInterface:
    """Main LLM interface that handles provider switching"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.providers: Dict[str, BaseLLMProvider] = {}
        self.provider_models: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._setup_providers()
    
    def _setup_providers(self):
        """Setup all configured providers"""
        try:
            # Load environment variables
            from dotenv import load_dotenv
            load_dotenv()
            provider_cfg = self.config.get('providers', {})
            default_env = {
                'openai': 'OPENAI_API_KEY',
                'openrouter': 'OPEN_ROUTER_API_KEY',
                'anthropic': 'ANTHROPIC_API',
                'local': None
            }

            for name, cfg in provider_cfg.items():
                if not cfg.get('enabled', False):
                    continue

                models = cfg.get('models', {}) or {}
                self.provider_models[name] = models
                default_model = models.get('default', {}).get('id')

                env_var = cfg.get('api_key_env') or default_env.get(name)
                api_key = os.getenv(env_var) if env_var else None

                try:
                    if name == 'openai':
                        if not api_key:
                            logger.warning("OpenAI provider enabled but OPENAI_API_KEY missing")
                            continue
                        instance = OpenAIProvider(api_key=api_key, model=default_model or 'gpt-4o-mini')
                    elif name == 'openrouter':
                        if not api_key:
                            logger.warning("OpenRouter provider enabled but OPEN_ROUTER_API_KEY missing")
                            continue
                        base_url = cfg.get('base_url', 'https://openrouter.ai/api/v1')
                        instance = OpenRouterProvider(api_key=api_key, base_url=base_url, model=default_model or 'anthropic/claude-3-haiku')
                    elif name == 'anthropic':
                        if not api_key:
                            logger.warning("Anthropic provider enabled but ANTHROPIC_API missing")
                            continue
                        instance = AnthropicProvider(api_key=api_key, model=default_model or 'claude-3-haiku-20240307')
                    elif name == 'local':
                        base_url = cfg.get('base_url', 'http://localhost:11434/v1')
                        instance = LocalProvider(base_url=base_url, model=default_model or 'llama3.2')
                    else:
                        logger.warning(f"Unknown LLM provider '{name}' - skipping")
                        continue

                    self.providers[name] = instance
                    logger.info(f"LLM provider '{name}' initialized")
                except Exception as provider_error:
                    logger.error(f"Failed to initialize provider '{name}': {provider_error}")
                    self.provider_models.pop(name, None)

        except Exception as e:
            logger.error(f"Error setting up LLM providers: {e}")
    
    def get_provider(self, provider_name: str) -> Optional[BaseLLMProvider]:
        """Get a specific provider"""
        return self.providers.get(provider_name)

    def _resolve_task_route(self, task: str) -> Optional[tuple]:
        """Resolve the first available provider/model route for a task"""
        task_cfg = self.config.get('tasks', {}).get(task, {})
        if not task_cfg.get('enabled', True):
            return None

        for option in task_cfg.get('fallback', []):
            provider_name = option.get('provider')
            if not provider_name:
                continue
            provider_instance = self.providers.get(provider_name)
            models = self.provider_models.get(provider_name, {})
            if not provider_instance:
                continue

            model_alias = option.get('model')
            model_cfg = {}
            model_id = None
            if model_alias:
                if model_alias in models:
                    model_cfg = models[model_alias]
                    model_id = model_cfg.get('id')
                else:
                    model_id = model_alias
            if not model_id:
                model_cfg = models.get('default', {})
                model_id = model_cfg.get('id')
            if not model_id:
                continue

            return provider_name, model_id, model_cfg

        return None

    def resolve_task_route(self, task: str) -> Optional[tuple]:
        """Public helper to expose task route resolution"""
        return self._resolve_task_route(task)

    async def generate(self, prompt: str, system_prompt: str = None, provider: str = None, model: str = None, **kwargs) -> LLMResponse:
        """Generate text using specified or default provider"""
        if not provider:
            # Fallback to default route from any task (first available provider)
            if not self.providers:
                return LLMResponse(
                    content="",
                    model="none",
                    provider="none",
                    error="No LLM providers configured"
                )
            provider = next(iter(self.providers.keys()))

        selected_provider = self.providers.get(provider)
        if not selected_provider:
            return LLMResponse(
                content="",
                model="none",
                provider=provider,
                error=f"Provider '{provider}' not available"
            )

        models = self.provider_models.get(provider, {})
        model_id = model
        if not model_id:
            model_id = models.get('default', {}).get('id')

        if model_id and hasattr(selected_provider, 'model'):
            original_model = selected_provider.model
            selected_provider.model = model_id
            try:
                result = await selected_provider.generate(prompt, system_prompt, **kwargs)
                return result
            finally:
                selected_provider.model = original_model
        else:
            return await selected_provider.generate(prompt, system_prompt, **kwargs)
    
    async def generate_tags(self, content: str) -> List[str]:
        """Generate tags for content"""
        route = self._resolve_task_route('tags')
        if not route:
            return []
        provider, model_id, model_cfg = route
        
        system_prompt = self.config.get('prompts', {}).get('tags', 
            "You are a tagging system. Generate 4-10 relevant tags for the given content. Return only comma-separated tags, no explanations.")
        
        response = await self.generate(
            prompt=f"Generate tags for this content:\n\n{content}",
            system_prompt=system_prompt,
            provider=provider,
            model=model_id,
            max_tokens=model_cfg.get('max_tokens', 100),
            temperature=model_cfg.get('temperature', 0.3)
        )
        
        if response.error:
            logger.error(f"Tag generation error: {response.error}")
            return []
        
        # Parse tags from response
        tags = [tag.strip() for tag in response.content.split(',') if tag.strip()]
        return tags[:10]  # Limit to 10 tags
    
    async def summarize_content(self, content: str, content_type: str = "text") -> str:
        """Summarize content (tweets, threads, READMEs)"""
        route = self._resolve_task_route('summary')
        if not route:
            return "Summary unavailable - no LLM provider configured"
        provider, model_id, model_cfg = route
        
        # Get prompts from config
        summary_prompts = self.config.get('prompts', {}).get('summary', {})
        
        if content_type == "thread":
            system_prompt = summary_prompts.get('thread', 
                "You are a thread summarization system. Provide a concise 2-3 sentence summary of this Twitter thread, capturing the main points and key insights.")
        elif content_type == "readme":
            system_prompt = summary_prompts.get('readme',
                "You are a README summarization system. Provide a concise 2-3 sentence summary of this GitHub repository README, focusing on what the project does and its key features.")
        else:
            system_prompt = summary_prompts.get('tweet',
                "You are a content summarization system. Provide a concise 1-2 sentence summary of this content.")
        
        response = await self.generate(
            prompt=f"Summarize this {content_type}:\n\n{content}",
            system_prompt=system_prompt,
            provider=provider,
            model=model_id,
            max_tokens=model_cfg.get('max_tokens', 200),
            temperature=model_cfg.get('temperature', 0.5)
        )
        
        if response.error:
            logger.error(f"Summarization error: {response.error}")
            return f"Summary unavailable - {response.error}"
        
        return response.content.strip()
    
    async def generate_alt_text(self, image_path: str) -> str:
        """Generate alt text for image using vision model"""
        route = self._resolve_task_route('alt_text')
        if not route:
            return "Alt text unavailable - no vision provider configured"
        provider, model_id, model_cfg = route
        
        try:
            import base64
            from pathlib import Path
            
            # Read and encode image
            image_file = Path(image_path)
            if not image_file.exists():
                return "Alt text unavailable - image file not found"
            
            with open(image_file, 'rb') as f:
                image_data = base64.b64encode(f.read()).decode('utf-8')
            
            # Get file extension for MIME type
            ext = image_file.suffix.lower()
            mime_type = "image/jpeg" if ext in ['.jpg', '.jpeg'] else f"image/{ext[1:]}"
            
            # Get prompt from config
            system_prompt = self.config.get('prompts', {}).get('alt_text',
                "You are an image description system. Provide a concise, descriptive alt text for this image that would be useful for accessibility. Focus on the main content and context, keeping it under 125 characters.")
            
            # Use vision model if available
            selected_provider = self.providers.get(provider)
            if not selected_provider:
                return "Alt text unavailable - provider not available"
            
            # For OpenRouter, we need to use the vision model
            if provider == 'openrouter' and hasattr(selected_provider, 'client'):
                vision_model = model_id or model_cfg.get('id') or 'z-ai/glm-4.5v'
                
                logger.debug(f"Generating alt text with {vision_model} for image: {image_file.name}")
                
                try:
                    response = await selected_provider.client.chat.completions.create(
                        model=vision_model,
                        messages=[
                            {
                                "role": "system",
                                "content": system_prompt
                            },
                            {
                                "role": "user",
                                "content": [
                                    {
                                        "type": "text",
                                        "text": "Please provide alt text for this image."
                                    },
                                    {
                                        "type": "image_url",
                                        "image_url": {
                                            "url": f"data:{mime_type};base64,{image_data}"
                                        }
                                    }
                                ]
                            }
                        ],
                        max_tokens=150,
                        temperature=0.3
                    )
                    
                    if not response.choices:
                        logger.error(f"No response choices for alt text generation: {image_path}")
                        return "Alt text generation failed - no response"
                    
                    alt_text = response.choices[0].message.content
                    if not alt_text:
                        logger.error(f"Empty alt text response for: {image_path}")
                        return "Alt text generation failed - empty response"
                        
                    alt_text = alt_text.strip()
                    
                    # Ensure it's under 125 characters as requested
                    if len(alt_text) > 125:
                        alt_text = alt_text[:122] + "..."
                    
                    logger.debug(f"Generated alt text ({len(alt_text)} chars): {alt_text}")
                    return alt_text
                    
                except Exception as api_error:
                    logger.error(f"OpenRouter API error for alt text {image_path}: {api_error}")
                    return f"Alt text generation failed - API error"
            
            # For other providers, return placeholder for now
            response = await self.generate(
                prompt=f"Provide alt text for this image (base64 omitted)",
                system_prompt=system_prompt,
                provider=provider,
                model=model_id,
                max_tokens=model_cfg.get('max_tokens', 150),
                temperature=model_cfg.get('temperature', 0.3)
            )

            if response.error:
                logger.error(f"Alt text generation error for {image_path}: {response.error}")
                return "Alt text generation failed"

            alt_text = response.content.strip()
            if len(alt_text) > 125:
                alt_text = alt_text[:122] + "..."
            return alt_text
            
        except Exception as e:
            logger.error(f"Alt text generation error for {image_path}: {e}")
            return "Alt text unavailable - generation error"
