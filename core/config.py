"""
Configuration management for Thoth
Simple configuration without external dependencies
"""

from pathlib import Path
from typing import Dict, Any, List
import json
import os
import logging

from .non_live_state import MIN_NON_LIVE_INTERVAL_HOURS

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_FILES = ["config.json", "control.json"]


def _validate_llm_task_route(
    *,
    errors: List[str],
    llm_tasks_config: Dict[str, Any],
    task_name: str,
) -> None:
    task_config = llm_tasks_config.get(task_name, {})
    if not task_config:
        return
    if not isinstance(task_config, dict):
        errors.append(f"llm.tasks.{task_name} must be an object")
        return
    if not task_config.get("enabled", False):
        return

    fallback = task_config.get("fallback")
    if not isinstance(fallback, list) or not fallback:
        errors.append(
            f"llm.tasks.{task_name}.fallback must be a non-empty list when {task_name} is enabled"
        )
        return

    if not any(
        isinstance(option, dict) and option.get("provider")
        for option in fallback
    ):
        errors.append(
            f"llm.tasks.{task_name}.fallback must include at least one provider when {task_name} is enabled"
        )


def load_env_file(env_path: str = '.env'):
    """Load environment variables from .env file if it exists"""
    env_file = Path(env_path)
    if env_file.exists():
        try:
            with open(env_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    # Skip empty lines and comments
                    if not line or line.startswith('#'):
                        continue
                    
                    # Parse KEY=VALUE format
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        
                        # Remove quotes if present
                        if (value.startswith('"') and value.endswith('"')) or \
                           (value.startswith("'") and value.endswith("'")):
                            value = value[1:-1]
                        
                        # Set environment variable if not already set
                        if key and not os.getenv(key):
                            os.environ[key] = value
            
            logger.debug(f"Loaded environment variables from {env_path}")
        except Exception as e:
            logger.warning(f"Could not load .env file {env_path}: {e}")
    else:
        logger.debug(f"No .env file found at {env_path}")


# Load environment variables from .env file on module import
load_env_file()


class Config:
    """Simple configuration manager"""
    
    def __init__(self):
        # Production runtime is driven by config.json plus control.json.
        self.data = {}
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value using dot notation"""
        keys = key.split('.')
        value = self.data
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def set(self, key: str, value: Any):
        """Set configuration value using dot notation"""
        keys = key.split('.')
        data = self.data
        
        for k in keys[:-1]:
            if k not in data:
                data[k] = {}
            data = data[k]
        
        data[keys[-1]] = value
    
    def load_from_file(self, config_file: str):
        """Load configuration from JSON file"""
        config_path = Path(config_file)
        if config_path.exists():
            try:
                with open(config_path, 'r') as f:
                    file_config = json.load(f)
                self._merge_config(self.data, file_config)
            except Exception as e:
                print(f"Warning: Could not load config file {config_file}: {e}")

    def reload(self, config_files: List[str] | None = None):
        """Reload configuration from one or more JSON files."""
        self.data = {}
        files = config_files or list(DEFAULT_CONFIG_FILES)
        for config_file in files:
            self.load_from_file(config_file)
    
    def _merge_config(self, base: Dict, override: Dict):
        """Recursively merge configuration dictionaries"""
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._merge_config(base[key], value)
            else:
                base[key] = value
    
    def validate(self) -> List[str]:
        """Validate configuration and return list of errors"""
        errors = []

        try:
            from .path_layout import build_path_layout

            path_layout = build_path_layout(self)
            path_layout.ensure_directories()
        except Exception as exc:
            errors.append(str(exc))
            path_layout = None

        try:
            from .archivist_topics import (
                load_archivist_topic_registry,
                resolve_archivist_topics_path,
            )

            explicit_archivist_path = bool(
                str(self.get("paths.archivist_topics_file", "") or "").strip()
            )
            archivist_registry_path = resolve_archivist_topics_path(self)
            archivist_registry = None
            if explicit_archivist_path or archivist_registry_path.exists():
                archivist_registry = load_archivist_topic_registry(
                    self,
                    required=explicit_archivist_path,
                )
        except Exception as exc:
            errors.append(str(exc))
            archivist_registry = None

        # Check required files exist
        required_files = ['paths.bookmarks_file', 'paths.cookies_file']
        for file_key in required_files:
            value = self.get(file_key)
            if not value:
                errors.append(f"Required path not configured: {file_key}")
                continue
            file_path = Path(value)
            if not file_path.exists():
                errors.append(f"Required file missing: {file_path} (config key: {file_key})")

        # Check directories can be created at their canonical locations.
        if path_layout:
            directory_targets = [
                ("paths.cache_dir", path_layout.cache_root),
                ("paths.images_dir", self.get("paths.images_dir")),
                ("paths.videos_dir", self.get("paths.videos_dir")),
                ("paths.media_dir", self.get("paths.media_dir")),
            ]

            for dir_key, value in directory_targets:
                if not value:
                    errors.append(f"Required directory path not configured: {dir_key}")
                    continue

                if dir_key == "paths.cache_dir":
                    dir_path = path_layout.cache_root
                else:
                    dir_path = Path(value)
                    if not dir_path.is_absolute():
                        dir_path = path_layout.vault_root / dir_path

                try:
                    dir_path.mkdir(parents=True, exist_ok=True)
                    if not dir_path.exists():
                        errors.append(
                            f"Cannot create directory: {dir_path} (config key: {dir_key})"
                        )
                except Exception as e:
                    errors.append(
                        f"Cannot create directory {dir_path}: {e} (config key: {dir_key})"
                    )
        
        # Validate rate limiting configuration
        rate_limit_config = self.get('rate_limit')
        if isinstance(rate_limit_config, dict):
            if rate_limit_config.get('requests_per_window', 0) <= 0:
                errors.append("rate_limit.requests_per_window must be positive")
            if rate_limit_config.get('window_duration', 0) <= 0:
                errors.append("rate_limit.window_duration must be positive")

        x_api_config = self.get('sources.x_api', {})
        if isinstance(x_api_config, dict) and x_api_config.get('enabled', False):
            required_x_api_keys = ('client_id', 'redirect_uri')
            for key in required_x_api_keys:
                value = x_api_config.get(key)
                if not value or not str(value).strip():
                    errors.append(
                        f"sources.x_api.{key} is required when X API auth is enabled"
                    )

            scopes = x_api_config.get('scopes')
            if not isinstance(scopes, list) or not scopes:
                errors.append(
                    "sources.x_api.scopes must be a non-empty list when X API auth is enabled"
                )
            else:
                required_scopes = {'bookmark.read', 'tweet.read', 'users.read'}
                missing = sorted(required_scopes.difference({str(scope) for scope in scopes}))
                if missing:
                    errors.append(
                        "sources.x_api.scopes must include: " + ", ".join(missing)
                    )

            if 'offline.access' not in {str(scope) for scope in scopes or []}:
                errors.append(
                    "sources.x_api.scopes must include offline.access so refresh tokens can be stored"
                )

            monitoring_config = x_api_config.get("monitoring", {}) or {}
            if monitoring_config:
                if not isinstance(monitoring_config, dict):
                    errors.append("sources.x_api.monitoring must be an object")
                elif monitoring_config.get("enabled", False):
                    accounts = monitoring_config.get("accounts")
                    if not isinstance(accounts, list) or not accounts:
                        errors.append(
                            "sources.x_api.monitoring.accounts must be a non-empty list when monitored-account capture is enabled"
                        )

                    webhook_secret_env = str(
                        monitoring_config.get("webhook_secret_env") or ""
                    ).strip()
                    if not webhook_secret_env:
                        errors.append(
                            "sources.x_api.monitoring.webhook_secret_env is required when monitored-account capture is enabled"
                        )
                    elif not os.getenv(webhook_secret_env):
                        errors.append(
                            f"{webhook_secret_env} is required when monitored-account capture is enabled"
                        )

                    normalized_scopes = {
                        str(scope).strip() for scope in scopes or [] if str(scope).strip()
                    }
                    if (
                        monitoring_config.get("auto_bookmark", True)
                        and "bookmark.write" not in normalized_scopes
                    ):
                        errors.append(
                            "sources.x_api.scopes must include bookmark.write when monitored-account auto-bookmarking is enabled"
                        )

                    x_monitor_cfg = llm_tasks_config.get("x_monitor", {})
                    if not isinstance(x_monitor_cfg, dict) or not x_monitor_cfg.get("enabled", False):
                        errors.append(
                            "llm.tasks.x_monitor must be enabled when monitored-account capture is enabled"
                        )

        x_api_sync_config = self.get("automation.x_api_sync", {})
        if x_api_sync_config:
            if not isinstance(x_api_sync_config, dict):
                errors.append("automation.x_api_sync must be an object")
            elif x_api_sync_config.get("enabled", False):
                try:
                    interval_hours = float(x_api_sync_config.get("interval_hours", 0))
                except (TypeError, ValueError):
                    interval_hours = 0
                if interval_hours < MIN_NON_LIVE_INTERVAL_HOURS:
                    errors.append(
                        "automation.x_api_sync.interval_hours must be at least "
                        f"{MIN_NON_LIVE_INTERVAL_HOURS:g}"
                    )

                try:
                    max_results = int(x_api_sync_config.get("max_results", 0))
                except (TypeError, ValueError):
                    max_results = 0
                if not 1 <= max_results <= 100:
                    errors.append("automation.x_api_sync.max_results must be between 1 and 100")

                max_pages = x_api_sync_config.get("max_pages")
                if max_pages is not None:
                    try:
                        parsed_max_pages = int(max_pages)
                    except (TypeError, ValueError):
                        parsed_max_pages = 0
                    if parsed_max_pages <= 0:
                        errors.append("automation.x_api_sync.max_pages must be positive when set")

        social_sync_config = self.get("automation.social_sync", {})
        if social_sync_config:
            if not isinstance(social_sync_config, dict):
                errors.append("automation.social_sync must be an object")
            elif social_sync_config.get("enabled", False):
                try:
                    social_interval_hours = float(social_sync_config.get("interval_hours", 0))
                except (TypeError, ValueError):
                    social_interval_hours = 0
                if social_interval_hours < MIN_NON_LIVE_INTERVAL_HOURS:
                    errors.append(
                        "automation.social_sync.interval_hours must be at least "
                        f"{MIN_NON_LIVE_INTERVAL_HOURS:g}"
                    )

        archivist_sync_config = self.get("automation.archivist", {})
        if archivist_sync_config:
            if not isinstance(archivist_sync_config, dict):
                errors.append("automation.archivist must be an object")
            elif archivist_sync_config.get("enabled", False):
                try:
                    archivist_interval_hours = float(
                        archivist_sync_config.get("interval_hours", 0)
                    )
                except (TypeError, ValueError):
                    archivist_interval_hours = 0
                if archivist_interval_hours < MIN_NON_LIVE_INTERVAL_HOURS:
                    errors.append(
                        "automation.archivist.interval_hours must be at least "
                        f"{MIN_NON_LIVE_INTERVAL_HOURS:g}"
                    )

        llm_config = self.get('llm', {})
        if not isinstance(llm_config, dict):
            llm_config = {}
        llm_tasks_config = llm_config.get("tasks", {})
        if not isinstance(llm_tasks_config, dict):
            llm_tasks_config = {}
        _validate_llm_task_route(
            errors=errors,
            llm_tasks_config=llm_tasks_config,
            task_name="translation",
        )
        _validate_llm_task_route(
            errors=errors,
            llm_tasks_config=llm_tasks_config,
            task_name="archivist",
        )
        _validate_llm_task_route(
            errors=errors,
            llm_tasks_config=llm_tasks_config,
            task_name="embedding",
        )
        _validate_llm_task_route(
            errors=errors,
            llm_tasks_config=llm_tasks_config,
            task_name="x_monitor",
        )

        if archivist_registry and any(
            topic.retrieval.requires_semantic() for topic in archivist_registry.topics
        ):
            embedding_cfg = llm_tasks_config.get("embedding", {})
            if not isinstance(embedding_cfg, dict) or not embedding_cfg.get("enabled", False):
                errors.append(
                    "llm.tasks.embedding must be enabled when archivist topics use semantic or hybrid retrieval"
                )

        # Check environment variables for LLM providers if enabled
        env_var_mapping = {
            'openai': 'OPENAI_API_KEY',
            'anthropic': 'ANTHROPIC_API',  # Using actual .env variable name
            'openrouter': 'OPEN_ROUTER_API_KEY'  # Using actual .env variable name
        }
        
        for provider in ['openai', 'anthropic', 'openrouter']:
            provider_config = llm_config.get(provider, {})
            if provider_config.get('enabled', False):
                env_var = env_var_mapping[provider]
                env_value = os.getenv(env_var)
                if not env_value or env_value.strip() == '':
                    errors.append(f"LLM provider {provider} is enabled but {env_var} environment variable is not set or empty")
        
        # Check YouTube API key if YouTube features are enabled
        youtube_config = self.get('youtube', {})
        if youtube_config.get('enable_embeddings', False) or youtube_config.get('enable_transcripts', False):
            youtube_api_key = os.getenv('YOUTUBE_API_KEY')
            if not youtube_api_key or youtube_api_key.strip() == '':
                errors.append("YouTube features are enabled but YOUTUBE_API_KEY environment variable is not set or empty")
        
        return errors
    
    def is_pipeline_stage_enabled(self, stage_path: str) -> bool:
        """Check if a pipeline stage is enabled using dot notation (e.g., 'documents.arxiv_papers')"""
        return self.get(f'pipeline.stages.{stage_path}', True)
    
    def get_processing_threshold(self, threshold_name: str, default: Any = None) -> Any:
        """Get processing threshold values (e.g., 'summary_min_chars', 'alt_text_delay_seconds')"""
        return self.get(f'processing.{threshold_name}', default)
    
    def get_download_setting(self, setting_name: str, default: Any = None) -> Any:
        """Get download configuration values (e.g., 'timeout_seconds', 'retry_attempts')"""
        return self.get(f'downloads.{setting_name}', default)
    
    def get_naming_pattern(self, pattern_type: str) -> str:
        """Get file naming pattern for a specific type (e.g., 'tweet', 'thread', 'media')"""
        return self.get(f'files.naming_patterns.{pattern_type}', '')
    
    def validate_and_warn(self) -> bool:
        """Validate configuration and log warnings for any issues. Returns True if valid."""
        errors = self.validate()
        
        if errors:
            logger.warning(f"Configuration validation found {len(errors)} issues:")
            for error in errors:
                logger.warning(f"  - {error}")
            return False
        
        logger.info("Configuration validation passed")
        return True


# Global config instance
config = Config()

# Load base config, then apply operator overrides from control.json if present.
config.reload()
