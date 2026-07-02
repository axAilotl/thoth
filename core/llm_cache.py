"""
LLM Result Cache - Simple disk-based cache for LLM results
Caches results by content hash to avoid redundant API calls
"""

import json
import hashlib
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime

from .config import config
from .path_layout import build_path_layout
from .sensitive_redaction import redact_sensitive_text

logger = logging.getLogger(__name__)

_CACHE_KEY_VERSION = "v2"


class LLMCache:
    """Simple disk-based cache for LLM results"""
    
    def __init__(self, cache_dir: str = None):
        if cache_dir:
            self.cache_dir = Path(cache_dir)
        else:
            self.cache_dir = build_path_layout(config).llm_cache_root
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Cache stats
        self.hits = 0
        self.misses = 0
    
    def _redact_cache_material(self, content: str) -> tuple[str, dict[str, Any] | None]:
        redaction = redact_sensitive_text(content)
        metadata = redaction.to_metadata() if redaction.has_findings else None
        return redaction.redacted_text, metadata

    def _generate_cache_key(self, content: str, task_type: str, model: str = "") -> str:
        """Generate cache key from content hash and task parameters"""
        hash_input = f"{_CACHE_KEY_VERSION}|{content}|{task_type}|{model}"
        content_hash = hashlib.sha256(hash_input.encode('utf-8')).hexdigest()[:32]
        return f"{task_type}_{_CACHE_KEY_VERSION}_{content_hash}"
    
    def get(self, content: str, task_type: str, model: str = "") -> Optional[Dict[str, Any]]:
        """Get cached result if available"""
        try:
            cache_key = self._generate_cache_key(content, task_type, model)
            cache_file = self.cache_dir / f"{cache_key}.json"
            
            if cache_file.exists():
                with open(cache_file, 'r', encoding='utf-8') as f:
                    cached_data = json.load(f)
                
                self.hits += 1
                logger.debug(f"LLM cache HIT for {task_type}: {cache_key}")
                result = cached_data.get('result')
                try:
                    from .llm_usage import record_llm_cache_hit

                    record_llm_cache_hit(
                        task_type=task_type,
                        model_provider=model,
                        content=content,
                        result=result,
                    )
                except Exception as usage_error:
                    logger.warning(
                        "Failed to record LLM cache usage for %s: %s",
                        task_type,
                        redact_sensitive_text(str(usage_error)).redacted_text,
                    )
                return result
            else:
                self.misses += 1
                logger.debug(f"LLM cache MISS for {task_type}: {cache_key}")
                return None
                
        except Exception as e:
            logger.warning(f"Error reading LLM cache for {task_type}: {e}")
            self.misses += 1
            return None
    
    def set(self, content: str, task_type: str, result: Dict[str, Any], model: str = ""):
        """Cache an LLM result"""
        try:
            redacted_content, redaction_metadata = self._redact_cache_material(content)
            cache_key = self._generate_cache_key(content, task_type, model)
            cache_file = self.cache_dir / f"{cache_key}.json"

            cache_data = {
                'task_type': task_type,
                'model': model,
                'content_hash': hashlib.sha256(redacted_content.encode('utf-8')).hexdigest()[:16],
                'cache_key_version': _CACHE_KEY_VERSION,
                'result': result,
                'cached_at': datetime.now().isoformat(),
                'content_length': len(redacted_content)
            }
            if redaction_metadata:
                cache_data['redaction'] = redaction_metadata

            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False)
            
            logger.debug(f"LLM cache SET for {task_type}: {cache_key}")
            
        except Exception as e:
            logger.warning(f"Error writing LLM cache for {task_type}: {e}")
    
    def clear(self):
        """Clear all cached results"""
        try:
            for cache_file in self.cache_dir.glob("*.json"):
                cache_file.unlink()
            logger.info(f"Cleared LLM cache directory: {self.cache_dir}")
        except Exception as e:
            logger.error(f"Error clearing LLM cache: {e}")
    
    def get_stats(self) -> Dict[str, int]:
        """Get cache statistics"""
        cache_files = list(self.cache_dir.glob("*.json"))
        return {
            'hits': self.hits,
            'misses': self.misses,
            'cached_results': len(cache_files),
            'hit_rate': self.hits / (self.hits + self.misses) if (self.hits + self.misses) > 0 else 0.0
        }
    
    def get_cache_info(self) -> Dict[str, Any]:
        """Get detailed cache information"""
        stats = self.get_stats()
        
        # Get cache size
        total_size = sum(f.stat().st_size for f in self.cache_dir.glob("*.json"))
        
        # Count by task type and provider from cache metadata.
        task_counts = {}
        model_counts = {}
        recent_entries = []
        for cache_file in self.cache_dir.glob("*.json"):
            try:
                with open(cache_file, 'r', encoding='utf-8') as handle:
                    payload = json.load(handle)
                task_type = payload.get('task_type') or cache_file.stem.split('_')[0]
                task_counts[task_type] = task_counts.get(task_type, 0) + 1
                model = payload.get('model') or 'unknown'
                model_counts[model] = model_counts.get(model, 0) + 1
                recent_entries.append({
                    'cache_key': cache_file.stem,
                    'task_type': task_type,
                    'model': model,
                    'cached_at': payload.get('cached_at'),
                    'content_length': payload.get('content_length'),
                })
            except Exception:
                pass
        recent_entries.sort(
            key=lambda item: item.get('cached_at') or '',
            reverse=True,
        )

        return {
            **stats,
            'cache_size_bytes': total_size,
            'cache_size_mb': round(total_size / (1024 * 1024), 2),
            'task_type_counts': task_counts,
            'model_counts': model_counts,
            'recent_entries': recent_entries[:10],
            'cache_dir': str(self.cache_dir)
        }


# Global cache instance
llm_cache = LLMCache()
