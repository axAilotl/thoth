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

logger = logging.getLogger(__name__)


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
    
    def _generate_cache_key(self, content: str, task_type: str, model: str = "") -> str:
        """Generate cache key from content hash and task parameters"""
        # Create hash from content + task type + model
        hash_input = f"{content}|{task_type}|{model}"
        content_hash = hashlib.sha256(hash_input.encode('utf-8')).hexdigest()[:16]
        return f"{task_type}_{content_hash}"
    
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
                return cached_data.get('result')
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
            cache_key = self._generate_cache_key(content, task_type, model)
            cache_file = self.cache_dir / f"{cache_key}.json"
            
            cache_data = {
                'task_type': task_type,
                'model': model,
                'content_hash': hashlib.sha256(content.encode('utf-8')).hexdigest()[:16],
                'result': result,
                'cached_at': datetime.now().isoformat(),
                'content_length': len(content)
            }
            
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
        
        # Count by task type
        task_counts = {}
        for cache_file in self.cache_dir.glob("*.json"):
            try:
                task_type = cache_file.stem.split('_')[0]
                task_counts[task_type] = task_counts.get(task_type, 0) + 1
            except Exception:
                pass
        
        return {
            **stats,
            'cache_size_bytes': total_size,
            'cache_size_mb': round(total_size / (1024 * 1024), 2),
            'task_type_counts': task_counts,
            'cache_dir': str(self.cache_dir)
        }


# Global cache instance
llm_cache = LLMCache()
