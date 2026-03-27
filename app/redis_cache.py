"""
Redis Cache Manager for high-concurrency optimization

Provides caching for:
- Query results
- Session memory
- Prompt templates
"""
import os
import json
import hashlib
from typing import Optional, Dict, Any
import redis.asyncio as redis
import structlog

logger = structlog.get_logger()


class RedisCacheManager:
    """Redis cache manager for query results, sessions, and prompts"""
    
    def __init__(self, redis_url: str = None):
        """
        Initialize Redis cache manager
        
        Args:
            redis_url: Redis connection URL (default from env)
        """
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.default_ttl = int(os.getenv("REDIS_CACHE_TTL", "3600"))  # 1 hour
        self.session_ttl = int(os.getenv("REDIS_SESSION_TTL", "86400"))  # 24 hours
        self._redis: Optional[redis.Redis] = None
        
        logger.info("RedisCacheManager initialized", redis_url=self.redis_url)
    
    async def connect(self):
        """Establish Redis connection"""
        if self._redis is None:
            try:
                self._redis = await redis.from_url(
                    self.redis_url,
                    encoding="utf-8",
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_keepalive=True
                )
                # Test connection
                await self._redis.ping()
                logger.info("Redis connection established")
            except Exception as e:
                logger.error("Failed to connect to Redis", error=str(e))
                raise
    
    async def close(self):
        """Close Redis connection"""
        if self._redis:
            await self._redis.close()
            self._redis = None
            logger.info("Redis connection closed")
    
    def _generate_query_hash(self, query: str, intent_type: str = "") -> str:
        """Generate cache key hash from query only (intent_type is ignored)"""
        # 只使用 query 生成哈希，不包含 intent_type
        # 这样相同的问题无论意图分类结果如何都会命中同一个缓存
        return hashlib.sha256(query.encode()).hexdigest()
    
    # ==================== Query Cache ====================
    
    async def get_query_cache(self, query: str, intent_type: str = "") -> Optional[Dict[str, Any]]:
        """
        Get cached query result
        
        Args:
            query: User query text
            intent_type: Intent classification type
            
        Returns:
            Cached result dict or None if not found
        """
        if not self._redis:
            await self.connect()
        
        query_hash = self._generate_query_hash(query, intent_type)
        cache_key = f"query:{query_hash}"
        
        try:
            cached_data = await self._redis.get(cache_key)
            if cached_data:
                logger.info("Query cache hit", query_preview=query[:50], intent_type=intent_type)
                return json.loads(cached_data)
            else:
                logger.info("Query cache miss", query_preview=query[:50], intent_type=intent_type)
                return None
        except Exception as e:
            logger.error("Failed to get query cache", error=str(e))
            return None
    
    async def set_query_cache(
        self,
        query: str,
        intent_type: str,
        result: Dict[str, Any],
        ttl: int = None
    ):
        """
        Set query result cache
        
        Args:
            query: User query text
            intent_type: Intent classification type
            result: Result dict to cache
            ttl: Time to live in seconds (default: 1 hour)
        """
        if not self._redis:
            await self.connect()
        
        query_hash = self._generate_query_hash(query, intent_type)
        cache_key = f"query:{query_hash}"
        ttl = ttl or self.default_ttl
        
        try:
            cached_data = json.dumps(result, ensure_ascii=False)
            await self._redis.setex(cache_key, ttl, cached_data)
            logger.info(
                "Query cache set",
                query_preview=query[:50],
                intent_type=intent_type,
                ttl=ttl
            )
        except Exception as e:
            logger.error("Failed to set query cache", error=str(e))
    
    # ==================== Session Memory ====================
    
    async def get_session_memory(self, session_id: str) -> Optional[str]:
        """
        Get session chat history
        
        Args:
            session_id: Session identifier
            
        Returns:
            Chat history string or None if not found
        """
        if not self._redis:
            await self.connect()
        
        cache_key = f"session:{session_id}"
        
        try:
            memory = await self._redis.get(cache_key)
            if memory:
                logger.info("Session memory hit", session_id=session_id)
                return memory
            else:
                logger.info("Session memory miss", session_id=session_id)
                return None
        except Exception as e:
            logger.error("Failed to get session memory", error=str(e))
            return None
    
    async def save_session_memory(
        self,
        session_id: str,
        memory: str,
        ttl: int = None
    ):
        """
        Save session chat history
        
        Args:
            session_id: Session identifier
            memory: Chat history string
            ttl: Time to live in seconds (default: 24 hours)
        """
        if not self._redis:
            await self.connect()
        
        cache_key = f"session:{session_id}"
        ttl = ttl or self.session_ttl
        
        try:
            await self._redis.setex(cache_key, ttl, memory)
            logger.info(
                "Session memory saved",
                session_id=session_id,
                memory_length=len(memory),
                ttl=ttl
            )
        except Exception as e:
            logger.error("Failed to save session memory", error=str(e))
    
    async def delete_session_memory(self, session_id: str):
        """Delete session memory"""
        if not self._redis:
            await self.connect()
        
        cache_key = f"session:{session_id}"
        
        try:
            await self._redis.delete(cache_key)
            logger.info("Session memory deleted", session_id=session_id)
        except Exception as e:
            logger.error("Failed to delete session memory", error=str(e))
    
    # ==================== Prompt Template Cache ====================
    
    async def get_prompt_template(self, template_key: str) -> Optional[str]:
        """
        Get cached prompt template
        
        Args:
            template_key: Template identifier
            
        Returns:
            Template content or None if not found
        """
        if not self._redis:
            await self.connect()
        
        cache_key = f"prompt:{template_key}"
        
        try:
            template = await self._redis.get(cache_key)
            if template:
                logger.info("Prompt template cache hit", template_key=template_key)
                return template
            else:
                logger.info("Prompt template cache miss", template_key=template_key)
                return None
        except Exception as e:
            logger.error("Failed to get prompt template", error=str(e))
            return None
    
    async def set_prompt_template(self, template_key: str, content: str):
        """
        Cache prompt template (no expiration - manual invalidation)
        
        Args:
            template_key: Template identifier
            content: Template content
        """
        if not self._redis:
            await self.connect()
        
        cache_key = f"prompt:{template_key}"
        
        try:
            await self._redis.set(cache_key, content)
            logger.info(
                "Prompt template cached",
                template_key=template_key,
                content_length=len(content)
            )
        except Exception as e:
            logger.error("Failed to cache prompt template", error=str(e))
    
    async def invalidate_prompt_template(self, template_key: str):
        """Invalidate prompt template cache"""
        if not self._redis:
            await self.connect()
        
        cache_key = f"prompt:{template_key}"
        
        try:
            await self._redis.delete(cache_key)
            logger.info("Prompt template cache invalidated", template_key=template_key)
        except Exception as e:
            logger.error("Failed to invalidate prompt template", error=str(e))
    
    # ==================== Utility Methods ====================
    
    async def clear_all_caches(self):
        """Clear all caches (use with caution)"""
        if not self._redis:
            await self.connect()
        
        try:
            await self._redis.flushdb()
            logger.warning("All caches cleared")
        except Exception as e:
            logger.error("Failed to clear caches", error=str(e))
    
    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        if not self._redis:
            await self.connect()
        
        try:
            info = await self._redis.info("stats")
            keyspace = await self._redis.info("keyspace")
            
            return {
                "total_connections": info.get("total_connections_received", 0),
                "total_commands": info.get("total_commands_processed", 0),
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0),
                "hit_rate": (
                    info.get("keyspace_hits", 0) /
                    max(info.get("keyspace_hits", 0) + info.get("keyspace_misses", 0), 1)
                ),
                "keys": keyspace.get("db0", {}).get("keys", 0) if keyspace else 0
            }
        except Exception as e:
            logger.error("Failed to get cache stats", error=str(e))
            return {}


# Global cache manager instance
_cache_manager: Optional[RedisCacheManager] = None


def get_cache_manager() -> RedisCacheManager:
    """Get or create global cache manager instance"""
    global _cache_manager
    if _cache_manager is None:
        _cache_manager = RedisCacheManager()
    return _cache_manager
