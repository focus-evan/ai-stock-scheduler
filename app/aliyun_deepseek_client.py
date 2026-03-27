"""
阿里云 DeepSeek API 客户端
支持思考模式（reasoning_content）和流式输出
"""
from typing import Optional, Dict, Any, Iterator
from openai import OpenAI
import structlog

logger = structlog.get_logger()


class AliyunDeepSeekClient:
    """
    阿里云 DeepSeek API 客户端
    
    支持：
    - 思考模式（enable_thinking）
    - 流式输出（reasoning_content + content）
    - Token 使用统计
    """
    
    def __init__(self, api_key: str, base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1"):
        """
        初始化阿里云 DeepSeek 客户端
        
        Args:
            api_key: 阿里云 API Key
            base_url: API 基础 URL
        """
        self.client = OpenAI(
            api_key=api_key,
            base_url=base_url
        )
        self.base_url = base_url
        logger.info("Initialized AliyunDeepSeekClient", base_url=base_url)
    
    def chat(
        self,
        messages: list,
        model: str = "deepseek-v3.2",
        enable_thinking: bool = False,
        temperature: float = 0.7,
        **kwargs
    ) -> Dict[str, Any]:
        """
        非流式聊天
        
        Args:
            messages: 消息列表，格式: [{"role": "user", "content": "..."}]
            model: 模型名称
            enable_thinking: 是否启用思考模式
            temperature: 温度参数
            **kwargs: 其他参数
            
        Returns:
            dict: 包含 reasoning_content, content, usage 的响应
        """
        logger.info("Starting non-streaming chat", model=model, enable_thinking=enable_thinking)
        
        completion = self.client.chat.completions.create(
            model=model,
            messages=messages,
            extra_body={"enable_thinking": enable_thinking},
            temperature=temperature,
            **kwargs
        )
        
        choice = completion.choices[0]
        message = choice.message
        
        result = {
            "content": message.content or "",
            "reasoning_content": getattr(message, "reasoning_content", None) or "",
            "usage": {
                "prompt_tokens": completion.usage.prompt_tokens if completion.usage else 0,
                "completion_tokens": completion.usage.completion_tokens if completion.usage else 0,
                "total_tokens": completion.usage.total_tokens if completion.usage else 0,
            }
        }
        
        logger.info(
            "Chat completed",
            content_length=len(result["content"]),
            reasoning_length=len(result["reasoning_content"]),
            total_tokens=result["usage"]["total_tokens"]
        )
        
        return result
    
    def stream_chat(
        self,
        messages: list,
        model: str = "deepseek-v3.2",
        enable_thinking: bool = False,
        temperature: float = 0.7,
        **kwargs
    ) -> Iterator[Dict[str, Any]]:
        """
        流式聊天
        
        Args:
            messages: 消息列表
            model: 模型名称
            enable_thinking: 是否启用思考模式
            temperature: 温度参数
            **kwargs: 其他参数
            
        Yields:
            dict: 包含 type, content 的流式响应
                - type: "reasoning" | "content" | "usage"
                - content: 文本内容
                - usage: Token 使用统计（仅在最后一个 chunk）
        """
        logger.info("Starting streaming chat", model=model, enable_thinking=enable_thinking)
        
        completion = self.client.chat.completions.create(
            model=model,
            messages=messages,
            extra_body={"enable_thinking": enable_thinking},
            stream=True,
            stream_options={"include_usage": True},
            temperature=temperature,
            **kwargs
        )
        
        reasoning_content = ""
        answer_content = ""
        
        for chunk in completion:
            # 处理 usage 信息
            if not chunk.choices:
                if chunk.usage:
                    yield {
                        "type": "usage",
                        "usage": {
                            "prompt_tokens": chunk.usage.prompt_tokens,
                            "completion_tokens": chunk.usage.completion_tokens,
                            "total_tokens": chunk.usage.total_tokens,
                        }
                    }
                continue
            
            delta = chunk.choices[0].delta
            
            # 处理思考内容
            if hasattr(delta, "reasoning_content") and delta.reasoning_content is not None:
                reasoning_content += delta.reasoning_content
                yield {
                    "type": "reasoning",
                    "content": delta.reasoning_content
                }
            
            # 处理回复内容
            if hasattr(delta, "content") and delta.content:
                answer_content += delta.content
                yield {
                    "type": "content",
                    "content": delta.content
                }
        
        logger.info(
            "Streaming chat completed",
            reasoning_length=len(reasoning_content),
            answer_length=len(answer_content)
        )


def get_aliyun_client(api_key: str, base_url: str = "https://dashscope.aliyuncs.com/compatible-mode/v1") -> AliyunDeepSeekClient:
    """
    工厂函数：创建阿里云 DeepSeek 客户端
    
    Args:
        api_key: 阿里云 API Key
        base_url: API 基础 URL
        
    Returns:
        AliyunDeepSeekClient: 客户端实例
    """
    return AliyunDeepSeekClient(api_key=api_key, base_url=base_url)
