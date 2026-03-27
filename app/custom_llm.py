from typing import Any, Optional, ClassVar, Dict
from llama_index.llms.openai import OpenAI
import structlog
import httpx
import os
try:
    # LlamaIndex >= 0.10
    from llama_index.core.base.llms.types import LLMMetadata
except Exception:  # pragma: no cover - fallback for older versions
    from llama_index.core.llms.types import LLMMetadata  # type: ignore

logger = structlog.get_logger()


def is_aliyun_dashscope(base_url: str) -> bool:
    """
    判断是否为阿里云 DashScope API
    
    Args:
        base_url: API 基础 URL
        
    Returns:
        bool: 是否为阿里云 DashScope
    """
    return "dashscope.aliyuncs.com" in base_url.lower()


class CustomOpenAI(OpenAI):
    """
    自定义 OpenAI LLM 类 - 支持模型名称映射
    
    继承自 llama_index 的 OpenAI 类，并实现模型名称映射功能，
    以支持阿里云 Qwen 模型、DeepSeek 模型等非标准 OpenAI 模型名称
    """
    
    # ✅ 使用 ClassVar 和类型注解以支持 Pydantic v2
    MODEL_MAPPINGS: ClassVar[Dict[str, str]] = {
        # 阿里云 Qwen 模型映射
        "qwen-turbo": "gpt-3.5-turbo",
        "qwen-plus": "gpt-3.5-turbo",
        "qwen-max": "gpt-4",
        "qwen-max-latest": "gpt-4",
        "qwen-long": "gpt-4",
        
        # 阿里云 QwQ 模型映射
        "qwq-plus-latest": "gpt-4",
        "qwq-plus": "gpt-4",
        
        # 阿里云 QVQ 视觉模型映射
        "qvq-plus-latest": "gpt-4-vision-preview",
        
        # DeepSeek 模型保持原名，直接透传到 DeepSeek API
        "deepseek-chat": "deepseek-chat",
        "deepseek-reasoner": "deepseek-reasoner",
        "deepseek-coder": "deepseek-coder",
        
        # OpenAI 模型 (直接映射)
        "gpt-4": "gpt-4",
        "gpt-4-turbo": "gpt-4-turbo-preview",
        "gpt-3.5-turbo": "gpt-3.5-turbo",
        
        # 其他可能的模型 (作为回退)
        "gpt-4-1106-preview": "gpt-4-turbo-preview",
        "gpt-4-0613": "gpt-4",
        "gpt-3.5-turbo-1106": "gpt-3.5-turbo",
    }
    
    def __init__(self, model: str, api_key: str, base_url: str, **kwargs):
        """
        初始化 CustomOpenAI 实例
        
        Args:
            model: 模型名称（可能是非标准的 OpenAI 名称，会被映射）
            api_key: API 密钥
            base_url: API 基础 URL
            **kwargs: 其他参数（temperature, timeout 等）
        """
        logger.info(f"Initializing CustomOpenAI with model: {model}")
        
        # 检测是否为阿里云 DashScope
        is_aliyun = is_aliyun_dashscope(base_url)
        original_model = model
        
        if is_aliyun:
            logger.info(f"Detected Aliyun DashScope API, using model: {model}")
            # 阿里云使用 deepseek-v3.2 模型
            if "deepseek" in model.lower():
                mapped_model = "deepseek-v3.2"
            else:
                mapped_model = model
        else:
            # 映射到 OpenAI 兼容名
            mapped_model = self.MODEL_MAPPINGS.get(model, model)
            logger.info(f"Mapped model '{model}' -> '{mapped_model}'")
        
        # 为 DeepSeek 配置更长的超时时间（默认 120 秒）
        if "timeout" not in kwargs:
            # 某些模型（如 DeepSeek）可能需要更长的处理时间
            kwargs["timeout"] = 120.0
            logger.info(f"Set default timeout to 120 seconds for model: {model}")
        
        # 使用映射名初始化父类
        super().__init__(
            model=mapped_model,
            api_key=api_key,
            api_base=base_url,
            **kwargs
        )
        
        # 在父类初始化后设置自定义属性（避免被 Pydantic 清除）
        object.__setattr__(self, '_is_aliyun', is_aliyun)
        object.__setattr__(self, '_original_model', original_model)
        
        # Expose timeout for diagnostics
        try:
            self._timeout = kwargs.get("timeout", 120.0)
        except Exception:
            pass

        # 配置底层 OpenAI 客户端的 httpx 设置以支持更长的超时
        # 这确保 DeepSeek 等服务有足够的时间来处理请求
        try:
            timeout_value = kwargs.get("timeout", 120.0)
            if self._client:
                # 为同步客户端配置 httpx 超时
                http_client = httpx.Client(
                    timeout=httpx.Timeout(
                        timeout=timeout_value,
                        connect=10.0,
                        read=timeout_value,
                        write=10.0,
                        pool=10.0
                    )
                )
                if hasattr(self._client, 'http_client'):
                    self._client.http_client = http_client
                logger.info(f"Configured httpx client with {timeout_value}s timeout")
        except Exception as e:
            logger.warning(f"Could not configure httpx client: {str(e)}")
        
        logger.info(f"CustomOpenAI initialized successfully with mapped model: {mapped_model}")

    def _prepare_chat_kwargs(self, **kwargs):
        """
        准备聊天请求的参数，为阿里云 API 添加特殊配置
        
        Args:
            **kwargs: 原始参数
            
        Returns:
            dict: 处理后的参数
        """
        # 使用 object.__getattribute__ 避免 Pydantic 的属性访问拦截
        try:
            is_aliyun = object.__getattribute__(self, '_is_aliyun')
        except AttributeError:
            is_aliyun = False
        
        if is_aliyun:
            # 阿里云 DeepSeek API 需要通过 extra_body 传递 enable_thinking
            if "extra_body" not in kwargs:
                kwargs["extra_body"] = {}
            
            # 启用思考模式（如果模型支持）
            try:
                original_model = object.__getattribute__(self, '_original_model')
            except AttributeError:
                original_model = ""
            
            if "deepseek" in original_model.lower():
                kwargs["extra_body"]["enable_thinking"] = False
                logger.info("Disabled thinking mode for Aliyun DeepSeek API (enable_thinking=False)")
            
            # 阿里云流式响应需要 stream_options
            if kwargs.get("stream", False):
                kwargs["stream_options"] = {"include_usage": True}
                logger.info("Enabled stream_options for Aliyun API")
        
        return kwargs

    def chat(self, messages, **kwargs):
        """
        重写 chat 方法以支持阿里云特殊参数
        
        Args:
            messages: 消息列表
            **kwargs: 其他参数
            
        Returns:
            ChatResponse: 聊天响应
        """
        # 准备阿里云特殊参数
        kwargs = self._prepare_chat_kwargs(**kwargs)
        
        # 调用父类方法
        return super().chat(messages, **kwargs)
    
    def stream_chat(self, messages, **kwargs):
        """
        重写 stream_chat 方法以支持阿里云特殊参数和 reasoning_content
        
        Args:
            messages: 消息列表
            **kwargs: 其他参数
            
        Yields:
            ChatResponse: 流式聊天响应
        """
        # 准备阿里云特殊参数
        kwargs = self._prepare_chat_kwargs(stream=True, **kwargs)
        
        # 调用父类方法
        for chunk in super().stream_chat(messages, **kwargs):
            # 如果是阿里云 API，可以在这里处理 reasoning_content
            # 但由于 llama_index 的限制，我们暂时只能透传
            yield chunk

    @property
    def metadata(self):
        """Return LLM metadata without relying on OpenAI model name registry.

        This bypasses openai_modelname_to_contextsize which doesn't know DeepSeek names.
        """
        try:
            return LLMMetadata(
                context_window=8192,
                num_output=1024,
                is_chat_model=True,
                is_function_calling_model=True,
                model_name=getattr(self, "model", "deepseek-chat"),
            )
        except Exception:
            # As a last resort, provide minimal metadata
            return LLMMetadata(
                context_window=8192,
                num_output=1024,
                is_chat_model=True,
                is_function_calling_model=False,
                model_name="deepseek-chat",
            )


def get_llm(api_key: str, base_url: str, model: str, **kwargs) -> CustomOpenAI:
    """
    工厂函数：创建并返回自定义 OpenAI LLM 实例
    
    Args:
        api_key: LLM API 密钥
        base_url: LLM API 基础 URL
        model: 模型名称（支持 DeepSeek, Aliyun Qwen, OpenAI 等）
        **kwargs: 其他参数（temperature, timeout 等）
    
    Returns:
        CustomOpenAI: 初始化的 LLM 实例
    
    Examples:
        # 使用 DeepSeek
        llm = get_llm(
            api_key="sk-xxxxx",
            base_url="https://api.deepseek.com",
            model="deepseek-chat",
            temperature=0.7
        )
        
        # 使用阿里云 Qwen
        llm = get_llm(
            api_key="sk-xxxxx",
            base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
            model="qwen-plus",
            temperature=0.7
        )
    """
    logger.info(f"Creating LLM instance with model: {model}, base_url: {base_url}")
    
    llm = CustomOpenAI(
        model=model,
        api_key=api_key,
        base_url=base_url,
        **kwargs
    )
    
    logger.info(f"LLM instance created successfully")
    return llm
