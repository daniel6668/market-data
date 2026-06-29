"""LLM 后端抽象 — 支持 DeepSeek/GLM/OpenAI/LM Studio"""
from openai import OpenAI

PROVIDERS = {
    "deepseek": {
        "base_url": "https://api.deepseek.com",
        "model": "deepseek-chat",
    },
    "glm": {
        "base_url": "https://open.bigmodel.cn/api/paas/v4",
        "model": "glm-4-flash",
    },
    "openai": {
        "base_url": "https://api.openai.com/v1",
        "model": "gpt-4o-mini",
    },
    "lmstudio": {
        "base_url": "http://localhost:1234/v1",
        "model": "local-model",
    },
}


def get_client(config: dict) -> OpenAI:
    """根据配置创建 LLM 客户端"""
    provider = config["llm"]["provider"]
    cfg = PROVIDERS.get(provider, PROVIDERS["deepseek"])
    return OpenAI(
        api_key=config["llm"]["api_key"],
        base_url=config["llm"].get("base_url", cfg["base_url"]),
    )


def chat(client: OpenAI, config: dict, messages: list, tools: list = None):
    """发送对话请求，支持 function calling"""
    provider = config["llm"]["provider"]
    cfg = PROVIDERS.get(provider, PROVIDERS["deepseek"])
    model = config["llm"].get("model", cfg["model"])
    kwargs = dict(model=model, messages=messages, temperature=0.3)
    if tools:
        kwargs["tools"] = tools
    return client.chat.completions.create(**kwargs)
