"""Gradio Chat UI"""
import sys
import os
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import gradio as gr
from src.agent.llm import get_client, chat
from src.agent.tools import TOOLS, execute_tool
from src.utils import load_config

config = load_config()

_conn = None

def _get_conn():
    global _conn
    if _conn is None:
        from src.db import get_connection
        _conn = get_connection(config)
    return _conn

SYSTEM_PROMPT = """你是一个 A 股投资分析助手。你可以调用工具来筛选股票、回测策略、分析个股、查看市场。

请用中文简洁回复。重要数据用表格展示。"""


def respond(message, history):
    llm_cfg = config.get("llm", {})
    if not llm_cfg.get("api_key"):
        return "⚠️ 请先在 config.yaml 中配置 LLM API Key"

    client = get_client(config)
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]

    # 兼容新旧 Gradio 的 history 格式
    for h in history:
        if isinstance(h, dict):
            messages.append(h)
        elif isinstance(h, (list, tuple)) and len(h) >= 2:
            messages.append({"role": "user", "content": str(h[0])})
            if h[1]:
                messages.append({"role": "assistant", "content": str(h[1])})
    messages.append({"role": "user", "content": message})

    try:
        resp = chat(client, config, messages, TOOLS)
    except Exception as e:
        return f"❌ API 调用失败: {e}"

    msg = resp.choices[0].message

    if msg.tool_calls:
        tool_calls_formatted = []
        for tc in msg.tool_calls:
            tool_calls_formatted.append({
                "id": tc.id,
                "type": "function",
                "function": {"name": tc.function.name, "arguments": tc.function.arguments},
            })
        messages.append({
            "role": "assistant",
            "content": msg.content,
            "tool_calls": tool_calls_formatted,
        })

        for tc in msg.tool_calls:
            try:
                args = json.loads(tc.function.arguments)
            except json.JSONDecodeError:
                args = {}
            try:
                result = execute_tool(tc.function.name, args, _get_conn())
            except Exception as e:
                result = f"工具执行失败: {e}"
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": result})

        try:
            resp2 = chat(client, config, messages)
            return resp2.choices[0].message.content or ""
        except Exception as e:
            return f"❌ LLM 响应失败: {e}"

    return msg.content or ""


def create_ui():
    with gr.Blocks(title="Market Data AI") as app:
        provider = config.get("llm", {}).get("provider", "未配置")
        model = config.get("llm", {}).get("model", "default")
        gr.Markdown(f"# 📊 Market Data AI — LLM: **{provider}** / {model}")

        with gr.Tab("💬 对话"):
            gr.ChatInterface(
                respond,
                examples=["帮我找 PE<30 的股票", "分析一下 600519", "今天市场怎么样？"],
            )
        with gr.Tab("⭐ 自选池"):
            gr.Markdown("### 自选池\n功能开发中")
        with gr.Tab("📊 策略"):
            gr.Markdown("### 策略向导\n功能开发中")
        with gr.Tab("🎯 信号"):
            gr.Markdown("### 信号面板\n功能开发中")
    return app


if __name__ == "__main__":
    app = create_ui()
    print("=" * 50)
    print("  Market Data AI 启动成功！")
    print("  http://127.0.0.1:7860")
    print("=" * 50)
    app.launch(server_name="0.0.0.0", server_port=7860)
