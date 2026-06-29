"""Gradio Chat UI — 四 Tab 界面"""
import sys
import os
import json

# 确保项目根在 sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import gradio as gr
from src.agent.llm import get_client, chat
from src.agent.tools import TOOLS, execute_tool
from src.db import get_connection
from src.utils import load_config

config = load_config()
conn = get_connection(config)

SYSTEM_PROMPT = """你是一个 A 股投资分析助手，帮助用户筛选股票、分析个股、回测策略。

你可以调用以下工具：
- search_stocks: 按 PE、RSI 等条件筛选股票
- run_backtest: 回测均线交叉策略
- analyze_stock: 分析个股基本面和技术面
- market_overview: 查看市场概况

请用中文简洁回复。当用户询问选股/分析/回测时，主动调用对应工具。"""


def respond(message, history):
    llm_cfg = config.get("llm", {})
    if not llm_cfg.get("api_key"):
        return ("⚠️ 请先在 config.yaml 中配置 LLM:\n\n"
                "```yaml\nllm:\n  provider: deepseek\n  api_key: sk-xxxx\n  model: deepseek-chat\n```\n\n"
                "支持: deepseek / glm / openai / lmstudio")

    client = get_client(config)
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for h in history:
        messages.append({"role": "user", "content": h[0]})
        if h[1]:
            messages.append({"role": "assistant", "content": h[1]})
    messages.append({"role": "user", "content": message})

    try:
        resp = chat(client, config, messages, TOOLS)
    except Exception as e:
        return f"❌ API 调用失败: {e}"

    msg = resp.choices[0].message

    if msg.tool_calls:
        # 记录 tool_calls
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

        # 执行工具
        for tc in msg.tool_calls:
            try:
                args = json.loads(tc.function.arguments)
            except json.JSONDecodeError:
                args = {}
            result = execute_tool(tc.function.name, args, conn)
            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": result,
            })

        try:
            resp2 = chat(client, config, messages)
            return resp2.choices[0].message.content or "（无内容）"
        except Exception as e:
            return f"❌ 工具调用后 LLM 失败: {e}"

    return msg.content or "（无内容）"


def create_ui():
    with gr.Blocks(title="Market Data AI") as app:
        provider = "未配置"
        model = "未配置"
        try:
            provider = config.get("llm", {}).get("provider", "未配置")
            model = config.get("llm", {}).get("model", "default")
        except Exception:
            pass
        gr.Markdown("# 📊 Market Data AI — A 股投资分析助手")
        gr.Markdown(f"LLM: **{provider}** | 模型: **{model}**")

        with gr.Tab("💬 对话"):
            gr.ChatInterface(
                respond,
                examples=[
                    "帮我找 PE<30 的股票",
                    "分析一下 600519",
                    "回测 000858 的均线策略",
                    "今天市场怎么样？",
                ],
            )

        with gr.Tab("⭐ 自选池"):
            gr.Markdown("### 自选池\n功能开发中，敬请期待...")

        with gr.Tab("📊 策略"):
            gr.Markdown("### 策略向导\n功能开发中，敬请期待...")

        with gr.Tab("🎯 信号"):
            gr.Markdown("### 信号面板\n功能开发中，敬请期待...")

    return app


if __name__ == "__main__":
    app = create_ui()
    print("=" * 50)
    print("  Market Data AI 启动成功！")
    print("  打开浏览器访问: http://127.0.0.1:7860")
    print("=" * 50)
    app.launch(server_name="127.0.0.1", server_port=7860, share=False, theme=gr.themes.Soft())
