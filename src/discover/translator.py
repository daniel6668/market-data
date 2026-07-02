"""NL → 结构化条件 翻译器"""
import json

TRANSLATE_SYSTEM_PROMPT = """你是金融数据查询翻译器。将用户的自然语言投资想法翻译为结构化筛选条件。

## 可用因子
| 因子名 | 含义 | 来源 |
|--------|------|------|
| pe_ttm | 市盈率(PE) | a_daily_basic |
| pb | 市净率(PB) | a_daily_basic |
| turnover_rate | 换手率 | a_daily_basic |
| ma5, ma10, ma20, ma60 | 移动均线 | stock_factors |
| rsi6, rsi14 | RSI | stock_factors |
| macd_dif, macd_dea | MACD | stock_factors |
| ret_5d, ret_20d | N日涨跌幅 | stock_factors |
| vol_ratio | 量比 | stock_factors |

## 操作符
- gt (大于), lt (小于), gte (≥), lte (≤): 因子 vs 数值
- cross_above (金叉), cross_below (死叉): 因子 vs 因子（value 写另一个因子名）

## 市场
- A (A股), ETF (ETF基金), HK (港股), US (美股), all (全部)

## 输出格式（严格JSON，无其他文字）
{
  "conditions": [
    {"factor": "因子名", "op": "操作符", "value": 数值或另一因子名}
  ],
  "universe": "A",
  "action": "screen",
  "backtest": {"start": "2025-01-01", "end": "2026-06-30", "rebalance": "monthly", "weights": "equal"}
}

## 规则
- 只输出JSON，不要解释
- 用户没提到回测需求时 action="screen"，提到回测时 action="backtest"
- 回测日期默认可从用户描述提取，否则用2025-01-01到today
- "低估值" = pe_ttm lt 15
- "放量" = vol_ratio gt 1.5
- "MACD金叉" = macd_dif cross_above macd_dea
- "超卖" = rsi14 lt 35
- "近期强势" = ret_20d gt 10
- 如果用户输入无法翻译为条件，返回 {"error": "无法理解", "hint": "..."}
"""


def translate_nl_to_conditions(client, config: dict, nl_text: str) -> dict:
    """将自然语言翻译为结构化筛选条件

    Returns: {conditions, universe, action, backtest} 或 {error, hint}
    """
    from src.agent.llm import chat as llm_chat
    messages = [
        {"role": "system", "content": TRANSLATE_SYSTEM_PROMPT},
        {"role": "user", "content": nl_text},
    ]
    try:
        resp = llm_chat(client, config, messages)  # 无 tools，纯对话
        text = resp.choices[0].message.content or ""
    except Exception as e:
        return {"error": f"LLM 调用失败: {e}", "hint": "请检查 API 配置"}

    # 提取 JSON（处理可能的 markdown 包裹）
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        text = "\n".join(lines[1:-1]) if lines[-1].strip() == "```" else "\n".join(lines[1:])
    try:
        plan = json.loads(text)
    except json.JSONDecodeError:
        return {"error": "JSON 解析失败", "hint": f"LLM 输出格式异常，原始文本: {text[:200]}", "raw": text}

    # 验证必填字段
    if "error" in plan:
        return plan
    if "conditions" not in plan:
        return {"error": "缺少 conditions 字段", "hint": str(plan)}
    return plan
