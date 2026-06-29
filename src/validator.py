"""数据校验模块 — 写入前检查行数/空值/日期连续性"""
import logging
import pandas as pd

logger = logging.getLogger("market_data")


def validate_daily(df: pd.DataFrame, ts_code: str = "") -> dict:
    """校验日线数据，返回问题清单。

    检查项:
    1. 空表 — df 为空或行数为 0
    2. 空值比例 — 关键字段空值超过 50%
    3. 日期连续性 — 交易日之间是否有异常跳空（仅对 A 股/港股有效）

    Returns: {"valid": bool, "issues": [str], "stats": dict}
    """
    issues = []
    stats = {"rows": 0, "null_pct": 0, "date_gaps": 0}

    if df is None or df.empty:
        return {"valid": False, "issues": ["空表"], "stats": stats}

    stats["rows"] = len(df)

    # 1. 空值检查（关键字段）
    key_cols = [c for c in ["close", "open", "high", "low"] if c in df.columns]
    if key_cols:
        null_count = df[key_cols].isnull().sum().sum()
        total = len(df) * len(key_cols)
        null_pct = null_count / total if total > 0 else 0
        stats["null_pct"] = round(null_pct, 4)
        if null_pct > 0.5:
            issues.append(f"空值比例 {null_pct:.0%} 过高（阈值 50%）")

    # 2. 日期连续性检查
    if "trade_date" in df.columns:
        dates = sorted(df["trade_date"].unique())
        if len(dates) > 2:
            gaps = 0
            for i in range(1, len(dates)):
                try:
                    d1 = pd.to_datetime(dates[i - 1])
                    d2 = pd.to_datetime(dates[i])
                    diff = (d2 - d1).days
                    # 超过 10 天的间隔可能是数据缺失（排除节假日）
                    if diff > 10:
                        gaps += 1
                except Exception:
                    pass
            stats["date_gaps"] = gaps
            if gaps > 5:
                issues.append(f"日期间隔异常 {gaps} 处（阈值 5）")

    valid = len(issues) == 0
    prefix = f"[{ts_code}] " if ts_code else ""
    if issues:
        for issue in issues:
            logger.warning(f"{prefix}数据校验: {issue}")
    else:
        logger.debug(f"{prefix}数据校验通过: {stats['rows']} 行, 空值 {stats['null_pct']:.1%}")

    return {"valid": valid, "issues": issues, "stats": stats}
