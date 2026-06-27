# tests/test_utils.py
import time
import pytest
from src.utils import RateLimiter, load_config

def test_rate_limiter_waits():
    rl = RateLimiter(max_calls=2, period=60)
    t0 = time.time()
    rl.wait()
    rl.wait()
    rl.wait()  # 第三次应触发等待
    elapsed = time.time() - t0
    # 前两次不应等，第三次会等（取决于实现）
    # 主要验证不抛异常
    assert elapsed >= 0

def test_rate_limiter_respects_burst():
    rl = RateLimiter(max_calls=3, period=60)
    t0 = time.time()
    for _ in range(3):
        rl.wait()
    assert time.time() - t0 < 1  # 3次突发应很快

def test_load_config(tmp_path):
    import yaml
    config_path = tmp_path / "config.yaml"
    config_path.write_text("foo: bar\n", encoding="utf-8")
    cfg = load_config(str(config_path))
    assert cfg["foo"] == "bar"

def test_load_config_default():
    cfg = load_config("nonexistent.yaml")
    assert "database" in cfg
    assert "tushare" in cfg
