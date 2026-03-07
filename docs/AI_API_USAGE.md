# 深度宏观推演 (AI) — API 用量说明

## 每次报告调用的 API 用量

本系统在每次生成报告时调用**一次**阿里云 DashScope（通义千问 qwen-plus）生成「深度宏观推演」章节。

### Token 估算（单次运行）

| 项目     | 约略范围   | 说明 |
|----------|------------|------|
| 输入     | 约 600–1000 tokens | system prompt + 指标 JSON（Warsh 约束、高风险指标、核心宏观） |
| 输出     | 约 700–1200 tokens | 市场风险备忘录（核心论断、流动性管道、隐形风险、交易建议） |
| **合计** | **约 1500–2200 tokens/次** | 每次 `py crisis_monitor.py` 仅调用 1 次 |

### 实际用量查看

运行完整报告后，在 **`outputs/crisis_monitor/run_trace.log`** 中会看到一行：

```
🤖 AI 深度推演 API 用量: 输入 XXX + 输出 YYY = ZZZ tokens
```

该行由 DashScope 返回的 `usage` 写入，为当次真实消耗。

---

## 费用参考（通义千问 qwen-plus，中国内地）

依据 [阿里云模型计费](https://help.aliyun.com/zh/model-studio/model-pricing)（0 &lt; Token ≤ 128K 档）：

- **输入**：0.8 元 / 百万 tokens  
- **输出**：2 元 / 百万 tokens  

单次报告（按 800 输入 + 900 输出估算）：

- 费用 ≈ 0.8×0.0008 + 2×0.0009 ≈ **0.0025 元/次**（约 1 分钱的 1/4）

**免费额度**：百炼开通后 90 天内，qwen-plus 输入、输出各 100 万 Token。  
约可跑 **500–600 次** 完整报告（仅算 AI 这一段）才会用满免费额度。

---

## 小结

- **每次报告**：约 **1 次 API 调用**，约 **1500–2200 tokens**。  
- **成本**：在免费额度内可忽略；超出后约 **0.002–0.003 元/次**。  
- **查看当次用量**：看 `outputs/crisis_monitor/run_trace.log` 中的「🤖 AI 深度推演 API 用量」行。
