# 运行前预检查说明

## 使用方式

在运行主报告（`python crisis_monitor_v2.py`）之前，建议先执行：

```bash
python scripts/check_regime_data.py
```

（若 `python` 不在 PATH，请用完整路径，例如：  
`C:\Users\Jiangshuo\AppData\Local\Programs\Python\Python313\python.exe scripts/check_regime_data.py`）

## 检查内容

1. **FRED API** 是否可用（`FRED_AVAILABLE`）
2. **FRED 序列**：Regime/Conflict/Structural 用到的 17 个序列是否可拉取，最新日期与条数
3. **Yahoo 序列**：^GSPC、GC=F、DX-Y.NYB、JPY=X、TLT、^JP10Y 是否可拉取
4. **Regime 计算**：`CrisisMonitor.run_all_checks()` 五模块是否正常跑完，各模块 status/value/reason 及 Composite Verdict

## 本次预检查结果摘要（供参考）

- **FRED**：15/17 可用。  
  - **ACMTP10**、**NFIBIDX** 当前拉取失败（多为网络/ConnectionError）。  
  - 若需 Bear Steepening 用 ACM 期限溢价，可稍后重试或检查 FRED API 限流/网络。  
  - NFIB 小企业信心在 FRED 上可能为其他 ID，可查后改 `config/catalog_fred.yaml`。
- **Yahoo**：5/6 可用。  
  - **^JP10Y** 无数据（Yahoo 可能已改代码或下架）。  
  - Japan 模块有 FRED 的 IRSTCI01JPM156N/IRLTLT01JPM156N 兜底，可不依赖 ^JP10Y。
- **Regime 计算**：全部执行成功，无异常；Composite Verdict 与各模块 Reason 会随数据更新变化。

若预检查中大量 ❌ 或 Regime 报错，建议先处理数据/网络再跑主报告。
