@echo off
echo 🔍 FRED数据有效性全面检查
echo =====================================
cd /d "D:\fred_crisis_monitor\daily_risk_dashboard"

echo.
echo 1️⃣ 配置文件验证
python validate_config.py

echo.
echo 2️⃣ 数据有效性检查
python check_data_validity.py

pause











