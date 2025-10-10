@echo off
echo 📥 下载FRED数据
echo =====================================
cd /d "D:\fred_crisis_monitor"
python scripts/sync_fred_http.py
pause
