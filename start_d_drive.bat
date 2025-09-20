@echo off
echo 🚀 启动D盘FRED危机监控系统...
cd /d "D:\fred_crisis_monitor"
python setup_d_drive_environment.py
python -m scripts.crisis_monitor
pause
