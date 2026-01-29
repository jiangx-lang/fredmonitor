@echo off
set TASK_NAME=FRED_Crisis_Monitor_Daily
set SCRIPT_PATH=D:\fred_crisis_monitor\scripts\send_daily_report.py
schtasks /Create /F /SC DAILY /ST 09:00 /TN "%TASK_NAME%" /TR "py \"%SCRIPT_PATH%\""
echo Task created: %TASK_NAME%
