@echo off
REM 创建每日 09:00 计划任务：执行 run_daily_report.bat（同步+FRED+QQ 发信）
REM 使用 cmd /c 保证工作目录与 PATH 中的 py 可用；需已安装 Python 且 py 在 PATH
set TASK_NAME=FRED_Crisis_Monitor_Daily
set BASE=D:\fred_crisis_monitor
set BAT=%BASE%\scripts\run_daily_report.bat
REM 直接运行 .bat：批处理内会 cd 到项目根；若任务失败请把下一行改为本机 Python 全路径
schtasks /Create /F /SC DAILY /ST 09:00 /TN "%TASK_NAME%" /TR "%BAT%" /RL HIGHEST
if %errorlevel% neq 0 (
    echo schtasks failed. Try running this .bat as Administrator.
    pause
    exit /b 1
)
echo.
echo Task created: %TASK_NAME%
echo   Time: daily 09:00
echo   Run:  %BAT%
echo   Logs: %BASE%\logs\daily_report_YYYYMMDD.log
echo.
echo If emails stopped: check logs folder, macrolab.env QQ_EMAIL_*, and QQ auth code expiry.
pause
