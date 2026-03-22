@echo off
REM 每日报告：先 FRED 全量同步，再生成报告并用 QQ SMTP 发邮件
REM 日志：logs\daily_report_YYYYMMDD.log（便于排查「一周没收到邮件」）

set BASE_DIR=D:\fred_crisis_monitor
cd /d "%BASE_DIR%"

if not exist "%BASE_DIR%\logs" mkdir "%BASE_DIR%\logs"
for /f %%t in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd"') do set LOGDATE=%%t
set LOGFILE=%BASE_DIR%\logs\daily_report_%LOGDATE%.log

echo ======================================== >> "%LOGFILE%"
echo %date% %time% 危机监控 - 每日报告开始 >> "%LOGFILE%"
echo ======================================== >> "%LOGFILE%"

echo ========================================
echo 危机监控 - 每日报告流程（日志: logs\daily_report_%LOGDATE%.log）
echo ========================================
echo.

echo [1/2] FRED 全量同步（可能 5-30 分钟）...
echo [1/2] FRED sync... >> "%LOGFILE%"
py scripts\sync_fred_http.py --before-report >> "%LOGFILE%" 2>&1
if %errorlevel% neq 0 (
    echo 警告: 同步有异常，继续用本地数据生成报告
    echo WARN sync exit %errorlevel% >> "%LOGFILE%"
)
echo.

echo [2/2] 生成报告并发送 QQ 邮件...
echo [2/2] report + email... >> "%LOGFILE%"
set CRISIS_MONITOR_SKIP_SYNC=1
py scripts\send_daily_report.py >> "%LOGFILE%" 2>&1
set SEND_EXIT=%errorlevel%
set CRISIS_MONITOR_SKIP_SYNC=
echo send_daily_report exit %SEND_EXIT% >> "%LOGFILE%"
echo %date% %time% 结束 exit %SEND_EXIT% >> "%LOGFILE%"

if %SEND_EXIT% neq 0 (
    echo.
    echo ERROR: 发邮件或生成报告失败，请打开日志: %LOGFILE%
)

echo.
echo ========================================
echo 每日报告流程结束（退出码 %SEND_EXIT%）
echo ========================================
exit /b %SEND_EXIT%
