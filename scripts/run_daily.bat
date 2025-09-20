@echo off
REM MacroLab 每日运行脚本
REM 一键执行：数据同步 -> 事实表渲染 -> AI分析

echo ========================================
echo MacroLab 每日宏观分析流程
echo ========================================
echo.

echo [1/3] 开始FRED数据同步...
python scripts\sync_fred_http.py
if %errorlevel% neq 0 (
    echo ✗ 数据同步失败
    pause
    exit /b 1
)
echo ✓ 数据同步完成
echo.

echo [2/3] 开始事实表渲染...
python scripts\render_fact_sheets_http.py
if %errorlevel% neq 0 (
    echo ✗ 事实表渲染失败
    pause
    exit /b 1
)
echo ✓ 事实表渲染完成
echo.

echo [3/3] 开始AI宏观分析...
python scripts\ai_assess.py
if %errorlevel% neq 0 (
    echo ✗ AI分析失败
    pause
    exit /b 1
)
echo ✓ AI分析完成
echo.

echo ========================================
echo 每日分析流程完成！
echo ========================================
echo.
echo 生成的文件：
echo - 数据湖: data\lake\fred.duckdb
echo - 事实表: data\fred\series\*\fact_sheet.md
echo - AI报告: outputs\macro_status\%date%.md
echo.

pause