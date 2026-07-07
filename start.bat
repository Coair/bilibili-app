@echo off
chcp 65001 >nul
title B站下载工具

echo.
echo ================================================
echo            B站下载工具 - 正在启动...
echo ================================================
echo.

:: 检查 Python 是否安装
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [错误] 未检测到 Python，请先安装 Python！
    echo 下载地址：https://www.python.org/downloads/
    echo 安装时请务必勾选 "Add Python to PATH"
    echo.
    pause
    exit /b 1
)

echo [√] Python 已就绪

:: 检查依赖是否安装
pip show flask >nul 2>&1
if %errorlevel% neq 0 (
    echo [!] 检测到依赖未安装，正在自动安装...
    pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
    if %errorlevel% neq 0 (
        echo [错误] 依赖安装失败，请检查网络连接后重试
        pause
        exit /b 1
    )
    echo [√] 依赖安装完成
) else (
    echo [√] 依赖已就绪
)

echo.
echo ================================================
echo   启动成功！浏览器将自动打开
echo   如果没有自动打开，请手动访问：
echo   http://localhost:5001
echo ================================================
echo.

:: 1.5秒后自动打开浏览器
timeout /t 2 /nobreak >nul
start http://localhost:5001

:: 启动 Flask 应用
python app.py

pause
