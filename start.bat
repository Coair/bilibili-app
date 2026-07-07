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

:: 自动安装/更新依赖（pip 会自动跳过已安装的包）
echo [!] 正在检查并安装依赖...
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple --disable-pip-version-check -q
if %errorlevel% neq 0 (
    echo [错误] 依赖安装失败，请检查网络连接后重试
    pause
    exit /b 1
)
echo [√] 依赖已就绪

echo.
echo ================================================
echo   正在启动服务，稍后浏览器将自动打开...
echo ================================================
echo.

:: 新窗口启动 Flask（最小化运行，关闭窗口即可停止服务）
start "B站下载工具 - 服务运行中" /min python app.py

:: 轮询等待 5001 端口就绪（最多等 20 秒）
echo [!] 等待服务启动...
set retry=0
:wait_loop
timeout /t 1 /nobreak >nul
set /a retry+=1
curl -s -o nul http://localhost:5001 2>nul
if %errorlevel% equ 0 goto open_browser
if %retry% lss 20 goto wait_loop

echo [警告] 服务启动超时，请手动访问 http://localhost:5001
goto end

:open_browser
start http://localhost:5001

echo.
echo ================================================
echo   服务已启动: http://localhost:5001
echo   浏览器已自动打开，请勿关闭 Flask 窗口
echo ================================================
echo.

:end
pause
