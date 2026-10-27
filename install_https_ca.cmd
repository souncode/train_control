@echo off
cd /d "%~dp0"

if not exist "%~dp0certs\traincontrol-ca.cer" (
    echo Certificate not found: certs\traincontrol-ca.cer
    pause
    exit /b 1
)

certutil -user -addstore -f Root "%~dp0certs\traincontrol-ca.cer"
if errorlevel 1 (
    echo.
    echo Unable to install TrainControl Local CA.
    pause
    exit /b 1
)

echo.
echo TrainControl Local CA installed for the current Windows user.
echo Close all browser windows, reopen the browser, then open https://localhost:820
pause
