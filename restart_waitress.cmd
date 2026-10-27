@echo off
cd /d "%~dp0"
timeout /t 5 /nobreak >nul
call "%~dp0start_waitress.cmd"
