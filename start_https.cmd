@echo off
cd /d "%~dp0"
echo HTTPS mode is disabled. Starting TrainControl over HTTP.
call "%~dp0start_waitress.cmd"
