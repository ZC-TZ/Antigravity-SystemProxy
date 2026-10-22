@echo off
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0install.ps1" %*
set "adapterExit=%errorlevel%"
if not "%adapterExit%"=="0" echo Installation failed. See the message above.
pause
exit /b %adapterExit%
