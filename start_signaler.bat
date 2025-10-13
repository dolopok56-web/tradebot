@echo off
set PYTHONUTF8=1
call "%~dp0\.venv\Scripts\activate.bat"
cd /d "%~dp0"
python -u signal_bot.py
echo.
echo ==== PRESS ANY KEY TO CLOSE ====
pause >nul
