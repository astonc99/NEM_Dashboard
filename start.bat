@echo off
title NEM Dashboard Launcher
cd /d "%~dp0"

echo ============================================================
echo  NEM Dashboard Launcher
echo ============================================================
echo.

:: ── Find uvicorn ─────────────────────────────────────────────────────────────
set UVICORN=uvicorn
if exist ".venv\Scripts\uvicorn.exe" set UVICORN=.venv\Scripts\uvicorn.exe
if exist "venv\Scripts\uvicorn.exe"  set UVICORN=venv\Scripts\uvicorn.exe
if exist "env\Scripts\uvicorn.exe"   set UVICORN=env\Scripts\uvicorn.exe
echo [1/4] Uvicorn : %UVICORN%

:: ── Add Node.js to PATH ──────────────────────────────────────────────────────
if exist "%ProgramFiles%\nodejs\npm.cmd" set PATH=%ProgramFiles%\nodejs;%PATH%
echo [2/4] npm     : checking...
call npm --version >nul 2>&1
if errorlevel 1 (
    echo.
    echo  ERROR: npm not found.
    echo  Open a NEW command prompt and try again.
    echo  If Node.js is not installed: https://nodejs.org
    echo.
    pause
    exit /b 1
)
echo [2/4] npm     : OK

:: ── Install frontend deps ────────────────────────────────────────────────────
echo [3/4] Frontend: checking...
if exist "frontend\node_modules" goto :deps_ok
echo        node_modules missing - running npm install (first run only)...
cd frontend
call npm install
cd ..
if errorlevel 1 (
    echo  ERROR: npm install failed. See output above.
    pause
    exit /b 1
)
:deps_ok
echo [3/4] Frontend: OK

:: ── Launch servers ───────────────────────────────────────────────────────────
echo [4/4] Starting servers...
set ROOT=%cd%
start "NEM API  (port 8000)" /d "%ROOT%" cmd /k "%UVICORN% api.main:app --reload"
timeout /t 2 >nul
start "NEM Frontend (port 5173)" /d "%ROOT%\frontend" cmd /k "call npm run dev"

echo.
echo  Dashboard : http://localhost:5173
echo  API docs  : http://localhost:8000/docs
echo.
echo  Two windows opened - API and Frontend.
echo  Visit http://localhost:5173 after a few seconds.
echo.
pause
