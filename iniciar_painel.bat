@echo off
cd /d "%~dp0"
title Painel de Apuracao 2026
echo ========================================================
echo    APURACAO ELEICOES 2026 - INICIANDO DASHBOARD ONLINE
echo ========================================================
echo.
echo Abrindo navegador em:
echo    http://localhost:8000
echo.
echo Pressione Ctrl+C nesta janela para encerrar quando quiser.
echo ========================================================
echo.

:: Abre o navegador automaticamente apos 1 segundo em segundo plano
start "" cmd /c "timeout /t 2 /nobreak >nul & start http://localhost:8000"

py app.py
pause
