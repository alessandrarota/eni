@echo off
echo Esecuzione del setup delle policy...
python policy_setup.py

IF %ERRORLEVEL% NEQ 0 (
    echo Il setup delle policy e' fallito.
    exit /b 1
)
echo Setup policy completato
