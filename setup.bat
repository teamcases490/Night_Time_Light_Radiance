@echo off
title NTLR V5 Pipeline Full Setup
setlocal

echo ==========================================
echo        NTLR V5 FULL ENV SETUP
echo ==========================================
echo.

REM -------------------------------------------------
REM Check Python
REM -------------------------------------------------
python --version >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo ❌ Python is not installed or not added to PATH.
    pause
    exit /b
)

REM -------------------------------------------------
REM Create Virtual Environment
REM -------------------------------------------------
IF NOT EXIST .venv (
    echo 🌐 Creating virtual environment...
    python -m venv .venv
) ELSE (
    echo ✅ .venv already exists
)

REM -------------------------------------------------
REM Activate Virtual Environment
REM -------------------------------------------------
call .venv\Scripts\activate

REM -------------------------------------------------
REM Upgrade pip/setuptools/wheel
REM -------------------------------------------------
echo 🔄 Upgrading pip tools...
python -m pip install --upgrade pip setuptools wheel

REM -------------------------------------------------
REM Create requirements.txt if missing
REM -------------------------------------------------
IF NOT EXIST requirements.txt (
    echo 📄 Creating requirements.txt...
    (
        echo pandas
        echo earthengine-api
        echo pydrive2
        echo google-auth
        echo google-auth-oauthlib
        echo google-auth-httplib2
        echo oauth2client
        echo pyOpenSSL
        echo requests
        echo tqdm
        echo numpy
        echo scipy
    ) > requirements.txt
) ELSE (
    echo ✅ requirements.txt already exists
)

REM -------------------------------------------------
REM Install Dependencies
REM -------------------------------------------------
echo 📦 Installing dependencies from requirements.txt...
pip install -r requirements.txt

REM -------------------------------------------------
REM Create Folder Structure
REM -------------------------------------------------
IF NOT EXIST data (
    mkdir data
    echo 📁 Created empty data folder
) ELSE (
    echo ✅ data folder already exists
)

IF NOT EXIST logs (
    mkdir logs
    echo 📁 Created logs folder
) ELSE (
    echo ✅ logs folder already exists
)

REM -------------------------------------------------
REM Create placeholder .gitkeep for empty folders
REM -------------------------------------------------
IF NOT EXIST data\.gitkeep (
    type nul > data\.gitkeep
)

IF NOT EXIST logs\.gitkeep (
    type nul > logs\.gitkeep
)

REM -------------------------------------------------
REM Create final output placeholder
REM -------------------------------------------------
IF NOT EXIST final_ntlr_enriched.csv (
    type nul > final_ntlr_enriched.csv
    echo 📄 Created final_ntlr_enriched.csv
)

REM -------------------------------------------------
REM Create sample location.csv if absent
REM -------------------------------------------------
IF NOT EXIST location.csv (
    echo Latitude,Longitude> location.csv
    echo 19.0760,72.8777>> location.csv
    echo 18.5204,73.8567>> location.csv
    echo 📄 Created sample location.csv
)

REM -------------------------------------------------
REM Final Instructions
REM -------------------------------------------------
echo.
echo ==========================================
echo ✅ SETUP COMPLETE
echo ==========================================
echo.
echo NEXT STEPS:
echo 1. Put client_secrets.json in this folder
echo 2. Authenticate Earth Engine:
echo    earthengine authenticate
echo 3. Run pipeline:
echo    python ntlr_pipeline.py
echo.
echo Virtual Environment:
echo    .venv\Scripts\activate
echo ==========================================

pause