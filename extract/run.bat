@echo off
setlocal

if "%~1"=="" (
    echo Usage: run_extraction.bat "C:\Path\To\Game"
    exit /b 1
)

set "G_PATH=%~1"
set "S_DIR=%~dp0"

:: Use venv if exists
if exist "%S_DIR%..\.venv\Scripts\python.exe" (
    set "PY="%S_DIR%..\.venv\Scripts\python.exe""
) else (
    set "PY=python"
)

echo [1/3] Syncing Definitions...
%PY% "%S_DIR%update_definitions.py" --game-path "%G_PATH%"
if %errorlevel% neq 0 exit /b %errorlevel%

echo [2/3] Building...
dotnet build "%S_DIR%SaintCoinach.Cmd\SaintCoinach.Cmd.csproj" -c Debug
if %errorlevel% neq 0 exit /b %errorlevel%

echo [3/3] Extracting...
pushd "%S_DIR%SaintCoinach.Cmd\bin\Debug\net7.0"
SaintCoinach.Cmd.exe "%G_PATH%"
popd

echo Done.
endlocal
