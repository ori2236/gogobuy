@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0ValidatePromotionExcel.ps1" %*
set EXIT_CODE=%ERRORLEVEL%
echo.
if not "%EXIT_CODE%"=="0" (
  echo נמצאו שגיאות. פתח את קובץ האקסל ותקן את העמודות סטטוס ושגיאות.
) else (
  echo הקובץ תקין ומוכן לשליחה.
)
pause
exit /b %EXIT_CODE%
