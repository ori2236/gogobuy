@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0CreatePromotionExcel.ps1"
set EXIT_CODE=%ERRORLEVEL%
echo.
if not "%EXIT_CODE%"=="0" (
  echo יצירת הקובץ נכשלה. קרא את ההודעה שמופיעה למעלה.
) else (
  echo הקובץ נוצר בהצלחה בתיקייה הנוכחית.
)
pause
exit /b %EXIT_CODE%
