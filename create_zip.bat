@echo off
setlocal EnableExtensions

REM Run from the folder where this BAT file is located.
cd /d "%~dp0"

set "SCRIPT_NAME=%~nx0"
set "PS1=%TEMP%\create_server_zip_%RANDOM%%RANDOM%.ps1"

> "%PS1%" (
  echo $ErrorActionPreference = 'Stop'
  echo Add-Type -AssemblyName System.IO.Compression.FileSystem
  echo $root = ^(Get-Item -LiteralPath '.'^).FullName
  echo $zip = Join-Path $root 'server.zip'
  echo $tmp = Join-Path ^([System.IO.Path]::GetTempPath^(^)^) ^('server_zip_' + [System.Guid]::NewGuid^(^).ToString^('N'^)^)
  echo New-Item -ItemType Directory -Path $tmp -Force ^| Out-Null
  echo try {
  echo   $excludeFiles = @^('package-lock.json','README.md','server.zip',$env:SCRIPT_NAME^)
  echo   $excludeDirs = @^('.vscode','node_modules','.git'^)
  echo   Get-ChildItem -LiteralPath $root -Force ^| Where-Object {
  echo     ^($excludeFiles -notcontains $_.Name^) -and ^(-not ^($_.PSIsContainer -and ^($excludeDirs -contains $_.Name^)^)^)
  echo   } ^| ForEach-Object {
  echo     Copy-Item -LiteralPath $_.FullName -Destination $tmp -Recurse -Force
  echo   }
  echo   if ^(Test-Path -LiteralPath $zip^) { Remove-Item -LiteralPath $zip -Force }
  echo   [System.IO.Compression.ZipFile]::CreateFromDirectory^($tmp, $zip^)
  echo   if ^(-not ^(Test-Path -LiteralPath $zip^)^) { throw 'server.zip was not created' }
  echo   $item = Get-Item -LiteralPath $zip
  echo   if ^($item.Length -le 22^) { throw 'server.zip was created but it looks empty' }
  echo   Write-Host ^('Created: ' + $zip^)
  echo   Write-Host ^('Size: ' + $item.Length + ' bytes'^)
  echo } finally {
  echo   if ^(Test-Path -LiteralPath $tmp^) { Remove-Item -LiteralPath $tmp -Recurse -Force -ErrorAction SilentlyContinue }
  echo }
)

echo Creating server.zip in:
echo %CD%
echo.

powershell -NoProfile -ExecutionPolicy Bypass -File "%PS1%"
set "EXIT_CODE=%ERRORLEVEL%"

del "%PS1%" >nul 2>&1

if not "%EXIT_CODE%"=="0" (
  echo.
  echo FAILED.
  echo.
  pause
  exit /b %EXIT_CODE%
)

echo.
echo Done. Check that this file exists:
echo %CD%\server.zip
echo.

dir /a:-d "%CD%\server.zip"
echo.
pause
