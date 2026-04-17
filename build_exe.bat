@echo off
setlocal

if not exist ".venv\Scripts\python.exe" (
  echo Virtual environment not found. Expected .venv\Scripts\python.exe
  exit /b 1
)

.venv\Scripts\python.exe -m PyInstaller ^
  --noconfirm ^
  --clean ^
  --onefile ^
  --name employee_location_app ^
  --add-data "config;config" ^
  launcher.py

echo.
echo Built executable:
echo dist\employee_location_app.exe

