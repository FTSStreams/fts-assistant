@echo off
git add .
git commit -m "Auto-commit: %date% %time%" || exit /b 0
git push origin main