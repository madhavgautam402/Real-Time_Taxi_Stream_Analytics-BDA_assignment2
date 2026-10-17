@echo off
setlocal
cd /d "%~dp0"

echo [1/4] Python environment
if not exist ".venv\Scripts\python.exe" python -m venv .venv
call ".venv\Scripts\activate.bat"
python -m pip install -q -r requirements.txt

echo [2/4] Kafka in Docker
docker info >nul 2>&1
if errorlevel 1 (
  echo Docker Desktop is not running. Start it and run this file again.
  exit /b 1
)
docker compose up -d
:waitkafka
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >nul 2>&1
if errorlevel 1 (
  timeout /t 3 /nobreak >nul
  goto waitkafka
)

echo [3/4] Pipeline: producer - taxi.raw - stage1 - taxi.clean - stage2 and stage3 - taxi.metrics and taxi.lsh - final consumer
python -m scripts.run_pipeline --run-id default

echo [4/4] Done. See results\results.jsonl and results\experiment_summary.csv
endlocal