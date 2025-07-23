@echo off
REM Iniciar los servicios en segundo plano con Docker Compose
docker-compose up -d

REM Esperar 2 segundos
timeout /t 2 > nul

REM Adjuntarse al contenedor llamado 'data-analysis'
docker attach data-analysis
