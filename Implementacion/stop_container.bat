@echo off
REM Detener y eliminar contenedores, redes y volúmenes definidos en docker-compose
docker-compose down -v
