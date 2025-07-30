#!/bin/bash
docker restart data-analysis
sleep 2
# Ejecuta el script dentro del contenedor
docker exec -it data-analysis python3 /app/src/main.py