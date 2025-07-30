#!/bin/bash

# Verifica si el contenedor está corriendo
if ! docker ps --filter "name=^/data-analysis$" --filter "status=running" | grep -q data-analysis; then
    echo "🔄 Contenedor no está corriendo. Levantando con docker-compose..."
    docker-compose up -d
    sleep 2
else
    echo "✅ Contenedor ya está corriendo. Usando instancia existente."
fi

# Ejecuta el script dentro del contenedor
docker exec -it data-analysis python3 /app/src/main.py
