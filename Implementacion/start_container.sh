#!/bin/bash

# Verifica si el contenedor está corriendo
if ! docker ps --filter "name=^/data-analysis$" --filter "status=running" | grep -q data-analysis; then
    echo "🔄 Contenedor no está corriendo. Levantando con docker-compose..."
    docker-compose up -d
    # Espera hasta que pip termine de instalar
    echo "⏳ Esperando que se instalen las dependencias..."
    while ! docker exec data-analysis pip show colorama &> /dev/null; do
        sleep 2
    done
    echo "✅ Dependencias instaladas correctamente."
else
    echo "✅ Contenedor ya está corriendo. Usando instancia existente."
fi

# Ejecuta el script principal
echo "🚀 Ejecutando script..."
docker exec -it data-analysis python3 /app/src/main.py