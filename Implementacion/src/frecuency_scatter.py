import os
import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter

# Configuración
archivo_csv = 'Mobility_Data_Slim.csv'
columna = 'identifier'  # Cambia esto
nombre_imagen = 'dispersion_identifier.png'
chunksize = 1_000_000  # Procesar en trozos de 1 millón
os.makedirs('img', exist_ok=True)  # Asegurarse de que el directorio exista

# Contar frecuencias
counter = Counter()
for chunk in pd.read_csv(archivo_csv, usecols=[columna], chunksize=chunksize):
    counter.update(chunk[columna].dropna().astype(str))

# Preparar datos
valores, frecuencias = zip(*counter.items())

# Crear y guardar el gráfico
plt.figure(figsize=(15, 10))
plt.scatter(range(len(frecuencias)), frecuencias, alpha=0.5, s=10, color='blue')
plt.title(f'Diagrama de dispersión de frecuencias - {columna}', fontsize=14)
plt.xlabel('Índice de valores únicos', fontsize=12)
plt.ylabel('Frecuencia (escala log)', fontsize=12)
plt.yscale('log')
plt.grid(True, which="both", ls="--")

# Guardar como PNG con alta calidad
output_path = os.path.join('img', nombre_imagen)
plt.savefig(output_path, dpi=300, bbox_inches='tight', format='png')
print(f"Diagrama guardado como: /img/{nombre_imagen}")
print(f"Valores únicos: {len(frecuencias)}")  
print(f"Frecuencia máxima: {max(frecuencias)} (valor: {valores[frecuencias.index(max(frecuencias))]})")  
print(f"Frecuencia mínima: {min(frecuencias)}")  

# Cerrar la figura para liberar memoria
plt.close()