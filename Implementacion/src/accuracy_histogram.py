import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Configuración
archivo_csv = "Mobility_Data_Slim.csv"
columna = "device_horizontal_accuracy"  
bins = 100  # Número de bloques/rangos 
os.makedirs("img", exist_ok=True)  # Asegurarse de que el directorio exista

# 1. Cargar datos por chunks (para ahorrar memoria)
frecuencias = pd.Series(dtype=float)
for chunk in pd.read_csv(archivo_csv, usecols=[columna], chunksize=1_000_000):
    frecuencias = pd.concat([frecuencias, chunk[columna].value_counts()])

# 2. Agrupar frecuencias por rangos de valores
counts, edges = np.histogram(frecuencias.index, bins=bins, weights=frecuencias.values)

# 3. Graficar
plt.figure(figsize=(12, 6))
plt.bar(edges[:-1], counts, width=np.diff(edges), align='edge', edgecolor='black', alpha=0.7)
plt.title("Frecuencias de Valores Agrupados por Rangos (0-200)")
plt.xlabel("Rango de Valores")
plt.ylabel("Frecuencia Total (Millones)")
plt.xticks(edges[::5], rotation=45)  # Mostrar cada 5° rango para legibilidad
plt.grid(axis='y', linestyle='--')

# 4. Guardar
output_path = "img/histograma_frecuencias_rangos.png"
plt.savefig(output_path, dpi=300, bbox_inches='tight')
plt.close()