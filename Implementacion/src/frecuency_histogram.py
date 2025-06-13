import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from collections import Counter

# Configuración
archivo_csv = "Mobility_Data_Slim.csv"
columna = "identifier"  
chunksize = 1_000_000  # Procesar por bloques
os.makedirs("img", exist_ok=True)  # Asegurarse de que el directorio exista

# Paso 1: Contar frecuencias (en bloques para ahorrar memoria)
counter = Counter()
for chunk in pd.read_csv(archivo_csv, usecols=[columna], chunksize=chunksize):
    counter.update(chunk[columna].dropna().astype(str))

# Convertir a Series de pandas
frecuencias = pd.Series(counter)

# Definir los rangos de 1000 en 1000
max_freq = frecuencias.max()
bins = [0] + [10**i for i in range(0, int(np.log10(max_freq)) + 2)]  # Ej: [0, 1, 10, 100, 1000, ...]
frecuencias_agrupadas = pd.cut(frecuencias, bins=bins, right=False).value_counts().sort_index()

# Paso 2: Crear gráfico de barras (logarítmico en Y)
plt.figure(figsize=(12, 7))
frecuencias_agrupadas.plot(kind='bar', logy=True, alpha=0.7, edgecolor='black')

# Mejorar formato de etiquetas en el eje X
plt.xticks(rotation=45, ha='right')  # Rotar etiquetas para mejor legibilidad
plt.title("Distribución de Frecuencias Agrupadas en Bloques de 1000 Repeticiones")
plt.xlabel("Rango de repeticiones (ej: [0, 1000) significa 0-999 repeticiones)")
plt.ylabel("Cantidad de valores únicos (log)")
plt.grid(True, which="both", ls="--", axis='y')

# Paso 3: Guardar
output_path = os.path.join("img", "histograma_frecuencias_agrupadas_1000.png")
plt.savefig(output_path, dpi=300, bbox_inches='tight')
plt.close()