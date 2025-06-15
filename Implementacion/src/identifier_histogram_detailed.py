import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from collections import Counter

# Configuración
archivo_csv = "Mobility_Data_Slim.csv"
columna = "identifier"  
chunksize = 1_000_000  
os.makedirs("img", exist_ok=True)

# Paso 1: Contar frecuencias
counter = Counter()
for chunk in pd.read_csv(archivo_csv, usecols=[columna], chunksize=chunksize):
    counter.update(chunk[columna].dropna().astype(str))
frecuencias = pd.Series(counter)

# Filtrar datos en grupos de frecuencias
frecuencias_bajas = frecuencias[(frecuencias >= 1) & (frecuencias <= 99)]
frecuencias_medias = frecuencias[(frecuencias >= 100) & (frecuencias <= 1000)]
frecuencias_altas = frecuencias[(frecuencias >= 1001) & (frecuencias <= 10000)]

# Configurar bins para cada grupo
bins_bajas = list(range(1, 100, 10))  # 1-99 en pasos de 10 
bins_medias = list(range(100, 1001, 100))  # 100-1000 en pasos de 100
bins_altas = list(range(1001, 10001, 1000))  # 1001-10000 en pasos de 1000

# Agrupar frecuencias
freciencias_bajas_agrupadas = pd.cut(frecuencias_bajas, bins=bins_bajas, right=False).value_counts().sort_index()
frecuencias_medias_agrupadas = pd.cut(frecuencias_medias, bins=bins_medias, right=False).value_counts().sort_index()
frecuencias_altas_agrupadas = pd.cut(frecuencias_altas, bins=bins_altas, right=False).value_counts().sort_index()

# --- Imprimir resumen en terminal ---
print("\n=== Resumen de frecuencias ===")
print(f"\n**Rango 1-100 repeticiones**:")
print(f" - Total de valores únicos: {len(frecuencias_bajas)}")
print(f"\n**Rango 100-1000 repeticiones**:")
print(f" - Total de valores únicos: {len(frecuencias_medias)}")
print(f"\n**Rango 1000-10000 repeticiones**:")
print(f" - Total de valores únicos: {len(frecuencias_altas)}")



# --- Gráfico 1: Frecuencias medias (1-100) ---
plt.figure(figsize=(10, 6))
freciencias_bajas_agrupadas.plot(kind='bar', color='skyblue', edgecolor='black', alpha=0.7)
plt.title("Distribución de Frecuencias (1-100 repeticiones)")
plt.xlabel("Rango de repeticiones")
plt.ylabel("Cantidad de valores únicos")
plt.xticks(rotation=45)
plt.grid(axis='y', linestyle='--')
plt.tight_layout()
plt.savefig(os.path.join("img", "histograma_1_100.png"), dpi=300)
plt.close()

# --- Gráfico 2: Frecuencias medias (100-1000) ---
plt.figure(figsize=(10, 6))
frecuencias_medias_agrupadas.plot(kind='bar', color='skyblue', edgecolor='black', alpha=0.7)
plt.title("Distribución de Frecuencias (100-1000 repeticiones)")
plt.xlabel("Rango de repeticiones")
plt.ylabel("Cantidad de valores únicos")
plt.xticks(rotation=45)
plt.grid(axis='y', linestyle='--')
plt.tight_layout()
plt.savefig(os.path.join("img", "histograma_100_1000.png"), dpi=300)
plt.close()

# --- Gráfico 3: Frecuencias altas (1001-10000) ---
plt.figure(figsize=(10, 6))
frecuencias_altas_agrupadas.plot(kind='bar', color='salmon', edgecolor='black', alpha=0.7)
plt.title("Distribución de Frecuencias (1001-10,000 repeticiones)")
plt.xlabel("Rango de repeticiones")
plt.ylabel("Cantidad de valores únicos")
plt.xticks(rotation=45)
plt.grid(axis='y', linestyle='--')
plt.tight_layout()
plt.savefig(os.path.join("img", "histograma_1001_10000.png"), dpi=300)
plt.close()

print("Gráficos guardados en /img/")