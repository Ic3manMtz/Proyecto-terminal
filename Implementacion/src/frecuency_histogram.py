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

# Paso 2: Crear histograma logarítmico
plt.figure(figsize=(12, 7))
bins = 10 ** np.linspace(0, np.log10(frecuencias.max()), 100)  # Bins logarítmicos
plt.hist(frecuencias, bins=bins, edgecolor='black', alpha=0.7)
plt.xscale('log')
plt.yscale('log')
plt.title("Distribución de Frecuencias (6M valores únicos)")
plt.xlabel("Frecuencia de aparición (log)")
plt.ylabel("Cantidad de valores (log)")
plt.grid(True, which="both", ls="--")

# Paso 3: Guardar
output_path = os.path.join("img", "histograma_identifier.png")
plt.savefig(output_path, dpi=300, bbox_inches='tight')
plt.close()