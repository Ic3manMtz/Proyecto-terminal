import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from collections import Counter
import sys
from tqdm import tqdm
import math

def format_count(count):
    if count >= 1_000_000:
        return f"{count/1_000_000:.1f}M"
    elif count >= 1_000:
        return f"{count/1_000:.1f}K"
    return str(count)

def main():
    # Configuración
    if len(sys.argv) < 2:
        print("Error: Debe especificar un archivo CSV como argumento")
        sys.exit(1)

    csv_file = sys.argv[1]
    filename = os.path.splitext(os.path.basename(csv_file))[0]
    column = "identifier"  
    chunksize = 1_000_000
    
    print(f"\nIniciando procesamiento del archivo: {csv_file}")
    print(f"Columna analizada: {column}")
    os.makedirs("img", exist_ok=True)
    print("Directorio 'img' verificado/creado")

    # Paso 1: Contar frecuencias
    print("\nProcesando datos y contando frecuencias...")
    counter = Counter()
    
    total_chunks = sum(1 for _ in pd.read_csv(csv_file, usecols=[column], chunksize=chunksize))
    
    with tqdm(total=total_chunks, unit=' chunk') as pbar:
        for chunk in pd.read_csv(csv_file, usecols=[column], chunksize=chunksize):
            counter.update(chunk[column].dropna().astype(str))
            pbar.update(1)

    frecuency = pd.Series(counter)
    total_unique_values = len(frecuency)
    max_freq = frecuency.max()
    
    print(f"✅ Datos procesados correctamente")
    print(f"Total de valores únicos: {total_unique_values:,}")
    print(f"Frecuencia máxima: {max_freq:,}")

    # Definir los rangos exponenciales
    bins = [0] + [10**i for i in range(0, int(np.log10(max_freq)) + 2)]  
    group_freq = pd.cut(frecuency, bins=bins, right=False).value_counts().sort_index()

    # Calcular estadísticas
    total_ocurrence = frecuency.sum()
    percentage_per_range = (group_freq / total_unique_values * 100).round(2)
    
    # Paso 2: Crear gráfico
    print("\nGenerando histograma con estadísticas...")
    plt.figure(figsize=(16, 9))  # Tamaño aumentado para mejor visualización
    ax = group_freq.plot(kind='bar', logy=True, alpha=0.7, edgecolor='black')
    
    # Formatear etiquetas de los bins
    formatted_labels = []
    for interval in group_freq.index.categories:
        left = int(interval.left)
        right = int(interval.right - 1)
        formatted_labels.append(f"{left}-{right}" if left != right else f"{left}")
    
    plt.xticks(range(len(formatted_labels)), formatted_labels, rotation=45, ha='right')
    
    # Configuración del gráfico
    plt.title(f"Histograma de Frecuencias de Identificadores\nArchivo: {filename}", fontsize=16, pad=20)
    plt.xlabel("Rango de Frecuencia", fontsize=14)
    plt.ylabel("Cantidad de Valores Únicos (log)", fontsize=14)
    plt.grid(True, which="both", ls="--", axis='y')
    
    # Estadísticas generales
    stats_text = (
        f"Total valores únicos: {format_count(total_unique_values)}\n"
        f"Total ocurrencias: {format_count(total_ocurrence)}\n"
        f"Frecuencia máxima: {format_count(max_freq)}"
    )
    plt.annotate(stats_text, 
                 xy=(0.95, 0.95), 
                 xycoords='axes fraction', 
                 fontsize=15,
                 ha='right', 
                 va='top',
                 bbox=dict(boxstyle='round', facecolor='white', alpha=0.9))
    
    max_val = group_freq.max()
    min_y = 0.9  # Valor mínimo en escala logarítmica
    
    for i, (count, porcent) in enumerate(zip(group_freq.values, percentage_per_range.values)):
        if count > 0:
            # Calcular posición vertical
            y_pos = count * 1.1 if count * 1.1 > min_y else min_y * 1.2
            
            # Texto a mostrar (porcentaje y cantidad)
            text = f"{porcent}%\n({format_count(count)})"
            
            # Añadir anotación
            ax.text(
                i, y_pos, text, 
                ha='center', va='bottom', 
                fontsize=15, 
                fontweight='bold',
                bbox=dict(
                    facecolor='white', 
                    alpha=0.85, 
                    edgecolor='lightgray', 
                    boxstyle='round,pad=0.3'
                )
            )

    # Paso 3: Guardar
    output_path = os.path.join("img", f"histograma_{column}_{filename}.png")
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    # Mostrar estadísticas en terminal
    print("\n=== DISTRIBUCIÓN DE FRECUENCIAS ===")
    for i, (intervalo, count) in enumerate(group_freq.items()):
        print(f"Rango {formatted_labels[i]}: {count:,}")
    
    print(f"\n✅ Histograma generado exitosamente")
    print(f"Archivo guardado en: {output_path}")
    print(f"Total ocurrencias analizadas: {total_ocurrence:,}\n")

if __name__ == "__main__":
    main()