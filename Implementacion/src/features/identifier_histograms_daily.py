import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from collections import Counter
import sys
from tqdm import tqdm
import math
from datetime import datetime

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
    identifier_col = "identifier"
    timestamp_col = "timestamp"
    chunksize = 1_000_000
    
    print(f"\nIniciando procesamiento del archivo: {csv_file}")
    print(f"Columnas analizadas: {identifier_col} y {timestamp_col}")
    
    # Crear directorios necesarios
    os.makedirs("img/daily_histograms", exist_ok=True)
    print("Directorios 'img/daily_histograms' verificados/creados")

    # Paso 1: Leer datos y agrupar por día
    print("\nProcesando datos y agrupando por día...")
    
    date_freqs = {}
    max_freq = 0
    
    # Primero determinamos el número total de chunks para la barra de progreso
    total_chunks = sum(1 for _ in pd.read_csv(csv_file, usecols=[timestamp_col, identifier_col], chunksize=chunksize))
    
    with tqdm(total=total_chunks, unit=' chunk') as pbar:
        for chunk in pd.read_csv(csv_file, usecols=[timestamp_col, identifier_col], chunksize=chunksize):
            try:
                # Convertir timestamp a fecha, manejando diferentes formatos
                chunk['date'] = pd.to_datetime(
                    chunk[timestamp_col], 
                    format='mixed',  # Permite inferir el formato para cada elemento
                    errors='coerce'  # Convierte errores a NaT
                ).dt.date
                
                # Eliminar filas con fechas inválidas
                chunk = chunk.dropna(subset=['date'])
                
                # Procesar cada día por separado
                for date, group in chunk.groupby('date'):
                    if date not in date_freqs:
                        date_freqs[date] = Counter()
                    
                    date_freqs[date].update(group[identifier_col].dropna().astype(str))
                    
                    current_max = date_freqs[date].most_common(1)[0][1] if date_freqs[date] else 0
                    if current_max > max_freq:
                        max_freq = current_max
            except Exception as e:
                print(f"\nError procesando chunk: {str(e)}")
                continue
            finally:
                pbar.update(1)

    if not date_freqs:
        print("\nError: No se encontraron datos válidos para procesar")
        sys.exit(1)

    # Definir los rangos exponenciales basados en la frecuencia máxima global
    bins = [0] + [10**i for i in range(0, int(np.log10(max_freq)) + 2)] if max_freq > 0 else [0, 1]
    
    # Procesar cada día para generar histogramas
    print("\nGenerando histogramas por día...")
    
    for date, counter in tqdm(date_freqs.items(), total=len(date_freqs), unit=' día'):
        frecuency = pd.Series(counter)
        total_unique_values = len(frecuency)
        total_ocurrence = frecuency.sum()
        
        # Agrupar en los rangos definidos
        group_freq = pd.cut(frecuency, bins=bins, right=False).value_counts().sort_index()
        percentage_per_range = (group_freq / total_unique_values * 100).round(2)
        
        # Crear el gráfico
        plt.figure(figsize=(16, 9))
        ax = group_freq.plot(kind='bar', logy=True, alpha=0.7, edgecolor='black')
        
        # Formatear etiquetas de los bins
        formatted_labels = []
        for interval in group_freq.index.categories:
            left = int(interval.left)
            right = int(interval.right - 1)
            formatted_labels.append(f"{left}-{right}" if left != right else f"{left}")
        
        plt.xticks(range(len(formatted_labels)), formatted_labels, rotation=45, ha='right')
        
        # Configuración del gráfico
        date_str = date.strftime('%Y-%m-%d')
        plt.title(f"Histograma de Frecuencias de Identificadores\nArchivo: {filename} - Fecha: {date_str}", fontsize=16, pad=20)
        plt.xlabel("Rango de Frecuencia", fontsize=14)
        plt.ylabel("Cantidad de Valores Únicos (log)", fontsize=14)
        plt.grid(True, which="both", ls="--", axis='y')
        
        # Estadísticas generales
        stats_text = (
            f"Total valores únicos: {(total_unique_values):,}\n"
            f"Total ocurrencias: {(total_ocurrence):,}\n"
            f"Frecuencia máxima: {(frecuency.max()):,}"
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
                y_pos = count * 1.1 if count * 1.1 > min_y else min_y * 1.2
                text = f"{porcent}%\n({format_count(count)})"
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

        # Guardar el gráfico en la subcarpeta daily_histograms
        output_path = os.path.join("img", "daily_histograms", f"histograma_{identifier_col}_{filename}_{date_str}.png")
        plt.tight_layout()
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
    
    print("\n✅ Histogramas generados exitosamente")
    print(f"Archivos guardados en: img/daily_histograms/")
    print(f"Total días procesados: {len(date_freqs)}")
    print(f"Frecuencia máxima global encontrada: {format_count(max_freq)}\n")

if __name__ == "__main__":
    main()