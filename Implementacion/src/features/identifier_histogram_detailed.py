import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from collections import Counter
import sys
from tqdm import tqdm

def format_count(count):
    if count >= 1_000_000:
        return f"{count/1_000_000:.1f}M"
    elif count >= 1_000:
        return f"{count/1_000:.1f}K"
    return str(count)

def create_histogram(data, bins, title, filename, color='skyblue', log_scale=False):
    # Agrupar los datos
    grouped = pd.cut(data, bins=bins, right=False).value_counts().sort_index()
    total_values = len(data)
    max_count = grouped.max()
    
    # Crear figura
    plt.figure(figsize=(14, 8))
    ax = grouped.plot(kind='bar', color=color, edgecolor='black', alpha=0.7, logy=log_scale)
    
    # Formatear etiquetas de bins
    bin_labels = []
    for interval in grouped.index.categories:
        left = int(interval.left)
        right = int(interval.right)
        bin_labels.append(f"{left}-{right-1}" if right-left > 1 else str(left))
    
    # Configuración del gráfico
    plt.xticks(range(len(bin_labels)), bin_labels, rotation=45, ha='right')
    plt.title(f"{title}\nTotal valores únicos: {format_count(total_values)}", fontsize=14, pad=20)
    plt.xlabel("Rango de repeticiones", fontsize=12)
    plt.ylabel("Cantidad de valores únicos" + (" (log)" if log_scale else ""), fontsize=12)
    plt.grid(True, which="both", ls="--", axis='y')
    
    min_y = 0.9
    # Añadir porcentajes y conteos sobre las barras
    for i, (count, interval) in enumerate(zip(grouped.values, grouped.index)):
        if count > 0:
            percentage = (count / total_values) * 100
            y_pos = count * 1.1 if count * 1.1 > min_y else min_y * 1.2
            text = f"{percentage:.2f}%\n({format_count(count)})"
            
            ax.text(i, y_pos, text, 
                   ha='center', va='bottom', 
                   fontsize=15, fontweight='bold',
                   bbox=dict(facecolor='white', alpha=0.8, edgecolor='lightgray', boxstyle='round,pad=0.2'))
    
    # Guardar gráfico
    output_path = os.path.join("img", filename)
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    return output_path

def main():
    # Configuración
    if len(sys.argv) < 2:
        print("Error: Debe especificar un archivo CSV")
        sys.exit(1)

    csv_file = sys.argv[1]
    filename_base = os.path.splitext(os.path.basename(csv_file))[0]
    column = "identifier"  
    chunksize = 1_000_000  
    os.makedirs("img", exist_ok=True)

    print(f"\n📊 Iniciando análisis de: {csv_file}")
    print(f"🔍 Columna analizada: {column}")
    
    # Paso 1: Contar frecuencias con barra de progreso
    print("\n🔢 Contando frecuencias...")
    counter = Counter()
    
    # Primera pasada: contar total de filas para la barra de progreso
    with tqdm(desc="  Contando filas totales", unit=' filas') as pbar:
        total_rows = 0
        for chunk in pd.read_csv(csv_file, usecols=[column], chunksize=chunksize):
            total_rows += len(chunk)
            pbar.update(len(chunk))
    
    # Segunda pasada: procesar datos con barra de progreso
    with tqdm(total=total_rows, desc="  Procesando datos", unit=' filas') as pbar:
        for chunk in pd.read_csv(csv_file, usecols=[column], chunksize=chunksize):
            counter.update(chunk[column].dropna().astype(str))
            pbar.update(len(chunk))

    frequencies = pd.Series(counter)
    total_unique = len(frequencies)
    print(f"\n✅ Datos procesados - Total valores únicos: {format_count(total_unique)}")

    # Filtrar datos en grupos de frecuencias
    print("\n📂 Clasificando frecuencias...")
    with tqdm(total=4, desc="  Progreso") as pbar:
        low_freq = frequencies[(frequencies >= 1) & (frequencies <= 99)]
        pbar.update(1)
        mid_freq = frequencies[(frequencies >= 100) & (frequencies <= 1000)]
        pbar.update(1)
        high_freq = frequencies[(frequencies >= 1001) & (frequencies <= 10000)]
        pbar.update(1)

    # Configurar bins para cada grupo
    low_bin = list(range(1, 100, 10)) + [100]
    mid_bin = list(range(100, 1001, 100)) + [1001]
    high_bin = list(range(1001, 10001, 1000)) + [10001]

    # --- Imprimir resumen en terminal ---
    print("\n=== 📈 Resumen de frecuencias ===")
    print(f"\n🔵 Rango 1-99 repeticiones:")
    print(f"   - Valores únicos: {format_count(len(low_freq))} ({len(low_freq)/total_unique:.1%})")
    
    print(f"\n🟢 Rango 100-1000 repeticiones:")
    print(f"   - Valores únicos: {format_count(len(mid_freq))} ({len(mid_freq)/total_unique:.1%})")
    
    print(f"\n🔴 Rango 1001-10000 repeticiones:")
    print(f"   - Valores únicos: {format_count(len(high_freq))} ({len(high_freq)/total_unique:.1%})")

    # --- Generar gráficos con barra de progreso ---
    print("\n🎨 Generando gráficos...")
    with tqdm(total=3, desc="  Progreso") as pbar:
        # Gráfico 1: Frecuencias bajas (1-100)
        low_path = create_histogram(
            low_freq, 
            bins=low_bin,
            title="Distribución de Frecuencias (1-99 repeticiones)",
            filename=f"histograma_1-99_{column}_{filename_base}.png",
            color='#4C72B0'
        )
        pbar.update(1)
        
        # Gráfico 2: Frecuencias medias (100-1000)
        mid_path = create_histogram(
            mid_freq,
            bins=mid_bin,
            title="Distribución de Frecuencias (100-1000 repeticiones)",
            filename=f"histograma_100-1k_{column}_{filename_base}.png",
            color='#55A868',
            log_scale=True
        )
        pbar.update(1)
        
        # Gráfico 3: Frecuencias altas (1001-10000)
        high_path = create_histogram(
            high_freq,
            bins=high_bin,
            title="Distribución de Frecuencias (1001-10,000 repeticiones)",
            filename=f"histograma_1k-10k_{column}_{filename_base}.png",
            color='#C44E52',
            log_scale=True
        )
        pbar.update(1)
    
    # Mostrar rutas de los archivos generados
    print("\n✅ Gráficos generados exitosamente:")
    print(f"📊 {low_path}")
    print(f"📊 {mid_path}")
    print(f"📊 {high_path}")

if __name__ == "__main__":
    main()