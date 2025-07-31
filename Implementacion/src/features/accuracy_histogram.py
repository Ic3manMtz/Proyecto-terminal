import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys
from tqdm import tqdm

def classify_tech(valor):
    if 1 <= valor <= 20:
        return 'GPS Satelital'
    elif 5 <= valor <= 50:
        return 'A-GPS (Asistido por red)'
    elif 20 <= valor <= 500:
        return 'Triangulación WiFi/Redes Móviles'
    else:
        return 'Fuera de rango'

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
    column = "device_horizontal_accuracy"  
    bins = 100
    
    print(f"\nIniciando procesamiento del archivo: {csv_file}")
    print(f"Columna analizada: {column}")
    
    os.makedirs("img", exist_ok=True)
    print("Directorio 'img' verificado/creado")

    # Procesamiento de datos
    print("\nProcesando datos y clasificando tecnologías...")
    frequency = pd.Series(dtype=float)
    tech_counts = {
        'GPS Satelital': 0,
        'A-GPS (Asistido por red)': 0,
        'Triangulación WiFi/Redes Móviles': 0,
        'Fuera de rango': 0
    }

    # Contar filas para la barra de progreso
    total_row = sum(1 for _ in pd.read_csv(csv_file, usecols=[column], chunksize=1_000_000))
    
    with tqdm(total=total_row, unit='M rows') as pbar:
        for chunk in pd.read_csv(csv_file, usecols=[column], chunksize=1_000_000):
            # Eliminar valores nulos y filtrar
            chunk_clean = chunk[column].dropna()
            
            # Contar tecnologías
            for valor in chunk_clean:
                tech = classify_tech(valor)
                tech_counts[tech] += 1
            
            # Procesar frecuencias
            counts = chunk_clean.value_counts()
            if not frequency.empty or not counts.empty:
                frequency = pd.concat([frequency, counts], axis=0).groupby(level=0).sum()
            pbar.update(1)

    # Calcular porcentajes
    total = sum(tech_counts.values())
    percentage = {k: (v/total)*100 for k, v in tech_counts.items()}

    # Generar histograma
    print("\nGenerando histograma con estadísticas...")
    counts, edges = np.histogram(frequency.index, bins=bins, weights=frequency.values)

    plt.figure(figsize=(14, 8))
    plt.bar(edges[:-1], counts, width=np.diff(edges), align='edge', edgecolor='black', alpha=0.7)
    
    # Añadir líneas divisorias de tecnologías
    plt.axvline(x=20, color='r', linestyle='--', alpha=0.5)
    plt.axvline(x=50, color='g', linestyle='--', alpha=0.5)
    plt.axvline(x=200, color='b', linestyle='--', alpha=0.5)
    
    # Formatear estadísticas para mostrar
    gps_str = f"GPS Satelital: {percentage['GPS Satelital']:.2f}%\n({format_count(tech_counts['GPS Satelital'])} reg)"
    agps_str = f"A-GPS: {percentage['A-GPS (Asistido por red)']:.2f}%\n({format_count(tech_counts['A-GPS (Asistido por red)'])} reg)"
    wifi_str = f"WiFi/Redes: {percentage['Triangulación WiFi/Redes Móviles']:.2f}%\n({format_count(tech_counts['Triangulación WiFi/Redes Móviles'])} reg)"
    
    # Añadir estadísticas en el histograma
    plt.text(10, max(counts)*0.9, gps_str, ha='center', color='r', fontsize=10, 
             bbox=dict(facecolor='white', alpha=0.8, edgecolor='r'))
    plt.text(40, max(counts)*0.8, agps_str, ha='center', color='g', fontsize=10, 
             bbox=dict(facecolor='white', alpha=0.8, edgecolor='g'))
    plt.text(190, max(counts)*0.7, wifi_str, ha='center', color='b', fontsize=10, 
             bbox=dict(facecolor='white', alpha=0.8, edgecolor='b'))

    
    # Título y etiquetas
    plt.title(f"Distribución de precisiones de {column}\nArchivo: {filename}", fontsize=14)
    plt.xlabel(f"Valores de {column} (metros)", fontsize=12)
    plt.ylabel("Frecuencia (Millones)", fontsize=12)
    plt.xticks(edges[::5], rotation=45)
    plt.grid(axis='y', linestyle='--')
    plt.tight_layout()

    # Guardar
    output_path = os.path.join("img", f"histograma_{column}_{filename}.png")
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    # Mostrar estadísticas en terminal
    print("\n=== DISTRIBUCIÓN DE TECNOLOGÍAS DE GEOLOCALIZACIÓN ===")
    for tech, count in tech_counts.items():
        print(f"{tech}: {count:,} registros ({percentage[tech]:.2f}%)")
    
    print(f"\n✅ Histograma generado exitosamente")
    print(f"Archivo guardado en: {output_path}")
    print(f"Total registros analizados: {total:,}\n")

if __name__ == "__main__":
    main()