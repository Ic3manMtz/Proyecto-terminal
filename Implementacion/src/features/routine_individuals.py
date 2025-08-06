import pandas as pd
import sys
import os
from tqdm import tqdm
from collections import defaultdict
import matplotlib.pyplot as plt

def analyze_multi_day_users(input_file):
    print(f"\nIniciando análisis multi-día del archivo: {input_file}")
    filename = os.path.splitext(os.path.basename(input_file))[0]
    
    # Configuración de procesamiento por chunks
    chunksize = 1_000_000
    
    # Diccionario para almacenar días únicos por identificador
    identifier_days = defaultdict(set)
    total_records = 0
    
    # Paso 1: Procesar archivo por chunks
    print("\nProcesando datos por chunks...")
    
    # Contar total de chunks para barra de progreso
    total_chunks = sum(1 for _ in pd.read_csv(input_file, usecols=['identifier', 'timestamp'], chunksize=chunksize))
    
    with tqdm(total=total_chunks, unit=' chunk', desc="Procesando") as pbar:
        for chunk in pd.read_csv(input_file, usecols=['identifier', 'timestamp'], chunksize=chunksize):
            # Limpiar datos
            chunk = chunk.dropna()
            
            # Convertir timestamp a datetime y extraer fecha
            # Manejar formato con microsegundos: '2022-11-07 02:04:21.000'
            chunk['timestamp'] = pd.to_datetime(chunk['timestamp'], format='mixed', errors='coerce')
            
            # Eliminar registros con timestamps inválidos
            chunk = chunk.dropna(subset=['timestamp'])
            
            # Extraer solo la fecha
            chunk['date'] = chunk['timestamp'].dt.date
            
            # Agrupar por identificador y obtener días únicos
            for identifier, group in chunk.groupby('identifier'):
                unique_dates = set(group['date'])
                identifier_days[identifier].update(unique_dates)
            
            total_records += len(chunk)
            pbar.update(1)
    
    print(f"✅ Procesados {total_records:,} registros")
    
    # Paso 2: Analizar resultados
    print("\nAnalizando patrones multi-día...")
    
    # Contar días por identificador
    days_per_identifier = {identifier: len(dates) for identifier, dates in identifier_days.items()}
    
    # Estadísticas generales
    total_identifiers = len(days_per_identifier)
    multi_day_identifiers = sum(1 for days in days_per_identifier.values() if days > 1)
    single_day_identifiers = total_identifiers - multi_day_identifiers
    
    # Distribución de días
    days_distribution = defaultdict(int)
    for days_count in days_per_identifier.values():
        days_distribution[days_count] += 1
    
    # Paso 3: Mostrar resultados
    print("\n" + "="*60)
    print("RESULTADOS DEL ANÁLISIS MULTI-DÍA")
    print("="*60)
    print(f"Total de identificadores únicos: {total_identifiers:,}")
    print(f"Identificadores con registros en un solo día: {single_day_identifiers:,} ({single_day_identifiers/total_identifiers*100:.2f}%)")
    print(f"Identificadores con registros en múltiples días: {multi_day_identifiers:,} ({multi_day_identifiers/total_identifiers*100:.2f}%)")
    
    if multi_day_identifiers > 0:
        max_days = max(days_per_identifier.values())
        avg_days_multi_day = sum(days for days in days_per_identifier.values() if days > 1) / multi_day_identifiers
        
        print(f"\nEstadísticas de identificadores multi-día:")
        print(f"Máximo número de días por identificador: {max_days}")
        print(f"Promedio de días (solo multi-día): {avg_days_multi_day:.2f}")
    
    # Paso 4: Distribución detallada
    print(f"\nDistribución de días por identificador:")
    print("-" * 40)
    
    # Mostrar primeros 20 valores de la distribución
    sorted_distribution = sorted(days_distribution.items())
    for days, count in sorted_distribution[:20]:
        percentage = count / total_identifiers * 100
        print(f"{days:2d} día(s): {count:8,} identificadores ({percentage:5.2f}%)")
    
    if len(sorted_distribution) > 20:
        remaining_count = sum(count for days, count in sorted_distribution[20:])
        remaining_percentage = remaining_count / total_identifiers * 100
        print(f"Otros:      {remaining_count:8,} identificadores ({remaining_percentage:5.2f}%)")
    
    # Paso 5: Generar gráfico
    print(f"\nGenerando visualización...")
    
    # Preparar datos para el gráfico (limitar a primeros 30 valores para legibilidad)
    plot_data = dict(sorted_distribution[:30])
    
    plt.figure(figsize=(15, 8))
    bars = plt.bar(plot_data.keys(), plot_data.values(), alpha=0.7, edgecolor='black')
    
    # Configuración del gráfico
    plt.title(f'Distribución de Días por Identificador\nArchivo: {filename}', fontsize=16, pad=20)
    plt.xlabel('Número de Días con Registros', fontsize=14)
    plt.ylabel('Cantidad de Identificadores', fontsize=14)
    plt.yscale('log')  # Escala logarítmica para mejor visualización
    plt.grid(True, alpha=0.3, axis='y')
    
    # Añadir etiquetas en las barras más significativas
    for i, (days, count) in enumerate(list(plot_data.items())[:10]):
        if count > 0:
            percentage = count / total_identifiers * 100
            plt.text(days, count * 1.1, f'{percentage:.1f}%', 
                    ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    # Estadísticas en el gráfico
    stats_text = (
        f"Total identificadores: {total_identifiers:,}\n"
        f"Multi-día: {multi_day_identifiers:,} ({multi_day_identifiers/total_identifiers*100:.1f}%)\n"
        f"Un solo día: {single_day_identifiers:,} ({single_day_identifiers/total_identifiers*100:.1f}%)"
    )
    
    plt.text(0.98, 0.98, stats_text, 
             transform=plt.gca().transAxes, 
             fontsize=12, ha='right', va='top',
             bbox=dict(boxstyle='round', facecolor='white', alpha=0.9))
    
    # Guardar gráfico
    os.makedirs("img", exist_ok=True)
    output_path = os.path.join("img", f"multi_day_analysis_{filename}.png")
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    # Paso 6: Generar archivo de resumen (opcional)
    print(f"\nGenerando archivo de resumen...")
    
    summary_data = []
    for identifier, dates in identifier_days.items():
        summary_data.append({
            'identifier': identifier,
            'num_days': len(dates),
            'first_date': min(dates),
            'last_date': max(dates),
            'date_span_days': (max(dates) - min(dates)).days + 1 if len(dates) > 1 else 1
        })
    
    summary_df = pd.DataFrame(summary_data)
    summary_path = f"{filename}_multi_day_summary.csv"
    summary_df.to_csv(summary_path, index=False)
    
    print(f"✅ Análisis completado exitosamente")
    print(f"📊 Gráfico guardado en: {output_path}")
    print(f"📋 Resumen guardado en: {summary_path}")
    print(f"📈 Total registros procesados: {total_records:,}")
    
    return {
        'total_identifiers': total_identifiers,
        'multi_day_identifiers': multi_day_identifiers,
        'single_day_identifiers': single_day_identifiers,
        'days_distribution': dict(days_distribution),
        'max_days': max(days_per_identifier.values()) if days_per_identifier else 0
    }

def main():
    # Verificar argumentos
    if len(sys.argv) < 2:
        print("Error: Debe especificar un archivo CSV como argumento")
        print("Uso: python multi_day_analysis.py <archivo.csv>")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    
    # Verificar que el archivo existe
    if not os.path.exists(csv_file):
        print(f"Error: El archivo '{csv_file}' no existe")
        sys.exit(1)
    
    # Ejecutar análisis
    try:
        results = analyze_multi_day_users(csv_file)
        print(f"\n🎉 Análisis multi-día completado exitosamente!")
        
    except Exception as e:
        print(f"\n❌ Error durante el análisis: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()