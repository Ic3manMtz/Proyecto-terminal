import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import folium
from tqdm import tqdm
import random
import os
from pathlib import Path

def generar_mapa_para_id(random_id, ddf):
    """Función para generar mapa para un ID específico"""
    with ProgressBar():
        print(f"\n🔍 Buscando coordenadas para ID: {random_id}...")
        filtered = ddf[ddf['identifier'] == random_id]
        coordenadas = filtered[['device_lat', 'device_lon']].compute()

    print("\n📍 Coordenadas encontradas:")
    print(coordenadas.to_string(index=False))
    print(f"\n✅ Total de ubicaciones únicas encontradas: {len(coordenadas)}")

    if not coordenadas.empty:
        avg_lat = coordenadas['device_lat'].mean()
        avg_lon = coordenadas['device_lon'].mean()
        
        mapa = folium.Map(location=[avg_lat, avg_lon], zoom_start=13)
        
        for idx, row in tqdm(coordenadas.iterrows(), total=len(coordenadas), desc="Añadiendo marcadores"):
            folium.Marker(
                [row['device_lat'], row['device_lon']],
                popup=f"ID: {random_id}\nLat: {row['device_lat']:.6f}\nLon: {row['device_lon']:.6f}",
                icon=folium.Icon(color='red', icon='info-sign')
            ).add_to(mapa)
        
        if len(coordenadas) > 10:
            from folium.plugins import HeatMap
            HeatMap(coordenadas.values.tolist(), radius=15).add_to(mapa)
        
        nombre_archivo = f"maps/mapa_{random_id}.html"
        mapa.save(nombre_archivo)
        return nombre_archivo, len(coordenadas)
    else:
        return None, 0

def main():
    # Configuración inicial
    Path("maps").mkdir(exist_ok=True)
    
    # 1. Cargar identificadores únicos
    with open('unique_values_100.txt', 'r') as f:
        identifiers = [line.strip() for line in f if line.strip()]
    
    print(f"\n📊 Total de identificadores únicos disponibles: {len(identifiers):,}")
    
    # 2. Preguntar al usuario cuántos quiere analizar
    while True:
        try:
            num_ids = int(input("\n¿Cuántos identificadores aleatorios quieres analizar? (1-100): "))
            if 1 <= num_ids <= 100:
                break
            print("Por favor ingresa un número entre 1 y 100")
        except ValueError:
            print("Entrada inválida. Por favor ingresa un número.")

    # 3. Seleccionar IDs aleatorios
    selected_ids = random.sample(identifiers, num_ids)
    print(f"\n🎲 IDs seleccionados aleatoriamente: {', '.join(selected_ids)}")
    
    # 4. Cargar datos (una sola vez para mejor performance)
    print("\n⏳ Cargando base de datos...")
    ddf = dd.read_csv('Mobility_Data_Slim.csv',
                     dtype={'identifier': 'object',
                            'device_lat': 'float64',
                            'device_lon': 'float64'})
    
    # 5. Procesar cada ID
    resultados = []
    for id in selected_ids:
        archivo, num_coords = generar_mapa_para_id(id, ddf)
        if archivo:
            resultados.append((id, archivo, num_coords))
    
    # 6. Mostrar resumen
    print("\n📝 Resumen de ejecución:")
    print(f"Identificadores procesados: {len(resultados)}/{num_ids}")
    print(f"Mapas generados exitosamente: {len(resultados)}")
    
    if resultados:
        print("\n🗂 Archivos generados:")
        for id, archivo, num_coords in resultados:
            print(f"- {id}: {os.path.abspath(archivo)} ({num_coords} coordenadas)")
    
    print("\n🎉 Proceso completado!")

if __name__ == "__main__":
    main()