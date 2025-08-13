import pandas as pd
import folium
from tqdm import tqdm
import os
from pathlib import Path
import psycopg2
from sqlalchemy import create_engine, text
import warnings
from datetime import datetime
import numpy as np
warnings.filterwarnings('ignore')

class DatabaseConnection:
    """Clase para manejar la conexión a PostgreSQL"""
    
    def __init__(self):
        # Configuración de la base de datos desde variables de entorno
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'postgres123'),
            'default_db': os.getenv('DB_NAME', 'postgres')  # DB por defecto para crear la nueva
        }
        self.target_db = 'trajectories'
        
        # Crear string de conexión para la base de datos trajectories
        self.connection_string = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.target_db}"
        
        self.csv_file = 'Mobility_Data_Slim_DeDuplicate.csv'
        self.engine = None
    
    def connect(self):
        """Establecer conexión a la base de datos"""
        try:
            print("🔌 Conectando a PostgreSQL...")
            print(f"   Host: {self.db_config['host']}")
            print(f"   Puerto: {self.db_config['port']}")
            print(f"   Base de datos: {self.target_db}")
            print(f"   Usuario: {self.db_config['user']}")
            
            self.engine = create_engine(self.connection_string)
            
            # Probar la conexión
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1")).fetchone()
                print("✅ Conexión a PostgreSQL exitosa!")
                return True
                
        except Exception as e:
            print(f"❌ Error al conectar con la base de datos: {e}")
            print(f"   String de conexión: {self.connection_string}")
            return False
    
    def get_coordinates_for_identifier(self, identifier):
        """Obtener coordenadas para un identifier específico"""
        try:
            query = """
            SELECT device_lat, device_lon, timestamp
            FROM mobility_data 
            WHERE identifier = %(identifier)s 
            AND device_lat IS NOT NULL 
            AND device_lon IS NOT NULL
            AND timestamp IS NOT NULL
            ORDER BY timestamp ASC
            """
            
            print(f"🔍 Ejecutando consulta para identifier: {identifier}")
            df = pd.read_sql_query(query, self.engine, params={'identifier': identifier})
            
            # Convertir timestamp a datetime con manejo de diferentes formatos
            if not df.empty and 'timestamp' in df.columns:
                print("🕒 Convirtiendo timestamps...")
                try:
                    # Intentar formato ISO8601 primero (maneja microsegundos automáticamente)
                    df['timestamp'] = pd.to_datetime(df['timestamp'], format='ISO8601')
                except:
                    try:
                        # Si falla, usar inferencia mixta
                        df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed', infer_datetime_format=True)
                    except:
                        # Último recurso: conversión automática
                        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
                
                # Eliminar filas donde el timestamp no se pudo convertir
                df = df.dropna(subset=['timestamp'])
                print(f"✅ Timestamps convertidos exitosamente. Registros válidos: {len(df)}")
                
            return df
            
        except Exception as e:
            print(f"❌ Error al ejecutar la consulta: {e}")
            return pd.DataFrame()
    
    def check_identifier_exists(self, identifier):
        """Verificar si un identifier existe en la base de datos"""
        try:
            query = "SELECT COUNT(*) as count FROM mobility_data WHERE identifier = %(identifier)s"
            df = pd.read_sql_query(query, self.engine, params={'identifier': identifier})
            return df['count'].iloc[0] > 0
            
        except Exception as e:
            print(f"❌ Error al verificar identifier: {e}")
            return False

def obtener_color_por_fecha(timestamp):
    """Asignar color basado en el día específico (6-15 noviembre 2022)"""
    # Extraer solo el día del timestamp
    dia = timestamp.day
    
    # Mapear cada día a un color específico
    colores_por_dia = {
        6: 'green',      # 6 nov - Verde
        7: 'blue',       # 7 nov - Azul
        8: 'purple',     # 8 nov - Morado
        9: 'orange',     # 9 nov - Naranja
        10: 'red',       # 10 nov - Rojo
        11: 'pink',      # 11 nov - Rosa
        12: 'gray',      # 12 nov - Gris
        13: 'darkblue',  # 13 nov - Azul oscuro
        14: 'darkred',   # 14 nov - Rojo oscuro
        15: 'darkgreen'  # 15 nov - Verde oscuro
    }
    
    return colores_por_dia.get(dia, 'black')  # Negro por defecto si no está en el rango

def crear_leyenda_temporal():
    """Crear HTML para la leyenda de colores por día"""
    leyenda_html = """
    <div style="position: fixed; 
                top: 10px; right: 10px; width: 220px; height: 280px; 
                background-color: white; border:2px solid grey; z-index:9999; 
                font-size:12px; padding: 10px; overflow-y: auto;">
    <h4 style="margin-top: 0;">📅 Leyenda por Día</h4>
    <p><i class="fa fa-circle" style="color:green"></i> 6 Nov 2022</p>
    <p><i class="fa fa-circle" style="color:blue"></i> 7 Nov 2022</p>
    <p><i class="fa fa-circle" style="color:purple"></i> 8 Nov 2022</p>
    <p><i class="fa fa-circle" style="color:orange"></i> 9 Nov 2022</p>
    <p><i class="fa fa-circle" style="color:red"></i> 10 Nov 2022</p>
    <p><i class="fa fa-circle" style="color:pink"></i> 11 Nov 2022</p>
    <p><i class="fa fa-circle" style="color:gray"></i> 12 Nov 2022</p>
    <p><i class="fa fa-circle" style="color:darkblue"></i> 13 Nov 2022</p>
    <p><i class="fa fa-circle" style="color:darkred"></i> 14 Nov 2022</p>
    <p><i class="fa fa-circle" style="color:darkgreen"></i> 15 Nov 2022</p>
    </div>
    """
    return leyenda_html

def generar_mapa_para_id(identifier, db_connection):
    """Función para generar mapa para un ID específico"""
    
    # Obtener coordenadas de la base de datos
    coordenadas = db_connection.get_coordinates_for_identifier(identifier)
    
    print(f"\n📍 Coordenadas encontradas: {len(coordenadas)}")
    if len(coordenadas) > 0:
        print("\nPrimeras 5 coordenadas:")
        print(coordenadas.head().to_string(index=False))
        print(f"\n✅ Total de ubicaciones encontradas: {len(coordenadas)}")
        
        # Mostrar rango de fechas
        if 'timestamp' in coordenadas.columns:
            fecha_min = coordenadas['timestamp'].min()
            fecha_max = coordenadas['timestamp'].max()
            print(f"📅 Rango temporal:")
            print(f"   • Desde: {fecha_min}")
            print(f"   • Hasta: {fecha_max}")
            print(f"   • Período: {(fecha_max - fecha_min).days} días")
    else:
        print("❌ No se encontraron coordenadas para este identifier")
        return None, 0

    if not coordenadas.empty:
        # Calcular centro del mapa
        avg_lat = coordenadas['device_lat'].mean()
        avg_lon = coordenadas['device_lon'].mean()
        
        # Crear mapa base
        mapa = folium.Map(location=[avg_lat, avg_lon], zoom_start=13)
        
        # Obtener rango de fechas para coloración
        if 'timestamp' in coordenadas.columns:
            fecha_min = coordenadas['timestamp'].min()
            fecha_max = coordenadas['timestamp'].max()
            
            # Añadir leyenda temporal
            leyenda = crear_leyenda_temporal()
            mapa.get_root().html.add_child(folium.Element(leyenda))
            
            # Mostrar distribución por días
            print(f"\n📊 Distribución por días:")
            distribucion_dias = coordenadas.groupby(coordenadas['timestamp'].dt.date).size().sort_index()
            for fecha, cantidad in distribucion_dias.items():
                print(f"   • {fecha.strftime('%d nov %Y')}: {cantidad} ubicaciones")
        

        if len(coordenadas) > 1:
            print("🔄 Añadiendo trayectoria cronológica...")
            # Crear lista de puntos en orden cronológico
            puntos_linea = []
            for idx, row in coordenadas.iterrows():
                puntos_linea.append([row['device_lat'], row['device_lon']])
            
            # Añadir línea poligonal al mapa
            folium.PolyLine(
                puntos_linea,
                color='blue',
                weight=3,
                opacity=0.7,
                tooltip=f'Trayectoria de {identifier}'
            ).add_to(mapa)
        else:
            print("ℹ️  Solo hay 1 punto - No se puede trazar trayectoria")
        
        # Añadir marcadores con colores por día
        print("\n📌 Añadiendo marcadores al mapa...")
        for idx, row in tqdm(coordenadas.iterrows(), total=len(coordenadas), desc="Procesando ubicaciones"):
            
            # Determinar color basado en el día del timestamp
            if 'timestamp' in coordenadas.columns:
                color = obtener_color_por_fecha(row['timestamp'])
                timestamp_str = row['timestamp'].strftime("%Y-%m-%d %H:%M:%S")
                dia_nombre = row['timestamp'].strftime("%d de noviembre")
                popup_text = f"ID: {identifier}\nLat: {row['device_lat']:.6f}\nLon: {row['device_lon']:.6f}\nFecha: {timestamp_str}\nDía: {dia_nombre}"
            else:
                color = 'red'  # Color por defecto si no hay timestamp
                popup_text = f"ID: {identifier}\nLat: {row['device_lat']:.6f}\nLon: {row['device_lon']:.6f}"
            
            folium.Marker(
                [row['device_lat'], row['device_lon']],
                popup=popup_text,
                icon=folium.Icon(color=color, icon='info-sign')
            ).add_to(mapa)
        
        # Añadir mapa de calor si hay muchos puntos
        if len(coordenadas) > 10:
            print("🔥 Añadiendo mapa de calor...")
            from folium.plugins import HeatMap
            heat_data = [[row['device_lat'], row['device_lon']] for idx, row in coordenadas.iterrows()]
            HeatMap(heat_data, radius=15).add_to(mapa)
        
        # Guardar mapa
        nombre_archivo = f"maps/mapa_{identifier}.html"
        mapa.save(nombre_archivo)
        return nombre_archivo, len(coordenadas)
    else:
        return None, 0

def obtener_identifier_usuario():
    """Solicita el identifier al usuario"""
    while True:
        identifier = input("\n🔍 Ingresa el identifier que quieres analizar: ").strip()
        
        if not identifier:
            print("❌ Por favor ingresa un identifier válido.")
            continue
            
        print(f"\n📝 Identifier ingresado: {identifier}")
        
        # Confirmar si quiere proceder
        confirmacion = input("¿Continuar con este identifier? (s/n): ").lower().strip()
        if confirmacion in ['s', 'si', 'sí', 'y', 'yes']:
            return identifier
        elif confirmacion in ['n', 'no']:
            print("Selecciona un nuevo identifier...")
            continue

def main():
    # Configuración inicial
    Path("maps").mkdir(exist_ok=True)
    
    print("🗺️  Generador de Mapas de Movilidad")
    print("=" * 40)
    
    # Establecer conexión a la base de datos
    db = DatabaseConnection()
    if not db.connect():
        print("❌ No se pudo establecer conexión con la base de datos.")
        return
    
    # Solicitar identifier del usuario
    identifier = obtener_identifier_usuario()
    
    # Verificar si el identifier existe
    print(f"\n🔎 Verificando si el identifier '{identifier}' existe en la base de datos...")
    
    if not db.check_identifier_exists(identifier):
        print(f"❌ El identifier '{identifier}' no se encontró en la base de datos.")
        print("\n💡 Sugerencias:")
        print("   - Verifica que el identifier esté escrito correctamente")
        print("   - Revisa si hay espacios en blanco adicionales")
        print("   - Asegúrate de que el identifier exista en la base de datos")
        return
    
    print(f"✅ Identifier '{identifier}' encontrado en la base de datos.")
    
    # Generar mapa
    print(f"\n🎯 Procesando identifier: {identifier}")
    archivo, num_coords = generar_mapa_para_id(identifier, db)
    
    # Mostrar resultados
    print("\n" + "=" * 50)
    print("📝 RESUMEN DE EJECUCIÓN")
    print("=" * 50)
    
    if archivo:
        print(f"✅ Mapa generado exitosamente!")
        print(f"📂 Archivo: {os.path.abspath(archivo)}")
        print(f"📍 Coordenadas procesadas: {num_coords}")
        print(f"🗂️  Ubicación: {os.path.dirname(os.path.abspath(archivo))}")
        
        if num_coords > 10:
            print("🔥 Mapa de calor incluido (más de 10 coordenadas)")
            
        print(f"\n💡 Para ver el mapa, abre el archivo HTML en tu navegador:")
        print(f"   {os.path.abspath(archivo)}")
    else:
        print(f"❌ No se pudo generar el mapa para el identifier '{identifier}'")
        print("   Esto puede suceder si no hay coordenadas válidas para este ID.")
    
    print("\n🎉 Proceso completado!")

if __name__ == "__main__":
    main()