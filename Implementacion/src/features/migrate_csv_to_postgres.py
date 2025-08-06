import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
from sqlalchemy import create_engine, text
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MobilityDataLoader:
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
        
        self.csv_file = 'Mobility_Data_Slim_DeDuplicate.csv'
        
    def create_database(self):
        try:
            # Conectar a la base de datos por defecto
            conn = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                database=self.db_config['default_db']
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            
            # Verificar si la base de datos ya existe
            cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (self.target_db,))
            exists = cur.fetchone()
            
            if not exists:
                cur.execute(f'CREATE DATABASE {self.target_db}')
                logger.info(f"Base de datos '{self.target_db}' creada exitosamente")
            else:
                logger.info(f"Base de datos '{self.target_db}' ya existe")
                
            cur.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error al crear la base de datos: {e}")
            raise
    
    def analyze_csv_structure(self):
        try:
            if not os.path.exists(self.csv_file):
                raise FileNotFoundError(f"Archivo {self.csv_file} no encontrado")
            
            # Leer una muestra del CSV para analizar estructura
            df_sample = pd.read_csv(self.csv_file, nrows=5)
            
            logger.info(f"Estructura del CSV:")
            logger.info(f"Columnas: {list(df_sample.columns)}")
            logger.info(f"Tipos de datos:")
            for col, dtype in df_sample.dtypes.items():
                logger.info(f"  {col}: {dtype}")
            
            return df_sample
            
        except Exception as e:
            logger.error(f"Error al analizar CSV: {e}")
            raise
    
    def create_table_from_csv(self, df_sample):
        try:
            # Crear conexión a la nueva base de datos
            engine = create_engine(
                f"postgresql://{self.db_config['user']}:{self.db_config['password']}@"
                f"{self.db_config['host']}:{self.db_config['port']}/{self.target_db}"
            )
            
            # Mapeo de tipos pandas a PostgreSQL
            type_mapping = {
                'object': 'TEXT',
                'int64': 'BIGINT',
                'int32': 'INTEGER',
                'float64': 'DOUBLE PRECISION',
                'float32': 'REAL',
                'bool': 'BOOLEAN',
                'datetime64[ns]': 'TIMESTAMP'
            }
            
            # Generar DDL para la tabla
            columns_ddl = []
            for col, dtype in df_sample.dtypes.items():
                pg_type = type_mapping.get(str(dtype), 'TEXT')
                # Limpiar nombres de columnas (reemplazar espacios y caracteres especiales)
                clean_col = col.lower().replace(' ', '_').replace('-', '_').replace('.', '_')
                columns_ddl.append(f"{clean_col} {pg_type}")
            
            # Crear la tabla
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS mobility_data (
                id SERIAL PRIMARY KEY,
                {','.join(columns_ddl)},
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            with engine.connect() as conn:
                conn.execute(text("DROP TABLE IF EXISTS mobility_data"))
                conn.execute(text(create_table_sql))
                conn.commit()
                
            logger.info("Tabla 'mobility_data' creada exitosamente")
            
            return engine
            
        except Exception as e:
            logger.error(f"Error al crear la tabla: {e}")
            raise
    
    def load_csv_to_table(self, engine):
        try:
            # Leer el CSV completo
            logger.info(f"Leyendo archivo CSV: {self.csv_file}")
            df = pd.read_csv(self.csv_file)
            
            # Limpiar nombres de columnas
            df.columns = [col.lower().replace(' ', '_').replace('-', '_').replace('.', '_') 
                         for col in df.columns]
            
            logger.info(f"Cargando {len(df)} registros a la tabla...")
            
            # Cargar datos en chunks para mejor rendimiento
            chunk_size = 1000
            total_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size else 0)
            
            for i, chunk in enumerate(pd.read_csv(self.csv_file, chunksize=chunk_size)):
                # Limpiar nombres de columnas del chunk
                chunk.columns = [col.lower().replace(' ', '_').replace('-', '_').replace('.', '_') 
                               for col in chunk.columns]
                
                chunk.to_sql('mobility_data', engine, if_exists='append', index=False, method='multi')
                logger.info(f"Procesado chunk {i+1}/{total_chunks}")
            
            logger.info("Datos cargados exitosamente")
            
            # Obtener estadísticas de la tabla
            with engine.connect() as conn:
                result = conn.execute(text("SELECT COUNT(*) FROM mobility_data"))
                count = result.fetchone()[0]
                logger.info(f"Total de registros en la tabla: {count}")
                
        except Exception as e:
            logger.error(f"Error al cargar datos: {e}")
            raise
    
    def run(self):
        try:
            logger.info("=== Iniciando proceso de carga de datos de movilidad ===")
            
            # 1. Crear base de datos
            self.create_database()
            
            # 2. Analizar estructura del CSV
            df_sample = self.analyze_csv_structure()
            
            # 3. Crear tabla
            engine = self.create_table_from_csv(df_sample)
            
            # 4. Cargar datos
            self.load_csv_to_table(engine)
            
            logger.info("=== Proceso completado exitosamente ===")
            
        except Exception as e:
            logger.error(f"Error en el proceso: {e}")
            raise

if __name__ == "__main__":
    loader = MobilityDataLoader()
    loader.run()