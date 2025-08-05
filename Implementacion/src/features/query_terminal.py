import pandas as pd
import os
import sys
import time
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm

from src.menus.menu import MainMenu

class DataTerminal:
    def __init__(self, file_path):
        self.file_path = file_path
        self.chunksize = 1_000_000  # Tamaño del chunk para procesamiento
        self.column_info = None
        self.dtypes = None
        self.date_columns = []
        self.file_size = os.path.getsize(file_path) / (1024 * 1024)  # Tamaño en MB
        
        # Inicializar información del dataset
        self._initialize_dataset_info()
        
    def get_filename(self):
        return self.file_path
    
    def _initialize_dataset_info(self):
        print("\n🔍 Analizando estructura del dataset...")
        
        # Leer solo la primera fila para obtener los nombres de las columnas
        with pd.read_csv(self.file_path, chunksize=1) as reader:
            first_chunk = next(reader)
            self.column_info = {
                col: {'dtype': str(first_chunk[col].dtype), 'sample': first_chunk[col].iloc[0]}
                for col in first_chunk.columns
            }
            
        # Intentar inferir tipos de datos para optimización
        self._infer_data_types()
        
        print(f"\n📊 Dataset cargado: {os.path.basename(self.file_path)}")
        print(f"📂 Tamaño del archivo: {self.file_size:.2f} MB")
        print(f"🗂 Columnas disponibles: {len(self.column_info)}")
        print("\nMuestra de columnas y tipos:")
        for i, (col, info) in enumerate(self.column_info.items()):
            if i < 5:  # Mostrar solo las primeras 5 columnas como muestra
                print(f"  {col}: {info['dtype']} (ej: {str(info['sample'])[:30]}...)")
        if len(self.column_info) > 5:
            print(f"  ...y {len(self.column_info)-5} columnas más")
    
    def _infer_data_types(self):
        print("\n🧠 Infiriendo tipos de datos para optimización...")
        sample = pd.read_csv(self.file_path, nrows=10000)
        
        self.dtypes = {}
        self.date_columns = []
        date_patterns = [
            '%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y', '%Y/%m/%d',
            '%Y-%m-%d %H:%M:%S', '%d-%m-%Y %H:%M:%S',
            '%Y%m%d', '%d%m%Y', '%m%d%Y'
        ]
        
        for col in sample.columns:
            col_dtype = sample[col].dtype
            col_data = sample[col].dropna()
            
            # Optimización para numéricos
            if col_dtype == 'float64':
                self.dtypes[col] = 'float32'
            elif col_dtype == 'int64':
                max_val = sample[col].max()
                min_val = sample[col].min()
                if min_val >= 0:
                    if max_val < 255:
                        self.dtypes[col] = 'uint8'
                    elif max_val < 65535:
                        self.dtypes[col] = 'uint16'
                    elif max_val < 4294967295:
                        self.dtypes[col] = 'uint32'
                else:
                    if min_val > -128 and max_val < 127:
                        self.dtypes[col] = 'int8'
                    elif min_val > -32768 and max_val < 32767:
                        self.dtypes[col] = 'int16'
                    elif min_val > -2147483648 and max_val < 2147483647:
                        self.dtypes[col] = 'int32'
            
            # Manejo avanzado de fechas para objetos
            elif col_dtype == 'object':
                is_date = False
                
                # Prueba con los patrones de fecha más comunes
                for pattern in date_patterns:
                    try:
                        if not col_data.empty:
                            pd.to_datetime(col_data, format=pattern, errors='raise')
                            self.dtypes[col] = 'datetime64[ns]'
                            self.date_columns.append(col)
                            is_date = True
                            break
                    except:
                        continue
                
                # Si no coincide con patrones conocidos, prueba con inferencia automática
                if not is_date and not col_data.empty:
                    try:
                        unique_vals = col_data.unique()[:100]  # Muestra de valores para prueba
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore")
                            if pd.to_datetime(unique_vals, errors='raise', infer_datetime_format=True).notna().all():
                                self.dtypes[col] = 'datetime64[ns]'
                                self.date_columns.append(col)
                                is_date = True
                    except:
                        pass
                
                # Si no es fecha, usar category si tiene baja cardinalidad
                if not is_date:
                    unique_count = col_data.nunique()
                    if 1 < unique_count < (len(col_data) / 2):
                        self.dtypes[col] = 'category'
                    else:
                        self.dtypes[col] = 'string'
            
            # Mantener tipo original si no se optimizó
            if col not in self.dtypes:
                self.dtypes[col] = col_dtype
        
        # Reporte de inferencia
        print(f"\n🔍 Columnas detectadas como fechas: {len(self.date_columns)}")
        if self.date_columns:
            print("📅 " + ", ".join(self.date_columns))
    
    def query(self, columns=None, condition=None, group_by=None, agg_func=None, limit=1000):
      results = []
      total_rows = 0
      processed_rows = 0
      
      # 1. Determinar columnas a parsear como fechas
      parse_columns = []
      if columns:
          # Solo incluir columnas de fecha que estén en la selección solicitada
          parse_columns = [col for col in self.date_columns if col in columns]
      else:
          # Si no se especifican columnas, usar todas las de fecha conocidas
          parse_columns = self.date_columns
      
      # 2. Contar filas totales (optimizado para solo leer una vez)
      print("\n📊 Contando filas totales...")
      with pd.read_csv(self.file_path, chunksize=self.chunksize) as reader:
          for chunk in reader:
              total_rows += len(chunk)
      
      # 3. Procesamiento principal con manejo mejorado de fechas
      print(f"\n🔍 Procesando {total_rows:,} filas...")
      with tqdm(total=total_rows, unit=' filas') as pbar:
          with pd.read_csv(
              self.file_path,
              chunksize=self.chunksize,
              dtype=self.dtypes,
              parse_dates=parse_columns,  # Solo parsear columnas de fecha necesarias
              infer_datetime_format=True  # Mejorar rendimiento en parseo de fechas
          ) as reader:
              
              for chunk in reader:
                  # Aplicar filtros si existen
                  if condition:
                      try:
                          # Convertir condiciones a formato datetime si aplica
                          for date_col in parse_columns:
                              if date_col in condition:
                                  chunk[date_col] = pd.to_datetime(chunk[date_col])
                          chunk = chunk.query(condition)
                      except Exception as e:
                          print(f"\n⚠️ Error en la condición: {e}")
                          return None
                  
                  # Seleccionar columnas específicas
                  if columns:
                      try:
                          chunk = chunk[columns]
                      except KeyError as e:
                          print(f"\n⚠️ Columna no encontrada: {e}")
                          return None
                  
                  processed_rows += len(chunk)
                  
                  # Manejo de agrupaciones
                  if group_by and agg_func:
                      try:
                          # Asegurar que la columna de agrupación esté parseada como fecha si es necesario
                          if group_by in parse_columns:
                              chunk[group_by] = pd.to_datetime(chunk[group_by])
                          grouped = chunk.groupby(group_by).agg(agg_func)
                          results.append(grouped)
                      except Exception as e:
                          print(f"\n⚠️ Error en agrupación: {e}")
                          return None
                  else:
                      results.append(chunk)
                  
                  # Control de límite
                  if processed_rows >= limit:
                      break
                  
                  pbar.update(len(chunk))
      
      # 4. Consolidación de resultados
      if not results:
          return pd.DataFrame()
      
      final_result = pd.concat(results)
      
      if group_by and agg_func:
          # Reagrupar si hubo procesamiento por chunks
          if group_by in parse_columns:
              final_result[group_by] = pd.to_datetime(final_result[group_by])
          final_result = final_result.groupby(group_by).agg(agg_func)
      else:
          final_result = final_result.head(limit)
      
      # Optimización final de tipos de datos
      if columns:
          for col in columns:
              if col in self.dtypes:
                  final_result[col] = final_result[col].astype(self.dtypes[col])
      
      return final_result
    
    def show_stats(self, column):
        print(f"\n📈 Calculando estadísticas para {column}...")
        
        stats = {
            'count': 0,
            'mean': 0,
            'std': 0,
            'min': None,
            'max': None,
            'unique': set()
        }
        
        total_rows = 0
        with pd.read_csv(self.file_path, chunksize=self.chunksize) as reader:
            for chunk in tqdm(reader, unit=' chunk'):
                if column not in chunk.columns:
                    print(f"\n⚠️ Columna '{column}' no encontrada")
                    return None
                
                col_data = chunk[column]
                total_rows += len(col_data)
                
                # Actualizar estadísticas
                stats['count'] += col_data.count()
                
                if pd.api.types.is_numeric_dtype(col_data):
                    stats['mean'] += col_data.sum()
                    stats['std'] += (col_data**2).sum()
                    chunk_min = col_data.min()
                    chunk_max = col_data.max()
                    
                    stats['min'] = chunk_min if stats['min'] is None else min(stats['min'], chunk_min)
                    stats['max'] = chunk_max if stats['max'] is None else max(stats['max'], chunk_max)
                
                if len(stats['unique']) < 1000:  # Limitar para no consumir mucha memoria
                    stats['unique'].update(col_data.unique())
        
        # Calcular estadísticas finales
        if pd.api.types.is_numeric_dtype(col_data):
            stats['mean'] = stats['mean'] / stats['count']
            stats['std'] = np.sqrt(stats['std']/stats['count'] - stats['mean']**2)
        
        print("\n📊 Estadísticas descriptivas:")
        print(f"Total filas: {total_rows:,}")
        print(f"Valores no nulos: {stats['count']:,} ({(stats['count']/total_rows*100):.1f}%)")
        
        if pd.api.types.is_numeric_dtype(col_data):
            print(f"\nMedia: {stats['mean']:.2f}")
            print(f"Desviación estándar: {stats['std']:.2f}")
            print(f"Mínimo: {stats['min']}")
            print(f"Máximo: {stats['max']}")
        
        print(f"\nValores únicos: {len(stats['unique'])}")
        if len(stats['unique']) <= 20:
            print("Muestra de valores únicos:", sorted(stats['unique'])[:20])
        
        # Generar histograma para columnas numéricas
        if pd.api.types.is_numeric_dtype(col_data):
            self._plot_histogram(column)
    
    def _plot_histogram(self, column, bins=50):
        print(f"\n📊 Generando histograma para {column}...")
        
        # Calcular rangos primero
        min_val = None
        max_val = None
        
        with pd.read_csv(self.file_path, chunksize=self.chunksize, usecols=[column]) as reader:
            for chunk in reader:
                chunk_min = chunk[column].min()
                chunk_max = chunk[column].max()
                
                min_val = chunk_min if min_val is None else min(min_val, chunk_min)
                max_val = chunk_max if max_val is None else max(max_val, chunk_max)
        
        if min_val is None or max_val is None:
            print("No hay datos numéricos para graficar")
            return
        
        # Crear histograma por chunks
        hist = np.zeros(bins)
        bin_edges = np.linspace(min_val, max_val, bins + 1)
        
        with pd.read_csv(self.file_path, chunksize=self.chunksize, usecols=[column]) as reader:
            for chunk in tqdm(reader, unit=' chunk'):
                valid_data = chunk[column].dropna()
                if len(valid_data) > 0:
                    chunk_hist, _ = np.histogram(valid_data, bins=bin_edges)
                    hist += chunk_hist
        
        # Graficar
        plt.figure(figsize=(12, 6))
        plt.bar(bin_edges[:-1], hist, width=np.diff(bin_edges), align='edge', alpha=0.7)
        plt.title(f'Distribución de {column}')
        plt.xlabel(column)
        plt.ylabel('Frecuencia')
        plt.grid(True, alpha=0.3)
        
        # Mostrar en ventana
        plt.show()
        
        # Guardar opcional
        save = input("\n¿Desea guardar el gráfico? (s/n): ").lower()
        if save == 's':
            os.makedirs('img/histograms', exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"img/histograms/hist_{column}_{timestamp}.png"
            plt.savefig(filename, dpi=300, bbox_inches='tight')
            print(f"Gráfico guardado como {filename}")
        
        plt.close()

def main():
    if len(sys.argv) < 2:
        print("Uso: python data_terminal.py <archivo_csv>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    if not os.path.exists(file_path):
        print(f"Error: El archivo {file_path} no existe")
        sys.exit(1)
    
    terminal = DataTerminal(file_path)
    
    while True:
        print("\n" + "="*50)
        print("TERMINAL DE CONSULTAS DE DATOS".center(50))
        print("="*50)
        print("\nOpciones disponibles:")
        print("1. Ejecutar consulta básica")
        print("2. Mostrar estadísticas de columna")
        print("3. Ver información del dataset")
        print("4. Salir")
        
        choice = input("\nSeleccione una opción: ")
        
        if choice == '1':
            print("\n📝 Ejecutar consulta")
            available_columns = pd.read_csv(terminal.get_filename(), nrows=0).columns.tolist()           
            columns = MainMenu.display_available_columns_multiple_selection(available_columns)
            condition = input("Condición de filtrado (ej: 'edad > 30', vacío para omitir): ")
            group_by = input("Columna para agrupar (vacío para omitir): ")
            agg_func = input("Función de agregación (count, sum, mean, etc, vacío para omitir): ")
            limit = input("Límite de resultados (default 1000): ")
            
            try:
                limit = int(limit) if limit else 1000
                
                start_time = time.time()
                result = terminal.query(
                    columns=columns,
                    condition=condition,
                    group_by=group_by if group_by else None,
                    agg_func=agg_func if agg_func else None,
                    limit=limit
                )
                
                if result is not None:
                    print(f"\n✅ Consulta completada en {time.time()-start_time:.2f} segundos")
                    print("\nResultados:")
                    print(result)
                    
                    # Opción para exportar
                    export = input("\n¿Exportar resultados a CSV? (s/n): ").lower()
                    if export == 's':
                        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                        filename = f"query_results_{timestamp}.csv"
                        result.to_csv(filename)
                        print(f"Resultados exportados a {filename}")
            except Exception as e:
                print(f"\n⚠️ Error en la consulta: {e}")
        
        elif choice == '2':
            print("\n📊 Estadísticas de columna")
            available_columns = pd.read_csv(terminal.get_filename(), nrows=0).columns.tolist()
            selected_index = MainMenu.display_available_columns(available_columns)
            target_column = available_columns[selected_index]
            terminal.show_stats(target_column)
        
        elif choice == '3':
            print("\nℹ️ Información del dataset")
            print(f"\nArchivo: {os.path.basename(file_path)}")
            print(f"Tamaño: {terminal.file_size:.2f} MB")
            print(f"Columnas: {len(terminal.column_info)}")
            print("\nDetalle de columnas:")
            for col, info in terminal.column_info.items():
                print(f"  {col}: {info['dtype']} (ej: {str(info['sample'])[:30]}...)")
        
        elif choice == '4':
            print("\n👋 Saliendo de la terminal...")
            break
        
        else:
            print("\n⚠️ Opción no válida. Intente nuevamente.")

if __name__ == "__main__":
    main()