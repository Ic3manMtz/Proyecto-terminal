import os
from time import sleep
from typing import List, Tuple, Optional


class MainMenu:
    # Definimos las opciones como variables de clase estáticas
    MAIN_OPTIONS: List[Tuple[str, str]] = [
        ("1", "Exploración inicial de datos"),
        ("2", "Número de registros"),
        ("3", "Eliminación de columnas"),
        ("4", "Valores únicos de una columna"),
        ("5", "Creación de histograma de frecuencias de la 'device_horizontal_accuracy'"),
        ("6", "Creación de histograma de frecuencias de la 'identifier'"),
        ("7", "Análisis de frecuencias de 'identifier'"),
        ("8", "Mostrar coordenadas de un 'identifier'"),
        ("9", "Obtener los identificadores con más de 100 repeticiones"),
        ("10", "Eliminar duplicados"),
        ("11", "Salir")
    ]

    @staticmethod
    def display_main_menu(converted_count: Optional[int] = None) -> str:
        print("\n" + "=" * 50)
        print(" " * 5 + "CARACTERIZACIÓN DE TRAYECTORIAS".center(40))
        print("=" * 50 + "\n")

        print("\tA continuación se muestran los pasos \na seguir para la caracterización:\n\n")

        for num, text in MainMenu.MAIN_OPTIONS.copy():
            print(f" {num}┃ {text}")

        print("\n" + "-" * 40)
        return input(" ➤ Seleccione una opción: ")

    @staticmethod
    def display_ask_filename(request) -> str:
        print("\n" + "=" * 50)
        print(" " * 5 + request.center(40))
        print("=" * 50 + "\n")

        return input(" ➤ Introduzca el nombre del archivo CSV: ")
    
    @staticmethod
    def display_available_columns(available_columns: List[str]) -> int:
        print("\n" + "=" * 50)
        print(" " * 5 + "COLUMNA A ANALIZAR".center(40))
        print("=" * 50 + "\n")

        for idx, col in enumerate(available_columns):
            print(f"{idx + 1}. {col}")

        try:
            selection = int(input("\n🔽 Ingresa el número de la columna que deseas analizar: ")) - 1
            if selection < 0 or selection >= len(available_columns):
                raise ValueError("Selección fuera de rango")
            return selection
        except ValueError as e:
            print(f"❌ Error: {e}")
            return -1