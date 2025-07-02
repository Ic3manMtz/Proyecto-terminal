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
        ("8", "Salir")
    ]

    PIPELINE_OPTIONS: List[Tuple[str, str]] = [
        ("1", "Convertir videos a frames"),
        ("2", "Detección de objetos y seguimiento"),
        ("3", "Clusterización de imágenes"),
        ("4", "Estadísticas de imágenes"),
        ("5", "Visualización de resultados"),
        ("6", "Visualización de videos con boxes"),
        ("7", "Volver al menú principal")
    ]

    @staticmethod
    def display_main_menu(converted_count: Optional[int] = None) -> str:
        print("\n" + "=" * 50)
        print(" " * 10 + "CARACTERIZACIÓN DE TRAYECTORIAS".center(40))
        print("=" * 50 + "\n")

        print("\tA continuación se muestran los pasos \na seguir para la caracterización:\n\n")

        for num, text in MainMenu.MAIN_OPTIONS.copy():
            print(f" {num}┃ {text}")

        print("\n" + "-" * 40)
        return input(" ➤ Seleccione una opción: ")

    @staticmethod
    def display_ask_filename(request) -> str:
        print("\n" + "=" * 50)
        print(" " * 10 + request.center(40))
        print("=" * 50 + "\n")

        return input(" ➤ Introduzca el nombre del archivo CSV: ")