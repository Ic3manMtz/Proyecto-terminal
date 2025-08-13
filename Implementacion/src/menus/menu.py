import os
from time import sleep
from typing import List, Tuple, Optional
from colorama import init, Fore, Style

class MainMenu:
    MAIN_OPTIONS: List[Tuple[str, str]] = [
        ("0", "Cargar archivo CSV"), # Listo
        ("1", "Exploración inicial de datos"), # Listo
        ("2", "Número de registros"), # Listo
        ("3", "Eliminación de columnas"), # Listo
        ("4", "Valores únicos de una columna"), # Listo
        ("5", "Creación de histograma de frecuencias de la columna 'device_horizontal_accuracy'"), # Listo
        ("6", "Creación de histograma de frecuencias de la columna 'identifier'"), # Listo
        ("7", "Análisis detallado de la columna 'identifier'"), # Listo
        ("8", "Eliminar duplicados"), # Listo
        ("9", "Creación de histogramas de la columna 'identifier' por día"), # Listo
        ("10", "Obtener individuos rutinarios"),
        ("11", "Migrar csv a postgres"),
        ("12", "Mostrar coordenadas de un identificador"),
        ("13", "Salir")
    ]

    @staticmethod
    def display_main_menu(name: str="") -> str:
        print("\n" + "=" * 50)
        print(" " * 5 + "CARACTERIZACIÓN DE TRAYECTORIAS".center(40))
        print("=" * 50 + "\n")

        print("\tA continuación se muestran los pasos \n\t a seguir para la caracterización:\n\n")


        for num, text in MainMenu.MAIN_OPTIONS.copy():
            if num == "0":
                text = f"{text} ({Fore.GREEN}Archivo actual: {name}{Style.RESET_ALL})"
            elif text.find("'") != -1:
                text = MainMenu.highlight_text(text)
            print(f" {num:>3}┃ {text}")

        print("\n" + "-" * 40)
        return input(" ➤ Seleccione una opción: ").strip()

    @staticmethod
    def display_ask_filename(request) -> str:
        print("\n" + "=" * 50)
        print(" " * 5 + request.center(40))
        print("=" * 50 + "\n")

        return input(" ➤ Introduzca el nombre del archivo CSV: ").strip()
    
    @staticmethod
    def display_available_columns(available_columns: List[str]) -> int:
        print("\n" + "=" * 50)
        print(" " * 5 + "COLUMNA A ANALIZAR".center(40))
        print("=" * 50 + "\n")

        for idx, col in enumerate(available_columns):
            print(f"{idx + 1}. {col}")

        try:
            selection = int(input("\n🔽 Ingresa el número de la columna que deseas analizar: ").strip()) - 1
            if selection < 0 or selection >= len(available_columns):
                raise ValueError("Selección fuera de rango")
            return selection
        except ValueError as e:
            print(f"❌ Error: {e}")
            return -1

    @staticmethod
    def highlight_text(text: str) -> str:
        before_quote, remaining = text.split("'", 1)
        
        # Split the remaining part to get the text to highlight
        if "'" in remaining:
            highlight_content, after_quote = remaining.split("'", 1)
        else:
            highlight_content = remaining
            after_quote = ""
        
        # Build the final result
        return f"{before_quote}'{Fore.YELLOW}{highlight_content}{Style.RESET_ALL}'{after_quote}"