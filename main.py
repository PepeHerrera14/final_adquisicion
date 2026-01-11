# José Herrera, Mateo Gómez-Acebo, Gonzalo Crespo y Diego Bertolín
# Adquisición de Datos – PROYECTO FINAL
# Grado en Ingeniería Matemática e Inteligencia Artificial
# ETSI ICAI

from apartado_1 import run_part_i
from apartado_2 import run_part_ii
from apartado_3 import run_part_iii


def mostrar_menu():
    print("\n" + "=" * 50)
    print(" PROYECTO FINAL - ADQUISICIÓN DE DATOS (F1) ")
    print("=" * 50)
    print("1. Ejecutar Apartado 1 (Scraping Wikipedia)")
    print("2. Ejecutar Apartado 2 (Pit-stops API Jolpica)")
    print("3. Ejecutar Apartado 3 (Merge de datos)")
    print("4. Ejecutar TODO (1 + 2 + 3)")
    print("0. Salir")
    print("=" * 50)


def main():
    while True:
        mostrar_menu()
        opcion = input("Selecciona una opción: ").strip()

        if opcion == "1":
            print("\n[EJECUTANDO] Apartado 1 - Scraping Wikipedia\n")
            run_part_i()
            print("\n[FINALIZADO] Apartado 1\n")

        elif opcion == "2":
            print("\n[EJECUTANDO] Apartado 2 - Pit-stops Jolpica API\n")
            run_part_ii(
                seasons=[2019, 2020, 2021, 2022, 2023, 2024],
                out_dir="data"
            )
            print("\n[FINALIZADO] Apartado 2\n")

        elif opcion == "3":
            print("\n[EJECUTANDO] Apartado 3 - Merge de datos\n")
            run_part_iii(
                data_dir="data",
                output_file="data/final_merged.csv"
            )
            print("\n[FINALIZADO] Apartado 3\n")

        elif opcion == "4":
            print("\n[EJECUTANDO] Proyecto completo\n")
            run_part_i()
            run_part_ii(
                seasons=[2019, 2020, 2021, 2022, 2023, 2024],
                out_dir="data"
            )
            run_part_iii(
                data_dir="data",
                output_file="data/final_merged.csv"
            )
            print("\n[FINALIZADO] Proyecto completo\n")

        elif opcion == "0":
            print("\nSaliendo del programa...\n")
            break

        else:
            print("\nOpción no válida. Inténtalo de nuevo.\n")


if __name__ == "__main__":
    main()
