import json
import os

def main():
    print("=========================================")
    print(" 🧠 UNIFICADOR DE MEMORIA (LOCAL + GITHUB)")
    print("=========================================")

    # 1. Cargar la memoria local
    local_data = {}
    if os.path.exists("data/registro_imagenes.json"):
        with open("data/registro_imagenes.json", "r", encoding="utf-8") as f:
            local_data = json.load(f)
        print(f"📦 Memoria Local leída: {len(local_data)} productos.")
    else:
        print("⚠️ No se encontró la memoria local.")

    # 2. Cargar la memoria de GitHub
    github_data = {}
    if os.path.exists("registro_github.json"):
        with open("registro_github.json", "r", encoding="utf-8") as f:
            github_data = json.load(f)
        print(f"☁️ Memoria de GitHub leída: {len(github_data)} productos.")
    else:
        print("⚠️ No se encontró 'registro_github.json'.")

    # 3. Unificar (La memoria local actualiza/sobreescribe a la de GitHub en caso de choque)
    unificado = github_data.copy()
    unificado.update(local_data)

    # 4. Guardar el archivo final
    with open("registro_unificado.json", "w", encoding="utf-8") as f:
        json.dump(unificado, f, indent=2)

    print(f"\n✅ ¡Fusión completada! El nuevo archivo tiene {len(unificado)} productos.")
    print("👉 Sube el archivo 'registro_unificado.json' a GitHub y renómbralo como 'registro_imagenes.json'")

if __name__ == "__main__":
    main()
