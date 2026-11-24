"""Usa en tu proyecto un modelo entrenado con Distributed ML.

Solo necesita Python 3.10 o posterior, sin PyTorch ni este repositorio:
copia este archivo y el modelo descargado (botón "Descargar modelo" en
Resultados, o GET /jobs/{job_id}/model) a tu proyecto.

Desde la terminal:

    python predecir.py modelo.json datos.csv predicciones.csv

Desde tu código:

    from predecir import cargar_modelo, predecir_fila
    modelo = cargar_modelo("modelo.json")
    clase, probabilidades = predecir_fila(modelo, {"Sex": "1", "Age": "0.3", ...})

El CSV debe traer las mismas columnas de entrada con las que se entrenó
(``input_columns`` en el modelo). La columna objetivo y las columnas
excluidas (identificadores) pueden estar o no; se ignoran.
"""

import csv
import json
import math
import sys
from pathlib import Path

FORMATO = "distributed-ml/logistic-regression@1"


def cargar_modelo(ruta: str | Path) -> dict:
    modelo = json.loads(Path(ruta).read_text(encoding="utf-8"))
    if modelo.get("format") != FORMATO:
        raise ValueError(f"El archivo no es un modelo de Distributed ML ({FORMATO}).")
    return modelo


def _codificar(modelo: dict, fila: dict) -> list[float]:
    """Aplica la misma normalización y one-hot que el entrenamiento."""
    numericas = {item["column"]: item for item in modelo["numeric"]}
    categoricas = {item["column"]: item["categories"] for item in modelo["categorical"]}
    valores: list[float] = []
    for columna in modelo["input_columns"]:
        if columna not in fila:
            raise ValueError(f"Falta la columna «{columna}».")
        dato = str(fila[columna]).strip()
        if columna in numericas:
            try:
                numero = float(dato)
            except ValueError as exc:
                raise ValueError(f"«{columna}» debe ser numérica; llegó «{dato}».") from exc
            valores.append((numero - numericas[columna]["mean"]) / numericas[columna]["std"])
        else:
            # Una categoría que no apareció en el entrenamiento queda en ceros.
            valores.extend(1.0 if dato == categoria else 0.0 for categoria in categoricas[columna])
    return valores


def predecir_fila(modelo: dict, fila: dict) -> tuple[str, dict[str, float]]:
    """Devuelve la clase predicha y la probabilidad de cada clase."""
    x = _codificar(modelo, fila)
    clases = modelo["class_labels"]
    logits = [
        modelo["bias"][k] + sum(valor * modelo["weights"][i][k] for i, valor in enumerate(x))
        for k in range(len(clases))
    ]
    maximo = max(logits)
    exponentes = [math.exp(valor - maximo) for valor in logits]
    total = sum(exponentes)
    probabilidades = {clase: e / total for clase, e in zip(clases, exponentes)}
    return max(probabilidades, key=probabilidades.get), probabilidades


def predecir_csv(modelo: dict, entrada: Path, salida: Path) -> int:
    with entrada.open(encoding="utf-8-sig", newline="") as origen:
        filas = list(csv.DictReader(origen))
    clases = modelo["class_labels"]
    with salida.open("w", encoding="utf-8", newline="") as destino:
        columnas = list(filas[0].keys()) if filas else []
        escritor = csv.DictWriter(
            destino, fieldnames=columnas + ["prediccion"] + [f"prob_{c}" for c in clases]
        )
        escritor.writeheader()
        for fila in filas:
            clase, probabilidades = predecir_fila(modelo, fila)
            escritor.writerow(
                {**fila, "prediccion": clase, **{f"prob_{c}": round(p, 6) for c, p in probabilidades.items()}}
            )
    return len(filas)


def main() -> None:
    if len(sys.argv) not in (3, 4):
        print("Uso: python predecir.py modelo.json datos.csv [predicciones.csv]")
        raise SystemExit(2)
    modelo = cargar_modelo(sys.argv[1])
    entrada = Path(sys.argv[2])
    salida = Path(sys.argv[3]) if len(sys.argv) == 4 else entrada.with_name(entrada.stem + "_predicciones.csv")
    total = predecir_csv(modelo, entrada, salida)
    # Solo caracteres que también muestra la consola de Windows.
    print(f"{total} filas predichas para '{modelo['target_column']}' -> {salida}")


if __name__ == "__main__":
    main()
