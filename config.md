# Distributed ML — Configuración de primer uso

Esta guía permite preparar el proyecto en un equipo nuevo o después de moverlo a la carpeta correcta del repositorio. Está pensada para el entorno de desarrollo local en Windows, usando Git Bash, Docker Desktop y Python.

## 1. Tecnologías requeridas

Instalar y verificar:

- Git (Git Bash).
- Python 3.14.x (el entorno de desarrollo actual usa Python 3.14.7).
- Docker Desktop, con el motor Linux/WSL2 iniciado.
- VS Code (opcional, recomendado).

Comandos de verificación desde Git Bash:

```bash
git --version
python --version
docker --version
docker compose version
```

## 2. Ubicar el proyecto

Clonar el repositorio en la ubicación deseada y entrar a su carpeta raíz:


```bash
git clone <URL_DEL_REPOSITORIO>
cd <CARPETA_DEL_REPOSITORIO>
```

La raíz del proyecto debe contener `backend/`, `frontend/`, `docs/` y `docker-compose.yml` (entre otros archivos).

## 3. Crear el entorno virtual Python

**No reutilizar ni copiar `.venv` de otro directorio o equipo.** Los entornos virtuales pueden guardar rutas absolutas asociadas a su ubicación original.

Desde la raíz del proyecto, en Git Bash:

```bash
python -m venv .venv
source .venv/Scripts/activate
```

Verificar que Python corresponde al entorno virtual:

```bash
which python
python --version
```

La ruta de `python` debería apuntar dentro de la carpeta del proyecto, a `.venv/Scripts/`.

## 4. Instalar dependencias del backend

Con el entorno virtual activado y desde la raíz:

```bash
python -m pip install --upgrade pip
python -m pip install -r backend/requirements.txt
```

Si se abre una nueva terminal, volver a activar el entorno:

```bash
source .venv/Scripts/activate
```

## 5. Configurar y levantar CockroachDB

Docker Desktop debe estar iniciado. Desde la raíz del proyecto:


```bash
docker compose up -d
```

Comprobar el estado de los contenedores:

```bash
docker compose ps
```

El Compose local configura tres nodos CockroachDB (`cockroach1`, `cockroach2`, `cockroach3`). Los puertos SQL publicados en el host son `26257`, `26258` y `26259`; las consolas de administración se publican en `8080`, `8081` y `8082`.

### Inicialización del clúster: solo si es un clúster nuevo

Si los volúmenes de datos de CockroachDB están vacíos y el clúster nunca se ha inicializado, ejecutar una sola vez:

```bash
docker compose exec cockroach1 cockroach init --insecure --host=cockroach1:26257
```

**No repetir la inicialización en un clúster ya inicializado.** Si se trasladó/copió el proyecto y Docker está usando los volúmenes anteriores, el clúster y sus datos pueden seguir existiendo; primero verificar el estado.

Consultar los nodos:

```bash
docker compose exec cockroach1 cockroach node status --insecure --host=cockroach1:26257
```

## 6. Base de datos y esquema

El backend usa la base de datos `distributed_ml`.

Consultar si la base existe:


```bash
docker compose exec cockroach1 cockroach sql --insecure --host=cockroach1:26257 --database=distributed_ml -e "SELECT current_database();"
```

Si el clúster es completamente nuevo, crear la base (una sola vez):

```bash
docker compose exec cockroach1 cockroach sql --insecure --host=cockroach1:26257 -e "CREATE DATABASE distributed_ml;"
```

El esquema inicial contiene cinco tablas: `nodes`, `datasets`, `jobs`, `tasks` y `results`, junto con sus relaciones e índices. Aplicar el SQL de creación de esquema desde la documentación/archivo SQL del proyecto cuando se prepare una base nueva. No ejecutar nuevamente el DDL en una base que ya tenga esas tablas, salvo que se trate de una migración deliberada.

Verificar las tablas:

```bash
docker compose exec cockroach1 cockroach sql --insecure --host=cockroach1:26257 --database=distributed_ml -e "SHOW TABLES;"
```

## 7. Configuración de la conexión backend

`backend/database/connection.py` usa la variable de entorno `DATABASE_URL` si está definida. Si no está definida, el valor predeterminado de desarrollo local es:

```text
postgresql://root@127.0.0.1:26257/distributed_ml?sslmode=disable
```

Este valor funciona cuando FastAPI se ejecuta directamente en el equipo (fuera de Docker) y CockroachDB publica el puerto `26257`.

Si una variable `DATABASE_URL` previa quedó definida en la terminal, puede sobrescribir el valor predeterminado. Para quitarla temporalmente en Git Bash:

```bash
unset DATABASE_URL
```

Si FastAPI se ejecuta dentro de un contenedor Docker, `127.0.0.1` ya no apunta al host ni a otro contenedor; se deberá configurar la URL con el nombre DNS del servicio de Compose, por ejemplo `cockroach1`, y el puerto interno `26257`. La ejecución del backend dentro de Docker no está configurada como parte del Compose actual.

## 8. Ejecutar FastAPI

Desde la raíz del proyecto, activar el entorno virtual e iniciar Uvicorn:


```bash
source .venv/Scripts/activate
cd backend
python -m uvicorn main:app --host 0.0.0.0 --port 8000
```

Mantener esta terminal abierta mientras se use la API.

## 9. Verificar la aplicación

En otra terminal de Git Bash, ejecutar:


```bash
curl.exe -i http://127.0.0.1:8000/health
```

Respuesta esperada (el orden o los valores de campos podrían variar según implementación, pero deben indicar estado saludable y base de datos disponible):

```json
{
  "status": "healthy",
  "application": "available",
  "database": "available"
}
```

Un HTTP `200 OK` y el indicador `database: available` confirman que FastAPI está activo y que la verificación de conexión a CockroachDB funcionó.

## 10. Puertos locales

| Servicio | Puerto del host |
|---|---:|
| FastAPI / Uvicorn | `8000` |
| CockroachDB SQL — nodo 1 | `26257` |
| CockroachDB SQL — nodo 2 | `26258` |
| CockroachDB SQL — nodo 3 | `26259` |
| CockroachDB Admin — nodo 1 | `8080` |
| CockroachDB Admin — nodo 2 | `8081` |
| CockroachDB Admin — nodo 3 | `8082` |

## 11. Qué se conserva y qué se recrea al mover el proyecto

- **Código y documentación:** copiar/checkout desde Git.
- **`.venv`:** recrear localmente; no compartirlo ni versionarlo.
- **Dependencias Python:** instalar desde `backend/requirements.txt`.
- **Contenedores:** recrearlos con `docker compose up -d`.
- **Datos CockroachDB:** viven en volúmenes Docker, no dentro de los archivos del repositorio. Copiar la carpeta del proyecto no copia estos volúmenes. Docker Compose puede usar volúmenes existentes o crear volúmenes nuevos según el nombre del proyecto/configuración; verificar los datos antes de asumir que la base está presente.
- **`.env`:** archivo local no versionado. Si el proyecto necesita variables adicionales, crear el `.env` a partir de `.env.example` y rellenarlo; no compartir secretos.
- **Archivos CSV/datasets locales:** pueden estar dentro de `backend/storage/`; confirmar política de respaldo/versionado y no subir datos sensibles.

## 12. Reiniciar el entorno en el día a día

Después de preparar el equipo, el flujo habitual es:


```bash
# Desde la raíz del proyecto
docker compose up -d
source .venv/Scripts/activate
cd backend
python -m uvicorn main:app --host 0.0.0.0 --port 8000
```

Para detener FastAPI, usar `Ctrl+C` en la terminal de Uvicorn. Para detener los contenedores:

```bash
docker compose down
```

`docker compose down` no elimina los volúmenes de datos por defecto. **No usar `docker compose down -v` a menos que se desee borrar los volúmenes y, con ellos, los datos persistidos de CockroachDB.**

## 13. Estado y límites de esta guía

Esta guía cubre la configuración local actual del backend y CockroachDB. No instala ni configura Workers, el proceso de entrenamiento, el scheduler, endpoints funcionales de datasets/jobs, ni despliegue de FastAPI en contenedor; esas piezas se implementarán en tareas posteriores.
