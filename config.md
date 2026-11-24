# Distributed ML — Configuración y ejecución local

Guía para preparar el proyecto en un equipo nuevo y ejecutar el sistema completo en un entorno local Windows, utilizando Git Bash, Docker Desktop y Python.

## 1. Requisitos

Instalar:

- Git y Git Bash.
- Python 3.14.x.
- Docker Desktop con el motor Linux/WSL2 iniciado.
- VS Code (opcional).

Verificar las instalaciones desde Git Bash:

```bash
git --version
python --version
docker --version
docker compose version
```

## 2. Obtener el proyecto

Desde Git Bash:

```bash
git clone <URL_DEL_REPOSITORIO>
cd distributed-ml
```

La raíz del proyecto contiene `backend/`, `frontend/` y `docker-compose.yml`.

## 3. Preparar Python

Desde la raíz del proyecto:

```bash
python -m venv .venv
source .venv/Scripts/activate
python -m pip install --upgrade pip
python -m pip install -r backend/requirements.txt
```

Verificar el entorno:

```bash
which python
python --version
```

La ruta debe apuntar a `.venv/Scripts/`.

No copiar ni reutilizar el entorno `.venv` de otro equipo.

## 4. Levantar CockroachDB

Asegurarse de que Docker Desktop esté iniciado.

Desde la raíz:

```bash
docker compose up -d
docker compose ps
```

El proyecto utiliza tres servicios CockroachDB:

| Nodo | Puerto SQL | Admin UI |
|---|---:|---:|
| cockroach1 | 26257 | 8080 |
| cockroach2 | 26258 | 8081 |
| cockroach3 | 26259 | 8082 |

### Inicialización de un clúster nuevo

Este paso se ejecuta únicamente cuando los volúmenes están vacíos y el clúster nunca ha sido inicializado:

```bash
docker compose exec cockroach1 cockroach init --insecure --host=cockroach1:26257
```

No repetir este comando sobre un clúster inicializado.

Verificar la membresía:

```bash
docker compose exec cockroach1 cockroach node status --insecure --host=cockroach1:26257
```

Se deben identificar los tres nodos.

### Acceso a las consolas de administración

- DB1: http://127.0.0.1:8080
- DB2: http://127.0.0.1:8081
- DB3: http://127.0.0.1:8082

## 5. Crear la base de datos y preparar el esquema

La base de datos utilizada por el proyecto es `distributed_ml`.

Consultar si ya existe:

```bash
docker compose exec cockroach1 cockroach sql --insecure --execute "SHOW DATABASES;"
```

Si no existe, crearla:

```bash
docker compose exec cockroach1 cockroach sql --insecure --execute "CREATE DATABASE distributed_ml;"
```

### Esquema de aplicación

La instalación necesita las tablas y estructuras de persistencia del proyecto, incluyendo:

- `nodes`
- `datasets`
- `jobs`
- `tasks`
- `results`
- `cluster_leadership`

**Importante:** antes de ejecutar una instalación desde cero, verificar el DDL vigente y las migraciones de `backend/database/`. No asumir que `backend/database/schema.sql` contiene el esquema completo ni ejecutar DDL antiguo sobre una base existente.

Para una base ya creada, aplicar `backend/database/migrations/001_make_requested_workers_nullable.sql` antes de desplegar esta versión. La migración solo elimina `NOT NULL` y conserva los valores históricos.

Una vez aplicado el esquema correspondiente, verificar:

```bash
docker compose exec cockroach1 cockroach sql --insecure --database=distributed_ml --execute "SHOW TABLES;"
```

La instalación solo está lista para continuar cuando las tablas requeridas por la implementación actual existen.

## 6. Configurar la conexión a CockroachDB

FastAPI y los Workers utilizan la variable `DATABASE_URL`.

La conexión local predeterminada es:

```bash
export DATABASE_URL="postgresql://root@127.0.0.1:26257/distributed_ml?sslmode=disable"
```

Ejecutar este comando en cada terminal Git Bash que vaya a iniciar FastAPI o un Worker.

Si se desea volver al valor predeterminado del proyecto:

```bash
unset DATABASE_URL
```

La configuración corresponde a un entorno local de desarrollo con CockroachDB en modo insecure. No utilizarla como configuración de producción.

## 7. Iniciar FastAPI

Abrir una terminal Git Bash desde la raíz del proyecto.

Activar el entorno:

```bash
source .venv/Scripts/activate
```

Configurar la conexión:

```bash
export DATABASE_URL="postgresql://root@127.0.0.1:26257/distributed_ml?sslmode=disable"
```

Entrar al backend:

```bash
cd backend
```

Iniciar el servidor:

```bash
python -m uvicorn main:app --host 127.0.0.1 --port 8000
```

Mantener esta terminal abierta.

Direcciones disponibles:

- Aplicación web: http://127.0.0.1:8000/
- Estado de la API: http://127.0.0.1:8000/health
- Estado técnico del clúster: http://127.0.0.1:8000/cluster/technical-status
- Documentación de API: http://127.0.0.1:8000/docs

FastAPI sirve tanto la interfaz como los endpoints de la aplicación.

## 8. Iniciar los Workers

Para el funcionamiento completo del entorno de demostración, iniciar tres procesos Worker independientes.

Abrir tres terminales Git Bash adicionales.

En cada terminal, desde la raíz del proyecto:

```bash
source .venv/Scripts/activate
export DATABASE_URL="postgresql://root@127.0.0.1:26257/distributed_ml?sslmode=disable"
cd backend
python -m workers.worker --backend-url http://127.0.0.1:8000
```

Ejecutar el mismo comando en las tres terminales.

Cada proceso se registra de manera independiente y publica su Heartbeat y métricas en CockroachDB.

Mantener las terminales abiertas mientras se quiera conservar los Workers activos.

## 9. Verificar el funcionamiento

Con CockroachDB, FastAPI y los tres Workers en ejecución, abrir otra terminal Git Bash.

### Verificar CockroachDB

```bash
docker compose ps
docker compose exec cockroach1 cockroach node status --insecure --host=cockroach1:26257
```

### Verificar FastAPI

```bash
curl -i http://127.0.0.1:8000/health
```

La respuesta debe indicar que la aplicación y la conexión a la base de datos están disponibles.

### Verificar la Vista técnica

```bash
curl -i http://127.0.0.1:8000/cluster/technical-status
```

También se puede consultar directamente desde el navegador:

http://127.0.0.1:8000/

La Vista técnica debe reflejar los estados reales de los nodos, el liderazgo vigente, los Workers y los registros disponibles.

La cantidad de registros y Workers puede variar según la actividad previa del entorno.

## 10. Orden habitual de arranque

Una vez configurado el equipo, el orden recomendado para iniciar una sesión de trabajo es:

**1. Terminal 1 — CockroachDB**

Desde la raíz:

```bash
docker compose up -d
docker compose ps
```

**2. Terminal 2 — FastAPI**

Desde la raíz:

```bash
source .venv/Scripts/activate
export DATABASE_URL="postgresql://root@127.0.0.1:26257/distributed_ml?sslmode=disable"
cd backend
python -m uvicorn main:app --host 127.0.0.1 --port 8000
```

**3. Terminales 3, 4 y 5 — Workers**

En cada terminal:

```bash
source .venv/Scripts/activate
export DATABASE_URL="postgresql://root@127.0.0.1:26257/distributed_ml?sslmode=disable"
cd backend
python -m workers.worker --backend-url http://127.0.0.1:8000
```

**4. Navegador**

Abrir:

http://127.0.0.1:8000/

Con esto se dispone del clúster de base de datos, la API, los procesos Worker y la interfaz de monitoreo.

## 11. Puertos utilizados

| Servicio | Puerto |
|---|---:|
| FastAPI / Frontend | 8000 |
| CockroachDB SQL — nodo 1 | 26257 |
| CockroachDB SQL — nodo 2 | 26258 |
| CockroachDB SQL — nodo 3 | 26259 |
| CockroachDB Admin — nodo 1 | 8080 |
| CockroachDB Admin — nodo 2 | 8081 |
| CockroachDB Admin — nodo 3 | 8082 |

## 12. Detener el entorno

Para detener cada Worker y FastAPI, presionar `Ctrl + C` en sus respectivas terminales.

Para detener los contenedores CockroachDB, desde la raíz:

```bash
docker compose down
```

Este comando conserva los volúmenes persistentes.

**No ejecutar `docker compose down -v`** salvo que se desee eliminar también los volúmenes y todos los datos persistidos del clúster.

## 13. Consideraciones importantes

- Cada equipo debe crear su propio `.venv`.
- Los datos de CockroachDB se conservan en volúmenes Docker y no se transfieren al copiar el repositorio.
- La inicialización del clúster y la creación del esquema son operaciones de primer uso, no pasos del arranque diario.
- FastAPI y los Workers deben utilizar una `DATABASE_URL` que apunte a una instancia accesible desde el equipo.
- Cada Worker es un proceso independiente. Para visualizar tres Workers activos, deben ejecutarse tres procesos.
- Los tres nodos CockroachDB se ejecutan en contenedores del mismo host físico; no representan tres servidores físicos independientes.
- La Vista técnica consulta información real del sistema, pero la disponibilidad de cada componente no debe confundirse con la replicación física de todos los datos en cada nodo.
