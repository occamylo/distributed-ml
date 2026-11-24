# Distributed ML

Sistema para gestionar datasets, configurar Jobs de aprendizaje automático,
coordinar Workers y consultar el estado técnico de un clúster CockroachDB.

## 1. Propósito

Distributed ML reúne una API, una interfaz web y procesos Worker que comparten
estado en CockroachDB. La interfaz permite cargar un CSV, elegir un algoritmo y
una columna objetivo, y guardar la configuración como un Job. Un Scheduler
asigna Tasks a Workers elegibles de acuerdo con su disponibilidad registrada.

## 2. Tecnologías

| Componente | Tecnología | Uso |
|---|---|---|
| API y servicio web | Python, FastAPI y Uvicorn | Rutas HTTP y servicio del frontend |
| Persistencia | Psycopg 3 y `psycopg_pool` | Acceso SQL síncrono a CockroachDB |
| Base de datos | CockroachDB | Datos de Workers, datasets, Jobs, Tasks y liderazgo |
| Infraestructura local | Docker Compose | Tres servicios CockroachDB |
| Interfaz | HTML, CSS y JavaScript | Formularios de configuración y Vista técnica |
| Coordinación | Biblioteca estándar de Python | Heartbeat, concurrencia, HTTP y leases |

Las dependencias Python están declaradas en
[`backend/requirements.txt`](backend/requirements.txt).

## 3. Arquitectura

### 3.1. Base de datos distribuida

[`docker-compose.yml`](docker-compose.yml) define tres servicios CockroachDB,
cada uno con su volumen de datos, puerto SQL local y puerto de administración.
La API y los Workers se ejecutan como procesos separados de esos servicios.

La conexión de la aplicación se configura mediante `DATABASE_URL`. En el
entorno local predeterminado, Psycopg intenta los puertos SQL configurados
para los tres nodos.

### 3.2. API y persistencia

[`backend/main.py`](backend/main.py) crea la aplicación FastAPI, inicializa el
pool de conexiones durante el ciclo de vida de la aplicación, registra los
routers y sirve `frontend/` desde `/`.

Las rutas de `backend/api/` gestionan Workers, datasets, Jobs y el estado
técnico. [`backend/database/repository.py`](backend/database/repository.py)
contiene las operaciones parametrizadas para leer y persistir esos registros.
El DDL se encuentra en [`backend/database/schema.sql`](backend/database/schema.sql).

### 3.3. Workers y Scheduler

Cada proceso Worker se registra en la API, publica heartbeat y métricas en
CockroachDB y participa en la coordinación por leases. Un Worker que mantiene
el liderazgo ejecuta el Scheduler.

El Scheduler consulta Jobs pendientes y selecciona dinámicamente los Workers
elegibles según estado, heartbeat, métricas de disponibilidad y carga
registrada. Divide las filas del dataset en particiones, crea Tasks y las
asigna a Workers. Cuando se confirman las asignaciones, actualiza el Job a
`running`.

### 3.4. Interfaz y monitoreo

El frontend ofrece un formulario de dataset y entrenamiento y una Vista
técnica. Esta última consulta `/cluster/technical-status` para mostrar salud
de la API y la base de datos, membresía, Workers, liderazgo, registros recientes,
rango y eventos reportados por CockroachDB.

## 4. Flujo de funcionamiento

1. FastAPI sirve la interfaz y las rutas de la aplicación.
2. El usuario selecciona un CSV. `POST /datasets/upload` valida su estructura,
   guarda el archivo con un nombre único en `backend/storage/datasets/` y
   persiste su ruta y metadatos.
3. La interfaz muestra los encabezados. El usuario elige un algoritmo y una
   columna `target`; las demás columnas se guardan como `features`.
4. `POST /jobs` vuelve a validar el CSV y persiste el Job como `pending`,
   incluidos el dataset, el algoritmo, el target y los nombres de las columnas.
   `GET /jobs/{job_id}` permite recuperar su configuración.
5. El Scheduler planifica los Jobs pendientes cuando existe un líder vigente y
   Workers elegibles. El número de Workers se determina dinámicamente; no se
   solicita en el formulario.
6. La Vista técnica consulta el estado operativo y los registros recientes
   desde FastAPI.

### Validación del CSV

La validación comprueba que el archivo sea legible y que sus encabezados y
filas tengan una estructura válida. También detecta encabezados vacíos o
duplicados y valores faltantes habituales. Para regresión lineal valida que el
target sea numérico y finito; para regresión logística comprueba que contenga
al menos dos valores distintos.

## 5. Modelo de datos

El esquema organiza la información en estas tablas:

| Tabla | Contenido |
|---|---|
| `nodes` | Identidad, estado, heartbeat y métricas de Workers |
| `datasets` | Ruta del archivo CSV y metadatos de carga |
| `jobs` | Dataset, algoritmo, target, parámetros y estado del Job |
| `tasks` | Particiones, asignaciones y estado de cada Task |
| `results` | Métricas y ubicación del resultado asociado a un Job |
| `cluster_leadership` | Worker líder, época y vigencia del lease |

Un Job referencia un Dataset y puede tener varias Tasks. Las Tasks pueden
asociarse a un nodo Worker; el esquema conserva información de asignación,
intento y lease. El campo `requested_workers` de Jobs es nullable para admitir
registros históricos; los Jobs nuevos no dependen de una cantidad solicitada.

## 6. API HTTP

| Método | Ruta | Función |
|---|---|---|
| `GET` | `/health` | Estado de FastAPI y del pool de base de datos |
| `GET` | `/cluster/technical-status` | Estado técnico agregado del sistema |
| `POST` | `/nodes` | Registro de un Worker |
| `POST` | `/nodes/{node_id}/heartbeat` | Heartbeat de un Worker |
| `POST` | `/datasets` | Creación de un registro de Dataset |
| `POST` | `/datasets/upload?filename={nombre}.csv` | Carga y validación de un CSV |
| `POST` | `/jobs` | Validación y creación de un Job pendiente |
| `GET` | `/jobs/{job_id}` | Recuperación de un Job |
| `GET` | `/jobs/{job_id}/tasks` | Tasks asociadas a un Job |

La carga de CSV envía el archivo como cuerpo de la solicitud. El backend
devuelve el identificador del Dataset, los encabezados y los metadatos
persistidos.

## 7. Organización del código

```text
.
├── backend/
│   ├── api/              # Rutas HTTP, validación CSV y estado técnico
│   ├── coordinator/      # Scheduler y módulos asociados a Tasks
│   ├── database/         # Conexión, repositorio y esquema SQL
│   ├── tests/            # Pruebas API, Scheduler y liderazgo
│   ├── workers/          # Worker y coordinación por leases
│   ├── main.py           # Aplicación FastAPI
│   └── requirements.txt
├── frontend/
│   ├── app.js
│   ├── index.html
│   └── styles.css
└── docker-compose.yml
```

Los archivos CSV cargados se guardan bajo `backend/storage/datasets/`; ese
directorio se crea cuando se realiza la primera carga.

## 8. Ejecución local

### Requisitos

- Python 3.10 o posterior.
- Docker Desktop con Docker Compose.

### Preparar el entorno

Desde PowerShell, en la raíz del proyecto:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r backend\requirements.txt
docker compose up -d
```

### Inicializar CockroachDB

En un clúster nuevo con volúmenes vacíos, inicializarlo una sola vez:

```powershell
docker compose exec cockroach1 cockroach init --insecure --host=cockroach1:26257
```

Crear la base de datos:

```powershell
docker compose exec cockroach1 cockroach sql --insecure --execute "CREATE DATABASE distributed_ml;"
```

Aplicar el esquema en una base de aplicación vacía:

```powershell
Get-Content -Raw .\backend\database\schema.sql | docker compose exec -T cockroach1 cockroach sql --insecure --database=distributed_ml
```

El esquema crea las tablas de aplicación y la fila inicial de liderazgo.
Aplicarlo una sola vez al inicializar la base; no volver a ejecutarlo sobre
tablas existentes.

### Iniciar la API

En una terminal de PowerShell:

```powershell
$env:DATABASE_URL = "postgresql://root@127.0.0.1:26257/distributed_ml?sslmode=disable"
Set-Location .\backend
..\.venv\Scripts\python.exe -m uvicorn main:app --host 0.0.0.0 --port 8000
```

La interfaz queda disponible en `http://127.0.0.1:8000/`.

### Iniciar un Worker

En otra terminal, desde la raíz del proyecto, configura `DATABASE_URL` y
ejecuta:

```powershell
$env:DATABASE_URL = "postgresql://root@127.0.0.1:26257/distributed_ml?sslmode=disable"
Set-Location .\backend
..\.venv\Scripts\python.exe -m workers.worker --backend-url http://127.0.0.1:8000 --node-id-file ..\worker-state\worker-1.id
```

Cada proceso Worker concurrente debe utilizar un archivo de identidad
diferente. Los servicios CockroachDB permanecen en Docker Compose; FastAPI y
cada Worker se inician por separado.

