# Distributed ML

Distributed ML es un proyecto académico para explorar cómo se relacionan
un servicio web, varios procesos Worker y una base de datos distribuida
en un sistema de entrenamiento de Machine Learning.

El objetivo arquitectónico es repartir el procesamiento de un dataset
entre Workers y persistir su estado en CockroachDB. El código actual
incluye el servicio FastAPI básico, el registro de Workers, la publicación
directa de Heartbeat y métricas en CockroachDB y primitivas persistentes
de liderazgo. La interfaz web, el Scheduler y el entrenamiento
distribuido todavía no están implementados.

## Características y estado

| Capacidad | Estado |
|---|---|
| Clúster local CockroachDB de tres nodos | Definido en Docker Compose; la replicación efectiva y el quórum no están verificados. |
| Registro inicial de Workers | Implementado con `POST /nodes`. |
| Heartbeat y métricas de Workers | Implementados: cada Worker publica directamente en CockroachDB cada 3 segundos por defecto. |
| Lease persistente de liderazgo | Tabla y operaciones de repositorio implementadas; no hay elección automática. |
| Frontend y gestión de datasets/Jobs | No implementados. |
| Scheduler, ejecución ML distribuida y recuperación de Tasks | Objetivos arquitectónicos; todavía no son flujos operativos. |

El diseño técnico de elección de coordinador, fencing, comunicación
entre Workers y recuperación está descrito en
[docs/distributed-coordination-architecture.md](docs/distributed-coordination-architecture.md).
Este README resume el sistema completo sin asumir que esas etapas futuras
ya funcionen.

## Arquitectura

La arquitectura distingue el clúster de datos del clúster de cómputo.
Los tres contenedores CockroachDB constituyen una única base lógica; los
Workers son procesos Python independientes. En la implementación
actual, FastAPI recibe el registro inicial y los Workers se conectan por
su cuenta a CockroachDB para las publicaciones periódicas.

```mermaid
flowchart LR
    User[Usuario]
    UI[Frontend web<br/>no implementado]
    API[FastAPI<br/>/health y nodos]
    Repo[Repository y pool psycopg]
    W1[Worker 1]
    W2[Worker 2]
    W3[Worker 3]

    subgraph DB[CockroachDB: un clúster, tres contenedores]
        Nodes[(nodes)]
        Leadership[(cluster_leadership)]
        AppData[(datasets, jobs, tasks, results)]
    end

    User -. uso previsto .-> UI
    UI -. solicitudes futuras .-> API
    W1 -->|POST /nodes al iniciar| API
    W2 -->|POST /nodes al iniciar| API
    W3 -->|POST /nodes al iniciar| API
    API --> Repo --> Nodes
    W1 -->|Heartbeat y métricas SQL cada 3 s| Nodes
    W2 -->|Heartbeat y métricas SQL cada 3 s| Nodes
    W3 -->|Heartbeat y métricas SQL cada 3 s| Nodes
    Repo --> Leadership
    Repo -. futura persistencia de Jobs y Tasks .-> AppData
    W1 -. elección automática futura .-> Leadership
```

### Componentes

- **FastAPI:** inicializa su pool de CockroachDB y ofrece `GET /health`,
  `POST /nodes` y `POST /nodes/{node_id}/heartbeat`. El endpoint HTTP de
  Heartbeat se conserva, pero el Worker actual no lo utiliza después
  del registro.
- **Workers:** cada proceso se registra una vez por HTTP, conserva el
  `node_id` recibido y mantiene su propio pool de CockroachDB. Publica el
  estado, el Heartbeat y las métricas cada 3 segundos por defecto. No
  ejecuta entrenamiento ni participa en una elección de liderazgo.
- **Repository y conexión:** proporcionan operaciones de persistencia
  usando el pool síncrono de Psycopg. El repositorio incluye operaciones
  para el lease, pero estas no son invocadas automáticamente por los
  Workers.
- **CockroachDB:** conserva nodos, metadatos de aplicación y la fila de
  liderazgo. Docker Compose define tres contenedores en el mismo host;
  tres contenedores no demuestran por sí solos replicación de datos ni
  tolerancia a la pérdida de un host.
- **Coordinador:** en el diseño objetivo, un Worker con el lease vigente
  coordinará Jobs y Tasks. La elección, el Scheduler y la comunicación
  Worker-a-Worker aún no están implementados.
- **Frontend:** el directorio `frontend/` está vacío; no hay interfaz
  ejecutable actualmente.

## Flujo de funcionamiento

### Registro y publicación de Workers

1. Se inicia FastAPI y cada Worker envía hostname, IP si está disponible,
   núcleos lógicos y RAM total a `POST /nodes`.
2. FastAPI crea un registro `offline` en `nodes` y devuelve el UUID.
3. El Worker conserva ese `node_id` y abre un pool propio usando
   `DATABASE_URL`.
4. Cada 3 segundos, el Worker actualiza solo su fila: la marca como
   `online`, fija `last_heartbeat` y `metrics_updated_at` con el reloj de
   CockroachDB, y publica CPU disponible, RAM disponible y tareas activas.
5. Si CockroachDB se interrumpe temporalmente, el proceso sigue vivo e
   intenta publicar de nuevo con el pool existente. FastAPI es necesario
   para el registro inicial, pero no para las publicaciones posteriores.

La CPU disponible se estima con contadores del sistema operativo; en
Linux se limita a la cuota cgroup cuando está disponible. La RAM
disponible se obtiene del sistema operativo y se limita a memoria cgroup
en Linux cuando aplica. Si una medición no puede obtenerse de forma
fiable, se guarda `NULL`. `active_tasks` es `0` porque no existe un
ejecutor de Tasks. Estas métricas son observaciones del sistema, no
garantías de elegibilidad para coordinar.

### Flujo de entrenamiento previsto

El flujo integral esperado es el siguiente. Los pasos de carga,
planificación, ejecución y resultados son objetivos de arquitectura; no
están expuestos por los módulos actuales.

1. El usuario carga un dataset; una futura API validará el archivo y
   guardará sus metadatos en `datasets`.
2. El usuario configura un Job; la API prevista registrará parámetros,
   algoritmo, columna objetivo y cantidad de Workers en `jobs`.
3. El coordinador preparará particiones y creará Tasks en `tasks`.
4. La membresía y las métricas disponibles estarán en `nodes`. Un
   Scheduler futuro elegirá Workers; actualmente no existe esa selección.
5. Las operaciones de lease permiten adquirir y validar liderazgo desde
   el repositorio. La elección automática de un Worker coordinador aún
   no existe.
6. El coordinador distribuirá Tasks a los Workers y controlará intentos,
   leases y época del coordinador.
7. Los Workers procesarán sus particiones; el proceso actual solo
   registra y publica Heartbeat/métricas.
8. El coordinador recibirá y agregará los resultados de las particiones.
9. El resultado final se almacenará en `results`.
10. Una futura interfaz consultará el estado del Job y sus métricas.

## Tecnologías

| Tecnología | Responsabilidad |
|---|---|
| Python 3.10 o posterior | API, repositorio y procesos Worker. |
| FastAPI y Uvicorn | API HTTP actual y servidor local. |
| Psycopg 3 y `psycopg_pool` | Acceso SQL síncrono y pools de conexión a CockroachDB. |
| CockroachDB | Persistencia SQL distribuida del proyecto. |
| Docker Compose | Ejecución local de los tres contenedores CockroachDB. |
| Biblioteca estándar de Python | Cliente HTTP del Worker y obtención de métricas del sistema. |
| `unittest` | Pruebas de integración del repositorio de liderazgo. |

`backend/requirements.txt` declara FastAPI, Uvicorn y Psycopg. PyTorch
forma parte del diseño previsto para el entrenamiento, pero no está
incluido en las dependencias actuales ni se ejecuta en este repositorio.
El frontend previsto utilizará HTML, CSS y JavaScript sin framework,
pero todavía no contiene archivos.

## Estructura del proyecto

```text
.
├── backend/
│   ├── main.py
│   ├── requirements.txt
│   ├── api/
│   │   ├── nodes.py
│   │   ├── datasets.py
│   │   ├── jobs.py
│   │   ├── cluster.py
│   │   └── training.py
│   ├── coordinator/
│   │   ├── scheduler.py
│   │   ├── partitioner.py
│   │   └── aggregator.py
│   ├── database/
│   │   ├── connection.py
│   │   ├── repository.py
│   │   ├── schema.sql
│   │   └── migrations/
│   │       └── 001_create_cluster_leadership.sql
│   ├── storage/
│   │   └── datasets/
│   ├── tests/
│   │   └── test_leadership_repository.py
│   └── workers/
│       ├── worker.py
│       └── training_engine.py
├── docs/
│   └── distributed-coordination-architecture.md
├── frontend/                 # Directorio vacío; UI no implementada
└── docker-compose.yml
```

`backend/api/nodes.py` implementa el registro y el endpoint HTTP de
Heartbeat. Los módulos de `datasets`, `jobs`, `cluster` y `training`,
así como `coordinator/` y `workers/training_engine.py`, son marcadores
sin esa funcionalidad. `backend/database/schema.sql` es un placeholder;
el esquema integral de referencia está en este README. La migración de
liderazgo sirve para aplicar esa tabla de forma aditiva a una base ya
existente; no sustituye el DDL de instalación limpia de abajo.

## Modelo de datos

El siguiente DDL es **un único esquema lógico integral de referencia
para una instalación nueva**. Incluye las columnas de coordinación e
intentos previstas aunque la lógica que las utilizará todavía no esté
implementada. No es una migración ni debe ejecutarse indiscriminadamente
sobre una base existente: crea las tablas desde cero y fallará si ya
existen. La inserción inicial del liderazgo usa `ON CONFLICT DO NOTHING`
para no reiniciar una fila existente.

En una instalación nueva, crear primero la base `distributed_ml` y
ejecutar este bloque dentro de esa base:

```sql
CREATE TABLE nodes (
    node_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    hostname STRING NOT NULL,
    ip_address STRING,
    status STRING NOT NULL DEFAULT 'offline',
    cpu_cores INT4,
    ram_mb INT4,
    last_heartbeat TIMESTAMPTZ,
    cpu_available_cores FLOAT8,
    ram_available_mb INT4,
    active_tasks INT4,
    metrics_updated_at TIMESTAMPTZ,
    coordination_url STRING,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_node_status
        CHECK (status IN ('online', 'offline', 'busy'))
);

CREATE TABLE datasets (
    dataset_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name STRING NOT NULL,
    file_path STRING NOT NULL,
    file_size_bytes INT8,
    row_count INT8,
    column_count INT4,
    file_format STRING NOT NULL DEFAULT 'csv',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE jobs (
    job_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id UUID NOT NULL REFERENCES datasets(dataset_id),
    algorithm STRING NOT NULL,
    target_column STRING NOT NULL,
    status STRING NOT NULL DEFAULT 'pending',
    parameters JSONB NOT NULL DEFAULT '{}'::JSONB,
    requested_workers INT4 NOT NULL,
    actual_workers INT4,
    train_ratio FLOAT8,
    seed INT8,
    error_message STRING,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    CONSTRAINT chk_job_status
        CHECK (status IN (
            'pending', 'running', 'completed', 'failed', 'cancelled'
        )),
    CONSTRAINT chk_requested_workers CHECK (requested_workers > 0),
    CONSTRAINT chk_train_ratio
        CHECK (train_ratio IS NULL OR (train_ratio > 0 AND train_ratio < 1))
);

CREATE TABLE tasks (
    task_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    node_id UUID REFERENCES nodes(node_id) ON DELETE SET NULL,
    partition_index INT4 NOT NULL,
    rows_assigned INT8,
    status STRING NOT NULL DEFAULT 'pending',
    attempts INT4 NOT NULL DEFAULT 0,
    attempt_id UUID,
    lease_until TIMESTAMPTZ,
    leader_epoch INT8,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error_message STRING,
    CONSTRAINT uq_job_partition UNIQUE (job_id, partition_index),
    CONSTRAINT chk_task_status
        CHECK (status IN (
            'pending', 'assigned', 'running', 'completed', 'failed'
        )),
    CONSTRAINT chk_task_attempts CHECK (attempts >= 0),
    CONSTRAINT chk_partition_index CHECK (partition_index >= 0)
);

CREATE TABLE results (
    result_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL UNIQUE REFERENCES jobs(job_id) ON DELETE CASCADE,
    accuracy FLOAT8,
    loss FLOAT8,
    duration_seconds FLOAT8 NOT NULL,
    metrics JSONB NOT NULL DEFAULT '{}'::JSONB,
    model_path STRING,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_result_duration CHECK (duration_seconds >= 0)
);

CREATE TABLE cluster_leadership (
    cluster_id STRING PRIMARY KEY,
    leader_node_id UUID REFERENCES nodes(node_id),
    epoch INT8 NOT NULL DEFAULT 0
        CONSTRAINT chk_leadership_epoch CHECK (epoch >= 0),
    lease_until TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_nodes_status ON nodes(status);
CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_dataset ON jobs(dataset_id);
CREATE INDEX idx_tasks_job ON tasks(job_id);
CREATE INDEX idx_tasks_node ON tasks(node_id);
CREATE INDEX idx_tasks_status_lease ON tasks(status, lease_until);

INSERT INTO cluster_leadership (
    cluster_id,
    leader_node_id,
    epoch,
    lease_until,
    updated_at
)
VALUES ('default', NULL, 0, now() - INTERVAL '1 second', now())
ON CONFLICT (cluster_id) DO NOTHING;
```

`task_attempts` no forma parte del modelo principal. Puede añadirse más
adelante si se necesita conservar historial detallado de cada intento;
por ahora, el esquema previsto mantiene el intento vigente en `tasks`.

## Instalación y configuración

### Requisitos

- Windows con PowerShell, Python 3.10 o posterior y Docker Desktop con
  Docker Compose v2.
- Acceso a Docker Hub durante la descarga inicial de la imagen
  CockroachDB.

### Preparar Python

Desde la raíz del repositorio:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r backend\requirements.txt
```

### Iniciar CockroachDB y crear el esquema

Desde la raíz del repositorio, para una instalación nueva con volúmenes
vacíos:

```powershell
docker compose up -d
docker exec distributed-ml-db1 cockroach init --insecure --host=localhost:26257
docker exec distributed-ml-db1 cockroach sql --insecure --database=defaultdb --execute "CREATE DATABASE IF NOT EXISTS distributed_ml;"
docker compose ps
```

Después, abrir una sesión SQL y pegar el único bloque DDL de la sección
Modelo de datos:

```powershell
docker exec -it distributed-ml-db1 cockroach sql --insecure --database=distributed_ml
```

La imagen Compose usa `--insecure` y está destinada al entorno local.
El Admin UI de DB1 se publica en <http://127.0.0.1:8080>; DB2 y DB3 usan
los puertos 8081 y 8082. La inicialización de cluster y el DDL de arriba
son únicamente para una instalación vacía. No ejecutar `cockroach init`
ni el bloque `CREATE TABLE` sobre un clúster existente.

### Configurar la conexión

El Backend y cada Worker leen `DATABASE_URL`. En PowerShell, establecer
esta variable en cada terminal que vaya a ejecutar un proceso:

```powershell
$env:DATABASE_URL = "postgresql://root@127.0.0.1:26257/distributed_ml?sslmode=disable"
```

Si no se define, `backend/database/connection.py` utiliza esa misma URL
local por defecto. El Worker utiliza un único endpoint configurado; no
descubre ni conmuta automáticamente entre DB1, DB2 y DB3.

## Ejecución

### Iniciar FastAPI

En una terminal, desde `backend/`:

```powershell
..\.venv\Scripts\python.exe -m uvicorn main:app --host 127.0.0.1 --port 8000
```

La API queda disponible en
<http://127.0.0.1:8000>; FastAPI también publica la documentación
interactiva en <http://127.0.0.1:8000/docs>.

### Iniciar Workers

En cada una de tres terminales independientes, desde `backend/` y con
`DATABASE_URL` configurada:

```powershell
..\.venv\Scripts\python.exe -m workers.worker --backend-url http://127.0.0.1:8000
```

Cada proceso crea un registro nuevo; no hay deduplicación por hostname.
El Worker requiere FastAPI para ese registro inicial. Una vez que tiene
su `node_id`, publica directamente en CockroachDB. El intervalo se puede
ajustar con `--heartbeat-interval`; el valor predeterminado es 3
segundos.

### Frontend

No hay archivos de aplicación en `frontend/`, por lo que actualmente no
existe un comando para iniciar una interfaz.

## Verificación y consulta

Endpoints disponibles:

| Método y ruta | Resultado |
|---|---|
| `GET /health` | Comprueba que FastAPI y su conexión a CockroachDB están disponibles; responde HTTP 503 si la DB no está disponible. |
| `POST /nodes` | Registra un Worker y devuelve el registro con su `node_id`. |
| `POST /nodes/{node_id}/heartbeat` | Marca ese Worker `online` y actualiza su Heartbeat por la API. El cliente Worker actual no usa esta ruta después del registro. |

Comprobar el backend desde PowerShell:

```powershell
Invoke-RestMethod -Uri "http://127.0.0.1:8000/health"
```

Comprobar nodos y liderazgo mediante SQL:

```powershell
docker exec distributed-ml-db1 cockroach sql --insecure --database=distributed_ml --execute "SELECT node_id, hostname, status, last_heartbeat, cpu_available_cores, ram_available_mb, active_tasks, metrics_updated_at FROM nodes ORDER BY created_at DESC;"
docker exec distributed-ml-db1 cockroach sql --insecure --database=distributed_ml --execute "SELECT cluster_id, leader_node_id, epoch, lease_until, updated_at FROM cluster_leadership;"
```

Las pruebas de integración existentes verifican las operaciones de
liderazgo contra CockroachDB. Requieren al menos dos Workers registrados
y que no haya un lease `default` vigente. Desde `backend/`:

```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

## Alcance y limitaciones

- El registro inicial depende de FastAPI; las publicaciones posteriores
  de Heartbeat y métricas dependen directamente del endpoint de DB
  configurado.
- No hay frontend ni API operativa para datasets, Jobs, Tasks o resultados.
- Los módulos de coordinación y entrenamiento no implementan Scheduler,
  elección automática, comunicación Worker-a-Worker, ejecución ML,
  agregación ni recuperación de tareas.
- El repositorio ofrece primitivas de lease persistente, pero ningún
  Worker las renueva o utiliza automáticamente. La existencia de la tabla
  no equivale a failover de coordinación.
- No hay monitor que marque automáticamente `offline` a un Worker cuyo
  Heartbeat haya vencido.
- Los tres contenedores DB y los Workers locales comparten un host; no se
  ha verificado el factor de replicación efectivo ni el quórum. No se
  garantiza tolerancia a la pérdida del host.
- El modo `--insecure` es para desarrollo local, no para despliegues
  expuestos a redes no confiables.
- `task_attempts` es opcional y no pertenece al esquema principal.
