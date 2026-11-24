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
python -m pip install torch --index-url https://download.pytorch.org/whl/cpu
python -m pip install -r backend/requirements.txt
```

La línea de `torch` instala PyTorch solo para CPU (unos 200 MB). Sin ella, en Linux `pip` descargaría la versión con CUDA, de varios GB, que el proyecto no usa. Al importarlo, PyTorch avisa que no encuentra NumPy; no hace falta instalarlo.

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

**Base nueva:** `backend/database/schema.sql` contiene el esquema completo, incluida la fila `default` de `cluster_leadership`. Copiarlo al contenedor y ejecutarlo con `-f`:

```bash
docker cp backend/database/schema.sql distributed-ml-db1:/tmp/schema.sql
docker exec distributed-ml-db1 cockroach sql --insecure --database=distributed_ml -f /tmp/schema.sql
```

No pasar el archivo por stdin desde PowerShell (`Get-Content ... | docker exec -i ...`): PowerShell antepone un BOM y CockroachDB responde `syntax error` en la primera línea. No ejecutar el esquema sobre una base que ya tenga las tablas.

**Base ya creada:** aplicar `backend/database/migrations/001_make_requested_workers_nullable.sql` antes de usar esta versión. Solo elimina `NOT NULL` de `jobs.requested_workers` y conserva los valores históricos; se puede ejecutar más de una vez:

```bash
docker cp backend/database/migrations/001_make_requested_workers_nullable.sql distributed-ml-db1:/tmp/001.sql
docker exec distributed-ml-db1 cockroach sql --insecure --database=distributed_ml -f /tmp/001.sql
```

Verificar que cada rango tiene tres réplicas repartidas en los tres nodos (cada fila debe mostrar `{1,2,3}`):

```bash
docker compose exec cockroach1 cockroach sql --insecure --database=distributed_ml --execute "SELECT table_name, range_id, replicas, lease_holder FROM [SHOW RANGES FROM DATABASE distributed_ml WITH TABLES, DETAILS] ORDER BY 1;"
```

Una vez aplicado el esquema correspondiente, verificar:

```bash
docker compose exec cockroach1 cockroach sql --insecure --database=distributed_ml --execute "SHOW TABLES;"
```

La instalación solo está lista para continuar cuando las tablas requeridas por la implementación actual existen.

## 6. Configurar la conexión a CockroachDB

FastAPI y los Workers utilizan la variable `DATABASE_URL`.

Si no se define, `backend/database/connection.py` usa esta URL local, que lista los tres nodos:

```bash
export DATABASE_URL="postgresql://root@127.0.0.1:26257,127.0.0.1:26258,127.0.0.1:26259/distributed_ml?sslmode=disable"
```

libpq prueba los hosts en orden: si DB1 cae, FastAPI y los Workers se conectan a DB2 o DB3 sin reiniciarse. Si se define una URL propia, conviene listar los tres hosts. El pool fija además, para cada conexión:

| Ajuste | Valor | Para qué |
|---|---|---|
| `connect_timeout` | 3 s por host | Pasar rápido al siguiente nodo si uno no responde |
| `statement_timeout` | 5 s | Sin quórum, CockroachDB deja las consultas esperando sin límite; el timeout las convierte en un error `QueryCanceled` que los Workers registran antes de reintentar |
| `check` del pool | activado | Descartar conexiones a un nodo caído antes de usarlas |

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
cd backend
python -m workers.worker --backend-url http://127.0.0.1:8000 --node-id-file ../.workers/w1.id
```

Ejecutar el mismo comando en las tres terminales, cambiando el archivo de identidad (`w1.id`, `w2.id`, `w3.id`). Con `--node-id-file` un Worker que se reinicia conserva su `node_id`; sin él, cada arranque registra un Worker nuevo.

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
- Los CSV subidos quedan en `backend/storage/datasets/`, que está en `.gitignore`. Al mover el proyecto hay que copiar esa carpeta junto con los volúmenes de CockroachDB; si no, los registros de `datasets` apuntarán a archivos inexistentes.
- CockroachDB v26 muestra el aviso "No license is installed. Throttling will begin after …": sin licencia limita el rendimiento a partir de esa fecha. Antes de la demo, instalar una licencia gratuita de Cockroach Labs o recrear el clúster.
