# Arquitectura objetivo de coordinación distribuida

**Estado:** lease persistente y publicación directa de Heartbeat/métricas
implementados; elección automática y coordinación distribuida siguen
pendientes.
**Alcance:** elección segura de coordinador, selección por capacidad,
comunicación y recuperación. Las secciones identifican qué primitivas
ya existen y cuáles siguen siendo propuestas.

## 1. Resumen

El objetivo es que los Workers puedan continuar la coordinación del
entrenamiento cuando FastAPI deja de estar disponible. Se recomienda que
el rol de coordinador activo pertenezca siempre a un Worker elegido con
un lease persistente en CockroachDB. FastAPI puede seguir siendo la API
pública habitual, pero como ingreso/proxy al coordinador activo, no como
autoridad exclusiva del estado distribuido.

Esta separación permite que una caída de FastAPI no obligue a elegir un
nuevo líder si el Worker coordinador sigue activo. Si también falla el
coordinador Worker, los demás Workers eligen otro mediante el lease.
Asumir el rol interno de coordinador no equivale por sí solo a reemplazar
la API pública ni a garantizar que la interfaz pueda enviar nuevas
solicitudes.

La tabla de liderazgo, las operaciones transaccionales básicas y la
publicación directa de Heartbeat/métricas ya están implementadas como
fundamento. Esta entrega no ejecuta una elección automática ni selecciona
candidatos por capacidad.

## 2. Estado actual verificado en el código

- `backend/main.py` inicializa el pool de base de datos, expone `/health`
  y registra el router de nodos.
- `POST /nodes` crea una fila nueva y devuelve el `node_id` generado por
  CockroachDB. Cada arranque del Worker crea otra fila; no existe
  deduplicación por hostname.
- `POST /nodes/{node_id}/heartbeat` actualiza `last_heartbeat`, establece
  `status='online'` y, mediante `repository.update_node`, actualiza
  `updated_at`.
- `backend/workers/worker.py` es un cliente Python invocable. Al arrancar
  obtiene hostname, IP y capacidad total, se registra contra la URL de
  FastAPI recibida por parámetro y conserva el UUID en memoria. Después
  inicializa su propio pool de CockroachDB y publica Heartbeat y métricas
  cada 3 segundos por defecto. Si la DB no está disponible, conserva el
  proceso y reintenta con el pool existente. No ejecuta tareas ni aloja
  una API de coordinación.
- El Worker mide CPU y RAM disponibles mediante el sistema operativo;
  informa `active_tasks=0` porque no hay ejecutor. Un valor que no se
  pueda medir de forma fiable queda en `NULL`. Los Heartbeats no provocan
  detección de expiración ni cambian automáticamente el estado a
  `offline`.
- `backend/database/schema.sql` define la tabla de lease y la fila inicial
  `default` como parte del esquema completo para una base nueva.
- `repository.py` dispone de operaciones para las cinco tablas de
  aplicación y para inicializar/consultar, adquirir, renovar, liberar y
  validar el lease. Las operaciones que cambian estado reintentan
  `SerializationFailure` hasta cinco intentos, con backoff acotado.
- La adquisición usa una actualización condicional por `cluster_id`,
  epoch observada y lease expirado/sin propietario. Un conflicto que no
  modifica una fila devuelve `None`; los errores de persistencia se
  propagan. La validación independiente no está integrada aún en las
  futuras mutaciones de Jobs/Tasks como fencing.
- El Worker líder ejecuta el Scheduler y el ciclo de entrenamiento
  (`coordinator/scheduler.py` y `coordinator/training_runner.py`). Los
  cambios de estado de Jobs y Tasks pasan por `update_job_as_leader` y
  `update_job_tasks_as_leader`, que validan propietario y época en la
  misma transacción (sección 5.3). FastAPI es necesario para el registro inicial; las
  publicaciones posteriores del Worker no dependen de FastAPI.
- La versión y el DDL desplegado se comprobaron en CockroachDB
  v26.2.2: las cinco tablas originales, la tabla de liderazgo y las
  cuatro columnas métricas de `nodes` se verificaron en el clúster real.
  También se verificó `num_replicas = 3`, con cada rango de
  `distributed_ml` replicado en los nodos 1, 2 y 3 (escenario D).

Por tanto, el estado `online` significa que el Worker publicó
recientemente a CockroachDB; no acredita que pueda coordinar o ejecutar
tareas.

## 3. Componentes propuestos

1. **CockroachDB compartida:** fuente de verdad para membresía, métricas,
   lease del coordinador y estado de Jobs/Tasks.
2. **Worker:** proceso con rol de ejecución y, si obtiene el lease, rol
   de coordinación. Cada Worker publica un endpoint HTTP interno
   direccionable y puede conectarse a CockroachDB sin pasar por FastAPI.
3. **Coordinador Worker:** el propietario vigente del lease procesa
   solicitudes de coordinación, asigna trabajo y supervisa su estado.
   Los Workers que no son líderes descubren su identidad desde la fila
   compartida y se comunican directamente con su endpoint.
4. **FastAPI:** puede seguir ofreciendo la API externa habitual y
   reenviar solicitudes al coordinador actual. No es el almacén de
   autoridad ni un requisito para la comunicación Worker-a-Worker.
5. **Ingreso estable para clientes:** un gateway redundante, detrás de
   un nombre estable, observa el lease y enruta hacia el coordinador
   Worker. El navegador no debe conectarse directamente a CockroachDB.

Cada Worker debe anunciar una URL alcanzable por sus pares. En una
instalación de varios hosts no puede usarse `127.0.0.1` como dirección
del coordinador. En la demo local, los procesos necesitarán puertos
distintos aunque compartan el mismo host.

El registro inicial continúa usando `POST /nodes`. Una vez obtenido el
`node_id`, cada Worker inicializa un pool propio y escribe Heartbeat y
métricas directamente en CockroachDB. Así, la caída posterior de FastAPI
no interrumpe esas publicaciones. El endpoint HTTP de Heartbeat se
conserva para clientes externos, pero no lo usa el proceso Worker.

## 4. Métricas, elegibilidad y selección por capacidad

### 4.1 Señales

Los valores `nodes.cpu_cores` y `nodes.ram_mb` representan capacidad
total reportada al iniciar el proceso. El Worker ahora publica
periódicamente estos campos de disponibilidad:

- CPU total y CPU disponible, expresadas en núcleos efectivos.
- RAM total y RAM disponible, en MB.
- Cantidad de Tasks activas (y, posteriormente, capacidad de Tasks
  concurrentes).
- Marca temporal de medición y URL anunciada del Worker.
- Estado operativo y último Heartbeat.

CPU disponible se estima con una muestra de contadores del sistema
operativo (Windows o Linux); en Linux la capacidad se limita a la cuota
cgroup cuando está disponible. RAM disponible viene de las métricas del
SO y se limita a memoria cgroup cuando está disponible. En otros sistemas
se usa la carga promedio si está soportada; si no, se guarda `NULL`.
Estas son estimaciones a nivel de sistema, no consumo exclusivo del
proceso Worker. La disponibilidad de Tasks proviene del ejecutor local,
no del SO; actualmente se informa cero.

El Worker publica una muestra cada 3 segundos por defecto, junto al
Heartbeat. Considerar obsoletos Heartbeat o métricas con más de 10
segundos es un umbral de diseño futuro; aún no existe un monitor que
marque nodos `offline`.

### 4.2 Elegibilidad

Antes de puntuar, descartar un candidato si se cumple cualquiera de
estas condiciones:

- Su estado persistido es `offline`, o su Heartbeat o sus métricas están
  obsoletos (más de 10 s como umbral inicial).
- No puede conectarse al quórum de CockroachDB o no puede servir su URL
  de coordinación.
- Tiene menos de 0.5 núcleos efectivos disponibles o menos de 512 MB de
  RAM disponible para el coordinador.

Los mínimos de CPU/RAM son umbrales iniciales configurables que deben
validarse con el workload real. `busy` no excluye automáticamente a un
Worker: puede ser candidato si aún supera los mínimos y tiene recursos
disponibles. No se debe inferir la disponibilidad a partir únicamente
del número de núcleos o del estado `online`.

### 4.3 Puntuación determinista

En un mismo snapshot de CockroachDB, para cada candidato elegible:

- `Ccap = min(cpu_total / max_cpu_total, 1)`
- `Rcap = min(ram_total / max_ram_total, 1)`
- `Cfree = min(cpu_disponible / cpu_total, 1)`
- `Rfree = min(ram_disponible / ram_total, 1)`
- `H = 1 / (1 + tasks_activas)`
- `F = max(0, 1 - edad_heartbeat / 10 s)`

Si un máximo del conjunto es cero, el candidato no es elegible por esa
dimensión. Calcular:

```text
score = 0.20*Ccap + 0.15*Rcap + 0.25*Cfree
      + 0.20*Rfree + 0.10*H + 0.10*F
```

La capacidad total representa el equipo; los cocientes disponibles
representan recursos libres; `H` representa carga de trabajo y `F`
vigencia/conectividad. Las condiciones de estado y elegibilidad son
filtros, no sustitutos de la puntuación.

Para que los procesos comparen resultados idénticos, calcular el score
en punto fijo (por ejemplo, 0–10 000), usando el mismo snapshot, los
mismos pesos/versiones de fórmula y aritmética entera. Ordenar por score
descendente y, en empate, por `node_id` ascendente. Todos los Workers
deben puntuar el mismo conjunto de filas con el reloj de CockroachDB;
no deben usar sus relojes locales para ordenar candidatos.

El score se vuelve a calcular solo al iniciar una elección. Si el
coordinador actual renueva su lease y sigue elegible, no se lo reemplaza
por una pequeña mejora de score. Esto evita oscilaciones. La puntuación
ordena candidatos, pero no otorga autoridad.

## 5. Elección segura y prevención de split-brain

### 5.1 Lease persistente

**Implementado:** `backend/database/schema.sql` crea la tabla
`cluster_leadership`, con `leader_node_id` nullable y FK a
`nodes(node_id)`, epoch no negativo, lease y timestamp. También inserta
el registro `default` con propietario nulo, epoch 0 y lease expirado
mediante `ON CONFLICT DO NOTHING`, sin sobrescribir un liderazgo vigente.

La tabla singleton contiene:

- `cluster_id` como clave primaria (una fila, por ejemplo `default`).
- `leader_node_id` como UUID del Worker propietario.
- `epoch` como entero monotónico, inicialmente 0.
- `lease_until` como timestamp de expiración.
- `updated_at` para diagnóstico.

`initialize_leadership()` también inicializa la fila de forma
idempotente; `get_leadership()` devuelve esos campos y `is_valid`,
calculado con el reloj de CockroachDB. La duración predeterminada del
lease es 10 segundos y puede pasarse a `acquire_leadership()` o
`renew_leadership()`. La renovación periódica cada 3 segundos es una
cadencia futura, no un proceso implementado. Las decisiones de vigencia
usan el reloj de CockroachDB dentro de consultas transaccionales, no los
relojes de Workers.

### 5.2 Flujo de adquisición

1. Los Workers leen un snapshot compartido de lease, nodos y métricas.
2. Si el lease vigente pertenece a un Worker saludable, todos lo
   reconocen; no se inicia una elección por la sola caída de FastAPI.
3. Si no hay propietario vigente, cada Worker calcula el mismo orden
   determinista de candidatos. El primero intenta adquirir el lease;
   los siguientes tienen ventanas de intento acotadas y ordenadas.
4. La adquisición es una actualización condicional y atómica de la fila:
   solo puede prosperar si el lease anterior expiró/no existe y el
   `epoch` observado sigue vigente. La transacción establece el nuevo
   propietario, incrementa `epoch` y fija `lease_until`.
5. Si el candidato preferido no adquiere autoridad dentro de su ventana,
   el siguiente candidato lo intenta. Una adquisición concurrente
   fallida obliga a releer la fila; no se considera liderazgo local.
6. El propietario renueva condicionalmente solo si coinciden
   `leader_node_id` y `epoch`. Si no puede renovar, detiene
   inmediatamente su trabajo de coordinación.

`acquire_leadership()` y `renew_leadership()` implementan las
actualizaciones condicionales y devuelven el registro actualizado o
`None` si no se modificó ninguna fila. `release_leadership()` devuelve
`bool` y libera solo la combinación exacta de clúster, propietario y
epoch. `validate_leadership()` devuelve `bool` y comprueba identidad,
epoch y lease vigente según el reloj de CockroachDB. Estas son
primitivas de repositorio: ningún Worker las invoca automáticamente.

Los errores de serialización/transacción de CockroachDB requieren
reintentar la transacción completa con backoff acotado. Si la base no
ofrece quórum, ningún nodo debe inventar una autoridad local: el sistema
falla cerrado hasta recuperar acceso consistente.

La integración real se probó en CockroachDB v26.2.2. Dos conexiones
independientes compitieron con la misma época observada y exactamente
una adquirió el lease; también se probaron renovación válida, expiración,
liberación, epoch monotónico y rechazo de propietario antiguo.

### 5.3 Fencing de líderes antiguos

Cada liderazgo tiene una época (`epoch`) mayor que cualquier época
anterior. Toda mutación de coordinación (asignaciones, cambios de estado,
aceptación de resultados) debe validar, dentro de la misma transacción,
que el propietario y la época siguen vigentes y que el lease no expiró.
La fila de liderazgo actúa como fencing token; las asignaciones de Tasks
guardan la época con la que se crearon.

CockroachDB serializa las transacciones concurrentes. Si un líder viejo
intenta escribir después de que otro incrementó la época, su condición
no coincide o su transacción debe abortar/reintentarse y fallar la
validación. No basta con que el proceso viejo crea que aún es líder.

La fila singleton y la escritura condicional impiden que dos candidatos
obtengan la misma época. El uso obligatorio de época en toda escritura
impide que un líder anterior siga modificando estado después de que se
otorgó una época nueva. La seguridad depende de que todas las escrituras
de coordinación pasen por estas transacciones.

## 6. Descubrimiento, comunicación y API pública

### Workers entre sí

Cada Worker consulta la fila `cluster_leadership` en CockroachDB al
iniciar y periódicamente (por ejemplo, cada segundo). Usa
`leader_node_id` para buscar la URL anunciada en la fila de `nodes` y
envía solicitudes HTTP al líder. Si cambia `epoch` o propietario,
descarta el destino anterior y vuelve a resolver la URL. La fila DB,
no una dirección `localhost` ni memoria local, es la fuente de verdad.

El coordinador Worker debe exponer un servidor HTTP interno en todos los
nodos. El proceso elegido activa las operaciones de coordinación; los
demás pueden ejecutar Tasks y consultar al líder. Un Worker que detecta
una época nueva no asume liderazgo por sí mismo: solo el propietario del
lease puede coordinar.

### Clientes y continuidad de la API

FastAPI puede continuar siendo el punto de entrada habitual y reenviar
operaciones al líder. Para conservar acceso cuando ese proceso cae,
los clientes necesitan un nombre estable independiente de FastAPI (por
ejemplo, un gateway/reverse proxy redundante detrás de DNS/VIP) que
observe el lease y enrute a la URL del líder. El navegador no debe
conectarse directamente a CockroachDB.

Ese gateway debe tener más de una instancia o depender de un balanceador
redundante; un proxy único sería otro punto único de fallo. Las
solicitudes públicas que se quieran mantener durante la caída de
FastAPI también deben estar implementadas detrás del coordinador Worker.
Si solo se implementa el protocolo interno, el entrenamiento en curso
podría continuar, pero la interfaz/API pública seguiría indisponible.
Asumir coordinación no equivale a reemplazar toda la API pública.

La URL actual `http://127.0.0.1:8000` es solo un destino de desarrollo
para el cliente Worker; no descubre al líder y deja de servir si FastAPI
cae. La configuración de red futura debe anunciar direcciones alcanzables
entre hosts y proveer a los Workers varios destinos de CockroachDB o un
endpoint estable de conexión. Para despliegues remotos también se
requieren autenticación y TLS; el modo `--insecure` del Compose actual
es únicamente local.

## 7. Asignación y recuperación de Tasks

La selección de líder y la asignación de cómputo son decisiones
separadas. El coordinador reserva CPU/RAM para la coordinación y no
asigna automáticamente la mayor carga al Worker líder. Implementado: el
Scheduler pone al coordinador último en la preferencia, así que solo
participa si faltan Workers y nunca recibe más Tasks que los demás. Las Tasks se
asignan usando los recursos libres, la carga activa y la capacidad
concurrente de cada Worker.

Como diseño objetivo:

1. El coordinador toma una Task pendiente mediante una transacción
   condicional, registra `node_id`, incrementa `attempts` y crea un
   `attempt_id` único, un `lease_until` y el `leader_epoch`.
2. El Worker ejecuta el intento y responde con el `attempt_id`.
3. Solo se acepta el resultado si ese intento sigue siendo el vigente y
   la actualización condicional de la Task tiene éxito.
4. Si el lease de la Task expira, el coordinador puede volver a
   asignarla. Un resultado tardío del intento anterior se rechaza.
5. Una Task lógica completada se incorpora una sola vez a la agregación.
   `UNIQUE(job_id, partition_index)` identifica la partición; la fila
   final única de `results` no sustituye la idempotencia por intento.

Esto ofrece procesamiento al menos una vez con aceptación idempotente,
no una garantía general de ejecución exactly-once.

**Implementado (Tarea 18).** Las reasignaciones usan
`reassign_task_as_leader`, que valida líder y época, el intento vigente y
el estado en la misma transacción. Cada Task admite 3 intentos (el
primero y 2 reintentos). Dentro de una ronda, los gradientes válidos se
conservan y solo se reintentan las Tasks fallidas, con los mismos pesos y
la misma época. La reasignación no se dispara por el vencimiento del
lease de la Task, sino por la falla observada y confirmada por Heartbeat.

## 8. Escenarios de fallo

### A — Falla un Worker ejecutor

El coordinador observa Heartbeats vencidos (10 s iniciales), marca el
nodo `offline` y deja expirar los leases de sus Tasks. Reasigna solo los
intentos recuperables; el token del intento evita que una respuesta
tardía sobrescriba al intento vigente.

**Implementado y probado:** al fallar las Tasks de un Worker en una
ronda, el coordinador espera a que su Heartbeat venza (unos 8 s en la
prueba), lo marca `offline` y reparte sus Tasks entre los Workers vivos
como intento 2. El Job terminó con el mismo resultado que el
entrenamiento local.

### B — Falla el Worker coordinador

Los Workers siguen leyendo CockroachDB. Al vencer el lease calculan la
clasificación compartida y adquieren el siguiente lease mediante la
actualización condicional. Después publican la nueva época y URL. La
operación continúa cuando hay quórum de DB, al menos un Worker elegible
y comunicación con los nodos ejecutores.

**Implementado y probado:** tras matar al líder, otro Worker adquirió el
lease 10 s después con una época nueva, retomó el Job desde los pesos
iniciales (no hay checkpoint) y, en su primera ronda, reasignó las Tasks
del líder caído. El Job terminó con el mismo resultado que el
entrenamiento local.

### C — Regresa el coordinador anterior

El proceso viejo lee la época vigente y se convierte en seguidor. Sus
escrituras con la época anterior son rechazadas. No retoma el liderazgo
por tener más capacidad ni por conservar estado local.

### D — Falla un nodo CockroachDB

Con tres réplicas efectivas por rango y quórum Raft de dos, un clúster de
tres nodos puede tolerar la pérdida de un nodo; no tolera perder dos
nodos del mismo quórum. Sin quórum, no se elige ni renueva líder.

Verificado en CockroachDB v26.2.2 con FastAPI y tres Workers en marcha:

- **Réplicas:** `num_replicas = 3` y cada rango de `distributed_ml` tiene
  réplicas en los nodos 1, 2 y 3.
- **Cae un nodo (DB1, DB2 o DB3):** sin interrupción. El pool usa una URL
  con los tres hosts, por lo que perder DB1 ya no deja a la aplicación sin
  base de datos. Antes del cambio, con una URL de un solo host, perder DB1
  detenía todos los Workers aunque DB2 y DB3 conservaban el quórum.
- **Caen dos nodos:** el nodo restante acepta conexiones, pero las consultas
  esperan un quórum que no existe. Con `statement_timeout` de 5 s, los
  Workers reciben `QueryCanceled` y reintentan, el líder revoca su
  autoridad al vencer el lease y `/health` responde 503 porque lee la tabla
  `nodes`. Antes del cambio, los Workers quedaban bloqueados sin registrar
  errores y `/health` respondía 200 con `SELECT 1`.
- **Caen los tres:** conexiones rechazadas en los tres hosts, reintentos sin
  terminar procesos y `/health` en 503.
- **Recuperación:** al volver el quórum, los procesos se reconectan solos en
  unos 10–20 s, sin pérdida de datos. Si la caída superó 10 s, el primer
  Worker que escribe puede marcar `offline` a los demás hasta su siguiente
  heartbeat.

### E — Falla el host físico

El Compose actual ejecuta los tres nodos DB en un único host, y los
Workers de esta tarea también son procesos locales. La caída del host,
del almacenamiento o del Docker Engine puede afectar a todos. Esta
topología no demuestra tolerancia a fallos físicos; se requieren hosts,
red y dominios de fallo independientes.

## 9. Estado del esquema y cambios futuros

`cluster_leadership` y las columnas métricas
`nodes.cpu_available_cores`, `nodes.ram_available_mb`,
`nodes.active_tasks` y `nodes.metrics_updated_at` ya están implementadas.
Los siguientes cambios siguen siendo propuestas y no se aplicaron:

1. **`nodes`:** añadir `coordination_url` para el endpoint interno futuro.
2. **`tasks`:** conservar `attempts`, `node_id` y restricciones actuales;
   añadir `attempt_id`, `lease_until` y `leader_epoch`. Evaluar un índice
   por estado/vencimiento para recuperar Tasks expiradas.
3. **`task_attempts` opcional:** solo si se requiere historial detallado;
   no es requisito para el MVP con el token vigente en `tasks`. El DDL
   objetivo se describe en el README y no está aplicado.

Antes de implementar estas propuestas debe verificarse el DDL desplegado,
las versiones de CockroachDB, las políticas de transacción/reintento y el
modelo de permisos. Ninguna de ellas está implementada.

## 10. Diferencias y etapas futuras

| Área | Estado actual | Objetivo propuesto |
|---|---|---|
| Ingreso | FastAPI necesario para registro inicial; métricas directas a DB | Ingreso estable enruta a FastAPI o al líder Worker |
| Coordinación | No hay Scheduler operativo; existe lease persistente | Un Worker coordina bajo el lease |
| Elección | Primitivas acquire/renew/release/validate implementadas | Elección automática y ordenada de candidatos |
| Capacidad | CPU/RAM total al registrar; métricas disponibles publicadas a DB | Selección por capacidad y umbrales de elegibilidad |
| Descubrimiento | URL de FastAPI configurada | Fila de líder + URL anunciada del Worker |
| Recuperación | No hay reasignación operativa | Leases de Tasks, intentos y actualizaciones idempotentes |
| Fallos físicos | Procesos locales en un host | Requiere hosts y dominios de fallo independientes |

Orden sugerido de implementación:

1. Verificar DDL/réplicas reales y acordar topología, direcciones,
   permisos DB y límites de recursos.
2. Definir/migrar la URL interna del Worker y los campos de leases de
   Tasks; validar permisos y política de reintentos.
3. Implementar el servidor HTTP interno del Worker, descubrimiento del
   líder y cambio de destino por época.
4. Implementar elección automática por capacidad, ventanas de fallback
   y fencing de todas las mutaciones usando las primitivas ya disponibles.
5. Implementar asignación y leases de Tasks, fencing de intentos,
   reintentos limitados y agregación idempotente.
6. Implementar FastAPI como proxy y un gateway externo redundante para
   la continuidad de la API pública.
7. Probar caídas de procesos, pérdida de un nodo DB, particiones de red
   y persistencia real; luego repetir en hosts físicos independientes.

## 11. Esquema inicial y pruebas de liderazgo

El esquema completo para una base nueva se encuentra en
[`backend/database/schema.sql`](../backend/database/schema.sql). En
PowerShell, desde la raíz del repositorio y con un clúster nuevo iniciado,
copiarlo al contenedor y ejecutarlo con `-f` (pasarlo por stdin desde
PowerShell antepone un BOM y CockroachDB responde `syntax error`):

```powershell
docker cp backend\database\schema.sql distributed-ml-db1:/tmp/schema.sql
docker exec distributed-ml-db1 cockroach sql --insecure --database=distributed_ml -f /tmp/schema.sql
```

El esquema crea todas las tablas e índices desde cero; no ejecutarlo
sobre una base existente. La fila `default` solo se inserta si falta y
no restablece epoch/propietario existentes.
`DATABASE_URL` permite cambiar el destino de las pruebas; sin ella, el
pool usa el host local configurado en `connection.py`.

Ejecutar desde `backend/`:

```powershell
python -m unittest discover -s tests -p "test_*.py" -v
```

Las ocho pruebas de integración se ejecutaron contra la instancia real
CockroachDB v26.2.2. La prueba concurrente usa dos conexiones obtenidas
del pool y verifica un único ganador. Las pruebas requieren al menos dos
Workers registrados y ningún lease `default` vigente; liberan el lease
al finalizar, pero conservan el epoch monotónico. No verifican
elección automática, recuperación de Tasks ni tolerancia a la pérdida de
un nodo DB.

