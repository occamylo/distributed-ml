# Arquitectura objetivo de coordinación distribuida

**Estado:** lease persistente implementado; elección automática y
coordinación distribuida siguen pendientes.  
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

La tabla de liderazgo y las operaciones transaccionales básicas ya se
implementaron como fundamento. Esta entrega no ejecuta una elección
automática, no selecciona candidatos por capacidad ni cambia el
comportamiento del Worker.

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
  obtiene hostname, IP y recursos básicos, se registra contra la URL de
  FastAPI recibida por parámetro y conserva el UUID en memoria. Envía un
  Heartbeat cada 3 segundos por defecto. No ejecuta tareas, no aloja una
  API de coordinación ni tiene reintentos ante la caída del Backend.
- El Worker actual reporta núcleos lógicos y RAM total al registrarse;
  no reporta CPU/RAM disponibles ni carga de tareas en cada Heartbeat.
  Los Heartbeats no provocan detección de expiración ni cambian
  automáticamente el estado a `offline`.
- `backend/database/migrations/001_create_cluster_leadership.sql`
  crea aditivamente la tabla de lease y la fila inicial `default`.
- `repository.py` dispone de operaciones para las cinco tablas de
  aplicación y para inicializar/consultar, adquirir, renovar, liberar y
  validar el lease. Las operaciones que cambian estado reintentan
  `SerializationFailure` hasta cinco intentos, con backoff acotado.
- La adquisición usa una actualización condicional por `cluster_id`,
  epoch observada y lease expirado/sin propietario. Un conflicto que no
  modifica una fila devuelve `None`; los errores de persistencia se
  propagan. La validación independiente no está integrada aún en las
  futuras mutaciones de Jobs/Tasks como fencing.
- Los módulos de Scheduler y entrenamiento aún no implementan esas
  responsabilidades. El cliente Worker depende actualmente de FastAPI
  para registro y Heartbeat.
- La versión y el DDL desplegado se comprobaron en CockroachDB
  v26.2.2: las cinco tablas existentes coinciden con el DDL de
  `README.md`, y la nueva tabla de liderazgo se creó con la migración.
  No se verificaron el factor de replicación efectivo ni el quórum.

Por tanto, el estado `online` solo significa que un Heartbeat reciente
fue aceptado por el endpoint actual; no acredita que el Worker pueda
coordinar o ejecutar tareas.

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

Para que el clúster pueda operar si FastAPI cae, el registro y las
actualizaciones de Heartbeat/recursos de los Workers también deberán
poder persistirse directamente usando la base compartida. Esto requiere
inicializar un pool en cada Worker o extraer la capa de persistencia a
un módulo compartido. El endpoint HTTP actual puede mantenerse para
clientes externos, pero no puede ser el único camino de registro o
Heartbeat en la arquitectura objetivo.

## 4. Métricas, elegibilidad y selección por capacidad

### 4.1 Señales

Los valores actuales `nodes.cpu_cores` y `nodes.ram_mb` representan
capacidad reportada al iniciar el proceso. No miden recursos disponibles
ni límites efectivos del contenedor. La propuesta requiere reportar
periódicamente:

- CPU total y CPU disponible, expresadas en núcleos efectivos.
- RAM total y RAM disponible, en MB.
- Cantidad de Tasks activas (y, posteriormente, capacidad de Tasks
  concurrentes).
- Marca temporal de medición y URL anunciada del Worker.
- Estado operativo y último Heartbeat.

El Worker puede obtener CPU disponible con muestras de contadores del
sistema operativo en una ventana de varios segundos, ajustadas a la
cuota de CPU del contenedor. En Linux se deben considerar los contadores
del sistema y límites cgroup; en Windows, contadores del sistema
operativo. La RAM disponible debe venir de la API/métrica de memoria del
SO y estar limitada por cgroup cuando aplique. La disponibilidad de
Tasks proviene del ejecutor local, no del SO.

Como punto inicial, enviar una muestra cada 3 segundos, junto al
Heartbeat, y considerar obsoletos Heartbeat o métricas con más de 10
segundos. Son valores configurables para la primera implementación, no
garantías temporales. El estado actual solo reporta capacidad total y
cada Heartbeat pasa por FastAPI.

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

**Implementado:** la migración
`backend/database/migrations/001_create_cluster_leadership.sql` crea la
tabla `cluster_leadership`, con `leader_node_id` nullable y FK a
`nodes(node_id)`, epoch no negativo, lease y timestamp. La migración
inserta el registro `default` con propietario nulo, epoch 0 y lease
expirado mediante `ON CONFLICT DO NOTHING`, por lo que volver a
aplicarla no sobrescribe un liderazgo vigente.

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
asigna automáticamente la mayor carga al Worker líder. Las Tasks se
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
no una garantía general de ejecución exactly-once. Las reglas de
reintento, límite de intentos y resultados parciales deben definirse
antes de implementar el Scheduler.

## 8. Escenarios de fallo

### A — Falla un Worker ejecutor

El coordinador observa Heartbeats vencidos (10 s iniciales), marca el
nodo `offline` y deja expirar los leases de sus Tasks. Reasigna solo los
intentos recuperables; el token del intento evita que una respuesta
tardía sobrescriba al intento vigente.

### B — Falla el Worker coordinador

Los Workers siguen leyendo CockroachDB. Al vencer el lease calculan la
clasificación compartida y adquieren el siguiente lease mediante la
actualización condicional. Después publican la nueva época y URL. La
operación continúa cuando hay quórum de DB, al menos un Worker elegible
y comunicación con los nodos ejecutores.

### C — Regresa el coordinador anterior

El proceso viejo lee la época vigente y se convierte en seguidor. Sus
escrituras con la época anterior son rechazadas. No retoma el liderazgo
por tener más capacidad ni por conservar estado local.

### D — Falla un nodo CockroachDB

Con tres réplicas efectivas por rango y quórum Raft de dos, un clúster de
tres nodos puede tolerar la pérdida de un nodo; no tolera perder dos
nodos del mismo quórum. Tres contenedores no prueban por sí solos que
cada rango tenga tres réplicas ni que estas estén distribuidas. La
topología/factor de replicación real debe verificarse antes de afirmar
esa continuidad. Sin quórum, no se elige ni renueva líder.

### E — Falla el host físico

El Compose actual ejecuta los tres nodos DB en un único host, y los
Workers de esta tarea también son procesos locales. La caída del host,
del almacenamiento o del Docker Engine puede afectar a todos. Esta
topología no demuestra tolerancia a fallos físicos; se requieren hosts,
red y dominios de fallo independientes.

## 9. Cambios de persistencia propuestos (no aplicados)

La tabla `cluster_leadership` ya está implementada. Los siguientes
cambios siguen siendo propuestas y no se aplicaron:

1. **`nodes`:** conservar `cpu_cores` y `ram_mb` como capacidad total;
   añadir disponibilidad dinámica (`cpu_available_cores`,
   `ram_available_mb`, `active_tasks`, `metrics_updated_at`) y
   `coordination_url`.
2. **`tasks`:** conservar `attempts`, `node_id` y restricciones actuales;
   añadir `attempt_id`, `lease_until` y `leader_epoch`. Evaluar un índice
   por estado/vencimiento para recuperar Tasks expiradas.
3. **Identidad del intento:** una tabla histórica de intentos solo es
   necesaria si se requiere auditoría detallada; no es requisito para el
   primer MVP con un token vigente en `tasks`.

Antes de definir migraciones debe verificarse el DDL desplegado, las
versiones de CockroachDB, las políticas de transacción/reintento y el
modelo de permisos. Ninguna de estas propuestas está implementada.

## 10. Diferencias y etapas futuras

| Área | Estado actual | Objetivo propuesto |
|---|---|---|
| Ingreso | FastAPI es necesario para los Workers | Ingreso estable enruta a FastAPI o al líder Worker |
| Coordinación | No hay Scheduler operativo; existe lease persistente | Un Worker coordina bajo el lease |
| Elección | Primitivas acquire/renew/release/validate implementadas | Elección automática y ordenada de candidatos |
| Capacidad | CPU/RAM total al registrar | Capacidad y disponibilidad medidas por Heartbeat |
| Descubrimiento | URL de FastAPI configurada | Fila de líder + URL anunciada del Worker |
| Recuperación | No hay reasignación operativa | Leases de Tasks, intentos y actualizaciones idempotentes |
| Fallos físicos | Procesos locales en un host | Requiere hosts y dominios de fallo independientes |

Orden sugerido de implementación:

1. Verificar DDL/réplicas reales y acordar topología, direcciones,
   permisos DB y límites de recursos.
2. Definir/migrar métricas y URL de Worker; probar su frescura y
   obtención desde el SO.
3. Implementar reporte periódico de métricas OS y Heartbeat directo a
   CockroachDB, sin depender de FastAPI.
4. Implementar el servidor HTTP interno del Worker, descubrimiento del
   líder y cambio de destino por época.
5. Implementar elección automática por capacidad, ventanas de fallback
   y fencing de todas las mutaciones usando las primitivas ya disponibles.
6. Implementar asignación y leases de Tasks, fencing de intentos,
   reintentos limitados y agregación idempotente.
7. Implementar FastAPI como proxy y un gateway externo redundante para
   la continuidad de la API pública.
8. Probar caídas de procesos, pérdida de un nodo DB, particiones de red
   y persistencia real; luego repetir en hosts físicos independientes.

## 11. Migración y pruebas de liderazgo

La migración aditiva se encuentra en
[`backend/database/migrations/001_create_cluster_leadership.sql`](../backend/database/migrations/001_create_cluster_leadership.sql).
En PowerShell, desde la raíz del repositorio y con Compose iniciado:

```powershell
Get-Content -Raw .\backend\database\migrations\001_create_cluster_leadership.sql | docker exec -i distributed-ml-db1 cockroach sql --insecure --database=distributed_ml
```

El script crea la tabla si no existe e inserta la fila `default` solo si
falta; no elimina datos ni restablece epoch/propietario existentes.
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

