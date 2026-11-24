# Contratos de acceso a datos — Distributed ML

**Versión:** 1.0 (Propuesta)  
**Estado:** Contrato base para implementación  
**Tecnologías:** Python, Psycopg 3 y CockroachDB

---

## 1. Objetivo

Este documento define de manera estricta el comportamiento del módulo `backend/database/repository.py`, encargado de realizar las operaciones de acceso a datos de la aplicación Distributed ML.

Su propósito es establecer un contrato común entre el backend, el Coordinator, el Scheduler y los Workers, evitando implementaciones ambiguas, comportamientos inesperados y complejidad innecesaria.

El repositorio debe ser:

- **Simple:** implementar únicamente operaciones necesarias para el MVP.
- **Predecible:** cada función debe tener un comportamiento de entrada y salida definido.
- **Consistente:** todas las operaciones deben seguir las mismas reglas de retorno, actualización y manejo de errores.
- **Responsable:** únicamente debe encargarse de la persistencia y recuperación de información.
- **Acotado:** no debe implementar lógica de entrenamiento, coordinación, planificación ni distribución de tareas.

---

## 2. Responsabilidad del Repository

El Repository será la única capa encargada de interactuar directamente con CockroachDB para las operaciones de persistencia de la aplicación.

### Responsabilidades

- Ejecutar consultas SQL.
- Insertar registros.
- Consultar registros.
- Actualizar registros.
- Eliminar registros cuando la operación esté autorizada.
- Devolver los datos recuperados o modificados.
- Propagar errores originados en la base de datos.
- Utilizar el pool de conexiones existente en `database/connection.py`.

### Responsabilidades excluidas

El Repository NO debe:

- Implementar endpoints HTTP ni FastAPI.
- Gestionar la disponibilidad de los Workers.
- Enviar tareas a Workers.
- Distribuir particiones.
- Crear particiones de datasets.
- Ejecutar algoritmos de Machine Learning.
- Gestionar épocas de entrenamiento.
- Ejecutar reintentos de tareas.
- Decidir cuándo un Job debe iniciar o finalizar.
- Calcular métricas de entrenamiento.
- Validar reglas de negocio del sistema.
- Crear, modificar o eliminar tablas.
- Modificar el esquema de la base de datos.
- Implementar mecanismos de recuperación de Jobs.
- Gestionar archivos CSV ni manipular su contenido.

Estas responsabilidades pertenecen a las demás capas del sistema.

---

## 3. Reglas generales de las operaciones

### 3.1. Identificadores

- Los identificadores de las entidades son UUID.
- Los UUID son generados por la base de datos al insertar registros.
- Las funciones de creación no deben exigir que el cliente genere el UUID.
- Las consultas y actualizaciones deben utilizar el UUID correspondiente como criterio de identificación.
- Los identificadores recibidos deben corresponder al tipo UUID esperado.

### 3.2. Formato de retorno

Todas las funciones deben utilizar exclusivamente los siguientes formatos:

| Operación | Retorno |
|---|---|
| Consulta de un registro | `dict` o `None` |
| Consulta de múltiples registros | `list[dict]` |
| Creación | `dict` del registro creado |
| Actualización | `dict` actualizado o `None` |
| Eliminación | `bool` |

Los registros devueltos deben incluir las columnas de la tabla que corresponda, incluyendo identificadores y campos temporales.

No se deben devolver objetos de conexión, cursores, clases ORM, modelos de dominio ni formatos personalizados innecesarios.

### 3.3. Registro inexistente

Cuando una consulta individual no encuentra el registro solicitado, devuelve `None`.

Cuando una actualización utiliza un UUID inexistente, devuelve `None`.

Cuando una consulta de múltiples registros no encuentra coincidencias, devuelve una lista vacía `[]`.

Cuando una eliminación intenta eliminar un registro inexistente, devuelve `False`.

Estos comportamientos no deben considerarse errores de ejecución.

### 3.4. Orden de las consultas

Todas las consultas que devuelvan múltiples registros deben tener un orden determinista.

- Listados generales: `created_at ASC`, seguido del identificador correspondiente `ASC` como desempate.
- Tasks de un Job: `partition_index ASC`, seguido de `task_id ASC`.

No se implementará paginación en esta versión.

### 3.5. Actualizaciones parciales

Las funciones de actualización deben recibir únicamente los campos que se desean modificar.

- Los campos omitidos deben conservar su valor actual.
- No se deben sobrescribir campos omitidos con `NULL`.
- Se debe actualizar exclusivamente la información indicada.
- Si no se reciben campos para modificar, no se ejecutará un `UPDATE` innecesario.
- Si el registro existe y no hay cambios solicitados, se puede devolver su estado actual.

La actualización debe identificar el registro mediante su UUID y devolver el registro actualizado.

### 3.6. Manejo de errores

El Repository debe propagar los errores reales de base de datos.

No debe:

- Ocultar errores.
- Convertir excepciones SQL en `None`.
- Convertir errores SQL en listas vacías.
- Retornar `False` ante errores de SQL como si una eliminación simplemente no hubiera encontrado el registro.
- Implementar reintentos automáticos de consultas o transacciones.

Las excepciones serán gestionadas por las capas superiores según corresponda.

### 3.7. Transacciones

Las operaciones de este módulo serán sencillas y atómicas.

Cada operación individual debe ejecutarse utilizando la gestión de conexiones y transacciones del pool existente.

No se implementarán transacciones que coordinen múltiples Jobs, Workers o iteraciones de entrenamiento desde el Repository.

Las operaciones que posteriormente requieran atomicidad entre varios registros deberán ser gestionadas desde la capa responsable del flujo correspondiente.

---

## 4. Contrato de `nodes`

La tabla `nodes` representa los Workers de cómputo disponibles para ejecutar tareas.

No representa los tres nodos físicos simulados de CockroachDB.

### Operaciones permitidas

#### `create_node(...)`

**Propósito:** Registrar un nuevo Worker.

Campos de entrada:

- `hostname` — obligatorio.
- `ip_address` — opcional.
- `status` — opcional, por defecto `offline`.
- `cpu_cores` — opcional.
- `ram_mb` — opcional.
- `last_heartbeat` — opcional.

Reglas:

- El `node_id` será generado por la base de datos.
- `created_at` y `updated_at` serán generados por la base de datos.
- Devuelve el registro completo creado.

#### `get_node(node_id)`

Obtiene un Worker por su UUID.

- Si existe, devuelve `dict`.
- Si no existe, devuelve `None`.

#### `list_nodes()`

Devuelve todos los Workers registrados.

- Orden: `created_at ASC`, `node_id ASC`.
- No aplica filtros adicionales ni paginación.

#### `update_node(node_id, fields)`

Permite actualizar únicamente los siguientes campos:

- `hostname`
- `ip_address`
- `status`
- `cpu_cores`
- `ram_mb`
- `last_heartbeat`

Reglas:

- `updated_at` debe actualizarse con `now()` en toda modificación efectiva del registro.
- `last_heartbeat` solo cambia cuando se incluye explícitamente en la actualización.
- Modificar el estado no debe modificar automáticamente `last_heartbeat`.
- No se puede modificar `node_id`, `created_at` ni `updated_at` directamente.

#### `delete_node(node_id)`

**No se implementará.**

Los Workers deben conservarse en el registro y cambiar su estado a `offline` cuando dejen de estar disponibles.

---

## 5. Contrato de `datasets`

La tabla `datasets` almacena metadatos de los archivos utilizados en los entrenamientos.

El contenido del CSV se almacena en el sistema de archivos local, no en CockroachDB.

### Operaciones permitidas

#### `create_dataset(...)`

Campos de entrada:

- `name`
- `file_path`
- `file_size_bytes`
- `row_count`
- `column_count`
- `file_format`

Reglas:

- `dataset_id` será generado por la base de datos.
- `created_at` será generado por la base de datos.
- `file_format` tendrá el valor predeterminado `csv` si no se especifica.
- Devuelve el registro completo creado.

#### `get_dataset(dataset_id)`

Obtiene un Dataset por su UUID.

- Devuelve `dict` si existe.
- Devuelve `None` si no existe.

#### `list_datasets()`

Devuelve todos los Datasets registrados.

- Orden: `created_at ASC`, `dataset_id ASC`.
- No aplica paginación.

#### `delete_dataset(dataset_id)`

Elimina un Dataset únicamente si la base de datos permite la operación.

Reglas:

- Devuelve `True` si se eliminó el registro.
- Devuelve `False` si el registro no existía.
- Si el Dataset está referenciado por uno o más Jobs, debe propagarse el error de integridad referencial de CockroachDB.
- No debe eliminar Jobs asociados.
- No debe eliminar archivos del sistema de almacenamiento.
- No debe implementar eliminación en cascada.

La eliminación física del archivo, si posteriormente se requiere, será responsabilidad de la capa de gestión de archivos.

---

## 6. Contrato de `jobs`

La tabla `jobs` representa solicitudes de entrenamiento y almacena su configuración y estado de ejecución.

### Operaciones permitidas

#### `create_job(...)`

Campos de entrada:

- `dataset_id`
- `algorithm`
- `target_column`
- `status`
- `parameters`
- `actual_workers`
- `train_ratio`
- `seed`
- `error_message`
- `started_at`
- `completed_at`

Reglas:

- `job_id` será generado por la base de datos.
- `created_at` será generado por la base de datos.
- `status` tendrá como valor predeterminado `pending`.
- `parameters` tendrá como valor predeterminado un objeto JSON vacío.
- `requested_workers`, `actual_workers`, `train_ratio`, `seed`, `error_message`, `started_at` y `completed_at` pueden ser nulos.
- Los Jobs nuevos dejan `requested_workers` en `NULL`; los valores históricos se conservan y no limitan la planificación.
- Devuelve el registro completo creado.

#### `get_job(job_id)`

Obtiene un Job por su UUID.

- Devuelve `dict` si existe.
- Devuelve `None` si no existe.

#### `list_jobs()`

Devuelve todos los Jobs registrados.

- Orden: `created_at ASC`, `job_id ASC`.
- No aplica paginación.

#### `update_job(job_id, fields)`

Permite modificar exclusivamente:

- `dataset_id`
- `algorithm`
- `target_column`
- `status`
- `parameters`
- `actual_workers`
- `train_ratio`
- `seed`
- `error_message`
- `started_at`
- `completed_at`

Reglas:

- No permite modificar `job_id` ni `created_at`.
- Los campos omitidos no se modifican.
- No debe cambiar automáticamente otros campos como consecuencia de modificar el estado.
- Devuelve el registro actualizado o `None` si el Job no existe.

La lógica de transición de estados y la decisión de cuándo actualizar `started_at` o `completed_at` pertenece al Coordinator o a la capa de gestión del Job, no al Repository.

#### `delete_job(job_id)`

**No se implementará en este MVP.**

Aunque el esquema permite eliminar Jobs y aplicar `ON DELETE CASCADE` sobre Tasks y Results, el sistema no necesita eliminación de Jobs desde el Repository durante esta etapa.

---

## 7. Contrato de `tasks`

La tabla `tasks` representa particiones lógicas de un Job.

Una Task no representa una época de entrenamiento ni un intento individual persistido. Es la unidad lógica asociada a una partición del dataset.

### Operaciones permitidas

#### `create_task(...)`

Campos de entrada:

- `job_id`
- `node_id`
- `partition_index`
- `rows_assigned`
- `status`
- `attempts`
- `started_at`
- `completed_at`
- `error_message`

Reglas:

- `task_id` será generado por la base de datos.
- `status` tendrá como valor predeterminado `pending`.
- `attempts` tendrá como valor predeterminado `0`.
- `node_id` puede ser nulo.
- Devuelve el registro completo creado.
- La restricción `UNIQUE(job_id, partition_index)` debe ser respetada.
- Si se intenta crear una partición duplicada para un mismo Job, se propaga el error de base de datos.

#### `get_task(task_id)`

Obtiene una Task por su UUID.

- Devuelve `dict` si existe.
- Devuelve `None` si no existe.

#### `list_tasks_by_job(job_id)`

Devuelve todas las Tasks asociadas a un Job.

- Orden: `partition_index ASC`, `task_id ASC`.
- Si no hay coincidencias, devuelve `[]`.

#### `update_task(task_id, fields)`

Permite modificar exclusivamente:

- `job_id`
- `node_id`
- `partition_index`
- `rows_assigned`
- `status`
- `attempts`
- `started_at`
- `completed_at`
- `error_message`

Reglas:

- No permite modificar `task_id`.
- Los campos omitidos no se modifican.
- No debe incrementar `attempts` automáticamente.
- No debe asignar Workers automáticamente.
- No debe cambiar estados de forma automática.
- Devuelve el registro actualizado o `None` si no existe.

El Repository no decide cuándo una tarea se asigna, ejecuta, reintenta, completa o falla.

#### `delete_task(task_id)`

**No se implementará.**

La eliminación de Tasks se realizará únicamente como consecuencia de la eliminación de su Job mediante las reglas de integridad referencial, si en el futuro se habilita esa operación.

---

## 8. Contrato de `results`

La tabla `results` almacena el resultado final de un entrenamiento.

Cada Job puede tener como máximo un resultado, garantizado por `UNIQUE(job_id)`.

### Operaciones permitidas

#### `create_result(...)`

Campos de entrada:

- `job_id`
- `accuracy`
- `loss`
- `duration_seconds`
- `metrics`
- `model_path`

Reglas:

- `result_id` será generado por la base de datos.
- `created_at` será generado por la base de datos.
- `metrics` tendrá como valor predeterminado un objeto JSON vacío.
- `model_path` es opcional.
- `accuracy` y `loss` pueden ser nulos.
- `duration_seconds` es obligatorio.
- Devuelve el registro completo creado.
- Si el Job ya tiene un resultado, se propaga el error de restricción de unicidad.
- No se debe sobrescribir un resultado existente.

#### `get_result(result_id)`

Obtiene un resultado por su UUID.

- Devuelve `dict` si existe.
- Devuelve `None` si no existe.

#### `get_result_by_job(job_id)`

Obtiene el resultado asociado a un Job.

- Devuelve `dict` si existe.
- Devuelve `None` si el Job aún no tiene resultado.

#### `update_result(...)`

**No se implementará.**

Los resultados se consideran registros finales e inmutables en el MVP. Si un entrenamiento necesita producir un resultado diferente, debe manejarse mediante un Job nuevo, no sobrescribiendo el resultado anterior.

#### `delete_result(...)`

**No se implementará.**

La eliminación de Results ocurrirá únicamente por las reglas de integridad referencial al eliminar un Job, si esa funcionalidad se habilita posteriormente.

---

## 9. Manejo de estados

El Repository debe respetar los estados definidos mediante restricciones en el esquema.

| Entidad | Estados permitidos |
|---|---|
| Node | `online`, `offline`, `busy` |
| Job | `pending`, `running`, `completed`, `failed`, `cancelled` |
| Task | `pending`, `assigned`, `running`, `completed`, `failed` |

El Repository no debe implementar máquinas de estados, validación de transiciones ni automatismos de estado.

Su responsabilidad es persistir el estado recibido y dejar que CockroachDB aplique las restricciones definidas por el esquema.

Si se intenta persistir un estado inválido, se propagará el error de la base de datos.

---

## 10. Manejo de JSONB

Las columnas `jobs.parameters` y `results.metrics` son de tipo JSONB.

Reglas:

- Deben recibirse como estructuras Python compatibles con JSON, principalmente `dict`.
- Deben almacenarse utilizando los mecanismos de adaptación de Psycopg 3.
- No se debe concatenar JSON manualmente en consultas SQL.
- No se deben utilizar cadenas JSON construidas manualmente para insertar datos.
- Al recuperar la información, deben devolverse en su representación Python correspondiente.

No se implementará un sistema de validación de esquemas JSON ni estructuras complejas de configuración.

---

## 11. Reglas de SQL y seguridad

Todas las consultas deben utilizar parámetros SQL.

Está prohibido construir consultas mediante interpolación de valores proporcionados por el usuario.

Los nombres de las columnas permitidas para las actualizaciones deben estar definidos de manera explícita por cada entidad; no se deben aceptar nombres de columnas arbitrarios.

Las consultas de actualización deben modificar únicamente los campos permitidos por sus respectivos contratos.

El Repository debe utilizar el pool de conexiones ya implementado en `database/connection.py`, sin crear pools adicionales ni conexiones independientes.

---

## 12. Resumen de operaciones

| Tabla | Crear | Consultar individual | Listar | Actualizar | Eliminar |
|---|---|---|---|---|
| `nodes` | Sí | Sí | Sí | Sí | No |
| `datasets` | Sí | Sí | Sí | No | Sí |
| `jobs` | Sí | Sí | Sí | Sí | No |
| `tasks` | Sí | Sí | Por Job | Sí | No |
| `results` | Sí | Sí | Por Job | No | No |

---

## 13. Exclusiones expresas del MVP

Para evitar que el Repository se convierta en una capa de lógica de negocio, no se incluirán:

- ORM.
- Modelos de dominio adicionales.
- Patrón Unit of Work.
- Repositorios separados por tabla.
- Paginación.
- Filtros dinámicos.
- Búsquedas avanzadas.
- Reintentos automáticos ante fallos SQL.
- Caché.
- Auditoría de modificaciones.
- Historial de estados.
- Historial de iteraciones.
- Historial de intentos de Tasks.
- Persistencia de subtareas por época.
- Eliminación de archivos.
- Recuperación de Jobs.
- Transacciones distribuidas entre componentes de aplicación.
- Nuevas tablas.

---

## 14. Consideraciones sobre iteraciones, épocas y reintentos

El esquema actual define una Task por partición lógica de un Job mediante la restricción:

`UNIQUE(job_id, partition_index)`

Por lo tanto, una partición no tendrá una fila independiente por época o por intento.

El Repository debe persistir únicamente la información de la Task existente, utilizando `attempts` como contador y los campos de estado disponibles.

El Coordinator será responsable de mantener el contexto de la iteración y del intento durante la ejecución, y de evitar mezclar resultados pertenecientes a diferentes iteraciones.

No se introducirá persistencia de historial detallado ni se modificarán las tablas para representar épocas o subtareas.

---

## 15. Criterios de aceptación

El módulo `repository.py` se considerará implementado cuando:

- [ ] Todas las operaciones definidas en este documento estén disponibles.
- [ ] Cada operación utilice el pool de conexiones existente.
- [ ] Los registros creados devuelvan sus valores completos.
- [ ] Las consultas individuales devuelvan `None` cuando no exista el registro.
- [ ] Las consultas múltiples devuelvan listas, incluso cuando no existan coincidencias.
- [ ] Las actualizaciones sean parciales y no sobrescriban campos omitidos.
- [ ] Los campos protegidos no puedan modificarse.
- [ ] Los listados tengan orden determinista.
- [ ] La eliminación de Datasets respete las FK.
- [ ] Los Results no puedan sobrescribirse mediante operaciones de actualización.
- [ ] Los errores SQL se propaguen correctamente.
- [ ] No se hayan agregado tablas, columnas, archivos o dependencias.
- [ ] No se haya implementado lógica de negocio dentro del Repository.
- [ ] No se hayan modificado archivos fuera del alcance autorizado.

---

## 16. Principio final

El Repository debe ser una capa de persistencia deliberadamente sencilla.

Su comportamiento debe poder entenderse leyendo las funciones disponibles, sin necesitar conocer el funcionamiento interno del Coordinator, Scheduler, Workers o algoritmos de Machine Learning.

**El Repository guarda, consulta, actualiza y elimina únicamente cuando está autorizado. No decide cómo funciona el sistema distribuido.**

## 17. Contratos propuestos para Job, Task y Scheduler — Tarea 9.1

**Estado: propuesta para revisión; no implica que estas operaciones estén implementadas.**

El código actual dispone de operaciones básicas de persistencia para Jobs y Tasks. `backend/workers/coordination.py` inicia un servicio HTTP interno y un ciclo de elección/renovación/liberación mediante el lease; esto no implementa planificación de Jobs ni consumo de Tasks. `backend/coordinator/scheduler.py`, `backend/coordinator/partitioner.py` y `backend/api/jobs.py` siguen siendo marcadores. El Worker publica Heartbeat y métricas, pero no consume Tasks. El contrato de abajo define el límite mínimo para una implementación posterior, sin incluir entrenamiento ni recuperación avanzada.

### 17.1. Contrato de Job

Un Job representa una solicitud de procesamiento asociada a un Dataset. Para planificarlo se necesitan `job_id`, `dataset_id` y `status`. `algorithm`, `target_column`, `parameters`, `train_ratio` y `seed` describen la ejecución futura; el Scheduler los conserva, pero no los interpreta para ejecutar entrenamiento.

- Solo un Job `pending` puede ser tomado para planificación.
- `actual_workers` representa el número de Workers distintos que recibieron al menos una Task del Job; no se calcula a partir de Workers solicitados ni seleccionados sin asignación.
- El Job pasa a `running` cuando se confirma la primera asignación de Task. `started_at` se establece en esa transición.
- `completed`, `failed` y `cancelled` son estados existentes en el esquema de referencia. Sus transiciones y la relación con la agregación o cancelación no forman parte de esta tarea.
- El Scheduler obtiene dinámicamente todos los Workers elegibles disponibles y crea las Tasks según esa cantidad. Si no hay ninguno, el Job permanece `pending` y no se crean Tasks ni asignaciones.
- `requested_workers` se conserva como campo nullable para compatibilidad con Jobs históricos; no se requiere ni se usa para la planificación.

Un Job debe referenciar un Dataset existente. La lectura del archivo y la determinación de sus filas o particiones no corresponden al contrato del Job.

### 17.2. Contrato de Task

Una Task es una unidad persistente de trabajo asociada a una partición lógica de un Job. `UNIQUE(job_id, partition_index)` identifica una sola Task por partición. `partition_index` debe respetar la restricción no negativa del esquema de referencia; `rows_assigned` expresa cuántas filas contiene la partición cuando ese dato se conoce.

Estados de la tabla de referencia:

| Estado | Significado propuesto |
|---|---|
| `pending` | Task en la cola, todavía sin Worker asignado. |
| `assigned` | Task reservada para el Worker indicado por `node_id`. |
| `running` | El Worker confirmó que inició el intento asignado. |
| `completed` | El intento terminó correctamente. |
| `failed` | El intento terminó con error; el detalle puede guardarse en `error_message`. |

Transiciones mínimas propuestas: `pending → assigned → running → completed|failed`. La asignación debe establecer `node_id`, crear un `attempt_id` nuevo, incrementar `attempts` una sola vez y guardar `lease_until` y `leader_epoch`. `started_at` se establece al confirmar el inicio; `completed_at`, al terminar el intento. Los campos de intento identifican y delimitan la asignación vigente; no representan un historial de intentos.

El vencimiento de `lease_until` no autoriza por sí solo a reasignar una Task en esta etapa. La política de recuperación, reintentos y transiciones desde una Task vencida queda fuera del alcance y debe definirse posteriormente. El ejecutor no debe aceptar como vigente un intento distinto del `attempt_id` asignado.

### 17.3. Criterios mínimos de elegibilidad de Workers

Los datos disponibles en `nodes` para planificar son `status`, `last_heartbeat`, `cpu_available_cores`, `ram_available_mb`, `active_tasks` y `metrics_updated_at`. El Worker actual publica las métricas de disponibilidad y fija `active_tasks` en cero porque aún no ejecuta Tasks.

Los criterios de elegibilidad acordados son:

- Exigir que `status` sea `online`.
- Exigir que `last_heartbeat` y `metrics_updated_at` tengan una antigüedad máxima de 10 segundos, evaluada con el reloj de CockroachDB.
- Exigir que `cpu_available_cores` sea finita y mayor que cero y que `ram_available_mb` sea mayor que cero. Los valores totales no sustituyen a los disponibles.
- Exigir que `active_tasks` sea un entero no negativo. Se usa únicamente para ordenar y priorizar Workers con menor carga; no es un límite de concurrencia.

No se establece un máximo de Tasks concurrentes por Worker en esta etapa. Se seleccionan todos los Workers elegibles disponibles; no se define un algoritmo de puntuación o balanceo avanzado.

La detección de desconexiones marca `offline` a los Workers cuyo Heartbeat haya vencido, siempre que haya un Worker activo ejecutando el mecanismo de detección. Las métricas vencidas excluyen al Worker de la selección del Scheduler, pero no cambian por sí solas su estado.

### 17.4. Responsabilidades y comunicación del Scheduler

El Scheduler es la lógica del Worker que posee el lease vigente. Su alcance mínimo propuesto es:

1. Consultar Jobs `pending` y los registros de Workers.
2. Filtrar y seleccionar todos los Workers que cumplan los criterios de elegibilidad en el ciclo de planificación.
3. Obtener una descripción de particiones de un Partitioner y crear Tasks persistentes.
4. Asignar Tasks a Workers y actualizar `actual_workers` y el estado del Job de forma consistente.
5. Detener nuevas operaciones de planificación si pierde la autoridad.

El flujo de comunicación propuesto usa CockroachDB como fuente compartida: la API (cuando se implemente) crea un Job `pending`; el Scheduler líder consulta Jobs y Workers; las asignaciones se persisten en `tasks`; el Worker ejecutor consulta o recibe las Tasks asignadas a su `node_id` y reporta su progreso mediante el contrato de intento. La elección del protocolo de consulta/entrega y reconocimiento del Worker sigue pendiente: el código actual no tiene un consumidor de Tasks ni una ruta interna para ello.

Responsabilidades:

- **API:** aceptar solicitudes de Job y persistirlas; no elegir Workers ni asignar Tasks.
- **Scheduler líder:** aplicar la política de elegibilidad, planificar y solicitar las mutaciones persistentes necesarias.
- **Repository:** ejecutar consultas y mutaciones atómicas parametrizadas; no seleccionar Workers ni decidir estados de negocio.
- **Worker ejecutor:** atender únicamente la asignación que le corresponde y reportar el intento vigente. La ejecución del algoritmo y la agregación quedan fuera de esta tarea.
- **Liderazgo:** el Worker coordinador debe operar con el `node_id` y `epoch` vigentes, y dejar de iniciar planificación al perderlos. La propiedad local `is_leader` no sustituye la validación persistente.

### 17.5. Persistencia y protección contra concurrencia

Las operaciones existentes `create_task`, `list_tasks_by_job`, `update_task`, `list_jobs`, `list_nodes` y `validate_leadership` permiten persistencia y consulta básicas, pero no bastan para implementar una cola distribuida segura:

- No hay consulta específica y ordenada de Jobs `pending`.
- `create_task` no recibe `attempt_id`, `lease_until` ni `leader_epoch`.
- `update_task` actualiza por `task_id`; no reclama condicionalmente una Task que siga `pending` ni admite los campos de asignación anteriores.
- No existe una operación atómica para que un solo Worker reclame una Task pendiente.
- No existe una mutación de Job y Tasks que las confirme conjuntamente.
- `validate_leadership` ejecutada por separado de una mutación de cola deja una carrera: el lease puede cambiar entre la validación y la escritura.

La siguiente implementación deberá definir operaciones de Repository para consultar Jobs pendientes, crear el conjunto inicial de Tasks, reclamar una Task pendiente y actualizarla solo si coinciden su estado esperado y el intento vigente. Las mutaciones críticas deben validar `cluster_id`, propietario y `epoch` dentro de la misma transacción que modifica la cola; una comprobación separada no constituye fencing suficiente. La transacción debe también respetar `UNIQUE(job_id, partition_index)` y evitar que dos consumidores asignen la misma Task.

### 17.6. Discrepancias y decisiones previas a la tarea 9.2

- **Correspondencia del esquema:** el DDL de referencia del README incluye `attempt_id`, `lease_until` y `leader_epoch`, pero no se debe asumir que existen en la base física. `schema.sql` no contiene el esquema integral y las funciones actuales del Repository no leen ni escriben esos campos. Antes de usarlos debe confirmarse el esquema desplegado; cualquier migración requiere una decisión separada.
- **Estado documental de liderazgo:** el código actual inicia elección desde el Worker, mientras que el README todavía describe la elección automática y la comunicación Worker-a-Worker como pendientes. Esta sección distingue la capacidad de liderazgo existente de las capacidades de Scheduler/cola aún no implementadas.
- **Particionado:** `partitioner.py` no implementa generación de particiones. Falta definir el número de Tasks por Job, cómo se distribuyen las filas y cómo puede el Worker reconstruir el segmento a partir de `partition_index` y `rows_assigned`, sin añadir campos por suposición.
- **Workers insuficientes:** debe aprobarse si un Job espera a reunir `requested_workers` o puede ejecutarse con menos; la propuesta conservadora está en 17.1.
- **Disponibilidad y capacidad:** los criterios acordados están descritos en 17.3; no se establece un máximo de Tasks concurrentes por Worker en esta etapa.
- **Entrega al ejecutor:** debe definirse el mecanismo de consulta, asignación y reconocimiento de Tasks. Actualmente el Worker solo publica Heartbeat/métricas.
- **Fencing del Repository:** se necesitan operaciones transaccionales que combinen validación del liderazgo y mutación de la cola; las funciones existentes no ofrecen esa atomicidad.

Hasta resolver estos puntos, el Scheduler y la cola descritos en esta sección son contratos propuestos, no capacidades implementadas.
