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
- `requested_workers`
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
- `actual_workers`, `train_ratio`, `seed`, `error_message`, `started_at` y `completed_at` pueden ser nulos.
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
- `requested_workers`
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