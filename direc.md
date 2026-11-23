# Directriz — Publicación directa de heartbeat y métricas de Workers en CockroachDB

## 1. Objetivo

Modificar el funcionamiento de los Workers para que, después de registrarse, publiquen periódicamente su heartbeat y sus métricas de capacidad directamente en CockroachDB, sin depender de FastAPI para mantener actualizado su estado.

Esta información servirá posteriormente para evaluar la disponibilidad y capacidad de los Workers durante la selección del coordinador.

**Esta subtarea NO implementa elección automática de coordinador, selección por capacidad ni ejecución distribuida de tareas.**

## 2. Inspección previa obligatoria

Antes de modificar código, revisar:

- `README.md`.
- `docs/distributed-coordination-architecture.md`.
- `backend/workers/worker.py`.
- `backend/database/connection.py`.
- `backend/database/repository.py`.
- `backend/database/schema.sql`.
- `backend/database/migrations/001_create_cluster_leadership.sql`.
- `backend/api/nodes.py`.
- `backend/main.py`.
- `backend/requirements.txt`.

Identificar cómo funciona actualmente el registro de Workers, cómo se establece la conexión con CockroachDB y qué columnas existen realmente en `nodes`.

No asumir que el contenido del README representa necesariamente el estado actual de la base de datos.

## 3. Registro inicial del Worker

Conservar el mecanismo actual de registro:

1. El Worker se registra mediante `POST /nodes`, utilizando el backend existente.
2. Recibe el `node_id` generado por CockroachDB.
3. Conserva ese identificador durante su ejecución.
4. Después del registro, establece su conexión directa con CockroachDB para publicar heartbeat y métricas.

No eliminar ni modificar el endpoint existente de registro.

No crear un segundo mecanismo de registro.

## 4. Publicación directa de heartbeat y métricas

Modificar exclusivamente el proceso Worker para que publique periódicamente la información directamente en CockroachDB.

La publicación debe realizarse cada 3 segundos, manteniendo el intervalo definido en la arquitectura.

Los datos que debe publicar son:

| Campo | Descripción |
|---|---|
| `last_heartbeat` | Fecha y hora del último heartbeat. |
| `status` | Estado actual del Worker (`online` mientras esté operativo). |
| `cpu_available_cores` | Capacidad estimada de CPU disponible. |
| `ram_available_mb` | Memoria RAM disponible en MB. |
| `active_tasks` | Número de tareas que está ejecutando el Worker. |
| `metrics_updated_at` | Fecha y hora de actualización de las métricas. |

Reutilizar los campos existentes `cpu_cores` y `ram_mb` para representar la capacidad total de CPU y RAM.

### Reglas de publicación

- El `node_id` debe corresponder al registro obtenido durante el registro inicial.
- Actualizar únicamente el registro del Worker correspondiente.
- Utilizar consultas parametrizadas.
- Las fechas de actualización deben generarse con el reloj de CockroachDB.
- No sobrescribir campos que no correspondan a esta publicación.
- No inventar valores de capacidad.
- Obtener las métricas mediante mecanismos compatibles con el entorno de ejecución del proyecto.
- No incorporar dependencias nuevas. Utilizar las dependencias existentes o la biblioteca estándar de Python.
- Si alguna métrica no puede obtenerse de forma fiable, documentar la limitación en lugar de enviar un valor ficticio.

Mientras todavía no exista ejecución de tareas, `active_tasks` debe reflejar correctamente que el Worker no está ejecutando tareas.

## 5. Cambios de esquema estrictamente necesarios

Agregar a la tabla `nodes` únicamente los campos que todavía no existan y que sean necesarios para almacenar las métricas:

- `cpu_available_cores`
- `ram_available_mb`
- `active_tasks`
- `metrics_updated_at`

Conservar todos los campos, restricciones e índices existentes.

La modificación debe ser aditiva y compatible con los registros actuales.

**No crear archivos de migración nuevos.**

No modificar ni eliminar la migración de liderazgo ya existente.

No reinicializar la base de datos, borrar tablas, eliminar volúmenes ni reconstruir el clúster.

Para aplicar los cambios necesarios en la base existente, utilizar una operación SQL aditiva y segura, sin crear un nuevo archivo de migración.

Si se necesita actualizar `schema.sql`, hacerlo sin convertir esta tarea en una reconstrucción innecesaria del esquema completo.

## 6. Persistencia y conexión

Reutilizar la infraestructura existente de conexión y repositorio siempre que sea técnicamente viable.

El Worker debe utilizar su propia conexión a CockroachDB, independiente de la conexión HTTP con FastAPI.

La publicación periódica no debe depender de que FastAPI permanezca disponible.

Si la conexión con CockroachDB falla:

- No finalizar inmediatamente el proceso Worker.
- Informar del error.
- Intentar restablecer la conexión y continuar publicando cuando sea posible.
- Evitar crear conexiones nuevas innecesariamente en cada iteración.

No implementar mecanismos avanzados de recuperación ni colas locales de publicaciones pendientes.

## 7. README: documentación del esquema final de la base de datos

Actualizar `README.md` para incluir una sección completa denominada:

**Esquema de datos final del proyecto**

Esta sección debe documentar cómo debe quedar la base de datos lógica al finalizar el proyecto, tomando como referencia:

- `docs/distributed-coordination-architecture.md`.
- Las cinco tablas originales.
- La tabla `cluster_leadership`.
- Los campos de métricas incorporados a `nodes`.
- Los campos previstos para la gestión de intentos y leases de tareas.

Documentar todas las tablas, sus columnas, tipos de datos, claves primarias, claves foráneas, restricciones e índices relevantes.

El esquema objetivo debe contemplar:

### `nodes`
Información de los Workers, incluyendo capacidad total, capacidad disponible, estado, heartbeat, métricas y los campos necesarios para la futura comunicación interna de coordinación.

### `datasets`
Metadatos de los conjuntos de datos registrados.

### `jobs`
Información de los trabajos de entrenamiento, sus parámetros, configuración y estado.

### `tasks`
Particiones de trabajo asignadas a los Workers, incluyendo los campos previstos para intentos, vencimiento del lease y época del coordinador.

### `results`
Resultados y métricas finales del entrenamiento.

### `cluster_leadership`
Propietario actual del liderazgo, época monotónica, vencimiento del lease y fecha de actualización.

Si la arquitectura contempla alguna tabla adicional necesaria para registrar intentos de ejecución, documentarla también, diferenciando si es obligatoria u opcional.

**Importante:** distinguir claramente entre:

1. Esquema que existe actualmente en CockroachDB.
2. Esquema objetivo que debe alcanzar el proyecto al finalizar su implementación.

No presentar columnas o tablas futuras como si ya estuvieran implementadas.

No implementar durante esta subtarea las columnas, tablas o restricciones que pertenezcan a funcionalidades futuras y que no sean necesarias para publicar las métricas actuales.

No duplicar documentación contradictoria. Mantener coherencia con `distributed-coordination-architecture.md`.

## 8. Pruebas y verificaciones obligatorias

Realizar pruebas reales contra el clúster CockroachDB del proyecto.

Verificar:

1. Los tres Workers pueden registrarse mediante el mecanismo existente.
2. Cada Worker obtiene y conserva su propio `node_id`.
3. Los tres Workers publican directamente sus métricas y heartbeat.
4. Los tres registros se actualizan de forma independiente.
5. `last_heartbeat` y `metrics_updated_at` avanzan periódicamente.
6. Las métricas almacenadas corresponden a valores obtenidos del entorno.
7. La publicación directa continúa funcionando si FastAPI deja de estar disponible después del registro inicial.
8. Los Workers no se detienen definitivamente ante una interrupción temporal de la conexión con CockroachDB.
9. Los registros existentes y el mecanismo de liderazgo conservan su integridad.

No basta con probar funciones aisladas o utilizar mocks para afirmar que la integración funciona.

## 9. Exclusiones estrictas

No implementar ni modificar:

- Elección automática del coordinador.
- Selección de coordinador basada en capacidad.
- Cálculo del score de elegibilidad.
- Adquisición, renovación o liberación automática del lease por parte de los Workers.
- Servidor HTTP interno de coordinación.
- Descubrimiento de coordinadores.
- Comunicación entre Workers.
- Asignación o ejecución de tareas.
- Scheduler.
- Particionamiento de datasets.
- Agregación de resultados.
- Entrenamiento de modelos.
- Recuperación de tareas fallidas.
- Gateway redundante o balanceador de carga.
- Cambios innecesarios en FastAPI.
- Cambios en `docker-compose.yml`.
- Nuevas dependencias.
- Nuevos archivos de migración.

No realizar refactorizaciones generales ni modificar archivos que no sean necesarios para cumplir esta directriz.

## 10. Entregable y reporte final

Al terminar, informar:

1. Archivos modificados.
2. Columnas agregadas a `nodes`.
3. Cómo se obtienen las métricas.
4. Cómo establece cada Worker su conexión directa con CockroachDB.
5. Cómo funciona el intervalo de publicación y la reconexión.
6. Qué contiene la documentación del esquema final en el README.
7. Resultados de las pruebas reales con los tres Workers.
8. Verificación de funcionamiento sin FastAPI después del registro.
9. Limitaciones encontradas.
10. Confirmación explícita de que no se implementaron funcionalidades fuera del alcance.

No afirmar que existe elección automática de coordinador ni tolerancia completa a fallos de coordinación.

**Criterio de finalización:** los tres Workers deben poder registrarse, mantener su heartbeat y publicar sus métricas directamente en CockroachDB, independientemente de la disponibilidad posterior de FastAPI. El README debe documentar correctamente tanto el esquema actual como el esquema objetivo final del proyecto.