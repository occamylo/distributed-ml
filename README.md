# Distributed ML

## Documento de arquitectura, flujo funcional, alcance y plan de implementación

**Versión:** 1.0\
**Propósito:** Documentar el comportamiento previsto del proyecto
Distributed ML, su arquitectura, tecnologías, flujo de entrenamiento,
distribución de trabajo, tolerancia a fallos, modelo de datos y
limitaciones explícitas.

------------------------------------------------------------------------

## 1. Objetivo del proyecto

Distributed ML es una aplicación web académica que permite cargar un
dataset CSV, configurar un entrenamiento de Machine Learning y
ejecutarlo mediante un conjunto de nodos de procesamiento (Workers)
coordinados por un Backend.

El proyecto busca demostrar dos conceptos distribuidos complementarios:

1.  **Base de datos distribuida:** almacenamiento persistente de la
    aplicación mediante un clúster CockroachDB de tres nodos, con
    distribución y replicación gestionadas por CockroachDB.
2.  **Clúster de cómputo:** ejecución de tareas de entrenamiento
    repartidas entre varios Workers. El sistema incorpora balanceo de
    carga, supervisión de Workers y reasignación de tareas incompletas
    cuando un Worker deja de estar disponible.

El objetivo es contar con una demostración funcional y acotada que
permita observar distribución, coordinación, persistencia,
disponibilidad de nodos, recuperación ante una falla de Worker y
resultados de entrenamiento. No se busca construir una plataforma de ML
distribuido de producción.

### Estado de implementación y arquitectura objetivo

El alcance descrito en este README es el objetivo del MVP, no una
afirmación de que todas sus funciones ya estén implementadas. El código
actual incluye el pool de CockroachDB, el health check de FastAPI, el
registro/Heartbeat básico de Workers y un cliente Worker invocable como
proceso Python. También incluye la tabla y las operaciones básicas del
lease persistente de liderazgo. El cliente Worker sigue dependiendo de
FastAPI; no ejecuta entrenamiento ni detecta Workers desconectados.

La elección automática del coordinador Worker, la puntuación de
capacidad, la comunicación Worker-a-Worker, la recuperación de Tasks y
un ingreso público tolerante a fallos son propuestas pendientes. El diseño
detallado está en
[docs/distributed-coordination-architecture.md](docs/distributed-coordination-architecture.md).
La conexión, el esquema y las operaciones de lease se verificaron contra
CockroachDB v26.2.2. No se verificaron el factor de replicación efectivo
ni el quórum, ni la integración real del cliente Worker con la tabla
`nodes`.

------------------------------------------------------------------------

## 2. Alcance y límites del MVP

### Alcance funcional previsto del MVP

Esta lista describe el alcance final deseado del MVP; las funciones no
marcadas como implementación actual siguen pendientes.

-   Aplicación web de uso local.
-   Carga y validación de archivos CSV.
-   Almacenamiento local de archivos CSV; la base de datos conserva sus
    metadatos y rutas, no el contenido binario del CSV.
-   Un Backend/API basado en FastAPI como punto de entrada inicial. La
    coordinación completa por Worker es una propuesta posterior.
-   Un clúster CockroachDB de **3 nodos**.
-   Tres Workers ejecutables como procesos Python independientes; el
    Compose actual no declara servicios Worker.
-   Registro y Heartbeat básicos de Workers (implementados); detección
    de desconexión y transición automática a `offline` pendientes.
-   Scheduler con asignación de tareas teniendo en cuenta Workers
    disponibles y carga/capacidad conocida.
-   Cola de tareas, particionamiento del dataset de entrenamiento y
    distribución de tareas.
-   Un algoritmo de ML implementado con PyTorch: Logistic Regression,
    con soporte de clasificación binaria y multiclase (por ejemplo,
    Softmax).
-   Entrenamiento distribuido mediante Data Parallelism: cálculo de
    gradientes locales, agregación ponderada y actualización del modelo
    global.
-   Reintentos limitados y reasignación de tareas incompletas cuando un
    Worker falla, si quedan Workers disponibles.
-   Evaluación con conjunto de prueba (holdout) y persistencia de
    métricas/resultados.
-   Interfaz web sencilla y actualización de estados por polling.
-   Pruebas y demostración de fallos controlados.

### Fuera del alcance del MVP

-   Despliegue en máquinas físicas independientes, cloud o múltiples
    hosts. Inicialmente los contenedores se ejecutan en un solo PC.
-   Worker Agent para que voluntarios conecten sus propios equipos desde
    Internet.
-   La recuperación del Coordinator/Backend no está implementada en el
    MVP centralizado; su arquitectura objetivo se documenta como una
    etapa posterior.
-   Ejecución simultánea de múltiples entrenamientos (MVP utiliza cola y
    ejecuta un Job a la vez).
-   Checkpoints persistentes que permitan reiniciar un entrenamiento
    completo desde el último estado guardado después de una caída del
    Coordinator o reinicio del host.
-   Elasticidad del entrenamiento: agregar o quitar Workers en mitad de
    una iteración de forma transparente. Los Workers disponibles pueden
    asumir tareas pendientes; el grupo de Workers se estabiliza para
    cada Job/iteración según la implementación.
-   Catálogo de múltiples algoritmos; el algoritmo inicial es Logistic
    Regression.
-   GPU, Ray, Ray Train, Kubernetes, gRPC, MinIO, WebSockets,
    Prometheus/Grafana.
-   Autenticación, roles, multiusuario y aislamiento de Jobs por
    propietario.
-   Garantía de que más Workers siempre reduzcan el tiempo de
    entrenamiento.
-   Garantía de recuperación ante cualquier falla simultánea o
    prolongada.
-   Sistema de recuperación de tareas de propósito general con ejecución
    exactamente una vez. La estrategia usa reintentos y protección de
    resultados duplicados a nivel lógico, no una garantía matemática de
    "exactly once".

El alcance debe mantenerse pequeño: la prioridad es que la distribución
de datos y cómputo, el entrenamiento, la persistencia y una recuperación
básica sean demostrables y funcionales.

------------------------------------------------------------------------

## 3. Tecnologías

  -----------------------------------------------------------------------
  Componente              Tecnología              Responsabilidad
  ----------------------- ----------------------- -----------------------
  Frontend                HTML5, CSS3, JavaScript Interfaz web,
                          vanilla                 formularios,
                                                  visualización de estado
                                                  y resultados

  Backend / Coordinator   Python + FastAPI        API, coordinación de
                                                  Jobs, Scheduler,
                                                  control de Workers,
                                                  flujo de entrenamiento

  Base de datos           CockroachDB             Persistencia SQL
                                                  distribuida,
                                                  replicación y
                                                  disponibilidad de datos

  Base de datos ---       Docker + Docker Compose Ejecutar tres nodos DB
  despliegue                                      en el entorno local

  Compute Workers         Python, contenedores    Ejecutar las tareas de
                          Docker                  cálculo asignadas

  ML                      PyTorch                 Logistic Regression,
                                                  cálculo de gradientes y
                                                  evaluación

  Comunicación inicial    HTTP/REST               Coordinator y Workers
                                                  intercambian registro,
                                                  tareas y resultados

  Almacenamiento CSV      Sistema de archivos     Guardar archivos de
                          local del Backend       datasets cargados

  Testing                 pytest                  Pruebas unitarias y de
                                                  integración esenciales
  -----------------------------------------------------------------------

No se utilizarán frameworks frontend como Angular, React o Vue. No se
incorporarán herramientas de orquestación o ML distribuido de alto nivel
como Ray, ya que el propósito es implementar y observar la coordinación
de manera explícita.

------------------------------------------------------------------------

## 4. Arquitectura general

Distributed ML tiene dos clústeres distintos, administrados por el
Backend/Coordinator. No son un único clúster y cumplen funciones
diferentes.

### 4.1 Clúster de base de datos distribuida

Un único clúster CockroachDB compuesto por tres nodos de base de datos:

-   DB Node 1
-   DB Node 2
-   DB Node 3

Los tres nodos forman **una sola base de datos lógica distribuida**, no
tres bases de datos independientes. CockroachDB administra internamente
el particionamiento/rangos, la replicación, el consenso y la
distribución de réplicas.

La configuración debe verificarse en ejecución. Levantar tres
contenedores no garantiza por sí solo que todos los datos tengan tres
réplicas ni que las réplicas estén distribuidas entre los tres nodos. Se
comprobará el factor de replicación y la ubicación real de réplicas. Se
busca una configuración de replicación de tres réplicas (RF=3) cuando el
entorno y la configuración lo permitan.

**Limitación física:** todos los nodos DB se ejecutan inicialmente en un
solo PC. La separación en contenedores permite simular procesos/nodos
distintos, pero comparten el hardware y el host. La caída del PC o del
Docker Engine afecta a los tres nodos.

### 4.2 Clúster de cómputo

Un clúster de Workers que ejecutan tareas de entrenamiento. El valor
inicial es **3 Workers**:

-   Worker 1
-   Worker 2
-   Worker 3

El estado actual permite ejecutar el mismo cliente Worker en procesos
Python independientes. Cada arranque crea su propio registro y conserva
el UUID devuelto mientras vive el proceso. El cliente se inicia
manualmente y usa HTTP hacia FastAPI; el `docker-compose.yml` actual solo
declara nodos CockroachDB, no servicios Worker.

Tres procesos en el mismo PC no son tres dominios de fallo físicos. Una
arquitectura multi-host requerirá direcciones anunciadas y conectividad
entre equipos; la elección de líder y el servidor de coordinación Worker
aún no están implementados.

### 4.3 Coordinator

**Implementado actualmente:** FastAPI inicializa el pool, expone
`/health`, `POST /nodes` y `POST /nodes/{node_id}/heartbeat`. El
Heartbeat actual marca el nodo `online` y actualiza `last_heartbeat`;
no existe expiración que lo cambie a `offline`. Los Workers dependen de
FastAPI para estas operaciones.

**Pendiente:** Scheduler, cola de tareas, asignación, entrenamiento,
agregación y recuperación no están implementados en estos módulos.

**Objetivo propuesto:** FastAPI puede permanecer como ingreso público,
pero los Workers descubren al coordinador activo desde CockroachDB y se
comunican con él directamente. La elección segura por lease y la
continuidad de la API pública se detallan en
[la propuesta de arquitectura](docs/distributed-coordination-architecture.md).

### 4.4 Vista de la implementación actual

``` text
 Worker process 1 ----\
 Worker process 2 -----+-- HTTP --> FastAPI
 Worker process 3 ----/             |  /health
                                    |  POST /nodes
                                    |  POST /nodes/{id}/heartbeat
                                    v
                              Repository / pool
                                    |
                              CockroachDB
```

Este diagrama describe solo el camino de registro/Heartbeat; no implica
que ya exista ejecución de Tasks, Scheduler ni coordinación distribuida.

### 4.5 Lease persistente de liderazgo

Se agregó la migración aditiva
[`001_create_cluster_leadership.sql`](backend/database/migrations/001_create_cluster_leadership.sql)
y operaciones en
[`repository.py`](backend/database/repository.py) para inicializar y
consultar el clúster `default`, adquirir, renovar, liberar y validar un
lease con época. La fila inicial no tiene propietario, tiene `epoch=0` y
un lease expirado.

La duración predeterminada del lease es 10 segundos y se puede
sobrescribir en las operaciones de adquisición/renovación. La cadencia
de 3 segundos es solo la referencia futura para un renovador periódico;
esta tarea no implementa ese proceso ni la elección automática de
Workers. Las consultas de vigencia usan el reloj de CockroachDB; los
conflictos de serialización se reintentan hasta cinco intentos con
backoff acotado.

La migración es idempotente y no restablece un epoch o propietario
existente. Desde PowerShell, con CockroachDB iniciado:

```powershell
Get-Content -Raw .\backend\database\migrations\001_create_cluster_leadership.sql | docker exec -i distributed-ml-db1 cockroach sql --insecure --database=distributed_ml
```

Las pruebas de integración usan la base real y requieren dos Workers
registrados y ningún lease `default` vigente:

```powershell
Set-Location .\backend
python -m unittest discover -s tests -p "test_*.py" -v
```

En CockroachDB v26.2.2 se ejecutaron correctamente las ocho pruebas,
incluida una adquisición concurrente con dos conexiones independientes:
solo una obtuvo el liderazgo. La fila quedó liberada y el epoch no se
reinició.

------------------------------------------------------------------------

## 5. Flujo completo de uso

### Paso 1. Iniciar el sistema

El Compose actual define los tres servicios CockroachDB. FastAPI y los
Workers se ejecutan como procesos separados. Para iniciar cada Worker
desde `backend/`, se usa
`python -m workers.worker --backend-url http://127.0.0.1:8000` en una
terminal independiente; se requieren tres terminales para tres procesos.
La URL debe apuntar al Backend que realmente esté en ejecución.

El proceso FastAPI comprueba su conexión a la base al iniciar. La
integración de esos procesos con una base CockroachDB activa debe
verificarse en el entorno de ejecución.

**Entorno actual:** tres nodos DB configurados por Compose; Workers
locales como procesos Python; todos comparten el mismo host físico.
No se declara una red Docker para los Workers.

### Paso 2. Registro de Workers y Heartbeat

Al arrancar, cada Worker:

1.  Obtiene hostname, IP si está disponible, núcleos y RAM total.
2.  Envía esa información a `POST /nodes`; el Backend crea una fila y
    devuelve el UUID generado por la base de datos.
3.  Conserva el `node_id` durante ese proceso. Un nuevo arranque crea
    otro registro; no hay deduplicación por hostname.
4.  Envía `POST /nodes/{node_id}/heartbeat` cada 3 segundos por defecto.
5.  El endpoint actualiza `last_heartbeat` y establece el estado
    `online`. El Repository actualiza también `updated_at`.

El registro de Workers se mantiene en la tabla `nodes`, que representa
**Workers de cómputo exclusivamente**, no nodos CockroachDB.

El Heartbeat actual no incluye métricas dinámicas y no existe un monitor
que marque `offline` a un nodo vencido. El timeout cercano a 10 segundos
es una referencia para el diseño objetivo, no un comportamiento
implementado. La propuesta de elección de coordinador y métricas está
en [la arquitectura distribuida](docs/distributed-coordination-architecture.md).

Un Heartbeat recibido solo confirma comunicación reciente con FastAPI;
no confirma salud de entrenamiento, disponibilidad de CPU/RAM ni
elegibilidad para coordinar.

### Paso 3. Carga del dataset

El usuario carga un CSV desde la interfaz.

El Backend:

1.  Recibe el archivo.
2.  Comprueba extensión/formato y que se pueda leer.
3.  Guarda el archivo en el almacenamiento local del Backend.
4.  Obtiene metadatos básicos: nombre, tamaño, filas y columnas.
5.  Registra el dataset en la tabla `datasets`.
6.  Devuelve el identificador del dataset para selección en el
    formulario.

**Límite:** el dataset no se guarda como blob en CockroachDB. La base de
datos guarda metadatos y ruta del archivo. El archivo depende del
almacenamiento local del host y no está replicado por CockroachDB. Por
tanto, tolerancia a fallos de DB no equivale a tolerancia a pérdida de
archivos CSV.

Se validarán CSV legibles y compatibles con el pipeline básico. No se
promete soporte universal para cualquier formato, codificación o
estructura irregular.

### Paso 4. Configuración del entrenamiento

El usuario selecciona/configura:

-   Dataset.
-   Columna objetivo (target).
-   Algoritmo disponible: Logistic Regression.
-   Parámetros del entrenamiento.
-   Cantidad solicitada de Workers (por defecto 3, limitada a los
    disponibles).
-   Proporción train/test.
-   Seed opcional para reproducibilidad de la partición y del proceso.

El Backend valida los valores y crea un Job con estado `pending`.

**Parámetros iniciales recomendados (ajustables y sujetos a pruebas):**

  ------------------------------------------------------------------------
  Parámetro                   Valor inicial sugerido Significado
  --------------------- ---------------------------- ---------------------
  Workers iniciales                                3 Instancias Worker
                                                     levantadas por
                                                     Compose

  Workers solicitados                  3 por defecto Número deseado; no
  por Job                                            puede superar los
                                                     elegibles

  Train ratio                                    0.8 80% para
                                                     entrenamiento y 20%
                                                     para test

  Seed                                            42 Valor reproducible de
                                                     ejemplo

  Algoritmo                      Logistic Regression Único algoritmo de
                                                     MVP

  Learning rate                                 0.01 Tasa de aprendizaje
                                                     inicial

  Epochs                                          20 Número de
                                                     iteraciones/épocas
                                                     iniciales

  Batch/partition            Tareas más pequeñas que Permitir balanceo y
  strategy                                   Workers reasignación

  Heartbeat                        3 s, inicialmente Frecuencia
                                                     ilustrativa

  Timeout Worker                  10 s, inicialmente Umbral ilustrativo de
                                                     desconexión

  Task retry limit        2 reintentos, inicialmente Límite ilustrativo de
                                                     reintentos por tarea

  Concurrent Jobs                                  1 Un entrenamiento
                                                     activo a la vez
  ------------------------------------------------------------------------

Los parámetros de ML son valores iniciales para la demo, no valores
óptimos para todos los datasets. Se ajustarán en la implementación. La
interfaz mostrará únicamente los parámetros que se decida exponer; no se
busca un panel de hiperparámetros extenso.

El Job se registra en `jobs` con estado `pending`, dataset, algoritmo,
target, parámetros, workers solicitados y configuración train/test.

### Paso 5. Validación y preparación de datos

El Coordinator carga el CSV desde el almacenamiento local y realiza las
validaciones necesarias para el algoritmo:

1.  Verifica la existencia de la columna target.
2.  Comprueba que el dataset tenga filas suficientes para separar
    entrenamiento y prueba.
3.  Identifica columnas numéricas/categóricas según el soporte
    implementado.
4.  Aplica el pipeline de preprocesamiento decidido (por ejemplo,
    codificación de etiquetas y normalización numérica).
5.  Separa los datos en train/test.
6.  Mantiene el test holdout fuera del entrenamiento distribuido.
7.  Divide únicamente el conjunto de entrenamiento en
    particiones/tareas.

Preprocesamiento debe ajustarse sin fuga de información: las
estadísticas de normalización y transformaciones aprendidas se calculan
con train y se aplican al test, no al revés.

**Limitación:** el MVP no busca resolver todos los casos de limpieza de
datos. Datasets con datos faltantes, columnas no compatibles, clases
insuficientes, tipos ambiguos o tamaños que excedan los recursos
disponibles pueden rechazarse o requerir ajustes. Se definirá y
documentará el conjunto de formatos de datos soportados durante la
implementación.

### Paso 6. Scheduler y balanceo de carga

El Coordinator selecciona Workers disponibles y crea tareas de
entrenamiento. El balanceo se realiza a nivel de tareas, no asignando
necesariamente una única partición fija por Worker.

El Scheduler considera, dentro de lo que el MVP pueda medir:

-   Estado del Worker (`online` y no ocupado, o con capacidad libre).
-   Tareas activas/asignadas.
-   Capacidad reportada (CPU/RAM).
-   Cantidad de trabajo pendiente.

La estrategia inicial será simple y explicable: distribuir las tareas
entre Workers elegibles, favoreciendo los que tengan menor carga y/o
mayor capacidad reportada. No se implementará un sistema sofisticado de
predicción de tiempos ni optimización de recursos.

### Paso 7. Particionamiento del trabajo

Para facilitar el balanceo y la recuperación, el entrenamiento se divide
en varias tareas más pequeñas que la cantidad de Workers. Ejemplo
ilustrativo:

-   3 Workers disponibles.
-   9 tareas/particiones para una iteración.
-   Cada tarea procesa una porción del conjunto de entrenamiento.
-   Un Worker puede recibir nuevas tareas al terminar las anteriores.

El número de particiones se define como parámetro interno del
Coordinator y se ajusta en pruebas. **9 tareas para 3 Workers es un
ejemplo inicial de diseño, no un valor definitivo ni una regla fija.**

Las tareas se registran en `tasks`, con un `partition_index` único por
Job. Se conserva la identidad de la partición entre iteraciones: los
datos de la partición no cambian arbitrariamente de dueño; lo que cambia
es qué Worker la ejecuta.

### Paso 8. Entrenamiento distribuido (Data Parallelism)

Se implementará un único modelo global de Logistic Regression. Los
Workers no entrenan modelos independientes que luego se mezclan.

En cada iteración:

1.  El Coordinator conserva los pesos actuales del modelo global.
2.  Envía a Workers elegibles tareas con su partición de entrenamiento y
    los mismos pesos actuales.
3.  Cada Worker calcula un gradiente local sobre su partición con esos
    pesos.
4.  Cada Worker devuelve el gradiente y el tamaño efectivo de su
    partición (número de muestras).
5.  El Coordinator valida y reúne resultados de todas las tareas válidas
    de la iteración.
6.  Agrega gradientes mediante promedio ponderado por número de
    muestras.
7.  Actualiza los pesos globales.
8.  Continúa a la siguiente iteración/época.

Agregación ponderada:

\[ G\_{global}=`\frac{\sum_i n_i G_i}{\sum_i n_i}`{=tex} \]

donde (G_i) es el gradiente de una partición y (n_i) la cantidad de
muestras que contribuyeron a ese gradiente.

Actualización del modelo:

\[ w\_{nuevo}=w\_{actual}-`\eta `{=tex}G\_{global} \]

donde (`\eta`{=tex}) es la tasa de aprendizaje.

Todos los gradientes agregados para una iteración deben corresponder a
los mismos pesos de partida y a la misma definición de iteración. No se
actualizará el modelo global con una colección incompleta de gradientes
de la iteración.

El algoritmo se limitará a la implementación funcional de Logistic
Regression. Para multiclase se contemplará un enfoque correcto, como
Softmax, en lugar de tratar todas las clases como una clasificación
binaria.

### Paso 9. Balanceo y ejecución de tareas

El Coordinator mantiene una cola de tareas. Los Workers toman/reciben
tareas según disponibilidad.

El diseño permite que:

-   Un Worker más rápido procese varias tareas en secuencia.
-   Un Worker más cargado no reciba trabajo innecesario mientras otros
    estén disponibles.
-   El Scheduler reasigne tareas pendientes a Workers libres.
-   La asignación no sea necesariamente igual en número de filas para
    todos los Workers, si sus capacidades son diferentes.

No se promete balanceo perfecto. Las diferencias de CPU, memoria,
latencia de comunicación, tamaño de particiones y sincronización pueden
influir. Con Workers en el mismo PC, la competencia por recursos del
host también afecta.

### Paso 10. Tolerancia a fallos de Worker

Se busca que la caída de un Worker no implique automáticamente perder el
entrenamiento completo, mientras queden Workers disponibles.

#### Ejemplo

Supongamos 3 Workers y 9 tareas para una iteración. Worker 2 deja de
responder mientras ejecuta una tarea.

El Coordinator:

1.  Detecta la ausencia de Heartbeats tras el timeout configurado.
2.  Marca Worker 2 como `offline`.
3.  Identifica las tareas asignadas a ese Worker que no tienen resultado
    válido confirmado.
4.  Mantiene los resultados válidos de tareas ya completadas.
5.  Reencola las tareas incompletas o con resultado no confirmado.
6.  Worker 1 y Worker 3 continúan con las tareas pendientes según su
    disponibilidad.
7.  Las tareas reasignadas se ejecutan con los mismos pesos globales y
    contexto de iteración.
8.  El Coordinator acepta un resultado válido de cada tarea lógica,
    evitando contar duplicados.
9.  Cuando todas las tareas requeridas de esa iteración tienen
    resultados válidos, agrega los gradientes y actualiza el modelo.
10. Continúa el entrenamiento con la siguiente iteración.

#### Reintentos y resultados duplicados

Una tarea puede haber sido procesada antes de la desconexión, pero el
Worker pudo fallar antes de que su respuesta llegara al Coordinator. En
ese caso, se puede ejecutar de nuevo.

La solución usará identificación estable de tarea/partición e iteración,
validación del estado de tarea y aceptación controlada del resultado,
para evitar que dos respuestas de la misma tarea se agreguen dos veces.

-   Los reintentos serán limitados (valor inicial sugerido: 2 reintentos
    adicionales).
-   Los resultados repetidos se tratarán como duplicados y no se
    agregarán más de una vez a la iteración.
-   Si se agota el límite de reintentos o no quedan Workers capaces de
    completar la iteración, el Job podrá fallar.
-   Los Workers sobrevivientes podrán completar las tareas pendientes si
    tienen recursos suficientes.

#### Alcance de la tolerancia a fallos

Incluida: caída de Worker durante el Job, detección,
reencolado/reasignación de tareas incompletas, reintentos limitados y
continuación de entrenamiento cuando sea posible.

No incluida: recuperación del Coordinator, recuperación completa después
de reiniciar el host, checkpointing del modelo para reanudar un Job
después de caída del Backend, garantía de disponibilidad indefinida,
tolerancia a fallos simultáneos de todos los Workers o recuperación de
archivos CSV perdidos.

### Paso 11. Evaluación del modelo

Cuando se completan las iteraciones:

1.  El Coordinator utiliza los pesos finales.
2.  Evalúa sobre el conjunto test separado previamente.
3.  Calcula métricas compatibles (accuracy y loss; otras métricas según
    el alcance implementado).
4.  Registra la duración total del entrenamiento.
5.  Guarda los resultados en `results`.
6.  Actualiza el Job a `completed`.

El conjunto de test no participa en la actualización de pesos ni en el
cálculo de gradientes.

### Paso 12. Persistencia y presentación de resultados

Se persistirán:

-   Configuración del Job.
-   Estado final.
-   Workers solicitados y utilizados.
-   Tareas y estado de ejecución/reintentos.
-   Métricas finales.
-   Duración.
-   Ruta de artefacto de modelo, si se implementa su guardado.

La interfaz muestra un estado global comprensible:

-   `pending`
-   `running`
-   `completed`
-   `failed`
-   `cancelled`

El usuario de entrenamiento no necesita ver detalles internos de
asignación de particiones, gradientes, sincronización o progreso
individual por Worker. La interfaz se enfoca en dataset, configuración,
estado global y resultados. La vista técnica de clúster/Workers sirve
para demostración académica o administración.

### Paso 13. Comparación local vs distribuida

Se realizará, si el tiempo lo permite, una comparación con el mismo
dataset, preprocesamiento, parámetros y seed:

-   Ejecución local (baseline).
-   Ejecución distribuida con 3 Workers.

Se compararán duración y métricas. No se asume que la ejecución
distribuida será siempre más rápida: en datasets pequeños, el overhead
de comunicación, particionamiento y agregación puede superar el
beneficio del paralelismo.

------------------------------------------------------------------------

## 6. Estados y comportamiento del sistema

### 6.1 Estado de Worker

  Estado      Significado
  ----------- ----------------------------------------------------
  `online`    Worker registrado y disponible para recibir tareas
  `busy`      Worker está procesando una o más tareas
  `offline`   Worker no está disponible o perdió comunicación

Los estados representan disponibilidad operacional básica, no un
monitoreo avanzado de salud.

### 6.2 Estado de Job

  Estado        Significado
  ------------- -----------------------------------------------------
  `pending`     Job creado, esperando ejecución
  `running`     Entrenamiento en curso
  `completed`   Entrenamiento y evaluación terminaron correctamente
  `failed`      Error no recuperable o reintentos agotados
  `cancelled`   Job cancelado, si se implementa la cancelación

### 6.3 Estado de Task

  Estado        Significado
  ------------- ------------------------------------------------------
  `pending`     Tarea esperando asignación/ejecución
  `assigned`    Tarea asignada a un Worker
  `running`     Worker está ejecutándola
  `completed`   Resultado validado y aceptado
  `failed`      Fallo de tarea; podrá reintentarse según la política

------------------------------------------------------------------------

## 7. Estructura de la base de datos

La base de datos lógica es CockroachDB, distribuida en tres nodos
simulados (contenedores). Las cinco tablas operativas descritas abajo
almacenan Workers, datasets, Jobs, Tasks y resultados. La tabla adicional
`cluster_leadership`, creada por la migración versionada, almacena el
lease del coordinador.

### Relaciones

-   `datasets` 1 a N `jobs`: un dataset puede utilizarse en varios
    entrenamientos.
-   `jobs` 1 a N `tasks`: un Job genera múltiples tareas/particiones.
-   `nodes` 1 a N `tasks`: un Worker puede ejecutar varias tareas.
    `node_id` puede ser NULL si una tarea se desasigna o el nodo deja de
    existir en la referencia.
-   `jobs` 1 a 0..1 `results`: un Job tiene como máximo un registro de
    resultado final.
-   Eliminar un Job elimina sus Tasks y Results debido a
    `ON DELETE CASCADE`.
-   No se puede eliminar un Dataset referenciado por un Job, debido a la
    FK sin cascada.
-   Los Workers se conservan en el registro y se marcan `offline` en
    lugar de borrarse por desconexiones.
-   `nodes` almacena Workers de cómputo, no DB Nodes.
-   `cluster_leadership` mantiene una fila por `cluster_id`; el liderazgo
    activo referencia un `node_id` de `nodes`.

### SQL del esquema

El siguiente esquema se conserva según lo solicitado. El SQL representa
el modelo lógico de la aplicación y debe ejecutarse/validarse en la
versión de CockroachDB utilizada.

``` sql
CREATE TABLE nodes (
    node_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    hostname STRING NOT NULL,
    ip_address STRING,
    status STRING NOT NULL DEFAULT 'offline',
    cpu_cores INT4,
    ram_mb INT4,
    last_heartbeat TIMESTAMPTZ,
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
    parameters JSONB NOT NULL DEFAULT '{}',
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
            'pending', 'running', 'completed',
            'failed', 'cancelled'
        )),
    CONSTRAINT chk_requested_workers
        CHECK (requested_workers > 0),
    CONSTRAINT chk_train_ratio
        CHECK (train_ratio IS NULL OR
               (train_ratio > 0 AND train_ratio < 1))
);

CREATE TABLE tasks (
    task_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL REFERENCES jobs(job_id)
        ON DELETE CASCADE,
    node_id UUID REFERENCES nodes(node_id)
        ON DELETE SET NULL,
    partition_index INT4 NOT NULL,
    rows_assigned INT8,
    status STRING NOT NULL DEFAULT 'pending',
    attempts INT4 NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error_message STRING,
    CONSTRAINT uq_job_partition
        UNIQUE (job_id, partition_index),
    CONSTRAINT chk_task_status
        CHECK (status IN (
            'pending', 'assigned', 'running',
            'completed', 'failed'
        )),
    CONSTRAINT chk_task_attempts
        CHECK (attempts >= 0),
    CONSTRAINT chk_partition_index
        CHECK (partition_index >= 0)
);

CREATE TABLE results (
    result_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL UNIQUE REFERENCES jobs(job_id)
        ON DELETE CASCADE,
    accuracy FLOAT8,
    loss FLOAT8,
    duration_seconds FLOAT8 NOT NULL,
    metrics JSONB NOT NULL DEFAULT '{}',
    model_path STRING,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_result_duration
        CHECK (duration_seconds >= 0)
);

-- Índices para consultas habituales

CREATE INDEX idx_nodes_status ON nodes(status);

CREATE INDEX idx_jobs_status ON jobs(status);

CREATE INDEX idx_jobs_dataset ON jobs(dataset_id);

CREATE INDEX idx_tasks_job ON tasks(job_id);

CREATE INDEX idx_tasks_node ON tasks(node_id);
```

### Notas del modelo

-   `nodes`: registro y estado de Workers.
-   `datasets`: metadatos y ubicación local de CSV.
-   `jobs`: configuración y ciclo de vida de cada entrenamiento.
-   `tasks`: unidades de trabajo asociadas a particiones de un Job, con
    asignación, estado e intentos.
-   `results`: métricas finales y ubicación opcional del artefacto.

**Nota de diseño pendiente durante implementación:** el esquema actual
tiene una fila por partición lógica y `UNIQUE(job_id, partition_index)`.
Como la tarea puede ejecutarse en varias iteraciones/épocas y
reintentarse, la implementación debe definir claramente cómo se
representa el ciclo de ejecución de una partición en las iteraciones y
cómo se actualizan sus estados/attempts. Para mantener el MVP acotado,
no se introduce una tabla adicional de subtareas por época en este
diseño; el Coordinator mantiene la identidad de la iteración y el
intento en el protocolo de ejecución y debe evitar que resultados de
iteraciones diferentes se mezclen. Si la implementación exige persistir
historial detallado de cada intento/iteración, eso requeriría ampliar el
esquema, lo cual no forma parte de este modelo base.

------------------------------------------------------------------------

## 8. Organización inicial del código

### Backend

``` text
backend/
├── main.py
├── api/
│   ├── nodes.py
│   ├── datasets.py
│   ├── jobs.py
│   ├── cluster.py
│   └── training.py
├── coordinator/
│   ├── scheduler.py
│   ├── partitioner.py
│   └── aggregator.py
├── workers/
│   ├── worker.py
│   └── training_engine.py
├── database/
│   ├── connection.py
│   ├── schema.sql
│   └── repository.py
└── storage/
    └── datasets/
```

### Frontend

``` text
frontend/
├── index.html
├── css/
│   └── styles.css
└── js/
    ├── app.js
    ├── api.js
    └── services/
        ├── cluster.service.js
        ├── nodes.service.js
        ├── datasets.service.js
        ├── jobs.service.js
        └── training.service.js
```

La estructura es una guía inicial. Se puede ajustar durante la
implementación sin cambiar los límites funcionales del proyecto.

------------------------------------------------------------------------

## 9. Qué mostrará la interfaz

### Dashboard

-   Estado general del sistema.
-   Estado del clúster de DB.
-   Número de Workers disponibles.
-   Jobs pendientes/activos/terminados.

### Clúster de base de datos

Vista de demostración de DB1, DB2 y DB3, su disponibilidad y estado
general. No se mostrará información interna compleja de ranges/replicas
al usuario de entrenamiento; esa información podrá verificarse por
herramientas de CockroachDB durante la demostración técnica.

### Clúster de cómputo

-   Worker 1, Worker 2 y Worker 3 (y la configuración de instancias).
-   Estado online/busy/offline.
-   Capacidades reportadas CPU/RAM.
-   Información básica de disponibilidad.

### Entrenamiento

-   Cargar/seleccionar CSV.
-   Seleccionar target.
-   Seleccionar algoritmo disponible.
-   Ajustar parámetros soportados.
-   Solicitar cantidad de Workers.
-   Iniciar entrenamiento.
-   Consultar estado global.

### Jobs y resultados

-   Dataset usado.
-   Configuración del entrenamiento.
-   Estado del Job.
-   Duración.
-   Métricas finales.
-   Mensaje de error si falla.

No se expondrá al usuario final de entrenamiento el progreso porcentual
individual de Workers, gradientes, particiones, sincronización ni logs
técnicos. La transparencia se centra en qué se está entrenando, con qué
configuración, estado general y resultado. La vista de clúster sirve
como demostración de infraestructura.

------------------------------------------------------------------------

## 10. Plan de trabajo de dos días

La distribución temporal es una meta, no garantía. Si surge un bloqueo,
se prioriza demostrar los fundamentos funcionales sobre completar todas
las mejoras visuales.

### Día 1 --- Infraestructura, persistencia y coordinación

1.  Preparar estructura del proyecto, entorno Python y Docker Compose.
2.  Levantar e inicializar CockroachDB con tres nodos.
3.  Verificar membresía, replicación y distribución de réplicas.
4.  Crear las cinco tablas, constraints e índices.
5.  Conectar FastAPI con CockroachDB.
6.  Implementar repositorios y operaciones esenciales.
7.  Implementar registro y Heartbeat de Workers.
    Estado: endpoints y cliente implementados; persistencia real
    de esos endpoints pendiente de verificación. El lease de liderazgo
    sí fue probado contra CockroachDB real.
8.  Levantar tres Workers y verificar registro/estados.
    Estado: tres procesos se ejecutaron contra un endpoint simulado;
    integración real con FastAPI y CockroachDB pendiente de verificación.
9.  Implementar Scheduler y cola básica de tareas.
10. Probar caída/reconexión de un Worker.
11. Verificar comportamiento ante caída de un nodo de DB.

### Día 2 --- Aplicación, ML distribuido y recuperación

1.  Crear frontend HTML/CSS/JavaScript.
2.  Dashboard y vistas de clúster.
3.  Formularios de dataset y entrenamiento.
4.  Carga de CSV y persistencia de metadatos.
5.  Validación y preparación del dataset.
6.  Implementar Logistic Regression con PyTorch.
7.  Verificar entrenamiento local (baseline).
8.  Implementar particionamiento y ejecución de gradientes en Workers.
9.  Implementar agregación ponderada y actualización global.
10. Implementar evaluación y persistencia de resultados.
11. Balanceo de tareas según disponibilidad/carga.
12. Implementar detección de caída, reencolado, reintentos y
    reasignación.
13. Pruebas con uno y tres Workers.
14. Simular caída de Worker durante entrenamiento y comprobar
    continuidad.
15. Comparar local vs. distribuido si el tiempo lo permite.
16. Correcciones críticas y preparar demo.

------------------------------------------------------------------------

## 11. Criterios de aceptación del proyecto

Distributed ML se considera una demo funcional cuando se logre:

Esta lista representa criterios objetivo del proyecto. No se deben
interpretar como evidencia de que los elementos estén implementados o
validados actualmente.

-   [ ] CockroachDB está funcionando como un único clúster de tres
    nodos.
-   [ ] Se verifica distribución/replicación, no solo que los
    contenedores están levantados.
-   [ ] FastAPI puede persistir y consultar la información de
    aplicación.
-   [ ] Tres Workers se registran y envían Heartbeats.
-   [ ] El usuario carga un CSV y se registra su metadata.
-   [ ] El usuario puede configurar y lanzar un Job de entrenamiento.
-   [ ] El entrenamiento ejecuta un algoritmo real de ML.
-   [ ] Los Workers procesan diferentes particiones y devuelven
    gradientes.
-   [ ] El Coordinator agrega gradientes y actualiza el modelo global.
-   [ ] Se calcula evaluación sobre datos de prueba separados.
-   [ ] Los resultados quedan persistidos y visibles.
-   [ ] El Scheduler reparte tareas y evita depender de una única
    asignación rígida por Worker.
-   [ ] Una caída de Worker se detecta.
-   [ ] Las tareas incompletas se reencolan/reasignan a Workers
    supervivientes.
-   [ ] El entrenamiento puede continuar si las tareas se recuperan y
    hay capacidad disponible.
-   [ ] El sistema maneja fallos no recuperables marcando el Job como
    `failed` y guardando una razón.

------------------------------------------------------------------------

## 12. Limitaciones técnicas y académicas

1.  **Un solo host:** los contenedores simulan nodos; no son equipos
    físicos independientes. No se demuestra tolerancia a la pérdida
    completa de la máquina.
2.  **Recursos compartidos:** Workers y DB compiten por CPU, RAM, disco
    y red del mismo PC.
3.  **Sin garantía de aceleración:** distribución puede ser más lenta
    para datasets pequeños por overhead.
4.  **Un algoritmo:** Logistic Regression; no es una plataforma general
    de ML.
5.  **Un Job a la vez:** se evita concurrencia para limitar RAM y
    complejidad.
6.  **Worker recovery limitado:** se reintentan tareas incompletas
    mientras existan Workers disponibles, pero no se garantiza
    recuperación de cualquier escenario.
7.  **Coordinator central en el estado actual:** si FastAPI cae, el
    cliente Worker actual no puede registrar ni enviar Heartbeats y no
    hay coordinación autónoma. El lease persistente ya está
    implementado, pero no la elección automática, la puntuación de
    capacidad ni el descubrimiento; estos siguen propuestos en
    [docs/distributed-coordination-architecture.md](docs/distributed-coordination-architecture.md),
    y no habilitan por sí solos el failover.
8.  **Sin checkpointing de entrenamiento:** si el Coordinator o host se
    reinicia, el Job en curso no se reanuda desde el último modelo
    guardado.
9.  **Datos en disco local:** CockroachDB replica metadatos, pero no los
    CSV del almacenamiento local.
10. **Heartbeats básicos:** el Worker envía una señal periódica, pero no
    hay detector de expiración ni transición automática a `offline`.
    En la arquitectura futura, congestión o pausas pueden causar falsos
    positivos; el lease/timeout debe calibrarse.
11. **Resultados repetidos:** la protección de duplicados será lógica,
    basada en identificadores y estado; no se promete ejecución
    exactly-once.
12. **Dataset condicionado:** formatos/tipos/valores que no soporte el
    pipeline se rechazan o requieren tratamiento previo.
13. **Tamaño limitado por recursos:** no se establece una cuota
    universal de filas o MB; el límite real dependerá del PC y se deberá
    probar para evitar agotamiento de RAM.
14. **Seguridad simplificada:** entorno local académico, sin
    autenticación multiusuario ni despliegue público seguro.
15. **Replicación DB no equivale a backup:** el proyecto no incorpora
    una estrategia de backups completa.
16. **No elasticidad dentro de la iteración:** las tareas pendientes
    pueden reasignarse, pero el protocolo del entrenamiento conserva el
    contexto/weights de esa iteración; no se agregan Workers
    arbitrariamente a mitad de un cálculo global sin control de
    sincronización.

------------------------------------------------------------------------

## 13. Estrategia de demostración

La presentación académica puede realizarse en esta secuencia:

1.  Mostrar la aplicación y el estado de DB1, DB2 y DB3.
2.  Mostrar los tres Workers registrados.
3.  Cargar un dataset CSV.
4.  Configurar un entrenamiento.
5.  Ejecutar con un Worker y registrar el resultado/tiempo (baseline).
6.  Ejecutar con tres Workers y mostrar que el trabajo se distribuye.
7.  Mostrar las métricas y comparar duración, aclarando el overhead.
8.  Ejecutar un nuevo entrenamiento y detener deliberadamente uno de los
    Workers.
9.  Mostrar detección de desconexión.
10. Mostrar reasignación de tareas pendientes a Workers supervivientes.
11. Mostrar que el Job continúa cuando logra completar las tareas de la
    iteración.
12. Detener un nodo CockroachDB y observar el comportamiento del
    clúster, explicando que tolerancia depende de quorum y replicación
    correctamente configurada.
13. Concluir con la arquitectura, los alcances y limitaciones.

------------------------------------------------------------------------

## 14. Principio rector del proyecto

El sistema debe hacer explícitos los conceptos distribuidos, pero
mantener sencilla la experiencia del usuario.

El usuario que entrena solo necesita cargar datos, seleccionar
parámetros, solicitar Workers, iniciar el Job y ver su
estado/resultados. El Coordinator y los componentes internos se encargan
del particionamiento, distribución, balanceo, agregación, supervisión y
recuperación de tareas.

**Prioridad de desarrollo:** infraestructura distribuida demostrable →
persistencia → Workers operativos → ML distribuido real → balanceo →
tolerancia a fallos → interfaz y refinamientos.

Esta priorización evita que la interfaz o características accesorias
consuman el tiempo disponible antes de completar la funcionalidad
central.
