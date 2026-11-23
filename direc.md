# Directriz — Subtarea 7: Implementar registro y Heartbeat de Workers

## Objetivo

Implementar el mecanismo básico de registro de Workers y Heartbeat para que el backend pueda registrar los nodos de cómputo y mantener actualizada la información de su disponibilidad mediante la base de datos existente.

El alcance se limita al registro, actualización de información operativa y recepción de Heartbeats.

## 1. Revisión previa

Antes de realizar cambios:

- Revisar el `README.md` para comprender el flujo general y las responsabilidades definidas para los Workers.
- Revisar la estructura actual del backend.
- Revisar la tabla `nodes` existente y sus campos.
- Revisar `backend/database/connection.py` y `backend/database/repository.py` para utilizar los mecanismos de conexión y persistencia ya implementados.
- Revisar los endpoints y módulos existentes para evitar duplicar funcionalidades.

Si alguna información necesaria no está definida, buscarla primero en el propio proyecto y su documentación. No asumir ni ampliar el diseño por cuenta propia.

## 2. Registro de Workers

Implementar el mecanismo necesario para que un Worker pueda registrarse en el backend.

El registro debe permitir almacenar la información correspondiente al Worker en la tabla `nodes` existente.

Campos involucrados:

- `node_id`: UUID generado por la base de datos.
- `hostname`: nombre del equipo.
- `ip_address`: dirección IP, cuando esté disponible.
- `status`: estado inicial correspondiente a un Worker no disponible, según los estados definidos en el esquema.
- `cpu_cores`: cantidad de núcleos de CPU disponibles, si se obtiene.
- `ram_mb`: memoria disponible o información de RAM reportada, según el mecanismo ya definido en el proyecto.
- `last_heartbeat`: fecha y hora del último Heartbeat recibido.
- `created_at` y `updated_at`: gestionados por la base de datos.

Utilizar las operaciones existentes del Repository. No implementar consultas SQL paralelas ni mecanismos de persistencia adicionales.

No crear nuevas tablas ni modificar el esquema.

## 3. Heartbeat

Implementar el mecanismo mediante el cual un Worker registrado comunica periódicamente al backend que continúa activo.

El Heartbeat debe:

- Identificar al Worker mediante su `node_id`.
- Actualizar el campo `last_heartbeat` con la fecha y hora actual.
- Actualizar `updated_at` de acuerdo con el contrato existente del Repository.
- Permitir que el backend identifique un Worker que está enviando Heartbeats válidos.
- Utilizar las operaciones de persistencia existentes.

El Heartbeat no debe modificar innecesariamente la información de hardware ni otros campos del Worker.

## 4. Estado del Worker

Utilizar exclusivamente los estados definidos en el esquema:

- `online`
- `offline`
- `busy`

En esta subtarea, implementar únicamente los cambios de estado estrictamente necesarios para el registro y el Heartbeat.

No implementar reglas de asignación de tareas ni lógica que determine cuándo un Worker debe estar `busy` por estar ejecutando entrenamientos.

No introducir máquinas de estados adicionales ni nuevos valores de estado.

## 5. Alcance de implementación

Realizar únicamente los cambios necesarios para implementar el registro y el Heartbeat de Workers dentro de la estructura actual del proyecto.

Se permite modificar o crear exclusivamente los archivos directamente necesarios para esta funcionalidad, respetando la arquitectura existente y utilizando los módulos de persistencia ya implementados.

No reorganizar ni refactorizar módulos existentes.

## 6. Exclusiones explícitas

Esta subtarea NO incluye:

- Implementar Scheduler.
- Asignar Tasks a Workers.
- Ejecutar tareas de entrenamiento.
- Implementar algoritmos de Machine Learning.
- Crear el sistema de particionamiento.
- Implementar distribución de datos.
- Gestionar Jobs.
- Implementar reintentos de Tasks.
- Implementar recuperación de Jobs.
- Crear un sistema de descubrimiento de Workers.
- Crear un Worker Agent externo o independiente del alcance establecido en el README.
- Implementar WebSockets.
- Implementar comunicación gRPC.
- Implementar autenticación.
- Implementar autorización.
- Implementar nuevas tablas.
- Modificar columnas, restricciones, índices o relaciones de la base de datos.
- Agregar dependencias.
- Implementar dashboards o monitoreo.
- Implementar mecanismos avanzados de gestión del cluster.
- Anticipar funcionalidades de tareas posteriores.

## 7. Restricciones técnicas

- Utilizar Python y FastAPI conforme al backend existente.
- Reutilizar el pool de conexiones existente.
- Utilizar exclusivamente las operaciones disponibles en `repository.py` para persistencia.
- Respetar el esquema actual de CockroachDB.
- Mantener la implementación simple.
- Evitar duplicar lógica.
- No implementar funcionalidades que no sean necesarias para esta subtarea.
- No modificar archivos ajenos al alcance.

## 8. Validación

Realizar únicamente validaciones pertinentes a esta subtarea.

Verificar, en la medida que el entorno lo permita:

- Que un Worker pueda registrarse.
- Que el registro quede persistido en `nodes`.
- Que el Worker pueda enviar un Heartbeat utilizando su identificador.
- Que `last_heartbeat` se actualice correctamente.
- Que el Heartbeat no sobrescriba información de hardware u otros campos no relacionados.
- Que los estados utilizados sean válidos para el esquema actual.
- Que la funcionalidad utilice el Repository existente.

Si alguna validación no puede realizarse, indicarlo explícitamente sin simular que fue ejecutada.

No realizar pruebas generales del proyecto, benchmarks ni validaciones correspondientes a tareas posteriores.

## 9. Informe final

Al finalizar, informar brevemente:

1. Archivos creados o modificados.
2. Funcionalidad de registro implementada.
3. Funcionamiento del Heartbeat.
4. Operaciones del Repository utilizadas.
5. Validaciones realizadas y resultados.
6. Limitaciones o puntos pendientes, únicamente si existen.

No implementar cambios adicionales después de completar esta subtarea.