# Directriz 9: Implementar Scheduler y cola básica de tareas

## Objetivo

Implementar el Scheduler y una cola básica de tareas para el proyecto, integrándolos con la arquitectura distribuida actual y el mecanismo de liderazgo ya implementado.

## Fuentes de información

Consulta exclusivamente:

- El código fuente existente en el repositorio.
- El `README.md` ubicado en la raíz del proyecto.

Identifica allí la estructura, los componentes existentes, los contratos, el modelo de datos y las responsabilidades de cada módulo antes de realizar cambios.

## Alcance de implementación

Implementa únicamente lo necesario para cumplir la tarea:

**9. Implementar Scheduler y cola básica de tareas.**

La implementación debe integrarse con la arquitectura y los mecanismos existentes, respetando las responsabilidades actuales de los componentes y evitando duplicar funcionalidades ya implementadas.

## Limitaciones obligatorias

- Modifica exclusivamente archivos de código necesarios para implementar esta tarea.
- No modifiques el README.md ni ningún otro documento.
- No modifiques el esquema de CockroachDB ni crees o ejecutes DDL.
- No alteres los datos existentes en la base de datos.
- No modifiques Docker Compose, la infraestructura ni la configuración del entorno.
- No reemplaces ni alteres los mecanismos existentes de registro, heartbeat, publicación de métricas, coordinación, elección, adquisición, renovación, validación o liberación del liderazgo.
- No implementes funcionalidades correspondientes a subtareas posteriores.
- No agregues dependencias ni tecnologías nuevas innecesarias.
- No realices refactorizaciones ajenas al alcance.
- No ejecutes pruebas, compilaciones, comandos Git ni operaciones de infraestructura.

Si encuentras que algún requisito indispensable no puede cumplirse con la estructura, los contratos o las capacidades existentes, informa el impedimento sin ampliar el alcance ni improvisar cambios fuera de las restricciones establecidas.

## Informe final

Al terminar, informa brevemente:

- Archivos de código modificados.
- Funcionalidad del Scheduler implementada.
- Funcionamiento de la cola básica de tareas.
- Integración con los mecanismos existentes.
- Limitaciones o impedimentos encontrados.

No realices ninguna modificación adicional fuera del alcance de esta directriz.