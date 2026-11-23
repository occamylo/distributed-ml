# Directriz: Servicio interno de coordinación y elección del líder

Implementa la siguiente etapa del proyecto: el servicio interno de coordinación entre Workers y la elección del líder mediante el mecanismo de lease persistente que ya existe.

## 1. Fuentes de información

Antes de modificar cualquier archivo, revisa exclusivamente:

- El código fuente actual del proyecto.
- El `README.md` ubicado en la raíz del proyecto.

Obtén de allí la estructura, los contratos, las operaciones existentes, el modelo de datos y las capacidades que ya están implementadas.

No introduzcas supuestos que contradigan la implementación existente.

## 2. Alcance de la implementación

Implementa únicamente lo necesario para:

1. Incorporar el servicio interno de comunicación y coordinación de los Workers.
2. Permitir que los Workers conozcan y se comuniquen con el coordinador vigente.
3. Implementar la elección del líder utilizando los mecanismos de lease persistente que ya existen en CockroachDB.
4. Permitir que el líder mantenga su lease mientras conserve legítimamente la autoridad.
5. Permitir que otro Worker pueda asumir el liderazgo cuando el lease vigente deje de ser válido.
6. Evitar que un Worker continúe ejerciendo funciones de coordinación cuando haya perdido su liderazgo.

La implementación debe respetar los contratos y las responsabilidades que ya existen en el proyecto.

## 3. Separación obligatoria de responsabilidades

La publicación periódica de heartbeat y métricas de los Workers ya está implementada.

**No confundas ni mezcles estas operaciones con la elección o el mantenimiento del liderazgo.**

En particular:

- Publicar métricas no concede liderazgo.
- Tener mayor capacidad disponible no concede liderazgo.
- Estar registrado y aparecer como `online` no concede liderazgo.
- La autoridad para coordinar debe depender exclusivamente de la adquisición y vigencia legítima del lease.
- La renovación del lease debe respetar las condiciones y mecanismos de autoridad existentes.

No dupliques ni reemplaces la publicación de métricas para implementar la coordinación.

## 4. Restricciones

- Modifica exclusivamente archivos de código necesarios para este alcance.
- No modifiques el `README.md`.
- No modifiques el modelo de datos ni ejecutes DDL.
- No alteres las tablas existentes ni los datos de CockroachDB.
- No crees una implementación paralela del mecanismo de lease.
- No reemplaces ni debilites los mecanismos de concurrencia, expiración, renovación y validación de autoridad existentes.
- No implementes todavía asignación de tareas, entrenamiento distribuido, recuperación de tareas ni agregación de resultados.
- No implementes un gateway redundante ni cambies el punto de entrada público de FastAPI.
- No amplíes el alcance hacia otras etapas futuras del proyecto.
- No agregues dependencias o componentes tecnológicos innecesarios.
- No ejecutes pruebas, compilaciones, comandos de Git ni operaciones sobre CockroachDB.

Si durante la revisión encuentras que falta algún requisito estructural indispensable que no puede resolverse exclusivamente mediante los cambios de código permitidos, no improvises una solución ni amplíes el alcance: informa el impedimento y los archivos o componentes involucrados.

## 5. Resultado esperado

Al finalizar, informa brevemente:

- Qué archivos modificaste.
- Qué responsabilidad implementa cada modificación.
- Cómo se diferencia la publicación de métricas de la adquisición y renovación del liderazgo.
- Cómo se utiliza el lease existente para determinar quién puede coordinar.
- Qué aspectos quedan pendientes para etapas posteriores.
- Si encontraste algún impedimento que requiera una decisión antes de continuar.

No realices modificaciones adicionales fuera de esta directriz.