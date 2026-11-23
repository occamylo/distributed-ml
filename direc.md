# Corrección acotada: Liberación del liderazgo durante el apagado del Worker

Realiza una corrección exclusivamente sobre la liberación del lease durante el apagado ordenado de un Worker.

## Fuentes de información

Consulta únicamente el código existente y el README.md del proyecto.

Identifica las operaciones de liderazgo ya implementadas en el repositorio y el ciclo de vida actual de `WorkerCoordination`.

## Cambio solicitado

En `WorkerCoordination.stop()`:

1. Comprueba si el Worker es actualmente el propietario del liderazgo.
2. Si es líder, intenta liberar el lease utilizando exclusivamente la operación `release_leadership` ya implementada en el repositorio.
3. Utiliza el identificador del Worker y la época correspondiente al liderazgo adquirido.
4. Si el Worker no es líder, no debe intentar liberar ningún lease.
5. Si la liberación falla, registra el error y continúa con el apagado normal del servicio. La expiración del lease debe seguir funcionando como mecanismo de respaldo.
6. Conserva el orden de apagado necesario para evitar que el Worker siga prestando el servicio de coordinación después de iniciar su retirada del liderazgo.

## Restricciones

- Modifica exclusivamente `coordination.py`, salvo que una dependencia directa haga indispensable un cambio mínimo en `worker.py`.
- No modifiques el mecanismo de adquisición, renovación o validación del lease.
- No modifiques la publicación de heartbeat ni métricas.
- No cambies el algoritmo actual de elección del líder.
- No introduzcas criterios nuevos de selección de candidatos.
- No intentes resolver las colisiones de puertos en esta corrección.
- No modifiques el esquema, no ejecutes DDL y no alteres CockroachDB.
- No modifiques el README ni otros documentos.
- No agregues dependencias.
- No ejecutes pruebas, compilaciones ni comandos Git.

Si la implementación actual no permite liberar el lease de forma segura utilizando las operaciones existentes, informa el impedimento sin crear mecanismos alternativos.

## Informe final

Indica:
- Archivos modificados.
- Comportamiento de apagado anterior y nuevo.
- Cómo se evita liberar el lease de otro Worker.
- Qué ocurre si falla la liberación.
- Confirmación de que no se modificaron otros mecanismos ni la base de datos.