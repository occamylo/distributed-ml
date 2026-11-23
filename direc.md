Revisa exclusivamente la estructura actual del proyecto y el README para determinar cómo se debe ejecutar correctamente un Worker desde Git Bash.

El comando documentado:

`python -m workers.worker --backend-url http://127.0.0.1:8000`

está fallando con:

`ModuleNotFoundError: No module named 'workers'`

Necesito que determines:

1. La ubicación real del módulo o script del Worker.
2. Desde qué directorio debe ejecutarse.
3. El comando exacto de Bash para iniciarlo utilizando el entorno virtual existente.
4. Si requiere alguna variable de entorno, dependencia, servicio o condición previa.
5. El procedimiento exacto para levantar un Worker adicional, si queremos simular varios nodos.

**Restricciones:**

- No modifiques ningún archivo.
- No ejecutes el Worker, backend, Docker, pruebas ni comandos que alteren el estado del sistema.
- No instales dependencias.
- No propongas cambios arquitectónicos ni refactorizaciones.
- Limítate a revisar el código existente y el README para dar instrucciones de ejecución correctas.

Entrega únicamente los comandos exactos que debo ejecutar y, brevemente, el orden en el que debo hacerlo para iniciar y verificar un Worker.