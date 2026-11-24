-- Para una base creada antes de que requested_workers dejara de usarse.
-- Los Jobs nuevos lo dejan en NULL: el Scheduler usa todos los Workers
-- elegibles (contrato.md §17.1). Solo quita NOT NULL; los valores históricos
-- se conservan y el CHECK (requested_workers > 0) sigue aceptando NULL.
-- Es idempotente: volver a ejecutarlo no cambia nada.
ALTER TABLE jobs ALTER COLUMN requested_workers DROP NOT NULL;
