import { createJob } from "./jobs.service.js";

// Mismas reglas que el backend (coordinator/partitioner.py): 3 particiones
// por Worker, nunca más que las filas de entrenamiento.
export const PARTITIONS_PER_WORKER = 3;
export const DEFAULTS = { learningRate: 0.5, epochs: 100, trainRatio: 0.8, seed: 42 };

// Estimación para la vista previa. La división real es estratificada por
// clase, así que el número exacto lo da la API al crear el Job.
export function estimatePlan({ rows, trainRatio, workers }) {
  if (!rows || rows < 2) return null;
  const test = Math.min(rows - 1, Math.max(1, Math.round(rows * (1 - trainRatio))));
  const train = rows - test;
  const partitions = Math.max(1, Math.min(train, workers * PARTITIONS_PER_WORKER));
  return {
    train,
    test,
    partitions,
    minRows: Math.floor(train / partitions),
    maxRows: Math.ceil(train / partitions),
  };
}

export function startTraining({ datasetId, target, workers, learningRate, epochs, trainRatio, seed }) {
  return createJob({
    dataset_id: datasetId,
    algorithm: "logistic_regression",
    target_column: target,
    requested_workers: workers,
    train_ratio: trainRatio,
    seed,
    parameters: { learning_rate: learningRate, epochs },
  });
}
