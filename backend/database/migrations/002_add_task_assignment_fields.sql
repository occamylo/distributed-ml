ALTER TABLE public.tasks
    ADD COLUMN IF NOT EXISTS attempt_id UUID;

ALTER TABLE public.tasks
    ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ;

ALTER TABLE public.tasks
    ADD COLUMN IF NOT EXISTS leader_epoch INT8;
