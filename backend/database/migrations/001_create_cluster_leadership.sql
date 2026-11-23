CREATE TABLE IF NOT EXISTS public.cluster_leadership (
    cluster_id STRING PRIMARY KEY,
    leader_node_id UUID NULL REFERENCES public.nodes (node_id),
    epoch INT8 NOT NULL DEFAULT 0 CONSTRAINT chk_leadership_epoch CHECK (epoch >= 0),
    lease_until TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO public.cluster_leadership (
    cluster_id,
    leader_node_id,
    epoch,
    lease_until,
    updated_at
)
VALUES ('default', NULL, 0, now(), now())
ON CONFLICT (cluster_id) DO NOTHING;
