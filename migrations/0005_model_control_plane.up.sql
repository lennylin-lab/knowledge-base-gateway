-- Model control plane: subject default models, embeddings capability, and
-- retrieval profiles. Additive only; the down migration reverts every change.

-- Subject default models (nullable FKs following the text-FK convention).
-- Both slots ship together so one migration covers chat and embeddings
-- defaults: requests omitting `model` backfill default_model (chat/responses)
-- or default_embedding_model (embeddings) before model resolution. An unset
-- slot (NULL) keeps the previous behavior (model required).
ALTER TABLE access_policies
    ADD COLUMN default_model TEXT REFERENCES model_catalog(public_name),
    ADD COLUMN default_embedding_model TEXT REFERENCES model_catalog(public_name);

-- Retrieval profile: an opaque JSON object of retrieval thresholds attached
-- to the catalog row and surfaced verbatim through model discovery. The
-- gateway enforces no schema; NULL means the row declares none.
ALTER TABLE model_catalog
    ADD COLUMN retrieval_profile JSONB;

-- The seeded mock model gains embeddings with a fixed declared vector width
-- and a demo retrieval profile, so local/seeded deployments can exercise the
-- embeddings proxy and model-discovery profile delivery out of the box.
-- Capability keys are additive; config_version marks the declaration change.
UPDATE model_catalog
SET capabilities = capabilities || jsonb_build_object(
        'embeddings', true,
        'embedding_dim', 256
    ),
    retrieval_profile = '{"vector_max_distance":0.35,"vector_rescue_margin":0.08,"vector_rescue_max_distance":0.45,"vector_rescue_trigger_max_distance":0.30,"rrf_min_relative":0.05,"bm25_min_score":0.0,"bm25_min_coverage":0.5,"max_query_length":512}'::jsonb,
    config_version = config_version + 1
WHERE public_name = 'gateway-echo';
