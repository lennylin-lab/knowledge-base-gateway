-- Revert the model control plane migration. Down drops columns in reverse
-- order of the up migration and restores the seeded mock model's capability
-- declaration (removes the additive embeddings keys).
UPDATE model_catalog
SET capabilities = capabilities - 'embeddings' - 'embedding_dim',
    retrieval_profile = NULL,
    config_version = config_version - 1
WHERE public_name = 'gateway-echo';

ALTER TABLE model_catalog
    DROP COLUMN retrieval_profile;

ALTER TABLE access_policies
    DROP COLUMN default_embedding_model,
    DROP COLUMN default_model;
