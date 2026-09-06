-- Replace identifiers before execution; provision separately from the runtime identity.
GRANT USE CATALOG ON CATALOG retail_dev TO `<runtime-service-principal>`;
GRANT USE SCHEMA, CREATE TABLE, CREATE MATERIALIZED VIEW ON SCHEMA retail_dev.bronze TO `<runtime-service-principal>`;
GRANT USE SCHEMA, CREATE TABLE, CREATE MATERIALIZED VIEW ON SCHEMA retail_dev.silver TO `<runtime-service-principal>`;
GRANT USE SCHEMA, CREATE TABLE, CREATE MATERIALIZED VIEW ON SCHEMA retail_dev.gold TO `<runtime-service-principal>`;
GRANT USE SCHEMA, CREATE TABLE, CREATE MATERIALIZED VIEW ON SCHEMA retail_dev.dq TO `<runtime-service-principal>`;
GRANT READ VOLUME ON VOLUME retail_dev.bronze.landing TO `<runtime-service-principal>`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME retail_dev.bronze.pipeline_state TO `<runtime-service-principal>`;
GRANT USE CATALOG ON CATALOG retail_dev TO `<producer-service-principal>`;
GRANT USE SCHEMA ON SCHEMA retail_dev.bronze TO `<producer-service-principal>`;
GRANT WRITE VOLUME, READ VOLUME ON VOLUME retail_dev.bronze.landing TO `<producer-service-principal>`;
-- Give BI principals SELECT on approved Gold views only; raw/quarantine contain PII.
