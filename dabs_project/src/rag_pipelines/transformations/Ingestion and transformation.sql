-- ============================================================================
-- TEST VERSION: SharePoint -> chunked RAG pipeline (UC Volume ingestion)
-- Lakeflow Spark Declarative Pipeline (SQL)
--
-- Flow:  UC Volume files  ->  ai_parse_document  ->  ai_prep_search  ->  chunks
--
-- This is a test copy of sharepoint_rag_pipeline.sql that replaces the
-- SharePoint ingestion with a Unity Catalog Volume so you can validate the
-- full pipeline without a SharePoint tenant.
--
-- Prereqs (one-time):
--   1. A Unity Catalog Volume containing test PDFs/DOCX files.
--      Create one with: CREATE VOLUME <catalog>.<schema>.<volume>;
--      Then upload files via the Databricks UI, CLI, or:
--        databricks fs cp my-file.pdf dbfs:/Volumes/<catalog>/<schema>/<volume>/
--   2. ai_prep_search Beta feature enabled on the Previews page.
--   3. Compute: DBR 18.2+ (ai_prep_search) / serverless env version >= 3.
--
-- Edit the placeholder below, then run as a Lakeflow pipeline.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1) BRONZE: incrementally ingest raw files from a UC Volume as binary.
--    Auto Loader (STREAM read_files) picks up new files automatically.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE classic_stable_aiy0te.bronze.documents_raw
AS SELECT *
FROM STREAM read_files(
  "/Volumes/classic_stable_aiy0te/bronze/documents",  -- <-- your UC Volume path
  format          => "binaryFile",
  pathGlobFilter  => "*.{pdf,docx}"
);

-- ---------------------------------------------------------------------------
-- 2) SILVER: parse each document into structured content.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW classic_stable_aiy0te.silver.documents_parsed
AS SELECT
  path,
  ai_parse_document(content) AS parsed
FROM classic_stable_aiy0te.bronze.documents_raw;

-- ---------------------------------------------------------------------------
-- 3) GOLD: chunk + enrich for RAG, then flatten one row per chunk.
--    Use chunk_to_embed as the embedding column and chunk_id as PK when you
--    build the Databricks AI Search (vector search) index on this table.
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW classic_stable_aiy0te.gold.documents_chunks
AS
WITH prepped AS (
  SELECT
    path,
    ai_prep_search(parsed) AS result
  FROM classic_stable_aiy0te.silver.documents_parsed
)
SELECT
  chunk.value:chunk_id::STRING          AS chunk_id,
  chunk.value:chunk_position::INT       AS chunk_position,
  chunk.value:chunk_to_retrieve::STRING AS chunk_to_retrieve,
  chunk.value:chunk_to_embed::STRING    AS chunk_to_embed,
  prepped.path                          AS source_uri
FROM
  prepped,
  LATERAL variant_explode(prepped.result:document.contents) AS chunk;

