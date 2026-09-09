/* ===========================================================================
   Data Lake catalogue.

   The brief asks for unstructured data alongside the star schema: employee
   photographs, held outside the database and joined back to the warehouse
   on the employee code.

   What lives where:

     the file        on disk, under /data/lake/employees/
     the metadata    here, one row per file
     the join key    employee_code, matching DimEmployees.employee_alternate_key

   The image bytes are deliberately not stored in ClickHouse. A columnar
   store is built for scanning many small values, not for holding blobs;
   putting the files in a column would bloat every part and slow down reads
   that have nothing to do with photographs. Keeping the file on disk and
   the description in a table is what makes this a lakehouse pattern rather
   than a database with pictures in it.

   Idempotent: safe to run more than once.
   =========================================================================== */

USE NorthwindDW;

CREATE TABLE IF NOT EXISTS LakeEmployeePhotos
(
    employee_code   Int32,
    file_name       String,
    file_path       String,
    content_type    String,
    size_bytes      UInt64,
    checksum_sha256 String,
    width_px        UInt16,
    height_px       UInt16,
    ingested_at     DateTime,
    source          String  DEFAULT 'generated',
    _version        UInt64  DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (employee_code);

/* ---------------------------------------------------------------------------
   The join the brief describes: text attributes from the warehouse, file
   location from the lake, employee_code as the link between them.

   LEFT JOIN, because an employee without a photograph is still an employee.
   A consumer checks whether file_path is empty rather than losing the row.
   --------------------------------------------------------------------------- */
CREATE VIEW IF NOT EXISTS v_EmployeeProfile AS
SELECT
    e.employee_key,
    e.employee_alternate_key  AS employee_code,
    e.full_name,
    e.title,
    e.hire_date,
    e.parent_employee_key,
    g.country,
    g.city,
    p.file_path,
    p.file_name,
    p.size_bytes,
    p.content_type,
    p.ingested_at
FROM v_DimEmployees_Current AS e
LEFT JOIN DimGeography       AS g ON g.geography_key = e.geography_key
LEFT JOIN LakeEmployeePhotos AS p FINAL ON p.employee_code = e.employee_alternate_key;
