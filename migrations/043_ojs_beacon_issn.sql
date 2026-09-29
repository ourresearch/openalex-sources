-- 043 is_ojs from the PKP Beacon (oxjob #1424, Casey 2026-09-29).
--
-- sources.is_ojs was "any ISSN in the legacy ojs_journal table" (78K ISSNs of
-- undocumented vintage). It now follows the PKP Beacon, the telemetry every
-- Open Journal Systems install reports (Harvard Dataverse
-- doi:10.7910/DVN/OCZNVY, CC0): true when any source ISSN belongs to a journal
-- the Beacon has seen running OJS, dormant installs included. Loaded by hand
-- from data/ojs_beacon/ (jobs/load_ojs_beacon); the single writer of is_ojs is
-- sources_lib.recompute_is_ojs.
--
-- Only is_ojs changes. OJS is not a source list: nothing here touches
-- source_list or sources.listed_in. ojs_journal stays for its is_oa column,
-- which feeds is_oa_high_oa_rate in jobs/apply_oa_flags (unchanged).

CREATE TABLE IF NOT EXISTS ojs_beacon_issn (
    issn  TEXT PRIMARY KEY,   -- NNNN-NNNX
    name  TEXT                -- Beacon context name (informational)
);
COMMENT ON TABLE ojs_beacon_issn IS
  'oxjob #1424: ISSNs of journals the PKP Beacon has seen running Open Journal Systems. Drives sources.is_ojs (sources_lib.recompute_is_ojs). Full replace per Beacon edition (jobs/load_ojs_beacon).';

COMMENT ON COLUMN sources.is_ojs IS
  'Derived (sources_lib.recompute_is_ojs): any source ISSN is in ojs_beacon_issn (PKP Beacon). oxjob #1424.';
