-- 030: additive v2 endpoint/set -> Source relationship store (oxjob #83.13)
--
-- Jason approved exactly one Source per (endpoint, set) route, with one Source
-- allowed to own many routes. This migration expands the schema beside all
-- three transitional relationship surfaces. It deliberately performs no seed,
-- link, reader/writer cutover, endpoint mutation, or legacy-store contraction.
--
-- pmh_set is an opaque route discriminator. NULL is the whole-endpoint/base
-- route; non-NULL values are scoped routes. The two partial unique indexes make
-- NULL/base uniqueness explicit and remain portable across supported PG14/17.
-- route_id is only a stable row identity; the logical route keys are enforced
-- independently. Active-endpoint coverage and set-resolution behavior belong
-- to the separately reviewed seed/cutover phases.

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
SET LOCAL search_path = pg_catalog, public, pg_temp;

CREATE TABLE public.endpoint_source_route (
    route_id        BIGINT GENERATED ALWAYS AS IDENTITY,
    endpoint_id     TEXT    NOT NULL,
    pmh_set         TEXT,
    source_id       BIGINT  NOT NULL,
    is_journal_host BOOLEAN NOT NULL DEFAULT FALSE,

    CONSTRAINT endpoint_source_route_pkey
        PRIMARY KEY (route_id),
    CONSTRAINT endpoint_source_route_endpoint_fkey
        FOREIGN KEY (endpoint_id)
        REFERENCES public.endpoint(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT endpoint_source_route_source_fkey
        FOREIGN KEY (source_id)
        REFERENCES public.sources(id)
        ON UPDATE RESTRICT ON DELETE RESTRICT,
    CONSTRAINT endpoint_source_route_pmh_set_nonblank
        CHECK (pmh_set IS NULL OR pg_catalog.btrim(pmh_set) <> '')
);

CREATE UNIQUE INDEX endpoint_source_route_base_key
    ON public.endpoint_source_route (endpoint_id)
    WHERE pmh_set IS NULL;

CREATE UNIQUE INDEX endpoint_source_route_set_key
    ON public.endpoint_source_route (endpoint_id, pmh_set)
    WHERE pmh_set IS NOT NULL;

CREATE INDEX endpoint_source_route_source_id_idx
    ON public.endpoint_source_route (source_id);

COMMENT ON TABLE public.endpoint_source_route IS
    'Canonical v2 endpoint/set-to-Source routes, expanded alongside transitional relationship stores. Empty on migration; seeded and cut over only under separate review.';
COMMENT ON COLUMN public.endpoint_source_route.route_id IS
    'Stable surrogate row identity; endpoint_id plus NULL/non-NULL pmh_set is the logical route key.';
COMMENT ON COLUMN public.endpoint_source_route.endpoint_id IS
    'Registry endpoint owning this route; endpoint deletion is restricted while the route exists.';
COMMENT ON COLUMN public.endpoint_source_route.pmh_set IS
    'Opaque set discriminator; NULL denotes the whole-endpoint/base route.';
COMMENT ON COLUMN public.endpoint_source_route.source_id IS
    'Exactly one Source selected for this logical route; Source deletion is restricted while the route exists.';
COMMENT ON COLUMN public.endpoint_source_route.is_journal_host IS
    'Route-scoped journal-host attribute reserved by the approved #805 relationship contract.';

REVOKE ALL PRIVILEGES ON TABLE public.endpoint_source_route FROM PUBLIC;
REVOKE ALL PRIVILEGES ON SEQUENCE public.endpoint_source_route_route_id_seq FROM PUBLIC;
