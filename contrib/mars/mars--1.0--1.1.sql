-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION mars" to load this file. \quit

CREATE OR REPLACE FUNCTION mars.preagg_sum(rel regclass)
    RETURNS TABLE(segid int, batch int, colid int, sum text)
AS '$libdir/mars', 'mars_preagg_sum'
    LANGUAGE C STRICT EXECUTE ON ALL SEGMENTS;

CREATE OR REPLACE FUNCTION mars.build_timeseries_table(rel REGCLASS, opts TEXT, default_compression BOOLEAN = true)
    RETURNS BOOLEAN AS $$
DECLARE
    cursCol           CURSOR FOR SELECT attname, atttypid, atttypmod, attnum FROM pg_attribute WHERE attrelid = rel::Oid AND attnum > 0 ORDER BY attnum;
    relns             name;
    relnm             name;
    mars_template_rel name;
    attrs             RECORD;
    partition_column  name;
    columns           text := '';
    distkey           text := '';
    encoding          text := '';
    typid             oid;
BEGIN
    EXECUTE format('LOCK %s IN ACCESS EXCLUSIVE MODE', rel);

    -- Check if rel is a partition table
    IF NOT EXISTS(SELECT * FROM pg_partitioned_table WHERE partrelid = rel) THEN
        RAISE EXCEPTION '% is not partition table', rel;
    END IF;

    -- Check if rel is partition by range
    IF NOT EXISTS(SELECT * FROM pg_partitioned_table WHERE partrelid = rel AND partstrat = 'r') THEN
        RAISE EXCEPTION '% is not partition by range', rel;
    END IF;

    IF NOT 1 = (SELECT partnatts FROM pg_partitioned_table WHERE partrelid = rel AND partstrat = 'r') THEN
        RAISE EXCEPTION '% is not partition by a timestamp column', rel;
    END IF;

    SELECT atttypid, attname FROM pg_partitioned_table ppt
                                      INNER JOIN pg_attribute pa ON ppt.partrelid = pa.attrelid
        AND pa.attnum = ANY(ppt.partattrs)
    WHERE partrelid = rel
    INTO typid, partition_column;

    IF typid NOT IN ('timestamp'::regtype::OID, 'timestamptz'::regtype::OID) THEN
        RAISE EXCEPTION '% is not partition by a timestamp column', rel;
    END IF;

    IF EXISTS(SELECT * FROM pg_inherits WHERE inhparent = rel ) THEN
        RAISE EXCEPTION '% requires to be an empty partition table, which does not contain any sub-partition.', rel;
    END IF;

    -- fetch relation namespace
    SELECT nspname, pg_class.relname
    FROM pg_class, pg_namespace
    WHERE pg_class.relnamespace = pg_namespace.oid
      AND pg_class.oid = rel
    INTO relns, relnm;

    IF relns = 'mars' THEN
        RAISE EXCEPTION '% can not define in mars schema', rel;
    END IF;

    -- so far all types default compression ars lz4
    IF default_compression THEN
        encoding := '(compresstype=lz4)';
    ELSE
        encoding := '(compresstype=none)';
    END IF;

    -- fetch all distribution key from partition relation
    SELECT mars.fetch_distkey(rel) INTO distkey;

    -- fetch all columns information from partition relation
    FOR attrs IN cursCol LOOP
        columns := concat(columns, format(', COLUMN %I encoding %s', attrs.attname, encoding));
    END LOOP;

    -- create mars template
    SELECT mars.fetch_template_table_name(rel) INTO mars_template_rel;
    EXECUTE format('CREATE TABLE %s (like %s INCLUDING DEFAULTS INCLUDING GENERATED %s) using mars with (%s) distributed by (%s)', mars_template_rel, rel, columns, opts, distkey);

    RETURN true;
END;
$$ LANGUAGE plpgsql VOLATILE;

-- fetch_target_mars_rel
-- give a partition root relation and a heap relation to find a mars which related by the bucket
-- return NULL if can not find a matched mars table.
--
-- prel: partition root relation
-- hrel: heap relation
-- bucket: the time_bucket interval which heap relation target to.
CREATE OR REPLACE FUNCTION mars.fetch_target_mars_rel(prel REGCLASS, hrel REGCLASS, bucket INTERVAL)
    RETURNS REGCLASS AS $$
DECLARE
    h_boundary  matrixts_internal.apm_boundary;
    m_boundary  matrixts_internal.apm_boundary;
    rec         RECORD;
BEGIN
    IF bucket = '0'::interval THEN
        RETURN NULL;
    END IF;

    h_boundary := matrixts_internal.apm_partition_boundary(hrel);

    -- bucket must completely cover heap two boundary
    IF bucket < (h_boundary.tots - h_boundary.fromts)::INTERVAL THEN
        RETURN NULL;
    END IF;

    FOR rec IN SELECT inhrelid
               FROM (SELECT pg_class.relam, pg_inherits.inhrelid
                     FROM (pg_inherits INNER JOIN pg_class ON pg_class.oid = pg_inherits.inhrelid)
                     WHERE inhparent = prel) as A(relam, inhrelid) INNER JOIN pg_am ON pg_am.oid = A.relam
               WHERE amname = 'mars' LOOP

        m_boundary := matrixts_internal.apm_partition_boundary(rec.inhrelid);

        -- compare both table left boundary, if the mars' time bucket is equal to heap time bucket
        -- and two table are adjacent without any gap.
        IF time_bucket(bucket, m_boundary.fromts) = time_bucket(bucket, h_boundary.fromts)
               AND (m_boundary.tots = h_boundary.fromts OR m_boundary.fromts = h_boundary.tots) THEN
            RETURN rec.inhrelid::regclass;
        END IF;
    END LOOP;

    RETURN NULL;
END
$$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION mars.merge_relation_(parent_rel REGCLASS, mars_rel REGCLASS, heap_rel REGCLASS)
RETURNS VOID AS $$
DECLARE
    template        REGCLASS;
    tagcol          text;
    timecol         text;
    m_boundary      matrixts_internal.apm_boundary; -- mars relation boundary
    h_boundary      matrixts_internal.apm_boundary; -- heap relation boundary
    fromts          timestamptz;
    tots            timestamptz;
BEGIN
    m_boundary := matrixts_internal.apm_partition_boundary(mars_rel);
    h_boundary := matrixts_internal.apm_partition_boundary(heap_rel);
    fromts = LEAST(m_boundary.fromts, h_boundary.fromts);
    tots = GREATEST(m_boundary.tots, h_boundary.tots);

    SELECT mars.fetch_template_table_name(parent_rel::oid)::regclass INTO template;

    SELECT replace(kv, 'timekey=', '')
    FROM (SELECT unnest(reloptions) kv FROM pg_class WHERE oid = template) t
    WHERE kv LIKE 'timekey=%' INTO timecol;

    SELECT replace(kv, 'tagkey=', '')
    FROM (SELECT unnest(reloptions) kv FROM pg_class WHERE oid = template) t
    WHERE kv LIKE 'tagkey=%' INTO tagcol;


    EXECUTE format('LOCK %s IN EXCLUSIVE MODE', heap_rel);

    EXECUTE format('INSERT INTO %s SELECT * FROM %s ORDER BY %I, %I', mars_rel, heap_rel, tagcol, timecol);

    EXECUTE format('LOCK %s IN ACCESS EXCLUSIVE MODE', parent_rel);
    EXECUTE format('DROP TABLE %s', heap_rel);
    EXECUTE format('ALTER TABLE %s DETACH PARTITION %s', parent_rel, mars_rel);
    EXECUTE format('ALTER TABLE %s ATTACH PARTITION %s FOR VALUES FROM(''%s'') TO (''%s'')', parent_rel, mars_rel, fromts, tots);
END
$$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION mars.compress_relation_(parent_rel REGCLASS, heap_rel REGCLASS)
    RETURNS text AS $$
DECLARE
    -- template relation
        template         regclass;
    -- partition relation
        parentrelname    name;
    -- partition relation's namespace
        relns            name;
        distkey          text := '';
    -- mars options
        tagcol           text;
        timecol          text;
        timebucket       interval;
        partition_option text;
    -- mars table columns
        columns          text := '';
        attrs            RECORD;
    -- mars table name and return value
        marstablename    text;

        cursCol CURSOR FOR
            SELECT
                attname
                 , atttypid
                 , atttypmod
                 , format('encoding (%s)', opt) AS encoding
            FROM (
                     SELECT
                         pat.attname
                          , pat.atttypid
                          , pat.atttypmod
                          , pat.attnum
                          , unnest(pae.attoptions) AS opt
                     FROM pg_attribute AS pat
                              INNER JOIN pg_attribute_encoding AS pae
                                         ON pat.attrelid = pae.attrelid AND pat.attnum = pae.attnum
                     WHERE pat.attrelid = template AND pat.attnum > 0
                     ORDER BY attnum
                 ) AS sub
            WHERE opt LIKE 'compresstype=%';
BEGIN
    EXECUTE format('LOCK %s IN EXCLUSIVE MODE', heap_rel);

    -- fetch relation namespace and relname
    SELECT nspname
    FROM pg_class, pg_namespace
    WHERE pg_class.relnamespace = pg_namespace.oid AND pg_class.oid = heap_rel
    INTO relns;

    SELECT pg_class.relname
    FROM pg_class
    WHERE pg_class.oid = parent_rel
    INTO parentrelname;

    SELECT mars.fetch_template_table_name(parent_rel::oid)::regclass INTO template;

    SELECT mars.create_new_name(parentrelname, false) INTO marstablename;

    SELECT quote_ident(replace(kv, 'timekey=', ''))
    FROM (SELECT unnest(reloptions) kv FROM pg_class WHERE oid = template) t
    WHERE kv LIKE 'timekey=%' INTO timecol;

    SELECT quote_ident(replace(kv, 'tagkey=', ''))
    FROM (SELECT unnest(reloptions) kv FROM pg_class WHERE oid = template) t
    WHERE kv LIKE 'tagkey=%' INTO tagcol;

    SELECT quote_ident(replace(kv, 'timebucket=', ''))::interval
    FROM (SELECT unnest(reloptions) kv FROM pg_class WHERE oid = template) t
    WHERE kv LIKE 'timebucket=%' INTO timebucket;

    -- fetch all distribution key from partition relation
    SELECT mars.fetch_distkey(template) INTO distkey;

    -- fetch columns information
    FOR attrs IN cursCol LOOP
            columns := concat(columns, format(', COLUMN %s %s', quote_ident(attrs.attname), attrs.encoding));
        END LOOP;

    EXECUTE format ('CREATE TABLE %I.%I (like %s INCLUDING DEFAULTS INCLUDING GENERATED %s) using mars with (tagkey=%s, timekey=%s, timebucket="%s") distributed by (%s)',
                    relns,
                    marstablename, -- namespace.marstable
                    template,
                    columns,
                    tagcol,
                    timecol,
                    timebucket::text,
                    distkey);

    -- data transfer
    EXECUTE format ('INSERT INTO %I.%I SELECT * FROM %s ORDER BY %s, %s',
                    relns,
                    marstablename,
                    heap_rel,
                    tagcol,
                    timecol);

    SELECT pg_get_expr(relpartbound, oid, true)
    FROM pg_class
    WHERE oid = heap_rel INTO partition_option;

    EXECUTE format('LOCK %s IN ACCESS EXCLUSIVE MODE', parent_rel);

    -- delete heap table
    EXECUTE format ('DROP TABLE %s', heap_rel);

    -- attach mars table
    SET mx_partition_without_validation TO ON;
    EXECUTE format ('ALTER TABLE %s ATTACH PARTITION %I.%I %s',
                    parent_rel,       -- parent table
                    relns,
                    marstablename,   -- namespace.marstable
                    partition_option -- FOR VALUES...
        );
    RESET mx_partition_without_validation;

    RETURN concat(quote_ident(relns), '.', quote_ident(marstablename));
END
$$ LANGUAGE plpgsql VOLATILE;


DROP FUNCTION mars.compress_partition(rel REGCLASS);

CREATE OR REPLACE FUNCTION mars.compress_partition(heap_rel REGCLASS, bucket INTERVAL = '0'::INTERVAL)
    RETURNS text AS $$
DECLARE
    parent_rel       REGCLASS;
    mars_rel         REGCLASS;
BEGIN
    -- Check if rel is sub-partition table
    IF NOT EXISTS(SELECT inhparent FROM pg_inherits WHERE inhrelid = heap_rel) THEN
        RAISE EXCEPTION '% is not in any time series table', heap_rel;
    ELSE
        SELECT inhparent FROM pg_inherits WHERE inhrelid = heap_rel INTO parent_rel;
    END IF;

    -- Check partition table is timeseries table
    IF NOT EXISTS(SELECT * FROM pg_class WHERE oid = (mars.fetch_template_table_name(parent_rel::oid)::regclass)) THEN
        RAISE EXCEPTION '% does not have time series template table', parent_rel;
    END IF;

    SELECT mars.fetch_target_mars_rel(parent_rel, heap_rel, bucket) into mars_rel;
    SET mx_partition_without_validation TO ON;
    IF mars_rel IS NULL THEN
        mars_rel := mars.compress_relation_(parent_rel, heap_rel);
    ELSE
        PERFORM mars.merge_relation_(parent_rel, mars_rel, heap_rel);
    END IF;
    RESET mx_partition_without_validation;

    RETURN mars_rel::text;
END
$$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION mars.add_partition(partition_rel regclass, starttm timestamp, finishtm timestamp, period interval)
    RETURNS TABLE(partion_name text, option text) AS $$
DECLARE
    template_mars_rel      regclass;
    partition_namespace    name;
    partition_relname      name;
    tagcol                 name;
    timecol                name;
    tag_attnum             smallint;
    time_attnum            smallint;
    has_tag_index          boolean := false;
    has_time_index         boolean := false;
    endtm                  timestamp;
    heaptable_name         name;
    heaptable              regclass;
    index_rec              RECORD;
    distkey                text := '';
BEGIN
    EXECUTE format('LOCK %s IN ACCESS EXCLUSIVE MODE', partition_rel);

    -- fetch relation namespace and relname
    SELECT nspname, pg_class.relname
    FROM pg_class, pg_namespace
    WHERE pg_class.relnamespace = pg_namespace.oid AND pg_class.oid = partition_rel
    INTO partition_namespace, partition_relname;

    BEGIN
        SELECT mars.fetch_template_table_name(partition_rel::oid)::regclass INTO template_mars_rel;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION '% is not a time-series partition table.', partition_rel;
    END;

    LOOP
        EXIT WHEN starttm >= finishtm;

        -- fetch current partition range
        IF starttm + period <= finishtm THEN
            endtm := starttm + period;
        ELSE
            endtm := finishtm;
        END IF;

        -- fetch all distribution key from partition relation
        SELECT mars.fetch_distkey(template_mars_rel) INTO distkey;

        -- fetch MARS options from template relation
        SELECT replace(kv, 'timekey=', '')
        FROM (SELECT unnest(reloptions) kv FROM pg_class WHERE oid = template_mars_rel) t
        WHERE kv LIKE 'timekey=%' INTO timecol;

        SELECT replace(kv, 'tagkey=', '')
        FROM (SELECT unnest(reloptions) kv FROM pg_class WHERE oid = template_mars_rel) t
        WHERE kv LIKE 'tagkey=%' INTO tagcol;

        -- format relation name
        SELECT mars.create_new_name(partition_relname, true) INTO heaptable_name;

        -- create relation as template partition relation's sub-partition
        EXECUTE format('CREATE TABLE %I.%I PARTITION OF %s FOR VALUES FROM (''%s'') TO (''%s'')',
                       partition_namespace,
                       heaptable_name, -- namespace.heaptable
                       partition_rel,
                       starttm,
                       endtm);

        heaptable = (quote_ident(partition_namespace) ||'.' || quote_ident(heaptable_name))::regclass;

        SELECT attnum FROM pg_attribute WHERE attrelid = heaptable AND attname = tagcol INTO tag_attnum;
        SELECT attnum FROM pg_attribute WHERE attrelid = heaptable AND attname = timecol INTO time_attnum;

        FOR index_rec IN SELECT indnatts, indkey
                         FROM (pg_index INNER JOIN pg_class ON indexrelid = oid AND indrelid = heaptable)
                                  INNER JOIN pg_am ON pg_am.oid = pg_class.relam AND pg_am.amname = 'btree'
                         WHERE indislive = true LOOP
                IF index_rec.indnatts > 0 AND index_rec.indkey[0] = time_attnum THEN
                    has_time_index = true;
                END IF;

                IF index_rec.indnatts > 1 AND index_rec.indkey[0] = tag_attnum AND index_rec.indkey[1] = time_attnum THEN
                    has_tag_index = true;
                END IF;
            END LOOP;

        IF NOT has_tag_index THEN
            EXECUTE format ('CREATE INDEX ON %s (%I, %I)', heaptable, tagcol, timecol);
        END IF;

        IF NOT has_time_index THEN
            EXECUTE format ('CREATE INDEX ON %s (%I DESC)', heaptable, timecol);
        END IF;

        -- update start timestamp
        starttm := endtm;
        partion_name := quote_ident(heaptable_name);

        SELECT pg_get_expr(relpartbound, oid, true)
        FROM pg_class
        WHERE oid = heaptable INTO option;

        RETURN NEXT;
    END LOOP;
END
$$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION mars.destroy_timeseries_table(partition_rel regclass)
    RETURNS BOOLEAN AS $$
DECLARE
    heap_template_rel name;
BEGIN
    SELECT mars.fetch_template_table_name(partition_rel::oid) INTO heap_template_rel;

    EXECUTE format('DROP TABLE %s', heap_template_rel);
    RETURN true;
EXCEPTION WHEN others THEN
    RETURN false;
END
$$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION mars.drop_catalog()
RETURNS event_trigger
LANGUAGE plpgsql AS $$
DECLARE
    _dropped RECORD;
    template REGCLASS;
BEGIN
    FOR _dropped IN SELECT * FROM pg_event_trigger_dropped_objects() LOOP
        IF _dropped.original THEN
            BEGIN
                SELECT mars.fetch_template_table_name(_dropped.objid)::regclass INTO template;
                PERFORM mars.destroy_timeseries_table(_dropped.objid);
            EXCEPTION WHEN others THEN
            END;
        END IF;
    END LOOP;
END;
$$;

CREATE EVENT TRIGGER try_drop_dependent ON sql_drop
EXECUTE FUNCTION mars.drop_catalog();
