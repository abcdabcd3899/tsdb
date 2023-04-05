-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION mars" to load this file. \quit

CREATE OR REPLACE FUNCTION
    mars.__list_with_options(rel regclass)
RETURNS TABLE(key text, value text) AS $$
DECLARE
BEGIN
    RETURN QUERY SELECT substr(kv, 1, strpos(kv, '=') - 1)
                      , substr(kv, strpos(kv, '=') + 1)
                   FROM (SELECT unnest(reloptions) AS kv
                           FROM pg_class
                          WHERE oid = rel) AS t;
END;
$$ LANGUAGE plpgsql STRICT VOLATILE;

CREATE OR REPLACE FUNCTION
    mars.__quote_and_join_names(names name[], sep text default ',')
RETURNS text AS $$
DECLARE
    result      text;
BEGIN
    SELECT string_agg(key, sep)
      FROM (SELECT quote_ident(unnest(names)) AS key) AS t
      INTO result;

    RETURN result;
END;
$$ LANGUAGE plpgsql STRICT VOLATILE;

CREATE OR REPLACE FUNCTION
    mars.__key_names_to_attnums(rel regclass, names name[])
RETURNS smallint[] AS $$
DECLARE
    result      smallint[];
    key         name;
    num         smallint;
BEGIN
    -- do not query pg_attribute with "IN names", it is not promised to output
    -- the attnums in the same order with the names.
    FOREACH key IN ARRAY names
    LOOP
        SELECT attnum
          FROM pg_attribute
         WHERE attrelid = rel
           AND attname = key
          INTO num;
        result := array_append(result, num);
    END LOOP;

    RETURN result;
END;
$$ LANGUAGE plpgsql STRICT VOLATILE;

CREATE OR REPLACE FUNCTION mars.__get_with_option_as_text(rel regclass)
RETURNS text AS $$
DECLARE
    result      text;
BEGIN
    SELECT string_agg(format('%s=%s', key, quote_ident(value)), ', ')
      FROM mars.__list_with_options(rel)
      INTO result;

    RETURN result;
END;
$$ LANGUAGE plpgsql STRICT VOLATILE;

CREATE OR REPLACE FUNCTION mars.__get_tagkey_names(rel REGCLASS)
RETURNS name[] AS $$
DECLARE
    option      text;
BEGIN
    SELECT value
      FROM mars.__list_with_options(rel)
     WHERE key = 'tagkey'
      INTO option;

    IF option IS NULL THEN
        RETURN NULL;
    END IF;

    BEGIN
        -- try to parse in the format '{tag1, tag2, ...}'
        RETURN option::name[];
    EXCEPTION WHEN OTHERS THEN
        -- it is in the format 'tag1'
        RETURN ARRAY[option]::name[];
    END;
END;
$$ LANGUAGE plpgsql STRICT VOLATILE;

CREATE OR REPLACE FUNCTION mars.__get_timekey_names(rel regclass)
RETURNS name[] AS $$
DECLARE
    option      text;
    result      name[];
BEGIN
    SELECT value
      FROM mars.__list_with_options(rel)
     WHERE key = 'timekey'
      INTO option;

    IF option IS NULL THEN
        RETURN NULL;
    END IF;

    BEGIN
        -- try to parse in one of the formats:
        -- * '{ts1 in ..., ts2 in ..., ...}'
        -- * '{ts1, ts2, ...}'
        SELECT array_agg(key)
          FROM (SELECT split_part(unnest(option::text[]),
                                  ' in ', 1)::text AS key) AS t
          INTO result;
    EXCEPTION WHEN OTHERS THEN
        -- it is in the format 'tag1'
        result := array[option]::name[];
    END;

    RETURN result;
END;
$$ LANGUAGE plpgsql STRICT VOLATILE;

CREATE OR REPLACE FUNCTION mars.merge_relation_(parent_rel REGCLASS, mars_rel REGCLASS, heap_rel REGCLASS)
RETURNS VOID AS $$
DECLARE
    template        REGCLASS;
    tagnames        name[];
    timenames       name[];
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

    tagnames := mars.__get_tagkey_names(template);
    timenames := mars.__get_timekey_names(template);

    EXECUTE format('LOCK %s IN EXCLUSIVE MODE', heap_rel);

    EXECUTE format('INSERT INTO %s SELECT * FROM %s ORDER BY %s',
                   mars_rel, heap_rel,
                   mars.__quote_and_join_names(array_cat(tagnames, timenames)));

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
        tagnames         name[];
        timenames        name[];
        with_option      text;
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

    with_option := mars.__get_with_option_as_text(template);

    tagnames := mars.__get_tagkey_names(template);
    timenames := mars.__get_timekey_names(template);

    -- fetch all distribution key from partition relation
    SELECT mars.fetch_distkey(template) INTO distkey;

    -- fetch columns information
    FOR attrs IN cursCol LOOP
            columns := concat(columns, format(', COLUMN %s %s', quote_ident(attrs.attname), attrs.encoding));
        END LOOP;

    EXECUTE format ('CREATE TABLE %I.%I (like %s INCLUDING DEFAULTS INCLUDING GENERATED %s) using mars with (%s) distributed by (%s)',
                    relns,
                    marstablename, -- namespace.marstable
                    template,
                    columns,
                    with_option,
                    distkey);

    -- data transfer
    EXECUTE format ('INSERT INTO %I.%I SELECT * FROM %s ORDER BY %s',
                    relns,
                    marstablename,
                    heap_rel,
                    mars.__quote_and_join_names(array_cat(tagnames, timenames)));

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

CREATE OR REPLACE FUNCTION mars.add_partition(partition_rel regclass, starttm timestamp, finishtm timestamp, period interval)
    RETURNS TABLE(partion_name text, option text) AS $$
DECLARE
    template_mars_rel      regclass;
    partition_namespace    name;
    partition_relname      name;
    tagnames               name[];
    timenames              name[];
    tagkeys                smallint[];
    timekeys               smallint[];
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
        tagnames := mars.__get_tagkey_names(template_mars_rel);
        timenames := mars.__get_timekey_names(template_mars_rel);

        tagkeys := mars.__key_names_to_attnums(template_mars_rel, tagnames);
        timekeys := mars.__key_names_to_attnums(template_mars_rel, timenames);

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

        FOR index_rec IN SELECT indnatts, indkey
                         FROM (pg_index INNER JOIN pg_class ON indexrelid = oid AND indrelid = heaptable)
                                  INNER JOIN pg_am ON pg_am.oid = pg_class.relam AND pg_am.amname = 'btree'
                         WHERE indislive = true LOOP
                IF index_rec.indkey::smallint[] = timekeys THEN
                    has_time_index = true;
                END IF;

                IF index_rec.indkey::smallint[] = array_cat(tagkeys, timekeys) THEN
                    has_tag_index = true;
                END IF;
            END LOOP;

        IF NOT has_tag_index THEN
            EXECUTE format('CREATE INDEX ON %s (%s)', heaptable,
                           mars.__quote_and_join_names(array_cat(tagnames,
                                                                 timenames)));
        END IF;

        IF NOT has_time_index THEN
            EXECUTE format('CREATE INDEX ON %s (%s)', heaptable,
                           mars.__quote_and_join_names(timenames));
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
