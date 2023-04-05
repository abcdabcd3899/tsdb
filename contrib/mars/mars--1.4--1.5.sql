-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION mars" to load this file. \quit

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
                IF ('{' || array_to_string(index_rec.indkey::smallint[], ',') || '}')::smallint[] = timekeys THEN
                    has_time_index = true;
                END IF;

                IF ('{' || array_to_string(index_rec.indkey::smallint[], ',') || '}')::smallint[] = array_cat(tagkeys, timekeys) THEN
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
