-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION mars" to load this file. \quit

CREATE FUNCTION mars_tableam_handler(INTERNAL) RETURNS table_am_handler
AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

CREATE ACCESS METHOD mars TYPE TABLE HANDLER mars_tableam_handler;

-- create mars schema and functions
CREATE SCHEMA IF NOT EXISTS mars;

CREATE OR REPLACE FUNCTION mars.ls(relname regclass, sno int)
  RETURNS TABLE(seg bigint, batch bigint, rowcount bigint[]) AS $$
DECLARE
  segrelid regclass;
BEGIN
  segrelid := (select mx_mars.segrelid from mx_mars where relid= relname::Oid);
  IF sno = -1 THEN
    RETURN QUERY EXECUTE 'select segno, batch, rowcount from '|| segrelid::name ||'' ;
  ELSE
    RETURN QUERY EXECUTE 'select segno, batch, rowcount from '|| segrelid::name || 'where segno = ' || sno;
  END IF;
end
$$ LANGUAGE plpgsql VOLATILE EXECUTE ON ALL SEGMENTS;

CREATE OR REPLACE FUNCTION mars.compress_info(relname regclass, sno int, batch bigint)
  RETURNS TABLE(SegNo int, Batch bigint, RowGroupId int, CompressionSize bigint, UncompressionSize bigint)
  AS '$libdir/mars', 'mars_compress_info'
  LANGUAGE C STRICT EXECUTE ON ALL SEGMENTS;

CREATE OR REPLACE FUNCTION mars.aux_size(rel regclass)
  RETURNS TABLE(segid int4, size text) AS $$
BEGIN
  RETURN QUERY EXECUTE 'SELECT gp_segment_id, pg_size_pretty(pg_relation_size(segrelid::regclass)) FROM mx_mars WHERE relid = ' || rel::Oid;
END
$$ LANGUAGE plpgsql VOLATILE EXECUTE ON ALL SEGMENTS;

-- typo name function
CREATE OR REPLACE FUNCTION mars.peragg_sum(rel regclass)
  RETURNS TABLE(segid int, batch int, colid int, sum text)
  AS '$libdir/mars', 'mars_preagg_sum'
  LANGUAGE C STRICT EXECUTE ON ALL SEGMENTS;

CREATE OR REPLACE FUNCTION mars.list_partition(rel regclass)
  RETURNS TABLE(relname text, storage name) AS $$
BEGIN
  RETURN QUERY EXECUTE format('SELECT inhrelid::regclass::text, amname ' ||
                              'FROM pg_class, pg_inherits, pg_am ' ||
                              'WHERE inhparent = %s AND inhrelid = pg_class.oid AND pg_am.oid = relam;', rel::oid);
END
$$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION mars.fetch_template_table_name(rel oid)
  RETURNS text AS $$
DECLARE
  template_name text;
BEGIN
  -- unquote relation name and format, then quote it if it is needed.
  SELECT concat('mars.',
                quote_ident(format('template_%s', rel))) INTO template_name;
  RETURN template_name;
END
$$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION mars.fetch_distkey(rel IN regclass)
  RETURNS text AS $$
DECLARE
  distkeys text;
BEGIN
  SELECT array_to_string(
    ARRAY(SELECT concat(quote_ident(attname), ' ', opcname)
          FROM pg_attribute AS attr
            , pg_opclass AS op
            , (
                SELECT unnest(distkey), unnest(distclass)
                FROM gp_distribution_policy
                WHERE localoid = rel
            ) AS d(key,class)
          WHERE op.oid = d.class
          AND attr.attnum = d.key
          AND attr.attrelid = rel
    ), ', '
  ) INTO distkeys;
  RETURN distkeys;
END
$$ LANGUAGE plpgsql VOLATILE;

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
  new_partition_oid oid;
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

  -- move original relation to partition template
  EXECUTE format('ALTER TABLE %s SET SCHEMA mars', rel);

  -- create a new partition using 'like' to rid of constraints which MARS does not support.
  EXECUTE format('CREATE TABLE %I.%I (like %s INCLUDING DEFAULTS INCLUDING GENERATED) partition by range (%I) distributed by (%s)',
    relns,
    relnm,            -- namespace.relname
    rel,              -- like ...
    partition_column, -- partition by
    distkey           -- distributed by
  );

  SELECT pg_class.oid
  FROM pg_class, pg_namespace
  WHERE pg_class.relnamespace = pg_namespace.oid
  AND pg_class.relname = relnm AND pg_namespace.nspname = relns
  INTO new_partition_oid;

  -- create mars template
  SELECT mars.fetch_template_table_name(new_partition_oid) INTO mars_template_rel;
  EXECUTE format('CREATE TABLE %s (like %s INCLUDING DEFAULTS INCLUDING GENERATED %s) using mars with (%s) distributed by (%s)', mars_template_rel, rel, columns, opts, distkey);

  RETURN true;
END;
$$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION mars.destroy_timeseries_table(partition_rel regclass)
  RETURNS BOOLEAN AS $$
DECLARE
  heap_template_rel name;
  mars_relname       text;
BEGIN
  EXECUTE format('LOCK %s IN ACCESS EXCLUSIVE MODE', partition_rel);

  SELECT quote_ident(pg_class.relname)
  FROM pg_class, pg_namespace
  WHERE pg_class.relnamespace = pg_namespace.oid
  AND pg_class.oid = partition_rel
  INTO mars_relname;

  SELECT mars.fetch_template_table_name(partition_rel::oid) INTO heap_template_rel;

  EXECUTE format('DROP TABLE %s', heap_template_rel);
  EXECUTE format('DROP TABLE mars.%s', mars_relname);
  RETURN true;
EXCEPTION WHEN others THEN
  RETURN false;
END
$$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION mars.create_new_name(relname text, isheap boolean)
  RETURNS text AS $$
DECLARE
  cut_length int := 55;
  part_ident text;
  part_name  text;
BEGIN
  IF isheap THEN
    part_name := '_1_prt_';
  ELSE
    part_name := '_1_mars';
  END IF;
  LOOP
    IF octet_length(substring(relname FROM 1 FOR cut_length)) > 55 THEN
      -- cut more bytes when multi-byte character in the name
      cut_length := cut_length - 1;
      CONTINUE;
    END IF;
    EXECUTE format($sub$
        WITH RECURSIVE newname(name, idx) AS (
            SELECT substring(%L FROM 1 FOR %s) || %L || 1, 1
          UNION ALL
            SELECT substring(%L FROM 1 FOR %s) || %L || newname.idx+1, newname.idx+1
            FROM pg_catalog.pg_class pc INNER JOIN newname ON pc.relname = newname.name
        )
        SELECT first_value(name) OVER (ORDER BY idx DESC)
        FROM newname LIMIT 1
    ;$sub$, relname, cut_length, part_name, relname, cut_length, part_name)
    INTO part_ident;

    IF octet_length(part_ident) > 63 THEN
      cut_length := cut_length - 1;
    ELSE
      EXIT;
    END IF;

    IF cut_length <= 0 THEN
      RAISE EXCEPTION 'cannot generate partition table name for %', relname;
    END IF;
  END LOOP;
  RETURN part_ident;
END
$$ LANGUAGE plpgsql VOLATILE ;

CREATE OR REPLACE FUNCTION mars.add_partition(partition_rel regclass, starttm timestamp, finishtm timestamp, period interval)
  RETURNS TABLE(partion_name text, option text) AS $$
DECLARE
  template_mars_rel      regclass;
  template_partition_rel regclass;
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
    SELECT format('mars.%I', partition_relname)::regclass INTO template_partition_rel;
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
      template_partition_rel,
      starttm,
      endtm);

    heaptable = (quote_ident(partition_namespace) ||'.' || quote_ident(heaptable_name))::regclass;

    -- swap to real partition
    EXECUTE format('ALTER TABLE %s DETACH PARTITION %I.%I',
      template_partition_rel, partition_namespace, heaptable_name);

    EXECUTE format('ALTER TABLE %s ATTACH PARTITION %I.%I FOR VALUES FROM (''%s'') TO (''%s'')',
      partition_rel, partition_namespace, heaptable_name, starttm, endtm);

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

CREATE OR REPLACE FUNCTION mars.add_partition(partition_rel regclass, starttm timestamp, period interval)
  RETURNS TABLE(partion_name text, option text) AS $$
BEGIN
  RETURN QUERY
  SELECT * from mars.add_partition(partition_rel, starttm, starttm + period, period);
END
$$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION mars.add_partition(partition_rel regclass, starttm timestamptz, finishtm timestamptz, period interval)
  RETURNS TABLE(partion_name text, option text) AS $$
BEGIN
  RETURN QUERY
  SELECT * from mars.add_partition(partition_rel, starttm::timestamp, finishtm::timestamp, period) ;
END
$$ LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION mars.compress_partition(rel regclass)
  RETURNS text AS $$
DECLARE
  -- template relation
  template         regclass;
  -- partition relation
  parentrel        regclass;
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
  EXECUTE format('LOCK %s IN EXCLUSIVE MODE', rel);

  -- Check if rel is sub-partition table
  IF NOT EXISTS(SELECT inhparent FROM pg_inherits WHERE inhrelid = rel) THEN
    RAISE EXCEPTION '% is not in any time series table', rel;
  ELSE
    SELECT inhparent FROM pg_inherits WHERE inhrelid = rel INTO parentrel;
  END IF;

  -- fetch relation namespace and relname
  SELECT nspname
  FROM pg_class, pg_namespace
  WHERE pg_class.relnamespace = pg_namespace.oid AND pg_class.oid = rel
  INTO relns;

  -- Check partition table is timeseries table
  IF NOT EXISTS(SELECT * FROM pg_class WHERE oid = (mars.fetch_template_table_name(parentrel::oid)::regclass)) THEN
    RAISE EXCEPTION '% does not have time series template table', parentrel;
  END IF;

  SELECT pg_class.relname
  FROM pg_class
  WHERE pg_class.oid = parentrel
  INTO parentrelname;

  SELECT mars.fetch_template_table_name(parentrel::oid)::regclass INTO template;

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
    rel,
    tagcol,
    timecol);

  SELECT pg_get_expr(relpartbound, oid, true)
  FROM pg_class
  WHERE oid = rel INTO partition_option;

  EXECUTE format('LOCK %s IN ACCESS EXCLUSIVE MODE', rel);

  -- delete heap table
  EXECUTE format ('DROP TABLE %s', rel);

  -- attach mars table
  EXECUTE format ('ALTER TABLE %s ATTACH PARTITION %I.%I %s',
    parentrel,       -- parent table
    relns,
    marstablename,   -- namespace.marstable
    partition_option -- FOR VALUES...
  );

  RETURN quote_ident(marstablename);
END
$$ LANGUAGE plpgsql VOLATILE;
