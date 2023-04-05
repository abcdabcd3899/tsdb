--
-- Verify mars tables of different file formats.
--

\set t_auto test_mars_file_format_t1_auto
\set t_v0_1 test_mars_file_format_t1_v0_1
\set t_v1_0 test_mars_file_format_t1_v1_0

\set encoding 'encoding (compresstype=lz4)'

set mars.default_file_format to 'auto';
\set tname :t_auto
\i sql/file_format.template

set mars.default_file_format to 'v0.1';
\set tname :t_v0_1
\i sql/file_format.template

set mars.default_file_format to 'v1.0';
\set tname :t_v1_0
\i sql/file_format.template

reset mars.default_file_format;

--
-- Verify mars tables of different aux table formats.
--

\set t_aux_auto test_mars_auxfile_format_t2_auto
\set t_aux_v1 test_mars_auxfile_format_t2_v1
\set t_aux_v2 test_mars_auxfile_format_t2_v2

set mars.default_auxfile_format to 'auto';
\set tname :t_aux_auto
\i sql/file_format.template

set mars.default_auxfile_format to 'v1';
\set tname :t_aux_v1
\i sql/file_format.template

set mars.default_auxfile_format to 'v2';
\set tname :t_aux_v2
\i sql/file_format.template

-- vi: et :
