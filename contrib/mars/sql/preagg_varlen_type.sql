\set tname_auto test_mars_preagg_varlen_type_auto
\set tname_v1 test_mars_preagg_varlen_type_v1

-- default, for now it means v2
\set tname :tname_auto
\i sql/preagg_varlen_type.template

-- v1
set mars.default_auxfile_format to 'v1';
show mars.default_auxfile_format;

\set tname :tname_v1
\i sql/preagg_varlen_type.template
