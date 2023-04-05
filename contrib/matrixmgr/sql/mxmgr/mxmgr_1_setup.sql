-- cleanup
--
-- start_ignore
DROP DATABASE IF EXISTS matrixmgr;
-- end_ignore

CREATE DATABASE matrixmgr;

\c matrixmgr

--
-- setup
--

-- should fail due to no matrixts
CREATE EXTENSION matrixmgr;

-- should success
CREATE EXTENSION matrixmgr CASCADE;
