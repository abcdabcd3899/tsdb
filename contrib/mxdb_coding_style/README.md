The MatrixDB Coding Style
=========================

In MatrixDB we basically follow the Postgres coding style, which is described
in `*.c` source files under this directory.

The rule of thumb is: check the surrounding code and try to imitate it.

We have loosed one rule to make it easier to follow: we do not strictly follow
the "align on column 12" rule, explained in `07_indentation.c`.  But this
looseness is only allowed when writing new code, if you are hacking existing
postgres/greenplum source files, follow the rule of thumb.

It is strongly recommended to adjust the settings of your favorite editors and
let them format the code for you.  We have provided the settings for some
editors in the `editors/` directory.
