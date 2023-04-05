/*-------------------------------------------------------------------------
 *
 * 00_main.c
 *	  The Coding Style In MatrixDB.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 *	  contrib/mxdb_coding_style/00_main.c
 *
 * NOTES
 *	  This is not a real project, it is used to demonstrate and explain the
 *	  coding style in MatrixDB.
 *
 *	  The rule of thumb: check the surrounding code and try to imitate it.
 *
 *	  This rule is actually stolen from the GLib project [1], but I think it
 *	  fits MatrixDB quite well.  MatrixDB is not a new project, it is based on
 *	  Greenplum, which is based on PostgreSQL, and PostgreSQL contains code old
 *	  and new, so you can find kinds of different coding styles in the code, so
 *	  when hacking existing code, always try to follow the same style used in
 *	  your context, follow the lines before and after your changes.
 *
 *	  Before we jump in to the details, I strongly recommend reading some
 *	  coding style guides in other projects, you do not need to care about
 *	  their styles, but you should take some questions with you during the
 *	  reading: What is coding style?  why is it important?
 *
 *	  - GLib [1], the GNOME foundation library
 *	  - Linux Kernel [2]
 *	  - WebKit [3], a browser engine
 *
 *	  [1]: https://developer.gnome.org/programming-guidelines/stable/c-coding-style.html
 *	  [2]: https://github.com/torvalds/linux/blob/master/Documentation/process/coding-style.rst
 *	  [3]: https://webkit.org/code-style-guidelines/
 *
 *	  In PostgreSQL, there is also a documented coding convention [4], but most
 *	  of its content is on error message style, not very useful to us.
 *
 *	  However, when reading the code and patches, we can get the sense on how
 *	  is PostgreSQL code formatted, we will try to document them in this file.
 *	  Keep in mind, if the code hacking by you is using a different style,
 *	  follow it instead of this document.
 *
 *	  [4]: https://www.postgresql.org/docs/12/source.html
 *
 *	  On the other hand, there is a tool, src/tools/pgindent, which is used to
 *	  "maintain uniform layout style in our C and Perl code", "at least once in
 *	  each development cycle", according to its README.  So in theory all the
 *	  PostgreSQL coding styles are defined in this tool.  We may leverage it to
 *	  format the code style automatically, however it always makes sense for a
 *	  developer to understand the underlying conventions and ideas.
 *
 *	  Please read the "*.c" files under this dir for details.
 *
 *	  VIM TIPS: The PostgreSQL coding style uses both tabs and spaces, to see
 *	  where are they used, first open a "*.c" file, v-split the window by
 *	  pressing "ctrl-w v", then ":set list" in one of the splited window, and
 *	  ":set nolist" in the other.  For example, the current line contains one
 *	  tab, where is it and why?
 *
 *-------------------------------------------------------------------------
 */

/*
 * TODO:
 * - Naming
 * - Functions
 *   - Global functions
 *   - Local functions
 *   - Macros
 * - Miscs
 *   - Lists
 *   - Try-catch
 */
