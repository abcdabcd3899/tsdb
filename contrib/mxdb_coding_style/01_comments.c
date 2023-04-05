/*-------------------------------------------------------------------------
 *
 * 01_comments.c
 *	  The Coding Style In MatrixDB - Comments.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 *	  contrib/mxdb_coding_style/01_comments.c
 *
 * NOTES
 *	  In PostgreSQL C code we always use, and only use, the C style comments.
 *	  This is documented in PostgreSQL Coding Conventions [1]:
 *
 *	  > To maintain a consistent coding style, do not use C++ style comments
 *	  > (`//` comments).
 *
 *	  In multi-line comments, the beginning "/ *" and ending "* /" must be put
 *	  in separate lines.
 *
 *	  There should be at least one blank, usually a space, after the leading
 *	  "*".
 *
 *	  A function header comment is always in multi-line style.
 *
 *	  [1]: https://www.postgresql.org/docs/12/source.html
 *
 *-------------------------------------------------------------------------
 */

/*
 * GOOD: This is a multi-line comment block.
 *
 * Always use the C style comments.
 */

/* GOOD: This is a single-line comment. */

// BAD: Do not use the C++ style comments.

/* BAD: There must be separate
 * beginning and ending lines
 * in the multi-line style. */

/*
 *BAD: There is no blank after the leading "*".
 */

/*
 * GOOD: This is a function.
 */
static void
Func1(void)
{
}

/* BAD: Do not use single-line style for function header comments. */
static void
Func2(void)
{
}

/*
 * There are some special rules for comments.
 *
 * If a comment block has no indent, like this one, then it will not be touched
 * by "pgindent", what you see is just what you get.  This is useful in putting
 * some formatted content.
 *
 * Such as a list:
 *
 * - This is a list in Markdown style;
 * - It is convinent.
 *
 * Or a code block, also in Markdown style:
 *
 *		int
 *		main(void)
 *		{
 *			return 0;
 *		}
 *
 * Or even some ascii art:
 *
 *		┌───────────────────┐
 *		│  ╔═══╗ Some Text  │▒
 *		│  ╚═╦═╝ in the box │▒
 *		╞═╤══╩══╤═══════════╡▒
 *		│ ├──┬──┤           │▒
 *		│ └──┴──┘           │▒
 *		└───────────────────┘▒
 *		 ▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒▒
 */

static void
Func3(void)
{
	/*
	 * However when a comment block is indented, like this one, it will be
	 * considered as normal text, and be reformatted by "pgindent".
	 */

	/*
	 * BAD: this does not work as expected:
	 * - hello
	 * - world
	 *
	 * Neither this:
	 *
	 *		int
	 *		main(void)
	 *		{
	 *			return 0;
	 *		}
	 */

	/* The above comment will be reformatted as below. */

	/*
	 * BAD: this does not work as expected: - hello - world
	 *
	 * Neither this:
	 *
	 * int main(void) { return 0; }
	 */

	/*
	 * "pgindent" allows a comment block to be marked as verbatim text by
	 * putting one or more "-" after the comment beginning line, for example:
	 */

	/*-
	 * GOOD: Note the above line, now we are verbatim text.
	 *
	 * This works:
	 * - hello
	 * - world
	 *
	 * As well as this:
	 *
	 *		int
	 *		main(void)
	 *		{
	 *			return 0;
	 *		}
	 */

	/*----------------------------------
	 *
	 * GOOD: This is more commonly seen.
	 *
	 * This works:
	 * - hello
	 * - world
	 *
	 * As well as this:
	 *
	 *		int
	 *		main(void)
	 *		{
	 *			return 0;
	 *		}
	 *
	 *----------------------------------
	 */
}
