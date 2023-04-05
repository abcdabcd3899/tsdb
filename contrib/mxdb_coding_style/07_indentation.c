/*-------------------------------------------------------------------------
 *
 * 07_indentation.c
 *	  The Coding Style In MatrixDB - Indentation and Alignment.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 *	  contrib/mxdb_coding_style/07_indentation.c
 *
 * NOTES
 *	  PostgreSQL has a complex and unique indentation and alignment style, it
 *	  is hard to explain it to people, it is hard to describe the style in
 *	  3rd-party formating tools, such as clang-format and CLion.
 *
 *	  Many of us do not like the style, including me.  The good thing is, you
 *	  do not need to love it, you do not need to like it, you just need to
 *	  follow it.  As long as we all follow the same style, it makes the code
 *	  easy to read and maintain.
 *
 *	  As mentioned in "00_main.c", PostgreSQL comes with a tool,
 *	  "src/tools/pgindent", which is used to re-format the code layout, so it
 *	  is the source of the indentation and alignment conventions.  We should
 *	  consider intergrating it to our committing or merging process; but at
 *	  least we, the human, should know the conventions, and try our best to
 *	  follow it manually, so there is no surprise when seeing the re-formatted
 *	  code.
 *
 *	  In this guide I will try to describe my understandings on the indentation
 *	  and alignment conventions while reading existing code, it might not be
 *	  100% the same with the "pgindent" tool, but at least not too far from it.
 *
 *	  VIM TIPS: Again, as mentioned in "00_main.c", you may find it useful to
 *	  view this file in 2 modes at the same time, one shows tabs as white
 *	  spaces, the other shows tabs as a notation.
 *
 *	  A high level rule is, always indent and align with tabs, and store tabs
 *	  as tabs instead of spaces.  This is documented in PostgreSQL Coding
 *	  Conventions [1]:
 *
 *	  > Source code formatting uses 4 column tab spacing, with tabs preserved
 *	  > (i.e., tabs are not expanded to spaces). Each logical indentation level
 *	  > is one additional tab stop.
 *
 *	  VIM TIPS: In vim you may want to put "set noet ts=4 sw=4" in your
 *	  "~/.vimrc".
 *
 *	  [1]: https://www.postgresql.org/docs/12/source.html
 *
 *	  Indentation is needed inside each "{}" blocks, as well as the "if", "for",
 *	  "while", "switch", "do ... while" blocks.
 *
 *	  When breaking a line at a function list or an expression, align at the
 *	  according "()", we will explain this in the following examples.
 *
 *	  VIM TIPS: you could put "set cino=(0" in your "~/.vimrc" to do the
 *	  alignment automatically.
 *
 *	  When the code can not be aligned with only tabs, pad 1~3 spaces after the
 *	  last tab.  When you see 4 or more spaces in your code, you probably need
 *	  to replace some of them with tabs.  This is not for function argument
 *	  list, but also for comments.  Remember the question we asked in
 *	  "00_main.c", "the current line contains one tab, where is it and why?",
 *	  the comments under a "NOTES" section have further indentation, that would
 *	  be 4 spaces, so the first 2 are replaced with a tab.
 *
 *	  As we are talking about tabs and spaces, there are also something to take
 *	  care of:
 *
 *	  - Do not leave tabs or spaces at end of line.  "git diff" can usually
 *		highlight this.  VIM TIPS: search for "/[ \t]$".
 *	  - Do not put tabs after a space.  "git diff" can also highlight this.
 *		VIM TIPS: search for "/ \t".
 *	  - Do not use 4 or more spaces together, convert some of them to tabs.
 *		VIM TIPS: search for "/ \{4,\}".
 *	  - There should be a LF, "\n", 0x0a, at end of file, this is ensured by
 *		most of the linux editors, but in some IDEs you have to manually put an
 *		empty line at end of the file.
 *
 *	  One special convention in PostgreSQL is on variable declarations, refer
 *	  to Func8() for details.
 *
 *-------------------------------------------------------------------------
 */

/*
 * BAD: Do not indent the return type.
 */
static void
Func1(int a, int b, int c, int d)
{
}

/*
 * BAD: Do not indent the function name.
 */
static void
Func2(int a, int b, int c, int d)
{
}

/*
 * BAD: Do not indent the function body.
 */
static void
Func3(int a, int b, int c, int d)
{
}

/*
 * BAD: Do not indent the function body.
 */
static void
Func4(int a, int b, int c, int d)
{
}

/*
 * BAD: The argument list should be aligned on the "(".
 */
static void
Func5(int a, int b,
	  int c, int d)
{
}

/*
 * BAD: Too many spaces are used for the alignment.
 */
static void
Func6(int a, int b,
	  int c, int d)
{
}

/*
 * GOOD: Align the argument list on the "(".
 *
 * The alignment is made with 1 tab and 2 spaces.
 */
static void
Func7(int a, int b,
	  int c, int d)
{
	/*
	 * GOOD: The arguments are aligned on the "(".
	 *
	 * The alignment is made with two tabs and 2 spaces.
	 */
	Func6(a, b,
		  c, d);

	/* BAD: Not aligned on the "(". */
	Func6(a, b,
		  c, d);

	/* BAD: Too many spaces are used for the alignment. */
	Func6(a, b,
		  c, d);

	/*
	 * GOOD: The alignment is correct.
	 *
	 * However it does not produce an easy-to-read code.
	 */
	if ((a == 0 ||
		 a == 1) &&
		(b == 0 ||
		 b == 1))
		return;

	/*
	 * GOOD: Still correct.
	 *
	 * But still hard to read.
	 */
	if ((a == 0 ||
		 a == 1) &&
		(b == 0 ||
		 b == 1))
		return;

	/* GOOD: Correct, and it is better than the above two. */
	if ((a == 0 || a == 1) &&
		(b == 0 || b == 1))
		return;

	/*
	 * BAD: Not aligned.
	 */
	if ((a == 0 ||
		 a == 1) &&
		(b == 0 ||
		 b == 1))
		return;

	/*
	 * BAD: Not aligned at the corresponding "(".
	 */
	if ((a == 0 || a == 1) && (b == 0 ||
							   b == 1))
		return;

	/*
	 * GOOD: The alignment is correct.
	 *
	 * However in practice avoid putting too many things in the "for" header.
	 */
	for (int i = 0, j = 0, k = 0;
		 j < 4 && k < 4;
		 ++i, j = i / 4, k = i % 4)
		break;

	switch (a)
	{
	/*
	 * GOOD: "case" and "default" have one additional indentation level
	 * than the "switch".
	 */
	case 0:
		/* GOOD: "case" body has one additional indentation level. */
		break;

	case 1:
	case 2:
		break;

	case 3:
		/*
		 * GOOD: "{}" as the "case" body also needs one additional
		 * indentation level.
		 */
		{
			/* GOOD: One additional indentation level inside "{}". */
			int x = 0;

			b = x;
		}
		break;

	default:
		break;
	}

	/* GOOD: Aligned on the "(". */
	a = ((b + c) *
		 (c + d));

	/*
	 * GOOD: Maybe.
	 *
	 * There is no clear document on how to align when breaking a line outside
	 * a "()".  In xmax_infomask_changed() the new line is aligned with no
	 * additional indentation.
	 */
	optimizer =
		a > 0 || b > 0 || a + b > c + d;

	/*
	 * GOOD: Maybe.
	 *
	 * However, in _bt_initmetapage() the new line is aligned with one
	 * additional indentation.
	 */
	optimizer =
		a > 0 || b > 0 || a + b > c + d;

	/*
	 * GOOD: Maybe.
	 *
	 * Moreover, in AlterPolicy() the "=" is put in the new line.
	 */
	optimizer = a > 0 || b > 0 || a + b > c + d;

	/*
	 * GOOD: Maybe.
	 *
	 * Personally I prefer this form, the "()" is unnecessary, but it helps to
	 * produce a nice code layout.
	 */
	optimizer = (a > 0 ||
				 b > 0 ||
				 a + b > c + d);

	/*
	 * GOOD: The "? :" operator requires no additional indentation.
	 *
	 * This form is seen in compute_infobits().
	 */
	a = (b > 0 ? c : d);

	/* GOOD: This form is suitable when "c" and "d" are complex expressions. */
	a = (b > 0 ? c : d);
}

/*
 * GOOD: No line-breaking is also good, and sometimes the best choice.
 */
static void
Func8(int a, int b, int c, int d)
{
	/*
	 * Declaring a variable is easy, but it requires special alignment in
	 * PostgreSQL.  Suppose the type begins at column 0, then the variable name
	 * should be aligned at column 12.  The alignment is also made with tabs.
	 */

	int i;
	const char c = '.';
	AttrNumber attrnum;
	/*	^			^
	 *	|			|
	 *	0123456789012
	 */

	/*
	 * This is also for pointers, and the "*" sign is put together with the
	 * variable names, so some spaces are needed to align pointers.
	 */
	int *ptr1;
	int **ptr2;
	/*	^			^
	 *	|			|
	 *	0123456789012
	 */

	/*
	 * If the type name is wider than (or equal to) 12 characters, no further
	 * alignment should be made.
	 */
	HeapScanDesc scan;
	HeapScanDescData scandata;
	/*	^			^
	 *	|			|
	 *	0123456789012
	 */

	/*
	 * A special case is that, if the type name width is equal to 11, a space
	 * is used to align the name instead of a tab.
	 */
	ScanKeyData scankey;
	/*	^			^
	 *	|			|
	 *	0123456789012
	 */
}
