/*-------------------------------------------------------------------------
 *
 * 05_white_space.c
 *	  The Coding Style In MatrixDB - White Spaces.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 *	  contrib/mxdb_coding_style/05_white_spaces.c
 *
 * NOTES
 *	  White spaces, such as spaces, tabs, as well as blank lines, are very
 *	  useful in increasing the code readability.  The PostgreSQL coding style
 *	  document does not mention them at all, but there does is a convention on
 *	  them throughout the code.
 *
 *	  In fact there are some common conventions on white space usage in the
 *	  world, not only in PostgreSQL, but also in many other projects, we will
 *	  also mention them.
 *
 *-------------------------------------------------------------------------
 */

/*
 * The first convention is: separate different things with white spaces.
 *
 * We have meet this in "03_includes.c", which separates include sessions with
 * blank lines.  Here are some more examples (but not all).
 */

/*
 * BAD: Should separate two structs with a blank line.
 */
struct Struct1
{
	int a;
};
struct Struct2
{
	int a;
};

/*
 * GOOD: Separate common attributes and per-type attributes.
 */
struct Struct3
{
	Scan scan;

	int a;
};

/*
 * GOOD: Separate type definitions and function declarations.
 */

typedef int Type4;
typedef int Type5;

static int Func6(void);

/*
 * GOOD: Separate variables and __code__.
 */
static int
Func6(void)
{
	int a = 0;
	int b = 0;

	return a + b;
}

/*
 * GOOD: Separate different groups of operations.
 */
static void
Func7(int a, int b)
{
	int sum;

	sum = a + b;
	if (sum > 10)
		elog(ERROR, "sum exceeds the limit");

	if (a == b)
		elog(ERROR, "a equals b");
}

/*
 * BAD: Should separate argument list with spaces.
 */
static int
Func8(int a, int b)
{
	/* BAD: Should separate arguments with a space */
	Func7(a, b);

	/* BAD: Should separate the "return" and the expression with a space. */
	return -a;
}

/*
 * The next convention is: make keywords obvious with spaces.
 *
 * There should usually be a space after the C language keywords, such as "if",
 * "for", "while", "do", "switch", "case".
 *
 * But usually we do not add a space after some function-like keywords, such as
 * "sizeof", "typeof", "alignof", "__attribute__".
 */

static int
Func9(int a)
{
	/* GOOD: No space after "typeof". */
	typeof(a) *ptr;

	/* GOOD: No space after "sizeof". */
	ptr = &a;
	*ptr = sizeof(int);

	/* GOOD: A space after "if". */
	if (a == 0)
		return 0;

	/* GOOD: A space after "for". */
	/* GOOD: Separate "for" sections with spaces. */
	for (a = 0; a < 4; ++a)
		break;

	/* GOOD: A space after "return". */
	return -a;
}

/*
 * The next convention is: make binary operators obvious with white spaces.
 *
 * There should usually be a white space before and after a binary operator,
 * such as "+", "-", "*", "/", "%", "&", "|", "^", "&&", "||", "<", ">", "<=",
 * ">=", "==", "!=", as well as "?", ":", and "=".
 *
 * The struct member operators, "." and "->", are usually not arounded by white
 * spaces.
 *
 * The white space is usually a space, but it can also be a line-break when
 * line breaking is needed.
 */

/*
 * GOOD: All the binary operators are arounded with spaces or line-breaks.
 */
static bool
Func10(Path *path, int a, int b)
{
	a += (a & b) + (a ^ b);
	a = a * (a + b);
	a = a > b ? a : b;

	/* GOOD: No white spaces before or after the "." and "->". */
	Assert(path->locus.numsegments > 0);

	return (a == b ||
			a == 0);
}

/*
 * The next convention is: make unary operators obvious by not adding spaces.
 *
 * To be accurate, do not add white spaces between an unary operator and its
 * target, but there should be a white space on the other side.
 *
 * Unary operators include "++", "--", "&", "*", "~", "!".
 */

static bool
Func11(int a, int b)
{
	/* BAD: Do not add white spaces between an unary operator and its target. */
	a++;
	++b;
	/* GOOD */
	a++;
	++b;

	/* BAD: There should be a space before "--b". */
	a = a + --b;
	/* GOOD */
	a = a + --b;

	/*
	 * GOOD: No space between "++" and its target, no space between "*" and its
	 * target, no space between "&" and its target.
	 *
	 * However in practice we should not put so many unary operators together.
	 */
	a = ++*&b;
}

/*
 * There are also other conventions on not adding white spaces.
 *
 * There should be no white space between a function and the "()", and usually
 * we do not put white spaces before and after "(" or ")" in PostgreSQL.
 *
 * There should be no white space before a "," or ";", but there should be a
 * white space after them.
 */

/*
 * BAD: Should not put spaces around the "()".
 */
static bool
Func12(int a, int b)
{
	int xs[2][2];

	/* BAD: Below are all bad styles */

	xs[0][0] = 0;
	xs[0][0] = 0;
	xs[0][0] = 0;
	xs[0][0] = 0;
	xs[0]
	  [0] = 0;

	Func11(a, b);
	Func11(a, b);
	Func11(a, b);
	Func11(a, b);
	Func11(a, b);
	Func11(a, b);

	for (a = 0; a < 4; ++i)
		break;

	for (a = 0; a < 4; ++i)
		break;

	for (a = 0; a < 4; ++i)
		break;

	for (a = 0; a < 4; ++i)
		break;
}
