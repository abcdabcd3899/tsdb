/*-------------------------------------------------------------------------
 *
 * 06_braces.c
 *	  The Coding Style In MatrixDB - Braces.
 *
 * Copyright (c) 2021-Present abcdabcd3899, Inc.
 *
 * IDENTIFICATION
 *	  contrib/mxdb_coding_style/06_braces.c
 *
 * NOTES
 *	  Braces, the "()", "[]", and "{}", are very important in any coding style.
 *
 *	  In PostgreSQL, the only thing special is "{}", which is documented in
 *	  PostgreSQL Coding Conventions [1]:
 *
 *	  > Layout rules (brace positioning, etc) follow BSD conventions.  In
 *	  > particular, curly braces for the controlled blocks of if, while,
 *	  > switch, etc go on their own lines.
 *
 *	  And for "if", "for", "while", only put the "{}" when necessary.
 *
 *	  [1]: https://www.postgresql.org/docs/12/source.html
 *
 *-------------------------------------------------------------------------
 */

/*
 * GOOD: The "{" has its own line, the "}" of "do ... while" is usually put
 * in the same line with "while".
 */
#define Macro1(a, b)   \
	do                 \
	{                  \
		Assert(a > b); \
	} while (0)

/*
 * GOOD: The "{" and "}" all have their own lines.
 */
enum Colors
{
	COLOR_WHITE,
	COLOR_RED,
	COLOR_BLACK
};

/*
 * GOOD: The "{" has its own line, the "}" of a combined "typedef struct" is
 * usually put in the same line with the new type name, "Point".
 */
typedef struct Point
{
	int x;
	int y;
} Point;

typedef struct Line
{
	Point p1;
	Point p2;
} Line;

/*
 * Where to put the "{}" for the struct & array initializers?
 *
 * - the outermost "{" is usually in the same line with the "=";
 * - the outermost "}" is usually in its own line;
 * - the inner, depends on the complexity of the structs, can be flexible;
 * - the key point is to make the code easy to read;
 *
 * Refer to cacheinfo, log_statement_options, default_reloptions(), etc., for
 * example.
 */

/*
 * GOOD: So this is good.
 */
static const Line lines1[] = {
	{{0, 0}, {0, 1}},
	{{0, 1}, {1, 1}},
	{{1, 1}, {1, 0}},
	{{1, 0}, {0, 0}}};

/*
 * GOOD: This is also good.
 */
static const Line lines2[] = {
	{{0, 0}, {0, 1}},
	{{0, 1}, {1, 1}},
	{{1, 1}, {1, 0}},
	{{1, 0}, {0, 0}}};

/*
 * BAD: The style must be consistent.
 */
static const Line lines3[] = {
	{{0, 0}, {0, 1}},
	{{0, 1}, {1, 1}},
	{{1, 1}, {1, 0}},
	{{1, 0}, {0, 0}}};

/*
 * GOOD: The "{}" of a function must have their own lines.
 */
static void
Func4(int a, int b)
{
	/*
	 * GOOD: For "if", "for", "while", etc., the "{" and "}" must have their
	 * own lines.
	 */

	if (a > b)
	{
		a = 1;
		b = 0;
	}
	else if (a < b)
	{
		a = 0;
		b = 1;
	}
	else
	{
		a = 0;
		b = 0;
	}

	for (int i = 0; i < 4; ++i)
	{
		a = 0;
		b = 0;
	}

	while (a < b)
	{
		a = 0;
		b = 0;
	}

	/* GOOD: In "do ... while" the "}" is in the same line with "while". */
	do
	{
		a = 0;
		b = 0;
	} while (a < b);

	/*
	 * GOOD: Do not put a "{}" if a single statement will do.
	 */

	if (a > b)
		return;

	if (a > b ||
		a < b)
		return;

	if (a == 0)
		return;
	else if (a == 1)
		return;
	else
		return;

	for (int i = 0; i < 4; ++i)
		a = i;

	while (a > 0)
		a = 0;

	/* GOOD: Do not put a "{}" for a "if" even if the "else" needs one. */

	if (a > b)
		return;
	else
	{
		a = b;
		b = 0;
	}

	if (a > b)
	{
		a = b;
		b = 0;
	}
	else
		return;

	/* GOOD: Put a "{}" for complex nesting. */
	if (a > 0)
	{
		for (int i = 0; i < 4; ++i)
		{
			if (i >= 0)
				break;
		}
	}

	/* GOOD: Keep in mind, always put a "{}" for nested "if". */
	if (a > 0)
	{
		if (b > 0)
			return;
		else
			b = 0;
	}
	else
		b = 1;

	/* GOOD: In "do ... while" there is always a "{}". */
	do
	{
		a = b;
	} while (false);

	/* GOOD: The "{}" of "switch" have their own lines. */
	switch (a)
	{
	case 0:
		/* GOOD: The "case" block requires no "{}" by default. */
		break;

	case 1:
		/*
		 * GOOD: The "case" block requires a "{}" to define new local
		 * variables.
		 */
		{
			int x = 0;

			a = x;
		}
		break;

	case 2:
		/*
		 * GOOD: The "case" block itself requires no "{}" if the new local
		 * variable is not defined directly under it.
		 */
		if (b == 0)
		{
			int x = 0;

			a = x;
		}
		break;

	default:
		break;
	}
}

/*
 * GOOD: some examples from PostgreSQL code.
 */

Datum nocachegetattr(HeapTuple tuple,
					 int attnum,
					 TupleDesc tupleDesc)
{
	if (!HeapTupleNoNulls(tuple))
	{
		/*
		 * there's a null somewhere in the tuple
		 *
		 * check to see if any preceding bits are null...
		 */
		int byte = attnum >> 3;
		int finalbit = attnum & 0x07;

		/* check for nulls "before" final bit of last byte */
		if ((~bp[byte]) & ((1 << finalbit) - 1))
			slow = true;
		else
		{
			/* check for nulls in any "earlier" bytes */
			int i;

			for (i = 0; i < byte; i++)
			{
				if (bp[i] != 0xFF)
				{
					slow = true;
					break;
				}
			}
		}
	}

	/* ... */

	/* If we know the next offset, we can skip the rest */
	if (usecache && att->attcacheoff >= 0)
		off = att->attcacheoff;
	else if (att->attlen == -1)
	{
		/*
		 * We can only cache the offset for a varlena attribute if the
		 * offset is already suitably aligned, so that there would be
		 * no pad bytes in any case: then the offset will be valid for
		 * either an aligned or unaligned value.
		 */
		if (usecache &&
			off == att_align_nominal(off, att->attalign))
			att->attcacheoff = off;
		else
		{
			off = att_align_pointer(off, att->attalign, -1,
									tp + off);
			usecache = false;
		}
	}
}

Datum transformRelOptions(Datum oldOptions, List *defList, const char *namspace,
						  char *validnsps[], bool acceptOidsOff, bool isReset)
{
	/* ... */

	if (namspace == NULL)
	{
		if (def->defnamespace != NULL)
			continue;
	}
	else if (def->defnamespace == NULL)
		continue;
	else if (strcmp(def->defnamespace, namspace) != 0)
		continue;

	/* ... */

	if (validnsps)
	{
		for (i = 0; validnsps[i]; i++)
		{
			if (strcmp(def->defnamespace, validnsps[i]) == 0)
			{
				valid = true;
				break;
			}
		}
	}
}
