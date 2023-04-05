/*-------------------------------------------------------------------------
 *
 * mx_compress.c
 *	  TODO file description
 *
 * FIXME: Currently we are allocating all the memory in the TopMemoryContext,
 * and the hash tables are reused between transactions.  Maybe it is better to
 * free them at end of each transactions.
 *
 *
 * Copyright (c) 2020-Present TODO
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_attribute_encoding.h"
#include "storage/mx_compress.h"
#include "utils/hashutils.h"
#include "utils/hsearch.h"

#ifndef CONTAINER_OF
#define CONTAINER_OF(ptr, type, member) \
	({ \
		const typeof(((type *) 0)->member) *__member_ptr = (ptr); \
		(type *) ( (char *) __member_ptr - offsetof(type, member) ); \
	})
#endif

/*
 * The hash record key of the shared compression functions.
 */
typedef struct _FuncsRecordKey
{
	char	   *compresstype;		/* Must be owned by us */
} _FuncsRecordKey;

/*
 * The hash record of the shared compression functions.
 */
typedef struct _FuncsRecord
{
	_FuncsRecordKey key;			/* Must be the first */

	int			ref;				/* The ref count, the record is auto
									 * released when ref is down to 0 */

	/*
	 * The compression functions.
	 *
	 * It is important to make it an array instead of a pointer.  In the
	 * mx_get_shared_compression_functions() we return the address of this
	 * array, so later in the mx_unget_shared_compression_functions() we can
	 * find out the record with the array address.
	 */
	PGFunction	funcs[NUM_COMPRESS_FUNCS];
} _FuncsRecord;

/*
 * The hash record key of the shared compression state.
 */
typedef struct _StateRecordKey
{
	bool		compression;		/* Compression or uncompression */
	char	   *compresstype;		/* Must be owned by us */
	int16		compresslevel;		/* The compression level */
	int32		blocksize;			/* The block size */
	TupleDesc	tupledesc;			/* The tuple description, must be owned by
									 * us, can be used to re-encode per column
									 * types, but not being used at the moment
									 */
} _StateRecordKey;

/*
 * The hash record of the shared compression state.
 */
typedef struct _StateRecord
{
	_StateRecordKey	key;			/* Must be the first */

	int			ref;				/* The ref count, the record is auto
									 * released when ref is down to 0 */
	PGFunction *funcs;				/* A shared ref of the compression funcs */

	/*
	 * The compression state, we must own the value instead of a pointer.  In
	 * the mx_get_shared_compression_state() we return the address of this
	 * state, so later in mx_unget_shared_compression_state() we can find out
	 * the record with the address.
	 */
	CompressionState state;
} _StateRecord;


/*
 * The hash table of the shared compression functions.
 */
static HTAB	   *_funcs_htab = NULL;

/*
 * The hash table of the shared compression state.
 */
static HTAB	   *_state_htab = NULL;


/*
 * Hash function for the functions record key.
 */
static uint32
_funcs_key_hash(const _FuncsRecordKey *key)
{
	return string_hash(key->compresstype,
					   strlen(key->compresstype) + 1);
}

/*
 * Compare function for the functions record key.
 *
 * The meaning of the return value is the same as memcmp().
 */
static int
_funcs_key_compare(const _FuncsRecordKey *key1, const _FuncsRecordKey *key2)
{
	return strcmp(key1->compresstype, key2->compresstype);
}

/*
 * Copy function for the functions record key.
 */
static _FuncsRecordKey *
_funcs_key_copy(_FuncsRecordKey *dst, const _FuncsRecordKey *src)
{
	dst->compresstype = pstrdup(src->compresstype);

	return dst;
}

/*
 * Initialize the functions hash table if not yet.
 */
static void
ensure_shared_compression_functions_htab(void)
{
	HASHCTL		hashctl;

	if (likely(_funcs_htab))
		return;

	/*
	 * Unless we can find a proper way to reset the hash table, we must
	 * keep it in the TopMemoryContext, otherwise its memory are reset at end
	 * of the transaction.
	 */
	hashctl.hcxt = TopMemoryContext;

	hashctl.hash = (HashValueFunc) _funcs_key_hash;
	hashctl.match = (HashCompareFunc) _funcs_key_compare;
	hashctl.keycopy = (HashCopyFunc) _funcs_key_copy;

	hashctl.keysize = sizeof(_FuncsRecordKey);
	hashctl.entrysize = sizeof(_FuncsRecord);

	_funcs_htab = hash_create("shared compression functions",
							  16 /* nelem */,
							  &hashctl,
							  HASH_CONTEXT |
							  HASH_ELEM | HASH_FUNCTION |
							  HASH_COMPARE | HASH_KEYCOPY);
}

/*
 * Get a shared ref of the compression functions.
 */
PGFunction *
mx_get_shared_compression_functions(char *compresstype)
{
	bool		found;
	_FuncsRecordKey key;
	_FuncsRecord *entry;
	MemoryContext oldcontext;

	if (!compresstype ||
		!compresstype[0] ||
		pg_strcasecmp("none", compresstype) == 0)
		return NULL;

	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	ensure_shared_compression_functions_htab();

	/* Do not duplicate the compresstype string unless necessary */
	key.compresstype = compresstype;

	entry = hash_search(_funcs_htab, &key, HASH_FIND, &found);

	if (!found)
	{
		PGFunction *funcs = get_funcs_for_compression(compresstype);

		entry = hash_search(_funcs_htab, &key, HASH_ENTER, NULL);
		entry->ref = 0;

		/* The record must hold its own copy of the compresstype string */
		entry->key.compresstype = pstrdup(compresstype);

		/*
		 * Refer to _FuncsRecord on why we need to make funcs an array instead
		 * of a pointer.
		 */
		memcpy(entry->funcs, funcs, sizeof(entry->funcs));

		/* Free the original copy of the funcs to reduce memory usage */
		pfree(funcs);
	}

	MemoryContextSwitchTo(oldcontext);

	entry->ref++;
	return entry->funcs;
}

/*
 * Return a shared ref of the compression functions.
 *
 * The argument "funcs" must be one returned by the
 * mx_get_shared_compression_functions().
 */
void
mx_unget_shared_compression_functions(PGFunction *funcs)
{
	_FuncsRecord *entry;

	Assert(_funcs_htab);

	/* Refer to _FuncsRecord on why we can make this lookup */
	entry = CONTAINER_OF(funcs, _FuncsRecord, funcs[0]);
	Assert(entry->ref > 0);

	entry->ref--;
	if (entry->ref == 0)
	{
		hash_search(_funcs_htab, &entry->key, HASH_REMOVE, NULL);

		pfree(entry->key.compresstype);
	}

	/* TODO: when to destroy the htag? */
}

/*
 * Hash function for the state record key.
 */
static uint32
_state_key_hash(const _StateRecordKey *key)
{
	uint32		hashcode = 0;
	int32		compresslevel = key->compresslevel;

	hashcode = hash_combine(hashcode,
							key->compression);
	hashcode = hash_combine(hashcode,
							string_hash(key->compresstype,
										strlen(key->compresstype) + 1));
	hashcode = hash_combine(hashcode,
							int32_hash(&compresslevel, sizeof(int32)));
	hashcode = hash_combine(hashcode,
							int32_hash(&key->blocksize, sizeof(int32)));
#if 0
	/* We do not care about tupledesc at present */
	hashcode = hash_combine(hashcode, hashTupleDesc(key->tupledesc));
#endif

	return hashcode;
}

/*
 * Compare function for the state record key.
 *
 * The meaning of the return value is the same as memcmp().
 */
static int
_state_key_compare(const _StateRecordKey *key1, const _StateRecordKey *key2)
{
	return !(key1->compression == key2->compression &&
			 key1->compresslevel == key2->compresslevel &&
			 key1->blocksize == key2->blocksize &&
			 strcmp(key1->compresstype, key2->compresstype) == 0);
#if 0
		/* We do not care about tupledesc at present */
		equalTupleDescs(key1->tupledesc, key2->tupledesc, false /* strict */)
#endif
}

/*
 * Copy function for the state record key.
 */
static _StateRecordKey *
_state_key_copy(_StateRecordKey *dst, const _StateRecordKey *src)
{
	dst->compression = src->compression;
	dst->compresstype = pstrdup(src->compresstype);
	dst->compresslevel = src->compresslevel;
	dst->blocksize = src->blocksize;
#if 0
	/* We do not care about tupledesc at present */
	/*
	 * TODO: if we want to compare in strict mode, use
	 * CreateTupleDescCopyConstr() instead.
	 */
	dst->tupledesc = CreateTupleDescCopy(src->tupledesc);
#else
	dst->tupledesc = NULL;
#endif

	return dst;
}

/*
 * Initialize the state hash table if not yet.
 */
static void
ensure_shared_compression_state_htab(void)
{
	HASHCTL		hashctl;

	if (likely(_state_htab))
		return;

	/*
	 * Unless we can find a proper way to reset the hash table, we must
	 * keep it in the TopMemoryContext, otherwise its memory are reset at end
	 * of the transaction.
	 */
	hashctl.hcxt = TopMemoryContext;

	hashctl.hash = (HashValueFunc) _state_key_hash;
	hashctl.match = (HashCompareFunc) _state_key_compare;
	hashctl.keycopy = (HashCopyFunc) _state_key_copy;

	hashctl.keysize = sizeof(_StateRecordKey);
	hashctl.entrysize = sizeof(_StateRecord);

	_state_htab = hash_create("shared compression states",
							  16 /* nelem */,
							  &hashctl,
							  HASH_CONTEXT |
							  HASH_ELEM | HASH_FUNCTION |
							  HASH_COMPARE | HASH_KEYCOPY);
}

/*
 * Get a shared ref of the compression state.
 *
 * The returned state has its own ref to compression functions, so it is
 * unnecessary for the caller to get one explicitly, but it is nothing serious
 * if the caller does get one.
 */
CompressionState *
mx_get_shared_compression_state(bool compression,
								char *compresstype,
								int16 compresslevel,
								int32 blocksize,
								TupleDesc tupledesc)
{
	bool		found;
	_StateRecordKey key;
	_StateRecord *entry;
	MemoryContext oldcontext;

	if (!compresstype ||
		!compresstype[0] ||
		pg_strcasecmp("none", compresstype) == 0)
		return NULL;

	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	ensure_shared_compression_state_htab();

	/* Do not duplicate the compresstype string unless necessary */
	key.compression = compression;
	key.compresstype = compresstype;
	key.compresslevel = compresslevel;
	key.blocksize = blocksize;
	key.tupledesc = tupledesc;

	entry = hash_search(_state_htab, &key, HASH_FIND, &found);

	if (!found)
	{
		PGFunction	constructor;
		StorageAttributes sa;
		CompressionState *state;

		entry = hash_search(_state_htab, &key, HASH_ENTER, NULL);
		entry->ref = 0;

		/* The record must hold its own copy of the compresstype string */
		entry->key.compresstype = pstrdup(compresstype);

		entry->funcs = mx_get_shared_compression_functions(compresstype);
		Assert(entry->funcs);

		constructor = entry->funcs[COMPRESSION_CONSTRUCTOR];

		sa.comptype = compresstype;
		sa.complevel = compresslevel;
		sa.blocksize = blocksize;

		state = callCompressionConstructor(constructor,
										   tupledesc,
										   &sa,
										   compression);

		/* Refer to _StateRecord on why we need to copy state */
		entry->state = *state;

		/*
		 * Free the original copy of the state to reduce memory usage, but do
		 * not desconstruct it.
		 */
		pfree(state);
	}

	MemoryContextSwitchTo(oldcontext);

	entry->ref++;
	return &entry->state;
}

/*
 * Return a shared ref of the compression state.
 *
 * The argument "state" must be one returned by the
 * mx_get_shared_compression_state().
 */
void
mx_unget_shared_compression_state(CompressionState *state)
{
	_StateRecord *entry;

	Assert(_state_htab);

	/* Refer to _StateRecord on why we can make this lookup */
	entry = CONTAINER_OF(state, _StateRecord, state);
	Assert(entry->ref > 0);

	entry->ref--;
	if (entry->ref == 0)
	{
		PGFunction	destructor;

		hash_search(_state_htab, &entry->key, HASH_REMOVE, NULL);

		destructor = entry->funcs[COMPRESSION_DESTRUCTOR];
		callCompressionDestructor(destructor, &entry->state);

		mx_unget_shared_compression_functions(entry->funcs);

		pfree(entry->key.compresstype);
	}

	/* TODO: when to destroy the htab? */
}
