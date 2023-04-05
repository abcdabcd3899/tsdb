/*-------------------------------------------------------------------------
 *
 * FileManager.cc
 *	  Physical file manager
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 * IDENTIFICATION
 *	  contrib/mars/FileManager.cc
 *
 *-------------------------------------------------------------------------
 */
#include "FileManager.h"
#include "Footer.h"

#include <string>

namespace mars
{

	::SMgrRelation
	RelationCreateStorage(::RelFileNode rnode,
						  char relpersistence,
						  ::SMgrImpl smgr_which)
	{
		return ::RelationCreateStorage(rnode, relpersistence, smgr_which);
	}

	char *
	GetRelationPath(::RelFileNode rnode, ::BackendId id, int32_t segno)
	{
		return ::aorelpathbackend(rnode, id, segno);
	}

	std::string
	FetchIdlePath(::RelFileNode rnode)
	{
		int32_t segno = 0; // FIXME: no hard-coding
		std::string path = GetRelationPath(rnode, InvalidBackendId, segno);
		return path;
	}

} // namespace mars end
