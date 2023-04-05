/*-------------------------------------------------------------------------
 *
 * FileManager.h
 *	  Physical file manager
 *
 * Copyright (c) 2021-Present abcdabcd3899 inc.
 *
 * IDENTIFICATION
 *	  contrib/mars/FileManager.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MATRIXDB_FILEMANAGER_H
#define MATRIXDB_FILEMANAGER_H

#include "wrapper.hpp"

#include <string>
#include <vector>

namespace mars
{

	::SMgrRelation
	RelationCreateStorage(::RelFileNode rnode, char relpersistence, ::SMgrImpl smgr_which);

	char *GetRelationPath(::RelFileNode rnode, ::BackendId id, int32_t segno);

	std::string
	FetchIdlePath(RelFileNode rnode);

	static constexpr std::size_t MAXIMUM_ROW_COUNT = 10240;
	static constexpr std::size_t MAXIMUM_WRITE_BUFFER_ROW_COUNT =
		MAXIMUM_ROW_COUNT * 10;

} // namespace mars

#endif // MATRIXDB_FILEMANAGER_H
