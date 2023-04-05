#ifndef MARS_TEST_MOCK_H
#define MARS_TEST_MOCK_H

extern "C" {

#include "../wrapper.hpp"

#undef fprintf
#undef printf

#undef elog
#define elog(level, msg...) do { \
	fprintf(stderr, msg); \
	if ((level) >= ERROR) { \
		pg_unreachable(); \
	} \
} while (0)

#undef ereport
#define ereport(level, msg...) do { \
	if ((level) >= ERROR) { \
		pg_unreachable(); \
	} \
} while (0)

#undef Assert
#define Assert(condition) do {} while(0)

} // extern "C"

#endif   /* MARS_TEST_MOCK_H */
