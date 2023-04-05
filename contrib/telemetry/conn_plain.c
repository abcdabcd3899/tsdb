/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <unistd.h>
#include <postgres.h>

#include <sys/socket.h>
#include <sys/time.h>

#include "conn_internal.h"
#include "conn_plain.h"
#include "port.h"

#define DEFAULT_TIMEOUT_MSEC 3000
#define MAX_PORT 65535

static void
set_error(int err)
{
	errno = err;

}

static int
get_error(void)
{
	return errno;
}

/* We cannot define `pg_strerror` here because there is a #define in PG12 that
 * sets `strerror` to `pg_strerror`. Instead, we handle the missing case for
 * pre-PG12 on Windows by setting `strerror` to the windows version of the
 * function and use `strerror` below. */

/*  Create socket and connect */
int
ts_plain_connect(Connection *conn, const char *host, const char *servname, int port)
{
	char strport[6];
	struct addrinfo *ainfo, hints = {
		.ai_family = AF_UNSPEC,
		.ai_socktype = SOCK_STREAM,
	};
	int ret;

	if (NULL == servname && (port <= 0 || port > MAX_PORT))
	{
		set_error(EINVAL);
		return -1;
	}

	/* Explicit port given. Use it instead of servname */
	if (port > 0 && port <= MAX_PORT)
	{
		snprintf(strport, sizeof(strport), "%d", port);
		servname = strport;
		hints.ai_flags = AI_NUMERICSERV;
	}

	/* Lookup the endpoint ip address */
	ret = getaddrinfo(host, servname, &hints, &ainfo);

	if (ret != 0)
	{
		ret = SOCKET_ERROR;

		/*
		 * The closest match for a name resolution error. Strictly, this error
		 * should not be used here, but to fix we need to support using
		 * gai_strerror()
		 */
		errno = EADDRNOTAVAIL;

		goto out;
	}

	ret = conn->sock = socket(ainfo->ai_family, ainfo->ai_socktype, ainfo->ai_protocol);


	if (IS_SOCKET_ERROR(ret))
		goto out_addrinfo;

	/*
	 * Set send / recv timeout so that write and read don't block forever. Set
	 * separately so that one of the actions failing doesn't block the other.
	 */
	if (ts_plain_set_timeout(conn, DEFAULT_TIMEOUT_MSEC) < 0)
	{
		ret = SOCKET_ERROR;
		goto out_addrinfo;
	}

	/* connect the socket */
	ret = connect(conn->sock, ainfo->ai_addr, ainfo->ai_addrlen);

out_addrinfo:
	freeaddrinfo(ainfo);

out:
	if (IS_SOCKET_ERROR(ret))
	{
		conn->err = ret;
		return -1;
	}

	return 0;
}

static ssize_t
plain_write(Connection *conn, const char *buf, size_t writelen)
{
	ssize_t ret;
	ret = send(conn->sock, buf, writelen, 0);

	if (ret < 0)
		conn->err = ret;
	return ret;
}

static ssize_t
plain_read(Connection *conn, char *buf, size_t buflen)
{
	ssize_t ret;
	ret = recv(conn->sock, buf, buflen, 0);

	if (ret < 0)
		conn->err = ret;
	return ret;
}

void
ts_plain_close(Connection *conn)
{
	if (!IS_SOCKET_ERROR(conn->sock))
		close(conn->sock);
	conn->sock = -1;
}

int
ts_plain_set_timeout(Connection *conn, unsigned long millis)
{
	/* we deliberately use a long constant here instead of a fixed width one because tv_sec is
	 * declared as a long */
	struct timeval timeout = {
		.tv_sec = millis / 1000L,
		.tv_usec = (millis % 1000L) * 1000L,
	};
	int optlen = sizeof(struct timeval);

	/*
	 * Set send / recv timeout so that write and read don't block forever. Set
	 * separately so that one of the actions failing doesn't block the other.
	 */
	conn->err = setsockopt(conn->sock, SOL_SOCKET, SO_RCVTIMEO, (const char *) &timeout, optlen);

	if (conn->err != 0)
		return -1;

	conn->err = setsockopt(conn->sock, SOL_SOCKET, SO_SNDTIMEO, (const char *) &timeout, optlen);

	if (conn->err != 0)
		return -1;

	return 0;
}

const char *
ts_plain_errmsg(Connection *conn)
{
	const char *errmsg = "no connection error";

	if (IS_SOCKET_ERROR(conn->err))
		errmsg = strerror(get_error());
	conn->err = 0;

	return errmsg;
}

static ConnOps plain_ops = {
	.size = sizeof(Connection),
	.init = NULL,
	.connect = ts_plain_connect,
	.close = ts_plain_close,
	.write = plain_write,
	.read = plain_read,
	.errmsg = ts_plain_errmsg,
};


void
_conn_plain_init(void)
{
	/*
	 * WSAStartup is required on Windows before using the Winsock API.
	 * However, PostgreSQL already handles this for us, so it is disabled here
	 * by default. Set WSA_STARTUP_ENABLED to perform this initialization
	 * anyway.
	 */

	ts_connection_register(CONNECTION_PLAIN, &plain_ops);
}

void
_conn_plain_fini(void)
{

}
