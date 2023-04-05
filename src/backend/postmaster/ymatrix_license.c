/*-------------------------------------------------------------------------
 *
 * postmaster/ymatrix_license.c
 *	  MatrixDB license checker
 *
 * Copyright (c) 2020-Present abcdabcd3899, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <openssl/aes.h>
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <pwd.h>
#include <string.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <time.h>

#include "miscadmin.h"
#include "catalog/pg_collation_d.h"
#include "cdb/cdbvars.h"
#include "fmgr.h"
#include "mb/pg_wchar.h"
#include "nodes/pg_list.h"
#include "regex/regex.h"
#include "utils/fmgrprotos.h"
#include "utils/memutils.h"

#include "ymatrix_license.h"

static const char *PUBLICKEY =
	"-----BEGIN PUBLIC KEY-----\n"
	"MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEArqmoo9K7dQ9+XN7TdsQW\n"
	"e0/w9tWnhZxzsQTaalvyvYTEX24zILCcwWDePYFMkzkI7+pJFNC6KzqSSwi5S7C5\n"
	"DPCViZmXiiww09Ka6Q8pKxT6d2ZurrYNGiyqz+b3H01IQiUR4J/4ZEh7pP0/AM6w\n"
	"r0zqY02s+xK8jmgdzKvH+gr/9hGFHQvJz/HZJeiDqp1Odf6jcSX3uIhiUNzaGPgJ\n"
	"ktWqAM5kMmMAqOJwanmV+bVEGM6aY+gC8YHJC3e8ASChBAifhksDgjEQUi+0O9Ke\n"
	"CyMvg5AaXZhdQQLHbBLkU59wSKTCXeyZYQEaKPFhd8wHMuFIm6y6eOnq+yZPAXlU\n"
	"lwIDAQAB\n"
	"-----END PUBLIC KEY-----";

static bool
mx_rsa_verify(RSA *rsa,
			  unsigned char *MsgHash,
			  size_t MsgHashLen,
			  const char *Msg,
			  size_t MsgLen)
{
#define SUCCESS 1
#define FAILED 0
	int status;
	EVP_PKEY *pubKey = EVP_PKEY_new();
	EVP_PKEY_assign_RSA(pubKey, rsa);
	EVP_MD_CTX *m_RSAVerifyCtx = EVP_MD_CTX_create();

	if (EVP_DigestVerifyInit(m_RSAVerifyCtx, NULL, EVP_sha256(), NULL, pubKey) != SUCCESS)
		return false;

	if (EVP_DigestVerifyUpdate(m_RSAVerifyCtx, Msg, MsgLen) != SUCCESS)
		return false;

	status = EVP_DigestVerifyFinal(m_RSAVerifyCtx, MsgHash, MsgHashLen);

#if OPENSSL_VERSION_NUMBER >= 0x10100000
	EVP_MD_CTX_free(m_RSAVerifyCtx);
#else
	EVP_MD_CTX_destroy(m_RSAVerifyCtx);
#endif

	return status == SUCCESS;
#undef SUCCESS
#undef FAILED
}

static bool
verify_signature(const char *publicKey, const char *plainText, unsigned char *signatureBase64, size_t length)
{
	BIO *keybio;
	RSA *rsa = NULL;
	bool result;

	keybio = BIO_new(BIO_s_mem());

	if (keybio == NULL)
		return NULL;

	BIO_puts(keybio, publicKey);

	rsa = PEM_read_bio_RSA_PUBKEY(keybio, NULL, NULL, NULL);

	result = mx_rsa_verify(rsa, signatureBase64, length, plainText, strlen(plainText));

	BIO_free(keybio);

	return result;
}

static List *
split_org(const char *org)
{
	List *ret = NIL;
	int org_sz = strlen(org);

	const char *head_str;
	const char *tail_str;
	char *str;

	for (head_str = org, tail_str = strstr(head_str, "; ");
		 (head_str != &org[org_sz]) && tail_str;
		 head_str = tail_str + 2, tail_str = strstr(head_str, "; "))
	{
		if (tail_str != head_str)
		{
			str = palloc(tail_str - head_str + 1);
			memcpy(str, head_str, tail_str - head_str);
			str[tail_str - head_str] = '\0';
			ret = lappend(ret, str);
		}
		else
			ret = lappend(ret, NULL);
	}

	/* the last part cause is not finished by semicolon */
	ret = lappend(ret, (void *)head_str);

	return ret;
}

static size_t
parse_hexstr(char *content, unsigned char **p_signature)
{
	char *ptr;
	unsigned char *signature;
	size_t sig_sz;

	sig_sz = strlen(content) / 2;

	signature = palloc0(sig_sz);
	ptr = content;

	for (size_t count = 0; count < sig_sz; count++)
	{
		if (sscanf(ptr, "%2hhx", &signature[count]) < 1)
			ereport(FATAL,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("licence check error: receive illegal char in signature file.")));

		ptr += 2;
	}

	*p_signature = signature;

	return sig_sz;
}

static bool
check_internal(const char *path)
{
#define SIG 0
#define ORG 1
	char *content[2] = {NULL, NULL};
	size_t content_sz[2] = {0, 0};
	unsigned char *signature;
	size_t sig_sz;

	{
		/* read license file */
		FILE *f = fopen(path, "r");
		int line_sz;

		line_sz = getline(&content[SIG], &content_sz[SIG], f);
		if (read <= 0)
		{
			fclose(f);
			return false;
		}
		else
			/* replace '\n' to '\0' */
			content[SIG][line_sz - 1] = '\0';

		line_sz = getline(&content[ORG], &content_sz[ORG], f);
		if (line_sz <= 0)
		{
			free(content[SIG]);
			fclose(f);
			return false;
		}

		fclose(f);
	}

	if (!(sig_sz = parse_hexstr(content[SIG], &signature)))
		return false;

	if (!verify_signature(PUBLICKEY, content[ORG], signature, sig_sz))
		return false;

	{
		/* check expire */
		double expire;
		time_t t_license;
		struct tm tm_license;

		time_t t_now = time(NULL);

		memset(&tm_license, 0, sizeof(struct tm));

		List *parts = split_org(content[ORG]);

		if (list_length(parts) < 3)
			ereport(FATAL,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("your license file is invalid.")));

		strptime((const char *)lsecond(parts), "%Y-%m-%d", &tm_license);
		t_license = mktime(&tm_license);

		expire = difftime(t_now, t_license);

		if (expire > 0)
			ereport(FATAL,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("your license is expired at %s.", (char *)lsecond(parts))));
	}

	free(content[SIG]);
	free(content[ORG]);

	return true;
#undef SIG
#undef ORG
}

static bool
isdir(const char *name)
{
	struct stat st;

	if (lstat(name, &st) == -1)
		return false;

	return S_ISDIR(st.st_mode);
}

static bool
find_license(char *dir_path, regex_t *re)
{
	DIR *d;
	struct dirent *dir;
	bool found = false;

	d = opendir(dir_path);
	if (d)
	{
		int wlen_file;
		pg_wchar *wstr_file;

		for (dir = readdir(d); !found && dir; dir = readdir(d))
		{
			/* skip if it is directory */
			if (isdir(dir->d_name))
				continue;

			wstr_file = palloc(strlen(dir->d_name) * sizeof(pg_wchar));

			wlen_file = pg_mb2wchar_with_len(dir->d_name,
											 wstr_file,
											 strlen(dir->d_name));

			/* find a candidate */
			if (pg_regexec(re, wstr_file, wlen_file, 0, NULL, 0, NULL, 0) == REG_OKAY)
			{
				int file_path_len = strlen(dir_path) + 1 + strlen(dir->d_name) + 1;
				char *file_path = palloc(file_path_len);
				snprintf(file_path, file_path_len, "%s/%s", dir_path, dir->d_name);
				file_path[file_path_len - 1] = '\0';

				/* verify signature*/
				found = check_internal(file_path);
			}
		}
	}

	closedir(d);
	return found;
}

void mx_license_check(void)
{
#define PATH_NUM 4

	/* only check license on QD postmaster */
	if (qdPostmasterPort != 0)
		return;

	struct passwd *pw;
	char *path[PATH_NUM];
	MemoryContext lincense_check_ctx;
	MemoryContext old_ctx;

	lincense_check_ctx = AllocSetContextCreate(TopMemoryContext,
											   "RowDescriptionContext",
											   ALLOCSET_DEFAULT_SIZES);

	old_ctx = MemoryContextSwitchTo(lincense_check_ctx);

	/* initialize search path */
	path[0] = palloc(MAXPGPATH);

	/* current dir */
	if (getcwd(path[0], MAXPGPATH) == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("licence check error: can not get current working directory.")));

	/* home dir: ~ */
	pw = getpwuid(getuid());
	if (pw == NULL)
		ereport(FATAL,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("licence check error: can not get home directory.")));

	path[1] = palloc(MAXPGPATH);
	path[1] = strcpy(path[1], pw->pw_dir);

	/* tmp dir */
	path[2] = palloc(MAXPGPATH);
	strcpy(path[2], "/tmp");

	/* `postgres` binary dir */
	{
		char *ptr = NULL;
		path[3] = palloc(MAXPGPATH);
		strcpy(path[3], my_exec_path);
		ptr = path[3];

		if ((ptr = strrchr(path[3], '/')) != NULL)
			*ptr = '\0';
		else
			ereport(FATAL,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("licence check error: can not find postgres binary directory.")));
	}

	{
		bool found = false;
		pg_wchar *wchar_pattern;
		regex_t re;
		int wlen;
		const char *license_name_pattern = "^(?:LICENSE.\\d{8}.\\d{6})$";

		wchar_pattern = palloc((strlen(license_name_pattern) + 1) * sizeof(pg_wchar));
		wlen = pg_mb2wchar_with_len(license_name_pattern,
									wchar_pattern,
									strlen(license_name_pattern));
		pg_regcomp(&re, wchar_pattern, wlen, REG_ADVANCED, DEFAULT_COLLATION_OID);

		for (int i = 0; found || i < PATH_NUM; i++)
			if ((found = find_license(path[i], &re)))
				break;

		if (!found)
			elog(PANIC, "Licence check error: can not find Licence");
	}

	MemoryContextSwitchTo(old_ctx);
	MemoryContextResetAndDeleteChildren(lincense_check_ctx);

#undef PATH_NUM
}
