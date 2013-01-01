#include "redis.h"

static char *urlencode(char const *s, int len, int *new_length)
{
	#define safe_emalloc(nmemb, size, offset)	zmalloc((nmemb) * (size) + (offset))
	static unsigned char hexchars[] = "0123456789ABCDEF";
	register unsigned char c;
	unsigned char *to, *start;
	unsigned char const *from, *end;
	
	from = (unsigned char *)s;
	end = (unsigned char *)s + len;
	start = to = (unsigned char *) safe_emalloc(3, len, 1);

	while (from < end) {
		c = *from++;

		if (c == ' ') {
			*to++ = '+';
#ifndef CHARSET_EBCDIC
		} else if ((c < '0' && c != '-' && c != '.') ||
				   (c < 'A' && c > '9') ||
				   (c > 'Z' && c < 'a' && c != '_') ||
				   (c > 'z')) {
			to[0] = '%';
			to[1] = hexchars[c >> 4];
			to[2] = hexchars[c & 15];
			to += 3;
#else /*CHARSET_EBCDIC*/
		} else if (!isalnum(c) && strchr("_-.", c) == NULL) {
			/* Allow only alphanumeric chars and '_', '-', '.'; escape the rest */
			to[0] = '%';
			to[1] = hexchars[os_toascii[c] >> 4];
			to[2] = hexchars[os_toascii[c] & 15];
			to += 3;
#endif /*CHARSET_EBCDIC*/
		} else {
			*to++ = c;
		}
	}
	*to = 0;
	if (new_length) {
		*new_length = to - start;
	}
	return (char *) start;
}

void ds_init()
{
	char *err   = NULL;
	
    server.ds_cache   = leveldb_cache_create_lru(server.ds_lru_cache);
	server.ds_options = leveldb_options_create();
    
    server.policy     = leveldb_filterpolicy_create_bloom(10);
    
	
	//leveldb_options_set_comparator(server.ds_options, cmp);
	leveldb_options_set_filter_policy(server.ds_options, server.policy);
    leveldb_options_set_create_if_missing(server.ds_options, server.ds_create_if_missing);
	leveldb_options_set_error_if_exists(server.ds_options, server.ds_error_if_exists);
	leveldb_options_set_cache(server.ds_options, server.ds_cache);
	leveldb_options_set_info_log(server.ds_options, NULL);
	leveldb_options_set_write_buffer_size(server.ds_options, server.ds_write_buffer_size);
	leveldb_options_set_paranoid_checks(server.ds_options, server.ds_paranoid_checks);
	leveldb_options_set_max_open_files(server.ds_options, server.ds_max_open_files);
	leveldb_options_set_block_size(server.ds_options, server.ds_block_cache_size);
	leveldb_options_set_block_restart_interval(server.ds_options, server.ds_block_restart_interval);
	leveldb_options_set_compression(server.ds_options, leveldb_snappy_compression);
	
    server.ds_db = leveldb_open(server.ds_options, server.ds_path, &err);
	if (err != NULL)
	{
		fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__,err);
		leveldb_free(err);
		exit(1);
	}
}

void ds_mget(redisClient *c)
{
	sds str; 
	int i, len, pos;
	size_t val_len;
    char *err, *tmp, *value;
    
	leveldb_readoptions_t *roptions;
    
	roptions = leveldb_readoptions_create();
	leveldb_readoptions_set_verify_checksums(roptions, 0);
	leveldb_readoptions_set_fill_cache(roptions, 1);
	
	str = sdsempty();
	for(i=1; i<c->argc; i++)
	{
		err     = NULL;
		value   = NULL;
		val_len = pos = 0;
		len     = strlen((char *)c->argv[i]->ptr);
		value   = leveldb_get(server.ds_db, roptions, (char *)c->argv[i]->ptr, len, &val_len, &err);
	    if(err != NULL)
		{
			addReplyError(c, err);
			sdsfree(str);
			leveldb_free(err);
			leveldb_free(value);
			leveldb_readoptions_destroy(roptions);
			return ;
		}
		else if(val_len > 0)
		{
			tmp = urlencode(value, val_len, &pos);
			str = sdscatlen(str, (char *)c->argv[i]->ptr, len);
			str = sdscatlen(str, "=", 1);
			str = sdscatlen(str, tmp, pos);
			str = sdscatlen(str, "&", 1);


			zfree(tmp);
			leveldb_free(value);
			tmp   = NULL;
			value = NULL;
		}
	}
	leveldb_readoptions_destroy(roptions);
	
	if(sdslen(str) == 0)
	{
		addReply(c,shared.nullbulk);
		sdsfree(str);
		return ;
	}
	str = sdsrange(str, 0, sdslen(str)-2);
    addReplyBulkCBuffer(c, str, sdslen(str));
    sdsfree(str);
}

void ds_get(redisClient *c)
{
	char* err = NULL;
    size_t val_len;
    char *key   = NULL;
    char *value = NULL;
    
	leveldb_readoptions_t *roptions;
    
	roptions = leveldb_readoptions_create();
	leveldb_readoptions_set_verify_checksums(roptions, 0);
	leveldb_readoptions_set_fill_cache(roptions, 1);

    key   = (char *)c->argv[1]->ptr;
    value = leveldb_get(server.ds_db, roptions, key, strlen(key), &val_len, &err);
    leveldb_readoptions_destroy(roptions);
    if(err != NULL)
    {
		addReplyError(c, err);
        leveldb_free(err);
		leveldb_free(value);

        return ;
    }
	else if(value == NULL)
    {
        addReply(c,shared.nullbulk);
        return ;
    }
    addReplyBulkCBuffer(c, value, val_len);
    leveldb_free(value);
}
void rl_get(redisClient *c)
{
	//从redis里取数据
	robj *o;

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) !== NULL) {

	    if (o->type != REDIS_STRING) {
	        addReply(c,shared.wrongtypeerr);
	        return REDIS_ERR;
	    } else {
	        addReplyBulk(c,o);
	        return REDIS_OK;
	    }
	}

	char* err = NULL;
    size_t val_len;
    char *key   = NULL;
    char *value = NULL;
    
	leveldb_readoptions_t *roptions;
    
	roptions = leveldb_readoptions_create();
	leveldb_readoptions_set_verify_checksums(roptions, 0);
	leveldb_readoptions_set_fill_cache(roptions, 1);

    key   = (char *)c->argv[1]->ptr;
    value = leveldb_get(server.ds_db, roptions, key, strlen(key), &val_len, &err);
    leveldb_readoptions_destroy(roptions);
    if(err != NULL)
    {
		addReplyError(c, err);
        leveldb_free(err);
		leveldb_free(value);

        return ;
    }
	else if(value == NULL)
    {
        addReply(c,shared.nullbulk);
        return ;
    }
    addReplyBulkCBuffer(c, value, val_len);
    leveldb_free(value);
}



void ds_mset(redisClient *c)
{
	int    i;
	double rc;
	char *key, *value;
    char *err = NULL;
    leveldb_writeoptions_t *woptions;
	leveldb_writebatch_t   *wb;

    rc = c->argc / 2;
	if(c->argc < 3 || (int)rc != rc)
	{
		addReply(c,shared.nullbulk);
		return ;
	}
	
	woptions = leveldb_writeoptions_create();
	wb       = leveldb_writebatch_create();
	for(i=1; i<c->argc; i++)
	{
		key   = (char *)c->argv[i]->ptr;
		value = (char *)c->argv[++i]->ptr;
		leveldb_writebatch_put(wb, key, strlen(key), value, strlen(value));
	}
	leveldb_write(server.ds_db, woptions, wb, &err);
	leveldb_writeoptions_destroy(woptions);
	leveldb_writebatch_destroy(wb);

	if(err != NULL)
	{
		addReplyError(c, err);
		leveldb_free(err);
		return ;
	}
	addReply(c,shared.ok);

    return ;
}

void ds_set(redisClient *c)
{
	char *key, *value;
    char *err = NULL;
    leveldb_writeoptions_t *woptions;

    woptions = leveldb_writeoptions_create();
    
    key   = (char *)c->argv[1]->ptr;
    value = (char *)c->argv[2]->ptr;
    leveldb_put(server.ds_db, woptions, key, strlen(key), value, strlen(value), &err);
    leveldb_writeoptions_destroy(woptions);
    if(err != NULL)
    {
		addReplyError(c, err);
        leveldb_free(err);
        return ;
    }
    addReply(c,shared.ok);
    return ;
}

void setGenericCommandNew(redisClient *c, int nx, robj *key, robj *val, robj *expire, int unit) {
    long long milliseconds = 0; /* initialized to avoid an harmness warning */

    if (expire) {
        if (getLongLongFromObjectOrReply(c, expire, &milliseconds, NULL) != REDIS_OK)
            return;
        if (milliseconds <= 0) {
            addReplyError(c,"invalid expire time in SETEX");
            return;
        }
        if (unit == UNIT_SECONDS) milliseconds *= 1000;
    }

    if (nx && lookupKeyWrite(c->db,key) != NULL) {
        addReply(c,shared.czero);
        return;
    }
    setKey(c->db,key,val);
    server.dirty++;
    if (expire) setExpire(c->db,key,mstime()+milliseconds);
    addReply(c, nx ? shared.cone : shared.ok);
}

void rl_set(redisClient *c)
{
	char *key, *value;
    char *err = NULL;
    leveldb_writeoptions_t *woptions;

    woptions = leveldb_writeoptions_create();
    
    key   = (char *)c->argv[1]->ptr;
    value = (char *)c->argv[2]->ptr;
    leveldb_put(server.ds_db, woptions, key, strlen(key), value, strlen(value), &err);
    leveldb_writeoptions_destroy(woptions);
    if(err != NULL)
    {
		addReplyError(c, err);
        leveldb_free(err);
        return ;
    }
    //addReply(c,shared.ok);

    //存到redis
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setGenericCommandNew(c,0,c->argv[1],c->argv[2],NULL,0);
}

void ds_delete(redisClient *c)
{
	int  i;
	char *key;
    char *err = NULL;
    leveldb_writeoptions_t *woptions;
	leveldb_writebatch_t   *wb;

    woptions = leveldb_writeoptions_create();
    
	if(c->argc < 3)
	{
		key   = (char *)c->argv[1]->ptr;
		leveldb_delete(server.ds_db, woptions, key, strlen(key), &err);
		leveldb_writeoptions_destroy(woptions);
		if(err != NULL)
		{
			addReplyError(c, err);
			leveldb_free(err);
			return ;
		}
		addReply(c,shared.ok);
		return ;
	}
	
	wb = leveldb_writebatch_create();
	for(i=1; i<c->argc; i++)
	{
		leveldb_writebatch_delete(wb, (char *)c->argv[i]->ptr, strlen((char *)c->argv[i]->ptr));
	}
	leveldb_write(server.ds_db, woptions, wb, &err);
	leveldb_writeoptions_destroy(woptions);
	leveldb_writebatch_destroy(wb);

	if(err != NULL)
	{
		addReplyError(c, err);
		leveldb_free(err);
		return ;
	}
	addReply(c,shared.ok);

    return ;
}

void delCommandNew(redisClient *c) {
    int deleted = 0, j;

    for (j = 1; j < c->argc; j++) {
        if (dbDelete(c->db,c->argv[j])) {
            signalModifiedKey(c->db,c->argv[j]);
            server.dirty++;
            deleted++;
        }
    }
    addReplyLongLong(c,deleted);
}

void rl_delete(redisClient *c)
{
	int  i;
	char *key;
    char *err = NULL;
    leveldb_writeoptions_t *woptions;
	leveldb_writebatch_t   *wb;

    woptions = leveldb_writeoptions_create();
    
	if(c->argc < 3)
	{
		key   = (char *)c->argv[1]->ptr;
		leveldb_delete(server.ds_db, woptions, key, strlen(key), &err);
		leveldb_writeoptions_destroy(woptions);
		if(err != NULL)
		{
			addReplyError(c, err);
			leveldb_free(err);
			return ;
		}
		//addReply(c,shared.ok);
		delCommandNew(c);
		return ;

	}
	
	wb = leveldb_writebatch_create();
	for(i=1; i<c->argc; i++)
	{
		leveldb_writebatch_delete(wb, (char *)c->argv[i]->ptr, strlen((char *)c->argv[i]->ptr));
	}
	leveldb_write(server.ds_db, woptions, wb, &err);
	leveldb_writeoptions_destroy(woptions);
	leveldb_writebatch_destroy(wb);

	if(err != NULL)
	{
		addReplyError(c, err);
		leveldb_free(err);
		return ;
	}
	//addReply(c,shared.ok);
	delCommandNew(c);
    return ;
}

void ds_close()
{
    leveldb_options_set_filter_policy(server.ds_options, NULL);
    leveldb_filterpolicy_destroy(server.policy);
	leveldb_close(server.ds_db);
	leveldb_options_destroy(server.ds_options);
	leveldb_cache_destroy(server.ds_cache);
}

