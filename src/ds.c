#include "redis.h"

/*
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
#else //CHARSET_EBCDIC
		} else if (!isalnum(c) && strchr("_-.", c) == NULL) {
			// Allow only alphanumeric chars and '_', '-', '.'; escape the rest 
			to[0] = '%';
			to[1] = hexchars[os_toascii[c] >> 4];
			to[2] = hexchars[os_toascii[c] & 15];
			to += 3;
#endif //CHARSET_EBCDIC
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
*/

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
	int i;
	size_t val_len;
    char *err, *value;
    
	leveldb_readoptions_t *roptions;
    
	roptions = leveldb_readoptions_create();
	leveldb_readoptions_set_verify_checksums(roptions, 0);
	leveldb_readoptions_set_fill_cache(roptions, 1);

	addReplyMultiBulkLen(c,c->argc-1);
	for(i=1; i<c->argc; i++)
	{
		err     = NULL;
		value   = NULL;
		val_len = 0;
		value   = leveldb_get(server.ds_db, roptions, c->argv[i]->ptr, sdslen((sds)c->argv[i]->ptr), &val_len, &err);
	    if(err != NULL)
		{
			addReplyError(c, err);
			leveldb_free(err);
			leveldb_free(value);
			leveldb_readoptions_destroy(roptions);
			return ;
		}
		else if(val_len > 0)
		{
			addReplyBulkCBuffer(c, value, val_len);
			leveldb_free(value);
			value = NULL;
		}
		else 
		{
			addReply(c,shared.nullbulk);

		}
	}
	leveldb_readoptions_destroy(roptions);
}

void ds_get(redisClient *c)
{
    char *err;
    size_t val_len;
    char *key   = NULL;
    char *value = NULL;
    
	leveldb_readoptions_t *roptions;
    
	roptions = leveldb_readoptions_create();
	leveldb_readoptions_set_verify_checksums(roptions, 0);
	leveldb_readoptions_set_fill_cache(roptions, 1);
    
    err   = NULL;
    key   = (char *)c->argv[1]->ptr;
    value = leveldb_get(server.ds_db, roptions, key, sdslen((sds)key), &val_len, &err);
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

    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.nullbulk)) != NULL) {

	    if (o->type == REDIS_STRING) {
	        addReplyBulk(c,o);
	        return;
	    }
	}

	ds_get(c);
}



void ds_mset(redisClient *c)
{
	int    i;
	char *key, *value;
    char *err = NULL;
    leveldb_writeoptions_t *woptions;
	leveldb_writebatch_t   *wb;

    if((c->argc%2) == 0)
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
		leveldb_writebatch_put(wb, key, sdslen((sds)key), value, sdslen((sds)value));
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


void ds_hincrby(redisClient *c)
{
    char *value;
    sds keyword, data;
    
    size_t val_len;
    char *err = NULL;
    
    int64_t val, recore;
    leveldb_writeoptions_t *woptions;
	leveldb_readoptions_t *roptions;
    
    
	roptions = leveldb_readoptions_create();
	leveldb_readoptions_set_verify_checksums(roptions, 0);
	leveldb_readoptions_set_fill_cache(roptions, 1);
    
    err     = NULL;
    val_len = 0;
    keyword = sdsempty();
    keyword = sdscpy(keyword, c->argv[1]->ptr);
    keyword = sdscatlen(keyword, "*", 1);
    keyword = sdscat(keyword, c->argv[2]->ptr);
    
    value = leveldb_get(server.ds_db, roptions, keyword, sdslen(keyword), &val_len, &err);
    leveldb_readoptions_destroy(roptions);

    if(err != NULL)
	{
        sdsfree(keyword);
		leveldb_free(err);
		if(val_len > 0) leveldb_free(value);
        addReplyError(c, err);
		return ;
	}
	else if(val_len < 1)
	{
        val = 0;
	}
    else
    {
        val = strtoll(value, NULL, 10);
    }
    
    err      = NULL;
    recore   = strtoll(c->argv[3]->ptr, NULL, 10);
    recore   = val + recore;
    data     = sdsfromlonglong(recore);
    
    woptions = leveldb_writeoptions_create();
    leveldb_put(server.ds_db, woptions, keyword, sdslen(keyword), data, sdslen(data), &err);
    leveldb_writeoptions_destroy(woptions);
    if(err != NULL)
    {
		addReplyError(c, err);
        leveldb_free(err);
    }
    else
    {
        addReplyLongLong(c, recore);
    }
    
    sdsfree(data);
    sdsfree(keyword);    
    leveldb_free(value);
    return ;
}

void ds_hmget(redisClient *c)
{
	int i;
	sds keyword;
    size_t val_len;
    char *key, *err = NULL, *value = NULL;
    
	leveldb_readoptions_t *roptions;
    
	roptions = leveldb_readoptions_create();
	leveldb_readoptions_set_verify_checksums(roptions, 0);
	leveldb_readoptions_set_fill_cache(roptions, 1);

	addReplyMultiBulkLen(c, c->argc-2);
    
    key     = (char *)c->argv[1]->ptr;
    keyword = sdsempty();
    
	for(i=2; i<c->argc; i++)
	{
		err     = NULL;
		value   = NULL;
		val_len = 0;
        
        sdsclear(keyword);
        keyword = sdscat(keyword, key);
        keyword = sdscatlen(keyword, "*", 1); 
        keyword = sdscat(keyword, c->argv[i]->ptr);
        
		value   = leveldb_get(server.ds_db, roptions, keyword, sdslen(keyword), &val_len, &err);
	    if(err != NULL)
		{
            sdsfree(keyword);
			leveldb_free(err);
			leveldb_free(value);
            addReplyError(c, err);
			leveldb_readoptions_destroy(roptions);
			return ;
		}
		else if(val_len > 0)
		{
			addReplyBulkCBuffer(c, value, val_len);
			leveldb_free(value);
			value = NULL;
		}
		else 
		{
			addReply(c,shared.nullbulk);

		}
	}
    
    sdsfree(keyword);
	leveldb_readoptions_destroy(roptions);
}

void ds_hmset(redisClient *c)
{
	int    i;
    sds  keyword;
	char *key, *field, *value;
    char *err = NULL;
    leveldb_writeoptions_t *woptions;
	leveldb_writebatch_t   *wb;
    
    if((c->argc%2) != 0)
    {
		addReply(c,shared.nullbulk);
		return ;        
    }
	
    keyword  = sdsempty();
	woptions = leveldb_writeoptions_create();
	wb       = leveldb_writebatch_create();
    key      = (char *)c->argv[1]->ptr;

    keyword = sdscat(keyword, key);
    keyword = sdscatlen(keyword, "*", 1);    
    leveldb_writebatch_put(wb, keyword, sdslen(keyword), "1", 1);
	for(i=2; i<c->argc; i++)
	{
		field = (char *)c->argv[i]->ptr;
		value = (char *)c->argv[++i]->ptr;
        
        sdsclear(keyword);
        keyword = sdscat(keyword, key);
        keyword = sdscatlen(keyword, "*", 1);
        keyword = sdscat(keyword, field);
		leveldb_writebatch_put(wb, keyword, sdslen(keyword), value, sdslen((sds)value));
	}
    sdsfree(keyword);
    
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

void ds_hset(redisClient *c)
{
    sds str;
    char *key, *field, *value, *err;
    leveldb_writeoptions_t *woptions;
	leveldb_writebatch_t   *wb;
    
    key   = (char *)c->argv[1]->ptr;
    field = (char *)c->argv[2]->ptr;
    value = (char *)c->argv[3]->ptr;
    
	woptions = leveldb_writeoptions_create();
	wb       = leveldb_writebatch_create();
    
    str      = sdsempty();
    str      = sdscpy(str, key);
    str      = sdscatlen(str, "*", 1);
            
    leveldb_writebatch_put(wb, str, sdslen(str), "1", 1);
    
    sdsclear(str);
    str      = sdscpy(str, key);
    str      = sdscatlen(str, "*", 1);
    str      = sdscat(str, field);
    leveldb_writebatch_put(wb, str, sdslen(str), value, sdslen((sds)value));
    
	leveldb_write(server.ds_db, woptions, wb, &err);
    
	leveldb_writeoptions_destroy(woptions);
	leveldb_writebatch_destroy(wb);
    sdsfree(str);
    
    addReply(c,shared.ok);
    return ;
}

void ds_hgetall(redisClient *c)
{
    sds str, header;
    char *keyword = NULL;
    
    const char *key, *value;
    size_t key_len, value_len, len, i;
    
    leveldb_iterator_t    *iter;
    leveldb_readoptions_t *roptions;
    
	roptions = leveldb_readoptions_create();
	leveldb_readoptions_set_verify_checksums(roptions, 0);
	leveldb_readoptions_set_fill_cache(roptions, 1);
    
    i       = 0;
    str     = sdsempty();
    iter    = leveldb_create_iterator(server.ds_db, roptions);
    
    str     = sdscpy(str, c->argv[1]->ptr);
    str     = sdscatlen(str, "*", 1);
    len     = sdslen(str);
    keyword = zmalloc(len+1);
    memcpy(keyword, str, len);

    leveldb_iter_seek(iter, keyword, len);
    
    if(!leveldb_iter_valid(iter))
    {
        sdsfree(str);
        zfree(keyword);
        addReply(c,shared.nullbulk);
        leveldb_iter_destroy(iter);
        leveldb_readoptions_destroy(roptions);
        return ;
    }
    
    sdsclear(str);
    leveldb_iter_next(iter);
    while(1)
    {
        if(!leveldb_iter_valid(iter))
            break;
        
        key_len = value_len = 0;
        key   = leveldb_iter_key(iter, &key_len);
        value = leveldb_iter_value(iter, &value_len);
        
        if(strncmp(keyword, key, len) != 0)
            break;
        
        str = sdscatprintf(str, "$%lu\r\n", key_len-len);
        str = sdscatlen(str, key+len, key_len-len);
        str = sdscatprintf(str, "\r\n$%lu\r\n", value_len);
        str = sdscatlen(str, value, value_len);
        str = sdscatlen(str, "\r\n", 2);
        
        i++;
        leveldb_iter_next(iter);
    }
    
    if(i == 0)
    {
        addReply(c,shared.nullbulk);
    }
    else
    {   
        header = sdsempty();
        header = sdscatprintf(header, "*%lu\r\n", i*2);
        header = sdscatlen(header, str, sdslen(str));
        addReplySds(c, header);
        sdsfree(header);
    }
    
    sdsfree(str);
    zfree(keyword);
    leveldb_iter_destroy(iter);
    leveldb_readoptions_destroy(roptions);
    return ;
}


void ds_hdel(redisClient *c)
{
	const char *key;
    size_t key_len, i;
    
    sds keyword;
    char *err = NULL;
    leveldb_writeoptions_t *woptions;
	leveldb_writebatch_t   *wb;

    leveldb_iterator_t    *iter;
    leveldb_readoptions_t *roptions;
    
	roptions = leveldb_readoptions_create();
	leveldb_readoptions_set_verify_checksums(roptions, 0);
	leveldb_readoptions_set_fill_cache(roptions, 1);
    
    iter    = leveldb_create_iterator(server.ds_db, roptions);
    
    keyword  = sdsempty();
    woptions = leveldb_writeoptions_create();
    
	if(c->argc < 3)
	{
        keyword = sdscpy(keyword, c->argv[1]->ptr);
        keyword = sdscatlen(keyword, "*", 1);
		
        leveldb_iter_seek(iter, keyword, sdslen(keyword));
        if(!leveldb_iter_valid(iter))
        {
            sdsfree(keyword);
            addReply(c,shared.nullbulk);
            leveldb_iter_destroy(iter);
            leveldb_readoptions_destroy(roptions);
            leveldb_writeoptions_destroy(woptions);
            return ;
        }
        
        wb = leveldb_writebatch_create();
        while(1)
        {
            key_len = 0;
            key     = leveldb_iter_key(iter, &key_len);
        
            if(strncmp(keyword, key, sdslen(keyword)) != 0)
                break;
            
            //printf("key = %s\r\n", key);
            leveldb_writebatch_delete(wb, key, key_len);
            leveldb_iter_next(iter);
            if(!leveldb_iter_valid(iter))
                break;
        }
    	
        leveldb_write(server.ds_db, woptions, wb, &err);
    	leveldb_writeoptions_destroy(woptions);
    	leveldb_writebatch_destroy(wb);
        
    	if(err != NULL)
    	{
    		addReplyError(c, err);
    		leveldb_free(err);
            
            sdsfree(keyword);
            leveldb_iter_destroy(iter);
            leveldb_readoptions_destroy(roptions);
            leveldb_writeoptions_destroy(woptions);
    		return ;
    	}
		addReply(c,shared.ok);
        
        sdsfree(keyword);
        leveldb_iter_destroy(iter);
        leveldb_readoptions_destroy(roptions);
        leveldb_writeoptions_destroy(woptions);
        
		return ;
	}
	
	wb = leveldb_writebatch_create();
	for(i=2; i<c->argc; i++)
	{
        sdsclear(keyword);
        keyword = sdscpy(keyword, c->argv[1]->ptr);
        keyword = sdscatlen(keyword, "*", 1);
        keyword = sdscat(keyword, c->argv[i]->ptr);
		leveldb_writebatch_delete(wb, keyword, sdslen(keyword));
	}
    
    sdsfree(keyword);
	leveldb_write(server.ds_db, woptions, wb, &err);
	leveldb_readoptions_destroy(roptions);
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

void ds_hget(redisClient *c)
{
    sds str;
    size_t val_len = 0;
    char *key = NULL, *field = NULL, *value = NULL, *err = NULL;
    
	leveldb_readoptions_t *roptions;
    
    key   = (char *)c->argv[1]->ptr;
    field = (char *)c->argv[2]->ptr;
    
	roptions = leveldb_readoptions_create();
	leveldb_readoptions_set_verify_checksums(roptions, 0);
	leveldb_readoptions_set_fill_cache(roptions, 1);
    
    str      = sdsnew(key);
    str      = sdscatlen(str, "*", 1);
    str      = sdscat(str, field);
    value = leveldb_get(server.ds_db, roptions, str, sdslen(str), &val_len, &err);
    leveldb_readoptions_destroy(roptions);
    
    if(err != NULL)
    {
		addReplyError(c, err);
        leveldb_free(err);
		if(val_len > 0) leveldb_free(value);

        return ;
    }
	else if(value == NULL)
    {
        addReply(c,shared.nullbulk);
        return ;
    }    
    sdsfree(str);
    
    addReplyBulkCBuffer(c, value, val_len);
    leveldb_free(value);
}


void ds_incrby(redisClient *c)
{
    sds  data;
    char *value;
    int64_t val, recore;
    
    size_t val_len;
    char *err = NULL;
    leveldb_writeoptions_t *woptions;
	leveldb_readoptions_t *roptions;
    
    
	roptions = leveldb_readoptions_create();
	leveldb_readoptions_set_verify_checksums(roptions, 0);
	leveldb_readoptions_set_fill_cache(roptions, 1);
    
    err     = NULL;
    val_len = 0;
    
    value = leveldb_get(server.ds_db, roptions, c->argv[1]->ptr, sdslen((sds)c->argv[1]->ptr), &val_len, &err);
    leveldb_readoptions_destroy(roptions);

    if(err != NULL)
	{
		leveldb_free(err);
		if(val_len > 0) leveldb_free(value);
        addReplyError(c, err);
		return ;
	}
	else if(val_len < 1)
	{
        val = 0;
	}
    else
    {
        val = strtoll(value, NULL, 10);
    }
    
    err      = NULL;
    recore   = strtoll(c->argv[2]->ptr, NULL, 10);
    recore   = val + recore;
    data     = sdsfromlonglong(recore);
    woptions = leveldb_writeoptions_create();
    
    leveldb_put(server.ds_db, woptions, c->argv[1]->ptr, sdslen((sds)c->argv[1]->ptr), data, sdslen(data), &err);
    leveldb_writeoptions_destroy(woptions);
    if(err != NULL)
    {
		addReplyError(c, err);
        leveldb_free(err);
    }
    else
    {
        addReplyLongLong(c, recore);
    }
    
    sdsfree(data);
    leveldb_free(value);
    return ;
}


void ds_append(redisClient *c)
{
	sds recore;
    char *value;
    
    size_t val_len;
    char *err = NULL;
    leveldb_writeoptions_t *woptions;
	leveldb_readoptions_t *roptions;
    
    
	roptions = leveldb_readoptions_create();
	leveldb_readoptions_set_verify_checksums(roptions, 0);
	leveldb_readoptions_set_fill_cache(roptions, 1);
    
    err     = NULL;
    val_len = 0;
    
    value = leveldb_get(server.ds_db, roptions, c->argv[1]->ptr, sdslen((sds)c->argv[1]->ptr), &val_len, &err);
    leveldb_readoptions_destroy(roptions);

    if(err != NULL)
	{
		leveldb_free(err);
		if(val_len > 0) leveldb_free(value);
        addReplyError(c, err);
		return ;
	}
	
    
    err      = NULL;
    recore   = sdsempty();
    if(val_len > 0)
    {
        recore = sdscpy(recore, value);
    }    
    recore = sdscat(recore, c->argv[1]->ptr);
    woptions = leveldb_writeoptions_create();
    
    leveldb_put(server.ds_db, woptions, c->argv[1]->ptr, sdslen((sds)c->argv[1]->ptr), recore, sdslen(recore), &err);
    leveldb_writeoptions_destroy(woptions);
    if(err != NULL)
    {
		addReplyError(c, err);
        leveldb_free(err);
    }
    else
    {
        addReply(c,shared.ok);
    }
    
    sdsfree(recore);
    leveldb_free(value);
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
    leveldb_put(server.ds_db, woptions, key, sdslen((sds)key), value, sdslen((sds)value), &err);
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

void rl_set(redisClient *c)
{
	char *key, *value;
    char *err = NULL;
    leveldb_writeoptions_t *woptions;

    woptions = leveldb_writeoptions_create();

    key   = (char *)c->argv[1]->ptr;
    value = (char *)c->argv[2]->ptr;
    leveldb_put(server.ds_db, woptions, key, sdslen((sds)key), value, sdslen((sds)value), &err);
    leveldb_writeoptions_destroy(woptions);
    if(err != NULL)
    {
        addReplyError(c, err);
        leveldb_free(err);
        return ;
    }
    //addReply(c,shared.ok);

    //存到redis
    setCommand(c);
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
		leveldb_delete(server.ds_db, woptions, key, sdslen((sds)key), &err);
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
		leveldb_writebatch_delete(wb, (char *)c->argv[i]->ptr, sdslen((sds)c->argv[i]->ptr));
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



void rl_delete(redisClient *c)
{
	ds_delete(c);
    delCommand(c);
}

void ds_close()
{
    leveldb_options_set_filter_policy(server.ds_options, NULL);
    leveldb_filterpolicy_destroy(server.policy);
	leveldb_close(server.ds_db);
	leveldb_options_destroy(server.ds_options);
	leveldb_cache_destroy(server.ds_cache);
}

