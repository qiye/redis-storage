#include "redis.h"

#define KEY_PREFIX_HASH "\xFF\x01"
#define KEY_PREFIX_ZSET "\xFF\x02"
#define KEY_PREFIX_SET  "\xFF\x03"
#define KEY_PREFIX_LENGTH 2
#define MEMBER_PREFIX   "\x01"
#define MEMBER_PREFIX_LENGTH 1

// 常规key不使用前缀，用户自己控制自己的key不带有上述前缀

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
    
    server.woptions = leveldb_writeoptions_create();
	server.roptions = leveldb_readoptions_create();
	leveldb_readoptions_set_verify_checksums(server.roptions, 0);
	leveldb_readoptions_set_fill_cache(server.roptions, 1);    
    
    leveldb_writeoptions_set_sync(server.woptions, 0);
}

void ds_exists(redisClient *c)
{
    int                   i;
    char                  *err;
    leveldb_iterator_t    *iter;
	char *kp;size_t kl;
    
    iter     = leveldb_create_iterator(server.ds_db, server.roptions);
    addReplyMultiBulkLen(c, c->argc-1);
	for(i=1; i<c->argc; i++)
	{
        leveldb_iter_seek(iter, c->argv[i]->ptr, sdslen((sds)c->argv[i]->ptr));
        if(leveldb_iter_valid(iter)){
		  
		  kp = (char *)leveldb_iter_key(iter,&kl);

		  if( sdslen((sds)c->argv[i]->ptr) == kl && 0 == memcmp(c->argv[i]->ptr,kp,kl))
			addReplyLongLong(c,1);
		  else
			addReplyLongLong(c,0);
		  
        }else
            addReplyLongLong(c, 0);
	}
    
    err = NULL;
    leveldb_iter_get_error(iter, &err);
    leveldb_iter_destroy(iter);
    
    if(err != NULL)
    {
		addReplyError(c, err);
        leveldb_free(err);

        return ;
    }
    
    return ;
}

void ds_hexists(redisClient *c)
{
    int                   i;
    sds                   key;
    char                  *err;
    leveldb_iterator_t    *iter;
	char *kp;size_t kl;
    
    key      = sdsempty();
    iter     = leveldb_create_iterator(server.ds_db, server.roptions);
    addReplyMultiBulkLen(c, c->argc-2);
	for(i=2; i<c->argc; i++)
	{
        sdsclear(key);
        key      = sdscatlen(key,KEY_PREFIX_HASH,KEY_PREFIX_LENGTH);
        key      = sdscat(key, c->argv[1]->ptr);
        key      = sdscatlen(key, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);
        key      = sdscat(key, c->argv[i]->ptr);
        
        leveldb_iter_seek(iter, key, sdslen(key));
        if(leveldb_iter_valid(iter)){
		  kp = (char*)leveldb_iter_key(iter,&kl);

			if( sdslen(key) == kl && 0 == memcmp(key,kp,kl))
			  addReplyLongLong(c,1);
			else
			  addReplyLongLong(c,0);
			
        }else
            addReplyLongLong(c, 0);
	}
    
    err = NULL;
    leveldb_iter_get_error(iter, &err);
    leveldb_iter_destroy(iter);
    sdsfree(key);
    if(err != NULL)
    {
		addReplyError(c, err);
        leveldb_free(err);

        return ;
    }
    
    return ;
}

/**
 * usage: ds_keys_count startKey endKey
 * return: integer
 */
void ds_keys_count(redisClient *c)
{
	char *err;
	
    const char *key;
    size_t key_len, len, i;
	
	leveldb_iterator_t *iter;

	char *skey,*ekey; //  key pair
	size_t skey_len,ekey_len;
	
	char *k1,*k2;
	int cmp;

	skey = c->argv[1]->ptr;
	skey_len = sdslen(c->argv[1]->ptr);

	ekey = c->argv[2]->ptr;
	ekey_len = sdslen(c->argv[2]->ptr);


	// return 0 when skey > ekey
	k1 = zmalloc(skey_len+1);
	k2 = zmalloc(ekey_len+1);
	memcpy(k1,skey,skey_len);
	memcpy(k2,ekey,ekey_len);
	k1[skey_len] = k2[ekey_len] = '\0';

	cmp = strcmp(k1,k2);
	
	if(cmp>0){
	  zfree(k1);
	  zfree(k2);
	  addReplyLongLong(c,0);
	  return;
	}

    i       = 0;
    len     = 0;

    iter = leveldb_create_iterator(server.ds_db, server.roptions);
	
	leveldb_iter_seek(iter,skey,skey_len);
	
	for(;leveldb_iter_valid(iter); leveldb_iter_next(iter))
    {
        key_len = 0;
        key     = leveldb_iter_key(iter, &key_len);
        
		// skip KEY_PREFIX_
		if(((unsigned char*)key)[0] == 0xFF){

            //位于haskkey 空间，后面的key无需扫描
            break;
            //continue;
        }

		if(k1 != NULL){
		  zfree(k1);
		  k1 = NULL;
		}

		k1 = zmalloc(key_len+1);
		memcpy(k1,key,key_len);
		k1[key_len] = '\0';
		
		cmp = strcmp(k1,k2);
		
		if(cmp > 0)
		  break;
		
		i++;
    }
    
    err = NULL;
    leveldb_iter_get_error(iter, &err);
	leveldb_iter_destroy(iter);

    if(err)
    {
		addReplyError(c, err);
		leveldb_free(err);
	}

	addReplyLongLong(c,i);

	if(k1) zfree(k1);
	if(k2) zfree(k2);

    return ;
}

/**
 * hashkey range counter
 *
 * usage: ds_hkeys_count startKey endKey
 * return: integer
 */
void ds_hkeys_count(redisClient *c)
{
	char *err;
	
    const char *key;
    size_t key_len, len, i;
	
	leveldb_iterator_t *iter;

	char *skey,*ekey; //  key pair
	size_t skey_len,ekey_len;
	
	char *k1,*k2;
	int cmp;
    
	skey = (char *)c->argv[1]->ptr;
	skey_len = sdslen(skey);

	ekey = (char *)c->argv[2]->ptr;
	ekey_len = sdslen(ekey);


	// return 0 when skey > ekey
	k1 = zmalloc(skey_len + KEY_PREFIX_LENGTH + MEMBER_PREFIX_LENGTH + 1);
	k2 = zmalloc(ekey_len + KEY_PREFIX_LENGTH + MEMBER_PREFIX_LENGTH + 1);
    memcpy(k1,KEY_PREFIX_HASH,KEY_PREFIX_LENGTH);
	memcpy(k1 + KEY_PREFIX_LENGTH,skey,skey_len);
    memcpy(k2,KEY_PREFIX_HASH,KEY_PREFIX_LENGTH);
	memcpy(k2 + KEY_PREFIX_LENGTH,ekey,ekey_len);
	k1[skey_len + KEY_PREFIX_LENGTH] = k2[ekey_len + KEY_PREFIX_LENGTH] = MEMBER_PREFIX[0];
	k1[skey_len + KEY_PREFIX_LENGTH + MEMBER_PREFIX_LENGTH] = k2[ekey_len + KEY_PREFIX_LENGTH + MEMBER_PREFIX_LENGTH] = '\0';

	cmp = strcmp(k1,k2);
	
	if(cmp>0){
	  zfree(k1);
	  zfree(k2);
	  addReplyLongLong(c,0);
	  return;
	}

    i       = 0;
    len     = 0;

    iter = leveldb_create_iterator(server.ds_db, server.roptions);
	
	leveldb_iter_seek(iter,k1,skey_len+1);
	
	for(;leveldb_iter_valid(iter); leveldb_iter_next(iter))
    {
        key_len = 0;
        key     = leveldb_iter_key(iter, &key_len);
        
		if(key_len < KEY_PREFIX_LENGTH + MEMBER_PREFIX_LENGTH
           || 0 != memcmp(key,KEY_PREFIX_HASH,KEY_PREFIX_LENGTH) // not hashkey
           ){

            //已经不在hashkey空间
            break;
        }
        if(((unsigned char *)key)[key_len-1] != MEMBER_PREFIX[0]){
            // hash member
            continue;
        }

		if(k1 != NULL){
		  zfree(k1);
		  k1 = NULL;
		}

		k1 = zmalloc(key_len+1);
		memcpy(k1,key,key_len);
		k1[key_len] = '\0';
		
		cmp = strcmp(k1,k2);
		
		if(cmp > 0)
		  break;
		
		i++;
    }
    
    err = NULL;
    leveldb_iter_get_error(iter, &err);
	leveldb_iter_destroy(iter);

    if(err)
    {
		addReplyError(c, err);
		leveldb_free(err);
	}

	addReplyLongLong(c,i);

	if(k1) zfree(k1);
	if(k2) zfree(k2);

    return ;
}


/**
 * usage: 
 *  1. ds_keys_asc
 *  2. ds_keys_asc key
 *  3. ds_keys_asc 100
 *  4. ds_keys_asc key 100
 */
void ds_keys_asc(redisClient *c)
{
    sds str, header;
	char *err;
	
    unsigned long limit;
    const char *key;
    size_t key_len, len, i;
	
	leveldb_iterator_t *iter;

	char *skey; // start key
	size_t skey_len=0;
	int is_limit;

	// max result = 1000,large limit will eat lot's of disk/network io.
	const unsigned long max_result = 1000;
	
    limit    = 30;   //default
	skey    = NULL; //seek first
    
	//limit    = strtoul(c->argv[1]->ptr, NULL, 10);
    
	if(c->argc > 3){
	  addReplyError(c,"too many arguments.");
	  return;
	}else if(c->argc == 1){
	  // default limit and seek to first
	}else if(c->argc == 2){
	  
	  //is digit ?
	  is_limit = 1;
	  len = sdslen(c->argv[1]->ptr);
	  i=0;
	  while(i<len){
		if(!isdigit(((char *)c->argv[1]->ptr)[i++])){
		  is_limit=0;
		  break;
		}
	  }

	  if(is_limit){
		limit = strtoul(c->argv[1]->ptr,NULL,10);
		
	  }else{

		// start key
		skey = c->argv[1]->ptr;
		skey_len = len;
	  }
	}else{
	  // 3 args
	  // argv[1] as start key
	  skey = c->argv[1]->ptr;
	  skey_len = sdslen(c->argv[1]->ptr);

	  // argv[2] as limit
	  limit = strtoul(c->argv[2]->ptr,NULL,10);
	  
	}

	//check limit
	if(limit == 0)
	  limit = 30;

	if(limit > max_result)
	  limit = max_result;
	

    i       = 0;
    len     = 0;
    str     = sdsempty();

    iter = leveldb_create_iterator(server.ds_db, server.roptions);
	
	if(skey) //seek key
	  leveldb_iter_seek(iter,skey,skey_len);
	else
	  leveldb_iter_seek_to_first(iter);
	
	for(;leveldb_iter_valid(iter); leveldb_iter_next(iter))
    {
        key_len = 0;
        key     = leveldb_iter_key(iter, &key_len);
        
		// skip hashtable and hash field
		if(((unsigned char*)key)[0] == 0xFF){

            //已经位于 hashkey的空间了，扫描后面的key已经没有意义

            break;
            //continue;
        }
        else
        {
            str = sdscatprintf(str, "$%zu\r\n", key_len);
            str = sdscatlen(str, key, key_len);
            str = sdscatlen(str, "\r\n", 2);
            i++;
        }

        if(i >= limit)
            break;
        
    }
    
    err = NULL;
    leveldb_iter_get_error(iter, &err);
	leveldb_iter_destroy(iter);

    if(err)
    {
		addReplyError(c, err);
		leveldb_free(err);
	}
	else if(i == 0)
    {
        addReply(c,shared.nullbulk);
    }
    else
    {   
        header = sdsempty();
        header = sdscatprintf(header, "*%zu\r\n", i);
        header = sdscatlen(header, str, sdslen(str));
        
        addReplySds(c, header);
		//addReplySds() will free header
    }
	sdsfree(str);
    return ;
}

/**
 * hashkey asc iterator
 * usage: 
 *  1. ds_hkeys_asc
 *  2. ds_hkeys_asc key
 *  3. ds_hkeys_asc 100
 *  4. ds_hkeys_asc key 100
 */
void ds_hkeys_asc(redisClient *c)
{
    sds str, header,skeyh;
	char *err;
	
    unsigned long limit;
    const char *key;
    size_t key_len, len, i;
	
	leveldb_iterator_t *iter;

	char *skey; // start key
	size_t skey_len=0;
	int is_limit;
    
	// max result = 1000,large limit will eat lot's of disk/network io.
	const unsigned long max_result = 1000;

    limit    = 30;   //default
	skey    = NULL; //seek first
    
	//limit    = strtoul(c->argv[1]->ptr, NULL, 10);
    
	if(c->argc > 3){
	  addReplyError(c,"too many arguments.");
	  return;
	}else if(c->argc == 1){
	  // default limit and seek to first
	}else if(c->argc == 2){
	  
	  //is digit ?
	  is_limit = 1;
	  len = sdslen(c->argv[1]->ptr);
	  i=0;
	  while(i<len){
		if(!isdigit(((char *)c->argv[1]->ptr)[i++])){
		  is_limit=0;
		  break;
		}
	  }

	  if(is_limit){
		limit = strtoul(c->argv[1]->ptr,NULL,10);
		
	  }else{

		// start key
		skey = c->argv[1]->ptr;
		skey_len = len;
	  }
	}else{
	  // 3 args
	  // argv[1] as start key
	  skey = c->argv[1]->ptr;
	  skey_len = sdslen(c->argv[1]->ptr);

	  // argv[2] as limit
	  limit = strtoul(c->argv[2]->ptr,NULL,10);
	  
	}

	//check limit
	if(limit == 0)
	  limit = 30;

	if(limit > max_result)
	  limit = max_result;
	
    i       = 0;
    len     = 0;
    str     = sdsempty();
    skeyh   = sdsempty();


    iter = leveldb_create_iterator(server.ds_db, server.roptions);
	
	if(skey){ //seek key
        skeyh = sdscatlen(skeyh,KEY_PREFIX_HASH,KEY_PREFIX_LENGTH);
        skeyh = sdscatlen(skeyh,skey,skey_len);
        skeyh = sdscatlen(skeyh,MEMBER_PREFIX,MEMBER_PREFIX_LENGTH);

        //fprintf(stderr,"SK: %s\n",skeyh);

        leveldb_iter_seek(iter,skeyh,sdslen(skeyh));
	}else{
        //hashkey空间下限是 KEY_PREFIX_HASH

        leveldb_iter_seek(iter,KEY_PREFIX_HASH,KEY_PREFIX_LENGTH);

        //如果失败，说明没有任何hashkey存在
    }
	
	for(;leveldb_iter_valid(iter); leveldb_iter_next(iter))
    {
        key_len = 0;
        key     = leveldb_iter_key(iter, &key_len);
        
        //fprintf(stderr,"FK: %s\n",key);

		if(key_len < KEY_PREFIX_LENGTH + MEMBER_PREFIX_LENGTH
           || 0 != memcmp(key,KEY_PREFIX_HASH,KEY_PREFIX_LENGTH) // not hashkey
           ){
            //已经不在hashkey空间
            break;
        }
        if(((unsigned char *)key)[key_len-1] != MEMBER_PREFIX[0]){
            // hash member
            continue;
        }
        else
        {
            str = sdscatprintf(str, "$%zu\r\n", key_len - MEMBER_PREFIX_LENGTH - KEY_PREFIX_LENGTH); // remove the MEMBER_PREFIX
            str = sdscatlen(str, key + KEY_PREFIX_LENGTH, key_len - KEY_PREFIX_LENGTH - MEMBER_PREFIX_LENGTH);
            str = sdscatlen(str, "\r\n", 2);
            i++;
        }

        if(i >= limit)
            break;
        
    }
    
    err = NULL;
    leveldb_iter_get_error(iter, &err);
	leveldb_iter_destroy(iter);

    if(err)
    {
		addReplyError(c, err);
		leveldb_free(err);
	}
	else if(i == 0)
    {
        addReply(c,shared.nullbulk);
    }
    else
    {   
        header = sdsempty();
        header = sdscatprintf(header, "*%zu\r\n", i);
        header = sdscatlen(header, str, sdslen(str));
        
        addReplySds(c, header);
		//addReplySds() will free header
    }
	sdsfree(str);
    sdsfree(skeyh);
    return ;
}

/**
 * usage: 
 *  1. ds_keys_desc
 *  2. ds_keys_desc key
 *  3. ds_keys_desc 100
 *  4. ds_keys_desc key 100
 */
void ds_keys_desc(redisClient *c)
{
    sds str, header;
	char *err;
	
    unsigned long limit;
    const char *key;
    size_t key_len, len, i;
	
	leveldb_iterator_t *iter;

	char *skey; // start key
	size_t skey_len=0;
	int is_limit;

	char *ck1=NULL,*ck2=NULL ; // compaire the first key

	// max result = 1000,large limit will eat lot's of disk/network io.
	const unsigned long max_result = 1000;
	
    limit    = 30;   //default
	skey    = NULL; //seek first
    
	//limit    = strtoul(c->argv[1]->ptr, NULL, 10);
    
	if(c->argc > 3){
	  addReplyError(c,"too many arguments.");
	  return;
	}else if(c->argc == 1){
	  // default limit and seek to first
	}else if(c->argc == 2){
	  
	  //is digit ?
	  is_limit = 1;
	  len = sdslen(c->argv[1]->ptr);
	  i=0;
	  while(i<len){
		if(!isdigit(((char *)c->argv[1]->ptr)[i++])){
		  is_limit=0;
		  break;
		}
	  }

	  if(is_limit){
		limit = strtoul(c->argv[1]->ptr,NULL,10);
		
	  }else{

		// start key
		skey = c->argv[1]->ptr;
		skey_len = len;
	  }
	}else{
	  // 3 args
	  // argv[1] as start key
	  skey = c->argv[1]->ptr;
	  skey_len = sdslen(c->argv[1]->ptr);

	  // argv[2] as limit
	  limit = strtoul(c->argv[2]->ptr,NULL,10);
	  
	}

	//check limit
	if(limit == 0)
	  limit = 30;

	if(limit > max_result)
	  limit = max_result;
	

    i       = 0;
    len     = 0;
    str     = sdsempty();

    iter = leveldb_create_iterator(server.ds_db, server.roptions);
	
	if(skey){ //seek key
	  leveldb_iter_seek(iter,skey,skey_len);
      
      // maybe, there's no stored key >= skey
      // we should seek to last
      if(!leveldb_iter_valid(iter))
          leveldb_iter_seek_to_last(iter);
      
      //普通key定位失败，说明skey已经大于当前db中的最大key了，直接定位到最后

	}else{
        //对于常规key，上限是 0xFF
        leveldb_iter_seek(iter,"\xFF",1);
        
        //如果失败，说明整个key空间只有常规key，直接定位到最后
        if(!leveldb_iter_valid(iter))
            leveldb_iter_seek_to_last(iter);
	}

	for(;leveldb_iter_valid(iter); leveldb_iter_prev(iter))
    {
        key_len = 0;
        key     = leveldb_iter_key(iter, &key_len);
        
		//fprintf(stderr,"current key:%s\n",key);

		// skip hashtable and hash field
		if(((unsigned char*)key)[0] == 0xFF){
            //前面的操作已经保证我们位于常规key空间的上限附近，这批数量不会太多，跳过
            continue;
        }
        else
        {
		  
		  if(skey && ck1 == NULL){
			// we are in reverse order,and seek() always stop at key>= what we want
			// so if current key > what we want ,we should skip it.

			// skey and key are both \0 terminated
			ck1 = (char*)zmalloc(skey_len+1);
			ck2 = (char*)zmalloc(key_len+1);
			memcpy(ck1,skey,skey_len);
			memcpy(ck2,key,key_len);
			ck1[skey_len] = '\0';
			ck2[key_len] = '\0';

			//fprintf(stderr,"\n\nkeyF:%s\nkeyS:%s\n\n",ck2,ck1);

			if(strcmp(ck2,ck1) >0 )
			  continue;

		  }


            str = sdscatprintf(str, "$%zu\r\n", key_len);
            str = sdscatlen(str, key, key_len);
            str = sdscatlen(str, "\r\n", 2);
            i++;
        }

        if(i >= limit)
            break;
        
    }
    
    err = NULL;
    leveldb_iter_get_error(iter, &err);
	leveldb_iter_destroy(iter);

    if(err)
    {
		addReplyError(c, err);
		leveldb_free(err);
	}
	else if(i == 0)
    {
        addReply(c,shared.nullbulk);
    }
    else
    {   
        header = sdsempty();
        header = sdscatprintf(header, "*%zu\r\n", i);
        header = sdscatlen(header, str, sdslen(str));
        
        addReplySds(c, header);
		//addReplySds() will free header
    }

	if(ck1)
	  zfree(ck1);
	if(ck2)
	  zfree(ck2);

	sdsfree(str);
    return ;
}


/**
 * hashkey desc itorator
 * usage: 
 *  1. ds_hkeys_desc
 *  2. ds_hkeys_desc key
 *  3. ds_hkeys_desc 100
 *  4. ds_hkeys_desc key 100
 */
void ds_hkeys_desc(redisClient *c)
{
    sds str, header,skeyh;
	char *err;
	
    unsigned long limit;
    const char *key;
    size_t key_len, len, i,e;
	
	leveldb_iterator_t *iter;

    unsigned char maxkey[KEY_PREFIX_LENGTH+1];
	char *skey; // start key
	size_t skey_len=0;
	int is_limit;

	char *ck1=NULL,*ck2=NULL ; // compaire the first key

	// max result = 1000,large limit will eat lot's of disk/network io.
	const unsigned long max_result = 1000;
	
    limit    = 30;   //default
	skey    = NULL; //seek first
    
	//limit    = strtoul(c->argv[1]->ptr, NULL, 10);
    
	if(c->argc > 3){
	  addReplyError(c,"too many arguments.");
	  return;
	}else if(c->argc == 1){
	  // default limit and seek to first
	}else if(c->argc == 2){
	  
	  //is digit ?
	  is_limit = 1;
	  len = sdslen(c->argv[1]->ptr);
	  i=0;
	  while(i<len){
		if(!isdigit(((char *)c->argv[1]->ptr)[i++])){
		  is_limit=0;
		  break;
		}
	  }

	  if(is_limit){
		limit = strtoul(c->argv[1]->ptr,NULL,10);
		
	  }else{

		// start key
		skey = c->argv[1]->ptr;
		skey_len = len;
	  }
	}else{
	  // 3 args
	  // argv[1] as start key
	  skey = c->argv[1]->ptr;
	  skey_len = sdslen(c->argv[1]->ptr);

	  // argv[2] as limit
	  limit = strtoul(c->argv[2]->ptr,NULL,10);
	  
	}

	//check limit
	if(limit == 0)
	  limit = 30;

	if(limit > max_result)
	  limit = max_result;

    //fprintf(stderr,"l:%d sk:%s\n",limit ,skey ? skey :"<nil>");

    i       = 0;
    e       = 0;
    len     = 0;
    str     = sdsempty();
    skeyh   = sdsempty();

    memcpy(maxkey,KEY_PREFIX_HASH,KEY_PREFIX_LENGTH);
    maxkey[KEY_PREFIX_LENGTH - 1] = KEY_PREFIX_HASH[KEY_PREFIX_LENGTH - 1]+1; // e.g. 0xFF01 -> 0xFF02

    iter = leveldb_create_iterator(server.ds_db, server.roptions);
	
	if(skey){ //seek key
        skeyh = sdscatlen(skeyh,KEY_PREFIX_HASH,KEY_PREFIX_LENGTH);
        skeyh = sdscatlen(skeyh,skey,skey_len);
        skeyh = sdscatlen(skeyh,MEMBER_PREFIX,MEMBER_PREFIX_LENGTH);
        leveldb_iter_seek(iter,skeyh,sdslen(skeyh));

        // maybe, there's no stored key >= skey
        // we should seek to last
        if(!leveldb_iter_valid(iter)){
            //定位到hashkey上限
            leveldb_iter_seek(iter,(char *)maxkey,KEY_PREFIX_LENGTH);

            if(!leveldb_iter_valid(iter)){
                //如果失败，表明hashkey上限之上已经没有key存在
                leveldb_iter_seek_to_last(iter);
            }
        }
    }else{
        //定位到hashkey上限
        leveldb_iter_seek(iter,(char *)maxkey,KEY_PREFIX_LENGTH);

        if(!leveldb_iter_valid(iter)){
            //如果失败，定位到最后
            leveldb_iter_seek_to_last(iter);
        }
	}
	for(;leveldb_iter_valid(iter); leveldb_iter_prev(iter))
    {
        key_len = 0;
        key     = leveldb_iter_key(iter, &key_len);
        
		//fprintf(stderr,"current key:%s\n",key);

		if(key_len < KEY_PREFIX_LENGTH + MEMBER_PREFIX_LENGTH
           || 0 != memcmp(key,KEY_PREFIX_HASH,KEY_PREFIX_LENGTH) // not hashkey
           ){

            //fprintf(stderr,"miss match key:%s\n",key);
            
            e++; //无效命中计数

            if( e > 10) break; //之前已经定位到hashkey空间上限附近，如果无效命中太多，说明已经偏离太多，不用继续往前扫描了。

            continue;
        }
        
        if(((unsigned char *)key)[key_len-1] != MEMBER_PREFIX[0] ){
            // hash member
            continue;
        }
        else
        {
		  
		  if(skey && ck1 == NULL){
              // we are in reverse order,and seek() always stop at key>= what we want
              // so if current key > what we want ,we should skip it.
              
              // skey and key are both \0 terminated
              ck1 = (char*)zmalloc(sdslen(skeyh)+1); //skeyh is avaliable,and contain key_prefix and member_prefix
              ck2 = (char*)zmalloc(key_len+1);
              memcpy(ck1,skeyh,sdslen(skeyh));
              memcpy(ck2,key,key_len);
              ck1[sdslen(skeyh)] = '\0';
              ck2[key_len] = '\0';
              
              //fprintf(stderr,"\n\nkeyF:%s\nkeyS:%s\n\n",ck2,ck1);

              if(strcmp(ck2,ck1) > 0)
                  continue;

		  }


          str = sdscatprintf(str, "$%zu\r\n", key_len - KEY_PREFIX_LENGTH - MEMBER_PREFIX_LENGTH); // remove the prefix part
          str = sdscatlen(str, key + KEY_PREFIX_LENGTH, key_len - KEY_PREFIX_LENGTH - MEMBER_PREFIX_LENGTH);
          str = sdscatlen(str, "\r\n", 2);
          i++;
        }

        if(i >= limit)
            break;
        
    }
    
    err = NULL;
    leveldb_iter_get_error(iter, &err);
	leveldb_iter_destroy(iter);

    if(err)
    {
		addReplyError(c, err);
		leveldb_free(err);
	}
	else if(i == 0)
    {
        addReply(c,shared.nullbulk);
    }
    else
    {   
        header = sdsempty();
        header = sdscatprintf(header, "*%zu\r\n", i);
        header = sdscatlen(header, str, sdslen(str));
        
        addReplySds(c, header);
		//addReplySds() will free header
    }

	if(ck1)
	  zfree(ck1);
	if(ck2)
	  zfree(ck2);

	sdsfree(str);
    sdsfree(skeyh);
    return ;
}

void ds_mget(redisClient *c)
{
	int i;
	size_t val_len;
    char *err, *value;

	addReplyMultiBulkLen(c,c->argc-1);
	for(i=1; i<c->argc; i++)
	{
		err     = NULL;
		value   = NULL;
		val_len = 0;
		value   = leveldb_get(server.ds_db, server.roptions, c->argv[i]->ptr, sdslen((sds)c->argv[i]->ptr), &val_len, &err);
	    if(err != NULL)
		{
			addReplyError(c, err);
			leveldb_free(err);
			leveldb_free(value);
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
}

void ds_get(redisClient *c)
{
    char *err;
    size_t val_len;
    char *key   = NULL;
    char *value = NULL;
    
    
    err   = NULL;
    key   = (char *)c->argv[1]->ptr;
    value = leveldb_get(server.ds_db, server.roptions, key, sdslen((sds)key), &val_len, &err);
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

	leveldb_writebatch_t   *wb;

    if((c->argc%2) == 0)
    {
		addReply(c,shared.nullbulk);
		return ;        
    }

	
	wb       = leveldb_writebatch_create();
	for(i=1; i<c->argc; i++)
	{
		key   = (char *)c->argv[i]->ptr;
		value = (char *)c->argv[++i]->ptr;
		leveldb_writebatch_put(wb, key, sdslen((sds)key), value, sdslen((sds)value));
	}
	leveldb_write(server.ds_db, server.woptions, wb, &err);
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
    
    err     = NULL;
    val_len = 0;
    keyword = sdsempty();
    keyword = sdscatlen(keyword,KEY_PREFIX_HASH,KEY_PREFIX_LENGTH);
    keyword = sdscat(keyword, c->argv[1]->ptr);
    keyword = sdscatlen(keyword, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);
    keyword = sdscat(keyword, c->argv[2]->ptr);
    
    value = leveldb_get(server.ds_db, server.roptions, keyword, sdslen(keyword), &val_len, &err);
    if(err != NULL)
	{
        sdsfree(keyword);
		if(val_len > 0) leveldb_free(value);
        addReplyError(c, err);
		leveldb_free(err);
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
    
    leveldb_put(server.ds_db, server.woptions, keyword, sdslen(keyword), data, sdslen(data), &err);
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
    

	addReplyMultiBulkLen(c, c->argc-2);
    
    key     = (char *)c->argv[1]->ptr;
    keyword = sdsempty();
    
	for(i=2; i<c->argc; i++)
	{
		err     = NULL;
		value   = NULL;
		val_len = 0;
        
        sdsclear(keyword);
        keyword = sdscatlen(keyword,KEY_PREFIX_HASH,KEY_PREFIX_LENGTH);
        keyword = sdscat(keyword, key);
        keyword = sdscatlen(keyword, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH); 
        keyword = sdscat(keyword, c->argv[i]->ptr);
        
		value   = leveldb_get(server.ds_db, server.roptions, keyword, sdslen(keyword), &val_len, &err);
	    if(err != NULL)
		{
            sdsfree(keyword);
			leveldb_free(value);
            addReplyError(c, err);
			leveldb_free(err);
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
}

void ds_hmset(redisClient *c)
{
	int    i;
    sds  keyword;
	char *key, *field, *value;
    char *err = NULL;
	leveldb_writebatch_t   *wb;
    
    if((c->argc%2) != 0)
    {
		addReply(c,shared.nullbulk);
		return ;        
    }
	
    keyword  = sdsempty();
	wb       = leveldb_writebatch_create();
    key      = (char *)c->argv[1]->ptr;

    keyword = sdscatlen(keyword,KEY_PREFIX_HASH,KEY_PREFIX_LENGTH);
    keyword = sdscat(keyword, key);
    keyword = sdscatlen(keyword, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);    
    leveldb_writebatch_put(wb, keyword, sdslen(keyword), "1", 1);
	for(i=2; i<c->argc; i++)
	{
		field = (char *)c->argv[i]->ptr;
		value = (char *)c->argv[++i]->ptr;
        
        sdsclear(keyword);
        keyword = sdscatlen(keyword,KEY_PREFIX_HASH,KEY_PREFIX_LENGTH);
        keyword = sdscat(keyword, key);
        keyword = sdscatlen(keyword, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);
        keyword = sdscat(keyword, field);
		leveldb_writebatch_put(wb, keyword, sdslen(keyword), value, sdslen((sds)value));
	}
    sdsfree(keyword);
    
	leveldb_write(server.ds_db, server.woptions, wb, &err);
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
	leveldb_writebatch_t   *wb;
    
    key   = (char *)c->argv[1]->ptr;
    field = (char *)c->argv[2]->ptr;
    value = (char *)c->argv[3]->ptr;
    
	wb       = leveldb_writebatch_create();
    
    str      = sdsempty();
    str      = sdscatlen(str,KEY_PREFIX_HASH,KEY_PREFIX_LENGTH);
    str      = sdscat(str, key);
    str      = sdscatlen(str, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);
            
    leveldb_writebatch_put(wb, str, sdslen(str), "1", 1);
    
    // append member
    str      = sdscat(str, field);
    leveldb_writebatch_put(wb, str, sdslen(str), value, sdslen((sds)value));
    
	leveldb_write(server.ds_db, server.woptions, wb, &err);
	leveldb_writebatch_destroy(wb);
    sdsfree(str);
    
    if(err != NULL)
	{
        addReplyError(c, err);
        leveldb_free(err);
	}else{
        //addReply(c,shared.ok);
        // keep the same return type as redis's hset
        // TODO: how to distinguish the create(return 1) and update(return 0) ?
        addReplyLongLong(c,1);
    }
    return ;
}
/**
 * set if not exists.
 * return 1 (not exists and saved)
 * return 0 (already exists)
 */
void ds_hsetnx(redisClient *c)
{
    char *value;
    sds keyword;
    
    size_t val_len;
    char *err = NULL;
    leveldb_writebatch_t   *wb;

    err     = NULL;
    val_len = 0;
    keyword = sdsempty();
    keyword = sdscatlen(keyword,KEY_PREFIX_HASH,KEY_PREFIX_LENGTH);
    keyword = sdscat(keyword, c->argv[1]->ptr);
    keyword = sdscatlen(keyword, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);

    wb       = leveldb_writebatch_create();
    // <hash_prefix>key<member prefix>
    leveldb_writebatch_put(wb, keyword, sdslen(keyword), "1", 1);

    // append hash field
    keyword = sdscat(keyword, c->argv[2]->ptr);
    
    value = leveldb_get(server.ds_db, server.roptions, keyword, sdslen(keyword), &val_len, &err);
    if(err != NULL)
	{
        //error
        sdsfree(keyword);
		if(val_len > 0) leveldb_free(value);
        addReplyError(c, err);
		leveldb_free(err);
        leveldb_writebatch_destroy(wb);
		return ;
	}
	else if(val_len > 0)
	{
        // already exists
        sdsfree(keyword);
        leveldb_free(value);
        addReplyLongLong(c,0);
        leveldb_writebatch_destroy(wb);
        return;
	}
    
    err      = NULL;
    leveldb_writebatch_put(wb, keyword, sdslen(keyword), (char *)c->argv[3]->ptr, sdslen((sds)c->argv[3]->ptr));
	leveldb_write(server.ds_db, server.woptions, wb, &err);
    if(err != NULL)
    {
		addReplyError(c, err);
        leveldb_free(err);
    }
    else
    {
        addReplyLongLong(c, 1);
    }
    
    sdsfree(keyword);    
    leveldb_writebatch_destroy(wb);
    return ;
}


void ds_hgetall(redisClient *c)
{
    sds str, header;
    char *keyword = NULL, *err;
    
    leveldb_iterator_t    *iter;
    
    const char *key, *value;
    size_t key_len, value_len, len, i;
    
    i       = 0;
    str     = sdsempty();
    
    str     = sdscatlen(str,KEY_PREFIX_HASH,KEY_PREFIX_LENGTH);
    str     = sdscat(str, c->argv[1]->ptr);
    str     = sdscatlen(str, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);
    len     = sdslen(str);
    keyword = zmalloc(len+1);
    memcpy(keyword, str, len);

	keyword[len] = '\0';
	
    sdsclear(str);
    iter = leveldb_create_iterator(server.ds_db, server.roptions);
    for(leveldb_iter_seek(iter, keyword, len);leveldb_iter_valid(iter); leveldb_iter_next(iter))
    {
        
        key_len = value_len = 0;
        key   = leveldb_iter_key(iter, &key_len);
		
		// IMPORTANT:
		// leveldb_iter_valid(iter) means the current key >= what we want
		// for example:  seek "thekey*" 
		//               thekey*  is valid
		//               thekey*1 is valid
		//               thekez*  is *ALSO VALID* 'z'>'y'
		// the code:
		//         if(key_len == len)
		//             continue;
		//         else if(strncmp(keyword, key, len) != 0)
		//             break;
		//  is ok, but will do lot's of useless loop in such condition:
		//  ds_hgetall "a" , and there's no a* in leveldb ,and lot's of b1 b2 ... bN in leveldb :( 

		// make sure the hashtable is the same
        if(key_len < len || strncmp(keyword, key, len) != 0)
            break;

		// skip the hashtable itself
		if(key_len == len)
            continue;

		// now, key is valid and get value here
        value = leveldb_iter_value(iter, &value_len);
        
        str = sdscatprintf(str, "$%zu\r\n", key_len-len);
        str = sdscatlen(str, key+len, key_len-len);
        str = sdscatprintf(str, "\r\n$%zu\r\n", value_len);
        str = sdscatlen(str, value, value_len);
        str = sdscatlen(str, "\r\n", 2);
        
        i++;
    }
    err = NULL;
    zfree(keyword);
    leveldb_iter_get_error(iter, &err);
    leveldb_iter_destroy(iter);
    
    if(err)
    {
		addReplyError(c, err);
		leveldb_free(err);
    }
    else if(i == 0)
    {
        addReply(c,shared.nullbulk);
    }
    else
    {   
	  
        header = sdsempty();
        header = sdscatprintf(header, "*%zu\r\n", (i*2));
        header = sdscatlen(header, str, sdslen(str));
        addReplySds(c, header);

        //sdsfree(header);

		// IMPORTANT:
		// addReplySds(c,ptr) will call sdsfree(ptr) before returned.
		// DO NOT call sdsfree(header) here !!!!
		// 崩溃了无数次，坑爹的gdb把问题定位在leveldb_iter_seek()上，
		// 丫问题居然在这里....
    }
    
    sdsfree(str);
    
    return ;
}


void ds_hkeys(redisClient *c)
{
    sds str, header;
    char *keyword = NULL, *err;
    
    leveldb_iterator_t    *iter;
    
    const char *key;
    size_t key_len, value_len, len, i;
    
    i       = 0;
    str     = sdsempty();
    
    str     = sdscatlen(str, KEY_PREFIX_HASH,KEY_PREFIX_LENGTH);
    str     = sdscat(str, c->argv[1]->ptr);
    str     = sdscatlen(str, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);
    len     = sdslen(str);
    keyword = zmalloc(len+1);
    memcpy(keyword, str, len);

	keyword[len] = '\0';
	
    sdsclear(str);
    iter = leveldb_create_iterator(server.ds_db, server.roptions);
    for(leveldb_iter_seek(iter, keyword, len);leveldb_iter_valid(iter); leveldb_iter_next(iter))
    {
        
        key_len = value_len = 0;
        key   = leveldb_iter_key(iter, &key_len);
		
		// make sure the hashtable is the same
        if(key_len < len || strncmp(keyword, key, len) != 0)
            break;

		// skip the hashtable itself
		if(key_len == len)
            continue;

        str = sdscatprintf(str, "$%zu\r\n", (key_len-len));
        str = sdscatlen(str, key+len, key_len-len);
        str = sdscatlen(str, "\r\n", 2);
        
        i++;
    }
    err = NULL;
    zfree(keyword);
    leveldb_iter_get_error(iter, &err);
    leveldb_iter_destroy(iter);
    
    if(err)
    {
		addReplyError(c, err);
		leveldb_free(err);
    }
    else if(i == 0)
    {
        addReply(c,shared.nullbulk);
    }
    else
    {   
	  
        header = sdsempty();
        header = sdscatprintf(header, "*%zu\r\n", (i*1));
        header = sdscatlen(header, str, sdslen(str));
        addReplySds(c, header);
    }
    
    sdsfree(str);
    
    return ;
}

void ds_hvals(redisClient *c)
{
    sds str, header;
    char *keyword = NULL, *err;
    
    leveldb_iterator_t    *iter;
    
    const char *key, *value;
    size_t key_len, value_len, len, i;
    
    i       = 0;
    str     = sdsempty();
    
    str     = sdscatlen(str,KEY_PREFIX_HASH,KEY_PREFIX_LENGTH);
    str     = sdscat(str, c->argv[1]->ptr);
    str     = sdscatlen(str, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);
    len     = sdslen(str);
    keyword = zmalloc(len+1);
    memcpy(keyword, str, len);

	keyword[len] = '\0';
	
    sdsclear(str);
    iter = leveldb_create_iterator(server.ds_db, server.roptions);
    for(leveldb_iter_seek(iter, keyword, len);leveldb_iter_valid(iter); leveldb_iter_next(iter))
    {
        
        key_len = value_len = 0;
        key   = leveldb_iter_key(iter, &key_len);
		
		// make sure the hashtable is the same
        if(key_len < len || strncmp(keyword, key, len) != 0)
            break;

		// skip the hashtable itself
		if(key_len == len)
            continue;

		// now, key is valid and get value here
        value = leveldb_iter_value(iter, &value_len);
        
        str = sdscatprintf(str, "$%zu\r\n", value_len);
        str = sdscatlen(str, value, value_len);
        str = sdscatlen(str, "\r\n", 2);
        
        i++;
    }
    err = NULL;
    zfree(keyword);
    leveldb_iter_get_error(iter, &err);
    leveldb_iter_destroy(iter);
    
    if(err)
    {
		addReplyError(c, err);
		leveldb_free(err);
    }
    else if(i == 0)
    {
        addReply(c,shared.nullbulk);
    }
    else
    {   
	  
        header = sdsempty();
        header = sdscatprintf(header, "*%zu\r\n", (i*1));
        header = sdscatlen(header, str, sdslen(str));
        addReplySds(c, header);
    }
    
    sdsfree(str);
    
    return ;
}


void ds_hlen(redisClient *c)
{
    sds str;
    char *keyword = NULL, *err;
    
    leveldb_iterator_t    *iter;
    
    const char *key;
    size_t key_len, value_len, len, i;
    
    i       = 0;
    str     = sdsempty();
    
    str     = sdscatlen(str, KEY_PREFIX_HASH,KEY_PREFIX_LENGTH);
    str     = sdscat(str, c->argv[1]->ptr);
    str     = sdscatlen(str, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);
    len     = sdslen(str);
    keyword = zmalloc(len+1);
    memcpy(keyword, str, len);

	keyword[len] = '\0';
	
    sdsclear(str);
    iter = leveldb_create_iterator(server.ds_db, server.roptions);
    for(leveldb_iter_seek(iter, keyword, len);leveldb_iter_valid(iter); leveldb_iter_next(iter))
    {
        
        key_len = value_len = 0;
        key   = leveldb_iter_key(iter, &key_len);
		
		// make sure the hashtable is the same
        if(key_len < len || strncmp(keyword, key, len) != 0)
            break;

		// skip the hashtable itself
		if(key_len == len)
            continue;

        i++;
    }
    err = NULL;
    zfree(keyword);
    leveldb_iter_get_error(iter, &err);
    leveldb_iter_destroy(iter);
    
    if(err)
    {
		addReplyError(c, err);
		leveldb_free(err);
    }
    else if(i == 0)
    {
        addReply(c,shared.nullbulk);
    }
    else
    {   
	  
        addReplyLongLong(c, i);
    }
    
    sdsfree(str);
    return ;
}



void ds_hdel(redisClient *c)
{
	const char *key;
    size_t key_len, i;
    
    sds keyword;
    char *err = NULL;
    
	leveldb_writebatch_t  *wb;
    leveldb_iterator_t    *iter;
    
    keyword  = sdsempty();
    
    //delete hashtable key
	if(c->argc < 3)
	{
        keyword = sdscatlen(keyword,KEY_PREFIX_HASH,KEY_PREFIX_LENGTH);
        keyword = sdscat(keyword, c->argv[1]->ptr);
        keyword = sdscatlen(keyword, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);
		
        
        iter = leveldb_create_iterator(server.ds_db, server.roptions);
        wb   = leveldb_writebatch_create();
        for(leveldb_iter_seek(iter, keyword, sdslen(keyword)); leveldb_iter_valid(iter); leveldb_iter_next(iter))
        {
            key_len = 0;
            key     = leveldb_iter_key(iter, &key_len);
        
            if(key_len < sdslen(keyword) || strncmp(keyword, key, sdslen(keyword)) != 0)
                break;
            
            leveldb_writebatch_delete(wb, key, key_len);
        }
        
        leveldb_write(server.ds_db, server.woptions, wb, &err);
        leveldb_writebatch_clear(wb);
    	leveldb_writebatch_destroy(wb);
        sdsfree(keyword);

        if(err != NULL)
    	{
    		addReplyError(c, err);
    		leveldb_free(err);    
    		return ;
    	}
        
        err = NULL;
        leveldb_iter_get_error(iter, &err);
        leveldb_iter_destroy(iter);
    
        if(err)
        {
    		addReplyError(c, err);
    		leveldb_free(err);
        }
     
		// No way to keep the same return type as redis's HDEL
		// HDEL needs at least 2 arguments
		// so,... send "OK"
        addReply(c,shared.ok);
	}
	
	wb = leveldb_writebatch_create();
	for(i=2; i<c->argc; i++)
	{
        sdsclear(keyword);
        keyword = sdscatlen(keyword,KEY_PREFIX_HASH,KEY_PREFIX_LENGTH);
        keyword = sdscat(keyword, c->argv[1]->ptr);
        keyword = sdscatlen(keyword, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);
        keyword = sdscat(keyword, c->argv[i]->ptr);
		leveldb_writebatch_delete(wb, keyword, sdslen(keyword));
	}
    
    sdsfree(keyword);
	leveldb_write(server.ds_db, server.woptions, wb, &err);
    leveldb_writebatch_clear(wb);
	leveldb_writebatch_destroy(wb);
    

	if(err != NULL)
	{
		addReplyError(c, err);
		leveldb_free(err);
		return ;
	}

	// keep the same return type as redis's HDEL
	// Return value
	//  Integer reply: the number of fields that were removed from the hash, not including specified but non existing fields.
	// TODO: count the delete operation
	addReplyLongLong(c,c->argc-2);

    return ;
}

void ds_hget(redisClient *c)
{
    sds str;
    size_t val_len = 0;
    char *key = NULL, *field = NULL, *value = NULL, *err = NULL;
    
    
    key   = (char *)c->argv[1]->ptr;
    field = (char *)c->argv[2]->ptr;
    
    str      = sdsempty();
    str      = sdscatlen(str,KEY_PREFIX_HASH,KEY_PREFIX_LENGTH);
    str      = sdscat(str,key);
    str      = sdscatlen(str, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);
    str      = sdscat(str, field);
    value = leveldb_get(server.ds_db, server.roptions, str, sdslen(str), &val_len, &err);    
    if(err != NULL)
    {
		addReplyError(c, err);
        leveldb_free(err);
		if(val_len > 0) leveldb_free(value);

        sdsfree(str);
        return ;
    }
	else if(value == NULL)
    {
        addReply(c,shared.nullbulk);
        
        sdsfree(str);
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
    
    err     = NULL;
    val_len = 0;
    
    value = leveldb_get(server.ds_db, server.roptions, c->argv[1]->ptr, sdslen((sds)c->argv[1]->ptr), &val_len, &err);
    if(err != NULL)
	{
		if(val_len > 0) leveldb_free(value);
        addReplyError(c, err);
		leveldb_free(err);
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
    
    leveldb_put(server.ds_db, server.woptions, c->argv[1]->ptr, sdslen((sds)c->argv[1]->ptr), data, sdslen(data), &err);
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
    
    err     = NULL;
    val_len = 0;
    
    value = leveldb_get(server.ds_db, server.roptions, c->argv[1]->ptr, sdslen((sds)c->argv[1]->ptr), &val_len, &err);
    if(err != NULL)
	{
		if(val_len > 0) leveldb_free(value);
        addReplyError(c, err);
		leveldb_free(err);
		return ;
	}
	
    
    err      = NULL;
    recore   = sdsempty();
    if(val_len > 0)
    {
        recore = sdscpylen(recore, value, val_len);
    }    
    recore = sdscat(recore, c->argv[2]->ptr);
    
    leveldb_put(server.ds_db, server.woptions, c->argv[1]->ptr, sdslen((sds)c->argv[1]->ptr), recore, sdslen(recore), &err);
    if(err != NULL)
    {
		addReplyError(c, err);
        leveldb_free(err);
    }
    else
    {
        addReplyLongLong(c, sdslen(recore));
    }
    
    sdsfree(recore);
    leveldb_free(value);
    return ;
}

void ds_set(redisClient *c)
{
	char *key, *value;
    char *err = NULL;
    
    key   = (char *)c->argv[1]->ptr;
    value = (char *)c->argv[2]->ptr;
    leveldb_put(server.ds_db, server.woptions, key, sdslen((sds)key), value, sdslen((sds)value), &err);
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

    key   = (char *)c->argv[1]->ptr;
    value = (char *)c->argv[2]->ptr;
    leveldb_put(server.ds_db, server.woptions, key, sdslen((sds)key), value, sdslen((sds)value), &err);
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
	leveldb_writebatch_t   *wb;
    
	if(c->argc < 3)
	{
		key   = (char *)c->argv[1]->ptr;
		leveldb_delete(server.ds_db, server.woptions, key, sdslen((sds)key), &err);
		if(err != NULL)
		{
			addReplyError(c, err);
			leveldb_free(err);
			return ;
		}
		//addReply(c,shared.ok);
		// keep the same return type as redis's del
		addReplyLongLong(c,1);
		return ;
	}
	
	wb = leveldb_writebatch_create();
	for(i=1; i<c->argc; i++)
	{
		leveldb_writebatch_delete(wb, (char *)c->argv[i]->ptr, sdslen((sds)c->argv[i]->ptr));
	}
	leveldb_write(server.ds_db, server.woptions, wb, &err);
    leveldb_writebatch_clear(wb);
	leveldb_writebatch_destroy(wb);
    wb = NULL;

	if(err != NULL)
	{
		addReplyError(c, err);
		leveldb_free(err);
		return ;
	}
	//addReply(c,shared.ok);
	// keep the same return type as redis's del
	// TODO:  count the delete operation
	//  I don't know how to count the delete operation, use argc-1 instand.
	addReplyLongLong(c,c->argc-1);

    return ;
}



void rl_delete(redisClient *c)
{
	ds_delete(c);
    delCommand(c);
}

void ds_close()
{
    leveldb_readoptions_destroy(server.roptions);
    leveldb_writeoptions_destroy(server.woptions);
    leveldb_options_set_filter_policy(server.ds_options, NULL);
    leveldb_filterpolicy_destroy(server.policy);
    leveldb_close(server.ds_db);
    leveldb_options_destroy(server.ds_options);
    leveldb_cache_destroy(server.ds_cache);
}

/* ----- for emacs ----- */
/* Local Variables:      */
/* mode: c               */
/* coding: utf-8-unix    */
/* tab-width: 4          */
/* c-basic-offset: 4     */
/* indent-tabs-mode: nil */
/* End:                  */
