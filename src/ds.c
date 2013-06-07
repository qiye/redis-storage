#include "redis.h"

#define KEY_PREFIX_HASH "\xFF\x01"
#define KEY_PREFIX_ZSET "\xFF\x02"
#define KEY_PREFIX_SET  "\xFF\x03"
#define KEY_PREFIX_LENGTH 2
#define MEMBER_PREFIX   "\x01"
#define MEMBER_PREFIX_LENGTH 1

// 常规key不使用前缀，用户自己控制自己的key不带有上述前缀

void ds_init() {
    char *err = NULL;

    server.ds_cache = leveldb_cache_create_lru(server.ds_lru_cache * 1048576);
    server.ds_options = leveldb_options_create();

    server.policy = leveldb_filterpolicy_create_bloom(10);


    //leveldb_options_set_comparator(server.ds_options, cmp);
    leveldb_options_set_filter_policy(server.ds_options, server.policy);
    leveldb_options_set_create_if_missing(server.ds_options, server.ds_create_if_missing);
    leveldb_options_set_error_if_exists(server.ds_options, server.ds_error_if_exists);
    leveldb_options_set_cache(server.ds_options, server.ds_cache);
    leveldb_options_set_info_log(server.ds_options, NULL);
    leveldb_options_set_write_buffer_size(server.ds_options, server.ds_write_buffer_size * 1048576);
    leveldb_options_set_paranoid_checks(server.ds_options, server.ds_paranoid_checks);
    leveldb_options_set_max_open_files(server.ds_options, server.ds_max_open_files);
    leveldb_options_set_block_size(server.ds_options, server.ds_block_size * 1024);
    leveldb_options_set_block_restart_interval(server.ds_options, server.ds_block_restart_interval);
    leveldb_options_set_compression(server.ds_options, leveldb_snappy_compression);

    server.ds_db = leveldb_open(server.ds_options, server.ds_path, &err);
    if (err != NULL) {
        fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, err);
        leveldb_free(err);
        exit(1);
    }

    server.woptions = leveldb_writeoptions_create();
    server.roptions = leveldb_readoptions_create();
    leveldb_readoptions_set_verify_checksums(server.roptions, 0);
    leveldb_readoptions_set_fill_cache(server.roptions, 1);

    leveldb_writeoptions_set_sync(server.woptions, 0);
}

void ds_exists(redisClient *c) {
    int i;
    char *err;
    leveldb_iterator_t *iter;
    char *kp;
    size_t kl;
    sds str, ret;

    iter = leveldb_create_iterator(server.ds_db, server.roptions);
    //addReplyMultiBulkLen(c, c->argc-1);

    str = sdsempty();

    for (i = 1; i < c->argc; i++) {
        leveldb_iter_seek(iter, c->argv[i]->ptr, sdslen((sds) c->argv[i]->ptr));
        if (leveldb_iter_valid(iter)) {

            kp = (char *) leveldb_iter_key(iter, &kl);

            if (sdslen((sds) c->argv[i]->ptr) == kl && 0 == memcmp(c->argv[i]->ptr, kp, kl))
                //addReplyLongLong(c,1);
                str = sdscatlen(str, ":1\r\n", 4);
            else
                //addReplyLongLong(c,0);
                str = sdscatlen(str, ":0\r\n", 4);

        } else
            //addReplyLongLong(c, 0);
            str = sdscatlen(str, ":0\r\n", 4);
    }

    err = NULL;
    leveldb_iter_get_error(iter, &err);
    leveldb_iter_destroy(iter);

    if (err != NULL) {
        addReplyError(c, err);
        leveldb_free(err);
        sdsfree(str);
        return;
    }

    ret = sdsempty();
    ret = sdscatprintf(ret, "*%d\r\n", c->argc - 1);
    ret = sdscatsds(ret, str);
    addReplySds(c, ret);
    sdsfree(str);
    return;
}

void ds_hexists(redisClient *c) {
    int i;
    sds key;
    char *err;
    leveldb_iterator_t *iter;
    char *kp;
    size_t kl;
    sds str, ret;

    key = sdsempty();
    iter = leveldb_create_iterator(server.ds_db, server.roptions);
    //addReplyMultiBulkLen(c, c->argc-2);

    str = sdsempty();

    for (i = 2; i < c->argc; i++) {
        sdsclear(key);
        key = sdscatlen(key, KEY_PREFIX_HASH, KEY_PREFIX_LENGTH);
        key = sdscatsds(key, c->argv[1]->ptr);
        key = sdscatlen(key, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);
        key = sdscatsds(key, c->argv[i]->ptr);

        leveldb_iter_seek(iter, key, sdslen(key));
        if (leveldb_iter_valid(iter)) {
            kp = (char*) leveldb_iter_key(iter, &kl);

            if (sdslen(key) == kl && 0 == memcmp(key, kp, kl))
                //addReplyLongLong(c,1);
                str = sdscatlen(str, ":1\r\n", 4);
            else
                //addReplyLongLong(c,0);
                str = sdscatlen(str, ":0\r\n", 4);

        } else
            //addReplyLongLong(c, 0);
            str = sdscatlen(str, ":0\r\n", 4);
    }

    err = NULL;
    leveldb_iter_get_error(iter, &err);
    leveldb_iter_destroy(iter);
    sdsfree(key);
    if (err != NULL) {
        addReplyError(c, err);
        leveldb_free(err);
        sdsfree(str);
        return;
    }

    ret = sdsempty();
    ret = sdscatprintf(ret, "*%d\r\n", c->argc - 2);
    ret = sdscatsds(ret, str);
    addReplySds(c, ret);
    sdsfree(str);
    return;
}

/**
 * usage: ds_keys_count startKey endKey
 * return: integer
 */
void ds_keys_count(redisClient *c) {
    char *err;

    const char *key;
    size_t key_len, len, i;

    leveldb_iterator_t *iter;

    char *skey, *ekey; //  key pair
    size_t skey_len, ekey_len;

    char *k1, *k2;
    int cmp;

    skey = c->argv[1]->ptr;
    skey_len = sdslen(c->argv[1]->ptr);

    ekey = c->argv[2]->ptr;
    ekey_len = sdslen(c->argv[2]->ptr);


    // return 0 when skey > ekey
    k1 = zmalloc(skey_len + 1);
    k2 = zmalloc(ekey_len + 1);
    memcpy(k1, skey, skey_len);
    memcpy(k2, ekey, ekey_len);
    k1[skey_len] = k2[ekey_len] = '\0';

    cmp = strcmp(k1, k2);

    if (cmp > 0) {
        zfree(k1);
        zfree(k2);
        addReplyLongLong(c, 0);
        return;
    }

    i = 0;
    len = 0;

    iter = leveldb_create_iterator(server.ds_db, server.roptions);

    leveldb_iter_seek(iter, skey, skey_len);

    for (; leveldb_iter_valid(iter); leveldb_iter_next(iter)) {
        key_len = 0;
        key = leveldb_iter_key(iter, &key_len);

        // skip KEY_PREFIX_
        if (((unsigned char*) key)[0] == 0xFF) {

            //位于haskkey 空间，后面的key无需扫描
            break;
            //continue;
        }

        if (k1 != NULL) {
            zfree(k1);
            k1 = NULL;
        }

        k1 = zmalloc(key_len + 1);
        memcpy(k1, key, key_len);
        k1[key_len] = '\0';

        cmp = strcmp(k1, k2);

        if (cmp > 0)
            break;

        i++;
    }

    err = NULL;
    leveldb_iter_get_error(iter, &err);
    leveldb_iter_destroy(iter);

    if (err) {
        addReplyError(c, err);
        leveldb_free(err);
    } else {
        addReplyLongLong(c, i);
    }

    if (k1) zfree(k1);
    if (k2) zfree(k2);

    return;
}

/**
 * hashkey range counter
 *
 * usage: ds_hkeys_count startKey endKey
 * return: integer
 */
void ds_hkeys_count(redisClient *c) {
    char *err = NULL;

    const char *key;
    size_t key_len, len, i;

    leveldb_iterator_t *iter;

    char *skey, *ekey; //  key pair
    size_t skey_len, ekey_len;

    char *k1, *k2;
    int cmp;

    skey = (char *) c->argv[1]->ptr;
    skey_len = sdslen(skey);

    ekey = (char *) c->argv[2]->ptr;
    ekey_len = sdslen(ekey);


    // return 0 when skey > ekey
    k1 = zmalloc(skey_len + KEY_PREFIX_LENGTH + MEMBER_PREFIX_LENGTH + 1);
    k2 = zmalloc(ekey_len + KEY_PREFIX_LENGTH + MEMBER_PREFIX_LENGTH + 1);
    memcpy(k1, KEY_PREFIX_HASH, KEY_PREFIX_LENGTH);
    memcpy(k1 + KEY_PREFIX_LENGTH, skey, skey_len);
    memcpy(k2, KEY_PREFIX_HASH, KEY_PREFIX_LENGTH);
    memcpy(k2 + KEY_PREFIX_LENGTH, ekey, ekey_len);
    k1[skey_len + KEY_PREFIX_LENGTH] = k2[ekey_len + KEY_PREFIX_LENGTH] = MEMBER_PREFIX[0];
    k1[skey_len + KEY_PREFIX_LENGTH + MEMBER_PREFIX_LENGTH] = k2[ekey_len + KEY_PREFIX_LENGTH + MEMBER_PREFIX_LENGTH] = '\0';

    cmp = strcmp(k1, k2);

    if (cmp > 0) {
        zfree(k1);
        zfree(k2);
        addReplyLongLong(c, 0);
        return;
    }

    i = 0;
    len = 0;

    iter = leveldb_create_iterator(server.ds_db, server.roptions);

    leveldb_iter_seek(iter, k1, skey_len + KEY_PREFIX_LENGTH + MEMBER_PREFIX_LENGTH);

    for (; leveldb_iter_valid(iter); leveldb_iter_next(iter)) {
        key_len = 0;
        key = leveldb_iter_key(iter, &key_len);

        if (key_len < KEY_PREFIX_LENGTH + MEMBER_PREFIX_LENGTH
                || 0 != memcmp(key, KEY_PREFIX_HASH, KEY_PREFIX_LENGTH) // not hashkey
                ) {

            //已经不在hashkey空间
            break;
        }
        if (((unsigned char *) key)[key_len - 1] != MEMBER_PREFIX[0]) {
            // hash member
            continue;
        }

        if (k1 != NULL) {
            zfree(k1);
            k1 = NULL;
        }

        k1 = zmalloc(key_len + 1);
        memcpy(k1, key, key_len);
        k1[key_len] = '\0';

        cmp = strcmp(k1, k2);

        if (cmp > 0)
            break;

        i++;
    }

    err = NULL;
    leveldb_iter_get_error(iter, &err);
    leveldb_iter_destroy(iter);

    if (err) {
        addReplyError(c, err);
        leveldb_free(err);
    } else {
        addReplyLongLong(c, i);
    }
    if (k1) zfree(k1);
    if (k2) zfree(k2);

    return;
}

/**
 * usage: 
 *  1. ds_keys_asc
 *  2. ds_keys_asc key
 *  3. ds_keys_asc 100
 *  4. ds_keys_asc key 100
 */
void ds_keys_asc(redisClient *c) {
    sds str, header;
    char *err;

    unsigned long limit;
    const char *key;
    size_t key_len, len, i;

    leveldb_iterator_t *iter;

    char *skey; // start key
    size_t skey_len = 0;
    int is_limit;

    // max result = 1000,large limit will eat lot's of disk/network io.
    const unsigned long max_result = 1000;

    limit = 30; //default
    skey = NULL; //seek first

    //limit    = strtoul(c->argv[1]->ptr, NULL, 10);

    if (c->argc > 3) {
        addReplyError(c, "too many arguments.");
        return;
    } else if (c->argc == 1) {
        // default limit and seek to first
    } else if (c->argc == 2) {

        //is digit ?
        is_limit = 1;
        len = sdslen(c->argv[1]->ptr);
        i = 0;
        while (i < len) {
            if (!isdigit(((char *) c->argv[1]->ptr)[i++])) {
                is_limit = 0;
                break;
            }
        }

        if (is_limit) {
            limit = strtoul(c->argv[1]->ptr, NULL, 10);

        } else {

            // start key
            skey = c->argv[1]->ptr;
            skey_len = len;
        }
    } else {
        // 3 args
        // argv[1] as start key
        skey = c->argv[1]->ptr;
        skey_len = sdslen(c->argv[1]->ptr);

        // argv[2] as limit
        limit = strtoul(c->argv[2]->ptr, NULL, 10);

    }

    //check limit
    if (limit == 0)
        limit = 30;

    if (limit > max_result)
        limit = max_result;


    i = 0;
    len = 0;
    str = sdsempty();

    iter = leveldb_create_iterator(server.ds_db, server.roptions);

    if (skey) //seek key
        leveldb_iter_seek(iter, skey, skey_len);
    else
        leveldb_iter_seek_to_first(iter);

    for (; leveldb_iter_valid(iter); leveldb_iter_next(iter)) {
        key_len = 0;
        key = leveldb_iter_key(iter, &key_len);

        // skip hashtable and hash field
        if (((unsigned char*) key)[0] == 0xFF) {

            //已经位于 hashkey的空间了，扫描后面的key已经没有意义

            break;
            //continue;
        } else {
            str = sdscatprintf(str, "$%zu\r\n", key_len);
            str = sdscatlen(str, key, key_len);
            str = sdscatlen(str, "\r\n", 2);
            i++;
        }

        if (i >= limit)
            break;

    }

    err = NULL;
    leveldb_iter_get_error(iter, &err);
    leveldb_iter_destroy(iter);

    if (err) {
        addReplyError(c, err);
        leveldb_free(err);
    } else if (i == 0) {
        addReply(c, shared.nullbulk);
    } else {
        header = sdsempty();
        header = sdscatprintf(header, "*%zu\r\n", i);
        header = sdscatlen(header, str, sdslen(str));

        addReplySds(c, header);
        //addReplySds() will free header
    }
    sdsfree(str);
    return;
}

/**
 * hashkey asc iterator
 * usage: 
 *  1. ds_hkeys_asc
 *  2. ds_hkeys_asc key
 *  3. ds_hkeys_asc 100
 *  4. ds_hkeys_asc key 100
 */
void ds_hkeys_asc(redisClient *c) {
    sds str, header, skeyh;
    char *err;

    unsigned long limit;
    const char *key;
    size_t key_len, len, i;

    leveldb_iterator_t *iter;

    char *skey; // start key
    size_t skey_len = 0;
    int is_limit;

    // max result = 1000,large limit will eat lot's of disk/network io.
    const unsigned long max_result = 1000;

    limit = 30; //default
    skey = NULL; //seek first

    //limit    = strtoul(c->argv[1]->ptr, NULL, 10);

    if (c->argc > 3) {
        addReplyError(c, "too many arguments.");
        return;
    } else if (c->argc == 1) {
        // default limit and seek to first
    } else if (c->argc == 2) {

        //is digit ?
        is_limit = 1;
        len = sdslen(c->argv[1]->ptr);
        i = 0;
        while (i < len) {
            if (!isdigit(((char *) c->argv[1]->ptr)[i++])) {
                is_limit = 0;
                break;
            }
        }

        if (is_limit) {
            limit = strtoul(c->argv[1]->ptr, NULL, 10);

        } else {

            // start key
            skey = c->argv[1]->ptr;
            skey_len = len;
        }
    } else {
        // 3 args
        // argv[1] as start key
        skey = c->argv[1]->ptr;
        skey_len = sdslen(c->argv[1]->ptr);

        // argv[2] as limit
        limit = strtoul(c->argv[2]->ptr, NULL, 10);

    }

    //check limit
    if (limit == 0)
        limit = 30;

    if (limit > max_result)
        limit = max_result;

    i = 0;
    len = 0;
    str = sdsempty();
    skeyh = sdsempty();


    iter = leveldb_create_iterator(server.ds_db, server.roptions);

    if (skey) { //seek key
        skeyh = sdscatlen(skeyh, KEY_PREFIX_HASH, KEY_PREFIX_LENGTH);
        skeyh = sdscatlen(skeyh, skey, skey_len);
        skeyh = sdscatlen(skeyh, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);

        //fprintf(stderr,"SK: %s\n",skeyh);

        leveldb_iter_seek(iter, skeyh, sdslen(skeyh));
    } else {
        //hashkey空间下限是 KEY_PREFIX_HASH

        leveldb_iter_seek(iter, KEY_PREFIX_HASH, KEY_PREFIX_LENGTH);

        //如果失败，说明没有任何hashkey存在
    }

    for (; leveldb_iter_valid(iter); leveldb_iter_next(iter)) {
        key_len = 0;
        key = leveldb_iter_key(iter, &key_len);

        //fprintf(stderr,"FK: %s\n",key);

        if (key_len < KEY_PREFIX_LENGTH + MEMBER_PREFIX_LENGTH
                || 0 != memcmp(key, KEY_PREFIX_HASH, KEY_PREFIX_LENGTH) // not hashkey
                ) {
            //已经不在hashkey空间
            break;
        }
        if (((unsigned char *) key)[key_len - 1] != MEMBER_PREFIX[0]) {
            // hash member
            continue;
        } else {
            str = sdscatprintf(str, "$%zu\r\n", key_len - MEMBER_PREFIX_LENGTH - KEY_PREFIX_LENGTH); // remove the MEMBER_PREFIX
            str = sdscatlen(str, key + KEY_PREFIX_LENGTH, key_len - KEY_PREFIX_LENGTH - MEMBER_PREFIX_LENGTH);
            str = sdscatlen(str, "\r\n", 2);
            i++;
        }

        if (i >= limit)
            break;

    }

    err = NULL;
    leveldb_iter_get_error(iter, &err);
    leveldb_iter_destroy(iter);

    if (err) {
        addReplyError(c, err);
        leveldb_free(err);
    } else if (i == 0) {
        addReply(c, shared.nullbulk);
    } else {
        header = sdsempty();
        header = sdscatprintf(header, "*%zu\r\n", i);
        header = sdscatlen(header, str, sdslen(str));

        addReplySds(c, header);
        //addReplySds() will free header
    }
    sdsfree(str);
    sdsfree(skeyh);
    return;
}

/**
 * usage: 
 *  1. ds_keys_desc
 *  2. ds_keys_desc key
 *  3. ds_keys_desc 100
 *  4. ds_keys_desc key 100
 */
void ds_keys_desc(redisClient *c) {
    sds str, header;
    char *err;

    unsigned long limit;
    const char *key;
    size_t key_len, len, i;

    leveldb_iterator_t *iter;

    char *skey; // start key
    size_t skey_len = 0;
    int is_limit;

    char *ck1 = NULL, *ck2 = NULL; // compaire the first key

    // max result = 1000,large limit will eat lot's of disk/network io.
    const unsigned long max_result = 1000;

    limit = 30; //default
    skey = NULL; //seek first

    //limit    = strtoul(c->argv[1]->ptr, NULL, 10);

    if (c->argc > 3) {
        addReplyError(c, "too many arguments.");
        return;
    } else if (c->argc == 1) {
        // default limit and seek to first
    } else if (c->argc == 2) {

        //is digit ?
        is_limit = 1;
        len = sdslen(c->argv[1]->ptr);
        i = 0;
        while (i < len) {
            if (!isdigit(((char *) c->argv[1]->ptr)[i++])) {
                is_limit = 0;
                break;
            }
        }

        if (is_limit) {
            limit = strtoul(c->argv[1]->ptr, NULL, 10);

        } else {

            // start key
            skey = c->argv[1]->ptr;
            skey_len = len;
        }
    } else {
        // 3 args
        // argv[1] as start key
        skey = c->argv[1]->ptr;
        skey_len = sdslen(c->argv[1]->ptr);

        // argv[2] as limit
        limit = strtoul(c->argv[2]->ptr, NULL, 10);

    }

    //check limit
    if (limit == 0)
        limit = 30;

    if (limit > max_result)
        limit = max_result;


    i = 0;
    len = 0;
    str = sdsempty();

    iter = leveldb_create_iterator(server.ds_db, server.roptions);

    if (skey) { //seek key
        leveldb_iter_seek(iter, skey, skey_len);

        // maybe, there's no stored key >= skey
        // we should seek to last
        if (!leveldb_iter_valid(iter))
            leveldb_iter_seek_to_last(iter);

        //普通key定位失败，说明skey已经大于当前db中的最大key了，直接定位到最后

    } else {
        //对于常规key，上限是 0xFF
        leveldb_iter_seek(iter, "\xFF", 1);

        //如果失败，说明整个key空间只有常规key，直接定位到最后
        if (!leveldb_iter_valid(iter))
            leveldb_iter_seek_to_last(iter);
    }

    for (; leveldb_iter_valid(iter); leveldb_iter_prev(iter)) {
        key_len = 0;
        key = leveldb_iter_key(iter, &key_len);

        //fprintf(stderr,"current key:%s\n",key);

        // skip hashtable and hash field
        if (((unsigned char*) key)[0] == 0xFF) {
            //前面的操作已经保证我们位于常规key空间的上限附近，这批数量不会太多，跳过
            continue;
        } else {

            if (skey && ck1 == NULL) {
                // we are in reverse order,and seek() always stop at key>= what we want
                // so if current key > what we want ,we should skip it.

                // skey and key are both \0 terminated
                ck1 = (char*) zmalloc(skey_len + 1);
                ck2 = (char*) zmalloc(key_len + 1);
                memcpy(ck1, skey, skey_len);
                memcpy(ck2, key, key_len);
                ck1[skey_len] = '\0';
                ck2[key_len] = '\0';

                //fprintf(stderr,"\n\nkeyF:%s\nkeyS:%s\n\n",ck2,ck1);

                if (strcmp(ck2, ck1) > 0)
                    continue;

            }


            str = sdscatprintf(str, "$%zu\r\n", key_len);
            str = sdscatlen(str, key, key_len);
            str = sdscatlen(str, "\r\n", 2);
            i++;
        }

        if (i >= limit)
            break;

    }

    err = NULL;
    leveldb_iter_get_error(iter, &err);
    leveldb_iter_destroy(iter);

    if (err) {
        addReplyError(c, err);
        leveldb_free(err);
    } else if (i == 0) {
        addReply(c, shared.nullbulk);
    } else {
        header = sdsempty();
        header = sdscatprintf(header, "*%zu\r\n", i);
        header = sdscatlen(header, str, sdslen(str));

        addReplySds(c, header);
        //addReplySds() will free header
    }

    if (ck1)
        zfree(ck1);
    if (ck2)
        zfree(ck2);

    sdsfree(str);
    return;
}

/**
 * hashkey desc itorator
 * usage: 
 *  1. ds_hkeys_desc
 *  2. ds_hkeys_desc key
 *  3. ds_hkeys_desc 100
 *  4. ds_hkeys_desc key 100
 */
void ds_hkeys_desc(redisClient *c) {
    sds str, header, skeyh;
    char *err;

    unsigned long limit;
    const char *key;
    size_t key_len, len, i, e;

    leveldb_iterator_t *iter;

    unsigned char maxkey[KEY_PREFIX_LENGTH + 1];
    char *skey; // start key
    size_t skey_len = 0;
    int is_limit;

    char *ck1 = NULL, *ck2 = NULL; // compaire the first key

    // max result = 1000,large limit will eat lot's of disk/network io.
    const unsigned long max_result = 1000;

    limit = 30; //default
    skey = NULL; //seek first

    //limit    = strtoul(c->argv[1]->ptr, NULL, 10);

    if (c->argc > 3) {
        addReplyError(c, "too many arguments.");
        return;
    } else if (c->argc == 1) {
        // default limit and seek to first
    } else if (c->argc == 2) {

        //is digit ?
        is_limit = 1;
        len = sdslen(c->argv[1]->ptr);
        i = 0;
        while (i < len) {
            if (!isdigit(((char *) c->argv[1]->ptr)[i++])) {
                is_limit = 0;
                break;
            }
        }

        if (is_limit) {
            limit = strtoul(c->argv[1]->ptr, NULL, 10);

        } else {

            // start key
            skey = c->argv[1]->ptr;
            skey_len = len;
        }
    } else {
        // 3 args
        // argv[1] as start key
        skey = c->argv[1]->ptr;
        skey_len = sdslen(c->argv[1]->ptr);

        // argv[2] as limit
        limit = strtoul(c->argv[2]->ptr, NULL, 10);

    }

    //check limit
    if (limit == 0)
        limit = 30;

    if (limit > max_result)
        limit = max_result;

    //fprintf(stderr,"l:%d sk:%s\n",limit ,skey ? skey :"<nil>");

    i = 0;
    e = 0;
    len = 0;
    str = sdsempty();
    skeyh = sdsempty();

    memcpy(maxkey, KEY_PREFIX_HASH, KEY_PREFIX_LENGTH);
    maxkey[KEY_PREFIX_LENGTH - 1] = KEY_PREFIX_HASH[KEY_PREFIX_LENGTH - 1] + 1; // e.g. 0xFF01 -> 0xFF02

    iter = leveldb_create_iterator(server.ds_db, server.roptions);

    if (skey) { //seek key
        skeyh = sdscatlen(skeyh, KEY_PREFIX_HASH, KEY_PREFIX_LENGTH);
        skeyh = sdscatlen(skeyh, skey, skey_len);
        skeyh = sdscatlen(skeyh, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);
        leveldb_iter_seek(iter, skeyh, sdslen(skeyh));

        // maybe, there's no stored key >= skey
        // we should seek to last
        if (!leveldb_iter_valid(iter)) {
            //定位到hashkey上限
            leveldb_iter_seek(iter, (char *) maxkey, KEY_PREFIX_LENGTH);

            if (!leveldb_iter_valid(iter)) {
                //如果失败，表明hashkey上限之上已经没有key存在
                leveldb_iter_seek_to_last(iter);
            }
        }
    } else {
        //定位到hashkey上限
        leveldb_iter_seek(iter, (char *) maxkey, KEY_PREFIX_LENGTH);

        if (!leveldb_iter_valid(iter)) {
            //如果失败，定位到最后
            leveldb_iter_seek_to_last(iter);
        }
    }
    for (; leveldb_iter_valid(iter); leveldb_iter_prev(iter)) {
        key_len = 0;
        key = leveldb_iter_key(iter, &key_len);

        //fprintf(stderr,"current key:%s\n",key);

        if (key_len < KEY_PREFIX_LENGTH + MEMBER_PREFIX_LENGTH
                || 0 != memcmp(key, KEY_PREFIX_HASH, KEY_PREFIX_LENGTH) // not hashkey
                ) {

            //fprintf(stderr,"miss match key:%s\n",key);

            e++; //无效命中计数

            if (e > 10) break; //之前已经定位到hashkey空间上限附近，如果无效命中太多，说明已经偏离太多，不用继续往前扫描了。

            continue;
        }

        if (((unsigned char *) key)[key_len - 1] != MEMBER_PREFIX[0]) {
            // hash member
            continue;
        } else {

            if (skey && ck1 == NULL) {
                // we are in reverse order,and seek() always stop at key>= what we want
                // so if current key > what we want ,we should skip it.

                // skey and key are both \0 terminated
                ck1 = (char*) zmalloc(sdslen(skeyh) + 1); //skeyh is avaliable,and contain key_prefix and member_prefix
                ck2 = (char*) zmalloc(key_len + 1);
                memcpy(ck1, skeyh, sdslen(skeyh));
                memcpy(ck2, key, key_len);
                ck1[sdslen(skeyh)] = '\0';
                ck2[key_len] = '\0';

                //fprintf(stderr,"\n\nkeyF:%s\nkeyS:%s\n\n",ck2,ck1);

                if (strcmp(ck2, ck1) > 0)
                    continue;

            }


            str = sdscatprintf(str, "$%zu\r\n", key_len - KEY_PREFIX_LENGTH - MEMBER_PREFIX_LENGTH); // remove the prefix part
            str = sdscatlen(str, key + KEY_PREFIX_LENGTH, key_len - KEY_PREFIX_LENGTH - MEMBER_PREFIX_LENGTH);
            str = sdscatlen(str, "\r\n", 2);
            i++;
        }

        if (i >= limit)
            break;

    }

    err = NULL;
    leveldb_iter_get_error(iter, &err);
    leveldb_iter_destroy(iter);

    if (err) {
        addReplyError(c, err);
        leveldb_free(err);
    } else if (i == 0) {
        addReply(c, shared.nullbulk);
    } else {
        header = sdsempty();
        header = sdscatprintf(header, "*%zu\r\n", i);
        header = sdscatlen(header, str, sdslen(str));

        addReplySds(c, header);
        //addReplySds() will free header
    }

    if (ck1)
        zfree(ck1);
    if (ck2)
        zfree(ck2);

    sdsfree(str);
    sdsfree(skeyh);
    return;
}

void ds_mget(redisClient *c) {
    int i;
    size_t val_len;
    char *err, *value;
    sds str, ret;

    str = sdsempty();

    // addReplyMultiBulkLen(c,c->argc-1);
    // err in loop will break the protocal
    for (i = 1; i < c->argc; i++) {
        err = NULL;
        value = NULL;
        val_len = 0;
        value = leveldb_get(server.ds_db, server.roptions, c->argv[i]->ptr, sdslen((sds) c->argv[i]->ptr), &val_len, &err);
        if (err != NULL) {
            addReplyError(c, err);
            leveldb_free(err);
            leveldb_free(value);
            sdsfree(str);
            return;
        } else if (val_len > 0) {
            str = sdscatprintf(str, "$%zu\r\n", val_len);
            str = sdscatlen(str, value, val_len);
            str = sdscatlen(str, "\r\n", 2);
            //addReplyBulkCBuffer(c, value, val_len);
            leveldb_free(value);
            value = NULL;
        } else {
            str = sdscatlen(str, "$-1\r\n", 5); //nullbulk
            //addReply(c,shared.nullbulk);

        }
    }

    ret = sdsempty();
    ret = sdscatprintf(ret, "*%d\r\n", c->argc - 1);
    ret = sdscatsds(ret, str);

    sdsfree(str);

    addReplySds(c, ret);
}

static void ds_getCommand(redisClient *c, int set) {
    char *err;
    size_t val_len;
    char *key = NULL;
    char *value = NULL;


    err = NULL;
    key = (char *) c->argv[1]->ptr;
    value = leveldb_get(server.ds_db, server.roptions, key, sdslen((sds) key), &val_len, &err);
    if (err != NULL) {
        addReplyError(c, err);
        leveldb_free(err);
        leveldb_free(value);

        return;
    } else if (value == NULL) {
        addReply(c, shared.nullbulk);
        return;
    }

    if(set) {
        robj *rv;
        rv = createStringObject(value, val_len);
        setKey(c->db, c->argv[1], rv);
        checkRlTTL(c->db, c->argv[1]);
    }

    addReplyBulkCBuffer(c, value, val_len);

    leveldb_free(value);

    return;
    
}

robj *ds_hgetToRobj(char *key, char *field, char **err) {
    sds keyword;
    size_t val_len;
    char *value = NULL;
    keyword = sdsempty();
    keyword = sdscatlen(keyword, KEY_PREFIX_HASH, KEY_PREFIX_LENGTH);
    keyword = sdscatsds(keyword, key);
    keyword = sdscatlen(keyword, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);
    keyword = sdscatsds(keyword, field);
    value = leveldb_get(server.ds_db, server.roptions, keyword, sdslen(keyword), &val_len, err);
    sdsfree(keyword);
    if (err != NULL) {
        leveldb_free(value);
        return NULL;
    } else if (value == NULL) {
        return NULL;
    }

    robj *rv;
    rv = createStringObject(value, val_len);
    leveldb_free(value);
    return rv;
}

void ds_get(redisClient *c) {
    ds_getCommand(c, 0);
    return;
}

void checkRlTTL(redisDb *db, robj *key) {
    if(server.rl_ttl) {
        if(server.rl_ttlcheck >= server.rl_ttl) {
            return;
        }
        long long expire = getExpire(db,key);                                  
        if(expire == -1 || expire-mstime() < server.rl_ttlcheck) {                         
            expire = server.rl_ttl * 1000;                                                                            
            setExpire(db, key, mstime()+expire);
        }
    }
}


static void rl_getCommand(redisClient *c, int set) {
    //从redis里取数据
    robj *o;

    if ((o = lookupKeyRead(c->db, c->argv[1])) == NULL) {
        ds_getCommand(c, set);
        checkRlTTL(c->db, c->argv[1]); 
        return;
    }

    if (o->type == REDIS_STRING) {
        addReplyBulk(c, o);
        checkRlTTL(c->db, c->argv[1]); 
        return;
    }

    addReply(c, shared.nullbulk);

}

void rl_get(redisClient *c) {
    rl_getCommand(c, 0);
    return;
}

void rl_getset(redisClient *c) {
    rl_getCommand(c, 1);
}


static int ds_msetCommand(redisClient *c, int reply) {
    int i;
    char *key, *value;
    char *err = NULL;

    leveldb_writebatch_t *wb;

    if ((c->argc % 2) == 0) {
        addReply(c, shared.nullbulk);
        return 0;
    }


    wb = leveldb_writebatch_create();
    for (i = 1; i < c->argc; i++) {
        key = (char *) c->argv[i]->ptr;
        value = (char *) c->argv[++i]->ptr;
        leveldb_writebatch_put(wb, key, sdslen((sds) key), value, sdslen((sds) value));
    }
    leveldb_write(server.ds_db, server.woptions, wb, &err);
    leveldb_writebatch_destroy(wb);

    if (err != NULL) {
        addReplyError(c, err);
        leveldb_free(err);
        return 0;
    }

    if(reply) {
        addReply(c, shared.ok);
    }

    return 1;
}

void ds_mset(redisClient *c) {
    ds_msetCommand(c, 1);
    return;
}


void rl_mset(redisClient *c) {
    if(ds_msetCommand(c, 0)) {
        msetCommand(c);
    }
}

static void rl_mgetCommand(redisClient *c, int set) {
    int i;
    size_t val_len;
    char *err, *value;
    addReplyMultiBulkLen(c,c->argc-1);
    for (i = 1; i < c->argc; i++) {
        robj *o = lookupKeyRead(c->db,c->argv[i]);
        if (o == NULL) {
            err = NULL;
            value = NULL;
            val_len = 0;
            value = leveldb_get(server.ds_db, server.roptions, c->argv[i]->ptr, sdslen((sds) c->argv[i]->ptr), &val_len, &err);
            if (err != NULL) {
                addReplyError(c, err);
                leveldb_free(err);
                leveldb_free(value);
                return;
            } else if (val_len > 0) {
                o = createStringObject(value, val_len);
                if(set) {
                    setKey(c->db, c->argv[i], o);
                    checkRlTTL(c->db, c->argv[1]);
                }
                leveldb_free(value);
                value = NULL;
                addReplyBulk(c,o);
            } else {
                addReply(c,shared.nullbulk);
            }
        } else {
            if (o->type != REDIS_STRING) {
                addReply(c,shared.nullbulk);
                return;
            } else {
                addReplyBulk(c,o);
            }
        }

    }
}

void rl_mget(redisClient *c) {
    rl_mgetCommand(c, 0);
}

void rl_mgetset(redisClient *c) {
    rl_mgetCommand(c, 1);
}

void ds_hincrby(redisClient *c) {
    char *value;
    sds keyword, data;

    size_t val_len;
    char *err = NULL;

    int64_t val, recore;

    err = NULL;
    val_len = 0;
    keyword = sdsempty();
    keyword = sdscatlen(keyword, KEY_PREFIX_HASH, KEY_PREFIX_LENGTH);
    keyword = sdscatsds(keyword, c->argv[1]->ptr);
    keyword = sdscatlen(keyword, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);
    keyword = sdscatsds(keyword, c->argv[2]->ptr);

    value = leveldb_get(server.ds_db, server.roptions, keyword, sdslen(keyword), &val_len, &err);
    if (err != NULL) {
        sdsfree(keyword);
        if (val_len > 0) leveldb_free(value);
        addReplyError(c, err);
        leveldb_free(err);
        return;
    } else if (val_len < 1) {
        val = 0;
    } else {
        val = strtoll(value, NULL, 10);
    }

    err = NULL;
    recore = strtoll(c->argv[3]->ptr, NULL, 10);
    recore = val + recore;
    data = sdsfromlonglong(recore);

    leveldb_put(server.ds_db, server.woptions, keyword, sdslen(keyword), data, sdslen(data), &err);
    if (err != NULL) {
        addReplyError(c, err);
        leveldb_free(err);
    } else {
        addReplyLongLong(c, recore);
    }

    sdsfree(data);
    sdsfree(keyword);
    leveldb_free(value);
    return;
}

/**
 * 跨hash取值
 * ds_mhget key field [key field ...]
 */
void ds_mhget(redisClient *c) {
    int i;
    sds keyword, str, ret;
    size_t val_len;
    char *err = NULL, *value = NULL;

    if (((c->argc - 1) % 2) != 0) {
        addReplyError(c, "syntax error");
        return;
    }

    //addReplyMultiBulkLen(c, c->argc-2);

    keyword = sdsempty();


    str = sdsempty();

    for (i = 1; i < c->argc - 1; i += 2) {
        err = NULL;
        value = NULL;
        val_len = 0;

        sdsclear(keyword);
        keyword = sdscatlen(keyword, KEY_PREFIX_HASH, KEY_PREFIX_LENGTH);
        keyword = sdscatsds(keyword, c->argv[i]->ptr);
        keyword = sdscatlen(keyword, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);
        keyword = sdscatsds(keyword, c->argv[i + 1]->ptr);

        //fprintf(stderr,"K:%s\n",keyword);

        value = leveldb_get(server.ds_db, server.roptions, keyword, sdslen(keyword), &val_len, &err);
        if (err != NULL) {
            sdsfree(keyword);
            leveldb_free(value);
            addReplyError(c, err);
            leveldb_free(err);
            sdsfree(str);
            return;
        } else if (val_len > 0) {
            //addReplyBulkCBuffer(c, value, val_len);

            str = sdscatprintf(str, "$%zu\r\n", val_len);
            str = sdscatlen(str, value, val_len);
            str = sdscatlen(str, "\r\n", 2);

            leveldb_free(value);
            value = NULL;
        } else {
            //addReply(c,shared.nullbulk);
            str = sdscatlen(str, "$-1\r\n", 5); //nullbulk
        }
    }

    sdsfree(keyword);

    ret = sdsempty();
    ret = sdscatprintf(ret, "*%d\r\n", (c->argc - 1) / 2);
    ret = sdscatsds(ret, str);
    sdsfree(str);

    addReplySds(c, ret);
}

static void ds_hmgetCommand(redisClient *c, int set) {
    int i;
    char *key, *err = NULL;
    addReplyMultiBulkLen(c, c->argc-2);
    robj *o;
    key = (char *) c->argv[1]->ptr;

    for (i = 2; i < c->argc; i++) {
        err = NULL;
        o = ds_hgetToRobj(key, (char *) c->argv[i]->ptr, &err);
        if(err != NULL) {
            addReplyError(c, err);
            leveldb_free(err);
            return;
        }
        if(o == NULL) {
            addReply(c, shared.nullbulk);
        } else {
            addReplyBulk(c, o);
            if(set) {
                robj *so;
                if ((so = hashTypeLookupWriteOrCreate(c,c->argv[1])) != NULL){
                    hashTypeTryConversion(so,c->argv,2,3);
                    hashTypeTryObjectEncoding(so,&c->argv[i], &o);
                    hashTypeSet(so,c->argv[i], o);
                    signalModifiedKey(c->db,c->argv[1]);
                }
            }
        }
    }

    if(set) {
        checkRlTTL(c->db, c->argv[1]);
    }
}

void ds_hmget(redisClient *c) {
    ds_hmgetCommand(c, 0);
}


static int ds_hmsetCommand(redisClient *c, int ret) {
    int i;
    sds keyword;
    char *key, *field, *value;
    char *err = NULL;
    leveldb_writebatch_t *wb;

    if ((c->argc % 2) != 0) {
        addReply(c, shared.nullbulk);
        return 0; 
    }

    keyword = sdsempty();
    wb = leveldb_writebatch_create();
    key = (char *) c->argv[1]->ptr;

    keyword = sdscatlen(keyword, KEY_PREFIX_HASH, KEY_PREFIX_LENGTH);
    keyword = sdscatsds(keyword, key);
    keyword = sdscatlen(keyword, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);
    leveldb_writebatch_put(wb, keyword, sdslen(keyword), "1", 1);
    for (i = 2; i < c->argc; i++) {
        field = (char *) c->argv[i]->ptr;
        value = (char *) c->argv[++i]->ptr;

        sdsclear(keyword);
        keyword = sdscatlen(keyword, KEY_PREFIX_HASH, KEY_PREFIX_LENGTH);
        keyword = sdscatsds(keyword, key);
        keyword = sdscatlen(keyword, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);
        keyword = sdscatsds(keyword, field);
        leveldb_writebatch_put(wb, keyword, sdslen(keyword), value, sdslen((sds) value));
    }
    sdsfree(keyword);

    leveldb_write(server.ds_db, server.woptions, wb, &err);
    leveldb_writebatch_destroy(wb);

    if (err != NULL) {
        addReplyError(c, err);
        leveldb_free(err);
        return 0;
    }
    if(ret) {
        addReply(c, shared.ok);
    }

    return 1;
}

void ds_hmset(redisClient *c) {
    ds_hmsetCommand(c, 1);
}

void rl_hmset(redisClient *c) {
    if(ds_hmsetCommand(c, 0)) {
        hmsetCommand(c);
        checkRlTTL(c->db, c->argv[1]);
    }
}

static int ds_hsetCommand(redisClient *c, int ret) {
    sds str;
    char *key, *field, *value, *err;
    leveldb_writebatch_t *wb;

    key = (char *) c->argv[1]->ptr;
    field = (char *) c->argv[2]->ptr;
    value = (char *) c->argv[3]->ptr;

    wb = leveldb_writebatch_create();

    str = sdsempty();
    str = sdscatlen(str, KEY_PREFIX_HASH, KEY_PREFIX_LENGTH);
    str = sdscatsds(str, key);
    str = sdscatlen(str, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);

    leveldb_writebatch_put(wb, str, sdslen(str), "1", 1);

    // append member
    str = sdscatsds(str, field);
    leveldb_writebatch_put(wb, str, sdslen(str), value, sdslen((sds) value));

    err = NULL;
    leveldb_write(server.ds_db, server.woptions, wb, &err);
    leveldb_writebatch_destroy(wb);
    sdsfree(str);

    if (err != NULL) {
        addReplyError(c, err);
        leveldb_free(err);
        return 0;
    } else {
        //addReply(c,shared.ok);
        // keep the same return type as redis's hset
        // TODO: how to distinguish the create(return 1) and update(return 0) ?
        if(ret) {
            addReplyLongLong(c, 1);
        }
        return 1;
    }
}

void ds_hset(redisClient *c) {
    ds_hsetCommand(c, 1);
}

/**
 * set if not exists.
 * return 1 (not exists and saved)
 * return 0 (already exists)
 */
void ds_hsetnx(redisClient *c) {
    char *value;
    sds keyword;

    size_t val_len;
    char *err = NULL;
    leveldb_writebatch_t *wb;

    err = NULL;
    val_len = 0;
    keyword = sdsempty();
    keyword = sdscatlen(keyword, KEY_PREFIX_HASH, KEY_PREFIX_LENGTH);
    keyword = sdscatsds(keyword, c->argv[1]->ptr);
    keyword = sdscatlen(keyword, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);

    wb = leveldb_writebatch_create();
    // <hash_prefix>key<member prefix>
    leveldb_writebatch_put(wb, keyword, sdslen(keyword), "1", 1);

    // append hash field
    keyword = sdscatsds(keyword, c->argv[2]->ptr);

    value = leveldb_get(server.ds_db, server.roptions, keyword, sdslen(keyword), &val_len, &err);
    if (err != NULL) {
        //error
        sdsfree(keyword);
        if (val_len > 0) leveldb_free(value);
        addReplyError(c, err);
        leveldb_free(err);
        leveldb_writebatch_destroy(wb);
        return;
    } else if (val_len > 0) {
        // already exists
        sdsfree(keyword);
        leveldb_free(value);
        addReplyLongLong(c, 0);
        leveldb_writebatch_destroy(wb);
        return;
    }

    err = NULL;
    leveldb_writebatch_put(wb, keyword, sdslen(keyword), (char *) c->argv[3]->ptr, sdslen((sds) c->argv[3]->ptr));
    leveldb_write(server.ds_db, server.woptions, wb, &err);
    if (err != NULL) {
        addReplyError(c, err);
        leveldb_free(err);
    } else {
        addReplyLongLong(c, 1);
    }

    sdsfree(keyword);
    leveldb_writebatch_destroy(wb);
    return;
}

void rl_hset(redisClient *c) {
    
    if(ds_hsetCommand(c, 0)) {
        hsetCommand(c);
        checkRlTTL(c->db, c->argv[1]); 
    }
}

void rl_hdel(redisClient *c) {
    robj *o;
    int j, deleted = 0;

    if ((o = lookupKeyWrite(c->db, c->argv[1])) == NULL ||
            checkType(c, o, REDIS_HASH)) {
        ds_hdel(c);
        return;
    }

    for (j = 2; j < c->argc; j++) {
        if (hashTypeDelete(o, c->argv[j])) {
            deleted++;
            if (hashTypeLength(o) == 0) {
                dbDelete(c->db, c->argv[1]);
                break;
            }
        }
    }
    if (deleted) {
        signalModifiedKey(c->db, c->argv[1]);
        server.dirty += deleted;
    }

    ds_hdel(c);

}

void ds_hgetall(redisClient *c) {
    sds str, header;
    char *keyword = NULL, *err;

    leveldb_iterator_t *iter;

    const char *key, *value;
    size_t key_len, value_len, len, i;

    i = 0;
    str = sdsempty();

    str = sdscatlen(str, KEY_PREFIX_HASH, KEY_PREFIX_LENGTH);
    str = sdscatsds(str, c->argv[1]->ptr);
    str = sdscatlen(str, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);
    len = sdslen(str);
    keyword = zmalloc(len + 1);
    memcpy(keyword, str, len);

    keyword[len] = '\0';

    sdsclear(str);
    iter = leveldb_create_iterator(server.ds_db, server.roptions);
    for (leveldb_iter_seek(iter, keyword, len); leveldb_iter_valid(iter); leveldb_iter_next(iter)) {

        key_len = value_len = 0;
        key = leveldb_iter_key(iter, &key_len);


        if (key_len < len || strncmp(keyword, key, len) != 0)
            break;


        if (key_len == len)
            continue;
        value = leveldb_iter_value(iter, &value_len);

        str = sdscatprintf(str, "$%zu\r\n", key_len - len);
        str = sdscatlen(str, key + len, key_len - len);
        str = sdscatprintf(str, "\r\n$%zu\r\n", value_len);
        str = sdscatlen(str, value, value_len);
        str = sdscatlen(str, "\r\n", 2);

        i++;
    }
    err = NULL;
    zfree(keyword);
    leveldb_iter_get_error(iter, &err);
    leveldb_iter_destroy(iter);

    if (err) {
        addReplyError(c, err);
        leveldb_free(err);
    } else if (i == 0) {
        addReply(c, shared.nullbulk);
    } else {

        header = sdsempty();
        header = sdscatprintf(header, "*%zu\r\n", (i * 2));
        header = sdscatlen(header, str, sdslen(str));
        addReplySds(c, header);
    }

    sdsfree(str);

    return;
}

void ds_hkeys(redisClient *c) {
    sds str, header;
    char *keyword = NULL, *err;

    leveldb_iterator_t *iter;

    const char *key;
    size_t key_len, value_len, len, i;

    i = 0;
    str = sdsempty();

    str = sdscatlen(str, KEY_PREFIX_HASH, KEY_PREFIX_LENGTH);
    str = sdscatsds(str, c->argv[1]->ptr);
    str = sdscatlen(str, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);
    len = sdslen(str);
    keyword = zmalloc(len + 1);
    memcpy(keyword, str, len);

    keyword[len] = '\0';

    sdsclear(str);
    iter = leveldb_create_iterator(server.ds_db, server.roptions);
    for (leveldb_iter_seek(iter, keyword, len); leveldb_iter_valid(iter); leveldb_iter_next(iter)) {

        key_len = value_len = 0;
        key = leveldb_iter_key(iter, &key_len);

        // make sure the hashtable is the same
        if (key_len < len || strncmp(keyword, key, len) != 0)
            break;

        // skip the hashtable itself
        if (key_len == len)
            continue;

        str = sdscatprintf(str, "$%zu\r\n", (key_len - len));
        str = sdscatlen(str, key + len, key_len - len);
        str = sdscatlen(str, "\r\n", 2);

        i++;
    }
    err = NULL;
    zfree(keyword);
    leveldb_iter_get_error(iter, &err);
    leveldb_iter_destroy(iter);

    if (err) {
        addReplyError(c, err);
        leveldb_free(err);
    } else if (i == 0) {
        addReply(c, shared.nullbulk);
    } else {

        header = sdsempty();
        header = sdscatprintf(header, "*%zu\r\n", (i * 1));
        header = sdscatlen(header, str, sdslen(str));
        addReplySds(c, header);
    }

    sdsfree(str);

    return;
}

void ds_hvals(redisClient *c) {
    sds str, header;
    char *keyword = NULL, *err;

    leveldb_iterator_t *iter;

    const char *key, *value;
    size_t key_len, value_len, len, i;

    i = 0;
    str = sdsempty();

    str = sdscatlen(str, KEY_PREFIX_HASH, KEY_PREFIX_LENGTH);
    str = sdscatsds(str, c->argv[1]->ptr);
    str = sdscatlen(str, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);
    len = sdslen(str);
    keyword = zmalloc(len + 1);
    memcpy(keyword, str, len);

    keyword[len] = '\0';

    sdsclear(str);
    iter = leveldb_create_iterator(server.ds_db, server.roptions);
    for (leveldb_iter_seek(iter, keyword, len); leveldb_iter_valid(iter); leveldb_iter_next(iter)) {

        key_len = value_len = 0;
        key = leveldb_iter_key(iter, &key_len);

        // make sure the hashtable is the same
        if (key_len < len || strncmp(keyword, key, len) != 0)
            break;

        // skip the hashtable itself
        if (key_len == len)
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

    if (err) {
        addReplyError(c, err);
        leveldb_free(err);
    } else if (i == 0) {
        addReply(c, shared.nullbulk);
    } else {

        header = sdsempty();
        header = sdscatprintf(header, "*%zu\r\n", (i * 1));
        header = sdscatlen(header, str, sdslen(str));
        addReplySds(c, header);
    }

    sdsfree(str);

    return;
}

void ds_hlen(redisClient *c) {
    sds str;
    char *keyword = NULL, *err;

    leveldb_iterator_t *iter;

    const char *key;
    size_t key_len, value_len, len, i;

    i = 0;
    str = sdsempty();

    str = sdscatlen(str, KEY_PREFIX_HASH, KEY_PREFIX_LENGTH);
    str = sdscatsds(str, c->argv[1]->ptr);
    str = sdscatlen(str, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);
    len = sdslen(str);
    keyword = zmalloc(len + 1);
    memcpy(keyword, str, len);

    keyword[len] = '\0';

    sdsclear(str);
    iter = leveldb_create_iterator(server.ds_db, server.roptions);
    for (leveldb_iter_seek(iter, keyword, len); leveldb_iter_valid(iter); leveldb_iter_next(iter)) {

        key_len = value_len = 0;
        key = leveldb_iter_key(iter, &key_len);

        // make sure the hashtable is the same
        if (key_len < len || strncmp(keyword, key, len) != 0)
            break;

        // skip the hashtable itself
        if (key_len == len)
            continue;

        i++;
    }
    err = NULL;
    zfree(keyword);
    leveldb_iter_get_error(iter, &err);
    leveldb_iter_destroy(iter);

    if (err) {
        addReplyError(c, err);
        leveldb_free(err);
    } else if (i == 0) {
        addReply(c, shared.nullbulk);
    } else {

        addReplyLongLong(c, i);
    }

    sdsfree(str);
    return;
}

void ds_hdel(redisClient *c) {
    const char *key;
    size_t key_len, i;

    sds keyword;
    char *err = NULL;

    leveldb_writebatch_t *wb;
    leveldb_iterator_t *iter;
    int dflag = 0;

    keyword = sdsempty();

    //delete hashtable key
    if (c->argc < 3) {
        keyword = sdscatlen(keyword, KEY_PREFIX_HASH, KEY_PREFIX_LENGTH);
        keyword = sdscatsds(keyword, c->argv[1]->ptr);
        keyword = sdscatlen(keyword, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);


        iter = leveldb_create_iterator(server.ds_db, server.roptions);
        wb = leveldb_writebatch_create();

        for (leveldb_iter_seek(iter, keyword, sdslen(keyword)); leveldb_iter_valid(iter); leveldb_iter_next(iter)) {
            key_len = 0;
            key = leveldb_iter_key(iter, &key_len);

            if (key_len < sdslen(keyword) || strncmp(keyword, key, sdslen(keyword)) != 0)
                break;

            leveldb_writebatch_delete(wb, key, key_len);
            if (!dflag) {
                dflag = 1;
            }
        }

        leveldb_write(server.ds_db, server.woptions, wb, &err);
        leveldb_writebatch_clear(wb);
        leveldb_writebatch_destroy(wb);
        sdsfree(keyword);

        if (err != NULL) {
            addReplyError(c, err);
            leveldb_free(err);
            return;
        }

        err = NULL;
        leveldb_iter_get_error(iter, &err);
        leveldb_iter_destroy(iter);

        if (err) {
            addReplyError(c, err);
            leveldb_free(err);

            return;
        }

        addReply(c, dflag ? shared.cone : shared.czero);
        return;
    }

    err = NULL;

    wb = leveldb_writebatch_create();
    for (i = 2; i < c->argc; i++) {
        sdsclear(keyword);
        keyword = sdscatlen(keyword, KEY_PREFIX_HASH, KEY_PREFIX_LENGTH);
        keyword = sdscatsds(keyword, c->argv[1]->ptr);
        keyword = sdscatlen(keyword, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);
        keyword = sdscatsds(keyword, c->argv[i]->ptr);
        leveldb_writebatch_delete(wb, keyword, sdslen(keyword));
    }

    sdsfree(keyword);
    leveldb_write(server.ds_db, server.woptions, wb, &err);
    leveldb_writebatch_clear(wb);
    leveldb_writebatch_destroy(wb);


    if (err != NULL) {
        addReplyError(c, err);
        leveldb_free(err);
        return;
    }


    addReplyLongLong(c, c->argc - 2);

    return;
}

static void ds_hgetCommand(redisClient *c, int set) {
    sds str;
    size_t val_len = 0;
    char *key = NULL, *field = NULL, *value = NULL, *err = NULL;


    key = (char *) c->argv[1]->ptr;
    field = (char *) c->argv[2]->ptr;

    str = sdsempty();
    str = sdscatlen(str, KEY_PREFIX_HASH, KEY_PREFIX_LENGTH);
    str = sdscatsds(str, key);
    str = sdscatlen(str, MEMBER_PREFIX, MEMBER_PREFIX_LENGTH);
    str = sdscatsds(str, field);
    value = leveldb_get(server.ds_db, server.roptions, str, sdslen(str), &val_len, &err);
    if (err != NULL) {
        addReplyError(c, err);
        leveldb_free(err);
        if (val_len > 0) leveldb_free(value);

        sdsfree(str);
        return;
    } else if (value == NULL) {
        addReply(c, shared.nullbulk);

        sdsfree(str);
        return;
    }
    sdsfree(str);

    if(set) {
        robj *o;

        if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) != NULL){
            robj *rv;
            rv = createStringObject(value, val_len);
            hashTypeSet(o,c->argv[2], rv);
        }
    }

    addReplyBulkCBuffer(c, value, val_len);
    leveldb_free(value);

}

void ds_hget(redisClient *c) {
    ds_hgetCommand(c, 0);
    return;
}

static void rl_hgetCommand(redisClient *c, int set) {
    robj *o;

    if ((o = lookupKeyRead(c->db, c->argv[1])) != NULL) {
        //addHashFieldToReply(c, o, c->argv[2]);
        //checkRlTTL(c->db, c->argv[1]); 
        if(o->type != REDIS_HASH) {
            addReply(c, shared.wrongtypeerr);
            return;
        }

        int ret;

        if (o->encoding == REDIS_ENCODING_ZIPLIST) {
            unsigned char *vstr = NULL;
            unsigned int vlen = UINT_MAX;
            long long vll = LLONG_MAX;

            ret = hashTypeGetFromZiplist(o, c->argv[2], &vstr, &vlen, &vll);
            if (ret < 0) {
                ds_hgetCommand(c, set);
            } else {
                if (vstr) {
                    addReplyBulkCBuffer(c, vstr, vlen);
                } else {
                    addReplyBulkLongLong(c, vll);
                }
                checkRlTTL(c->db, c->argv[1]); 
            }

        } else if (o->encoding == REDIS_ENCODING_HT) {
            robj *value;

            ret = hashTypeGetFromHashTable(o, c->argv[2], &value);
            if (ret < 0) {
                //addReply(c, shared.nullbulk);
                ds_hgetCommand(c, set);
                checkRlTTL(c->db, c->argv[1]); 
            } else {
                addReplyBulk(c, value);
            }

        } else {
            redisPanic("Unknown hash encoding");
        }
        return;
    }
    ds_hgetCommand(c, set);

}

void rl_hget(redisClient *c) {
    rl_hgetCommand(c, 0);
    return;
}

void rl_hgetset(redisClient *c) {
    rl_hgetCommand(c, 1);
    return;
}

static void rl_hmgetCommand(redisClient *c, int set) {
    robj *o;
    o = lookupKeyRead(c->db, c->argv[1]);
    if (o == NULL) {
        ds_hmgetCommand(c, set);
        return;
    }

    if(o->type != REDIS_HASH) {
        addReply(c, shared.wrongtypeerr);
        return;
    }
    checkRlTTL(c->db, c->argv[1]);

    int i;
    char *key, *err = NULL;
    addReplyMultiBulkLen(c, c->argc-2);

    key = (char *) c->argv[1]->ptr;

    for (i = 2; i < c->argc; i++) {
        int ret;

        if (o == NULL) {
            addReply(c, shared.nullbulk);
            return;
        }

        if (o->encoding == REDIS_ENCODING_ZIPLIST) {
            unsigned char *vstr = NULL;
            unsigned int vlen = UINT_MAX;
            long long vll = LLONG_MAX;

            ret = hashTypeGetFromZiplist(o, c->argv[i], &vstr, &vlen, &vll);
            if (ret < 0) {
                //addReply(c, shared.nullbulk);
                err = NULL;
                robj *vo = ds_hgetToRobj(key, (char *) c->argv[i]->ptr, &err);
                if(err != NULL) {
                    addReplyError(c, err);
                    leveldb_free(err);
                    return;
                }
                if(vo == NULL) {
                    addReply(c, shared.nullbulk);
                } else {
                    addReplyBulk(c, vo);
                    if(set) {
                        robj *so;
                        if ((so = hashTypeLookupWriteOrCreate(c,c->argv[1])) != NULL){
                            hashTypeTryConversion(so,c->argv,2,3);
                            hashTypeTryObjectEncoding(so,&c->argv[i], &vo);
                            hashTypeSet(so,c->argv[i], vo);
                            signalModifiedKey(c->db,c->argv[1]);
                        }
                    }
                }
            } else {

                if (vstr) {
                    addReplyBulkCBuffer(c, vstr, vlen);
                } else {
                    addReplyBulkLongLong(c, vll);
                }
                
            }

        } else if (o->encoding == REDIS_ENCODING_HT) {
            robj *value;

            ret = hashTypeGetFromHashTable(o, c->argv[i], &value);
            if (ret < 0) {
                //addReply(c, shared.nullbulk);
                err = NULL;
                robj *vo = ds_hgetToRobj(key, (char *) c->argv[i]->ptr, &err);
                if(err != NULL) {
                    addReplyError(c, err);
                    leveldb_free(err);
                    return;
                }
                if(vo == NULL) {
                    addReply(c, shared.nullbulk);
                } else {
                    addReplyBulk(c, vo);
                    if(set) {
                        robj *so;
                        if ((so = hashTypeLookupWriteOrCreate(c,c->argv[1])) != NULL){
                            hashTypeTryConversion(so,c->argv,2,3);
                            hashTypeTryObjectEncoding(so,&c->argv[i], &vo);
                            hashTypeSet(so,c->argv[i], vo);
                            signalModifiedKey(c->db,c->argv[1]);
                        }
                    }
                }
            } else {
                addReplyBulk(c, value);
            }

        } else {
            redisPanic("Unknown hash encoding");
        }

    }
}

void rl_hmget(redisClient *c) {
    rl_hmgetCommand(c, 0);
}

void rl_hmgetset(redisClient *c) {
    rl_hmgetCommand(c, 1);
}

void ds_incrby(redisClient *c) {
    sds data;
    char *value;
    int64_t val, recore;

    size_t val_len;
    char *err = NULL;

    err = NULL;
    val_len = 0;

    value = leveldb_get(server.ds_db, server.roptions, c->argv[1]->ptr, sdslen((sds) c->argv[1]->ptr), &val_len, &err);
    if (err != NULL) {
        if (val_len > 0) leveldb_free(value);
        addReplyError(c, err);
        leveldb_free(err);
        return;
    } else if (val_len < 1) {
        val = 0;
    } else {
        val = strtoll(value, NULL, 10);
    }

    err = NULL;
    recore = strtoll(c->argv[2]->ptr, NULL, 10);
    recore = val + recore;
    data = sdsfromlonglong(recore);

    leveldb_put(server.ds_db, server.woptions, c->argv[1]->ptr, sdslen((sds) c->argv[1]->ptr), data, sdslen(data), &err);
    if (err != NULL) {
        addReplyError(c, err);
        leveldb_free(err);
    } else {
        addReplyLongLong(c, recore);
    }

    sdsfree(data);
    leveldb_free(value);
    return;
}

void ds_append(redisClient *c) {
    sds recore;
    char *value;

    size_t val_len;
    char *err = NULL;

    err = NULL;
    val_len = 0;

    value = leveldb_get(server.ds_db, server.roptions, c->argv[1]->ptr, sdslen((sds) c->argv[1]->ptr), &val_len, &err);
    if (err != NULL) {
        if (val_len > 0) leveldb_free(value);
        addReplyError(c, err);
        leveldb_free(err);
        return;
    }


    err = NULL;
    recore = sdsempty();
    if (val_len > 0) {
        recore = sdscpylen(recore, value, val_len);
    }
    recore = sdscatsds(recore, c->argv[2]->ptr);

    leveldb_put(server.ds_db, server.woptions, c->argv[1]->ptr, sdslen((sds) c->argv[1]->ptr), recore, sdslen(recore), &err);
    if (err != NULL) {
        addReplyError(c, err);
        leveldb_free(err);
    } else {
        addReplyLongLong(c, sdslen(recore));
    }

    sdsfree(recore);
    leveldb_free(value);
    return;
}

void ds_set(redisClient *c) {
    char *key, *value;
    char *err = NULL;

    key = (char *) c->argv[1]->ptr;
    value = (char *) c->argv[2]->ptr;
    leveldb_put(server.ds_db, server.woptions, key, sdslen((sds) key), value, sdslen((sds) value), &err);
    if (err != NULL) {
        addReplyError(c, err);
        leveldb_free(err);
        return;
    }
    addReply(c, shared.ok);
    return;
}

void rl_set(redisClient *c) {
    char *key, *value;
    char *err = NULL;

    key = (char *) c->argv[1]->ptr;
    value = (char *) c->argv[2]->ptr;
    leveldb_put(server.ds_db, server.woptions, key, sdslen((sds) key), value, sdslen((sds) value), &err);
    if (err != NULL) {
        addReplyError(c, err);
        leveldb_free(err);
        return;
    }
    //addReply(c,shared.ok);

    //存到redis
    setCommand(c);
    checkRlTTL(c->db, c->argv[1]); 

}

void ds_delete(redisClient *c) {
    int i;
    char *key;
    char *err = NULL;
    leveldb_writebatch_t *wb;

    if (c->argc < 3) {
        key = (char *) c->argv[1]->ptr;
        leveldb_delete(server.ds_db, server.woptions, key, sdslen((sds) key), &err);
        if (err != NULL) {
            addReplyError(c, err);
            leveldb_free(err);
            return;
        }
        addReply(c, shared.cone);
        return;
    }

    wb = leveldb_writebatch_create();
    for (i = 1; i < c->argc; i++) {
        leveldb_writebatch_delete(wb, (char *) c->argv[i]->ptr, sdslen((sds) c->argv[i]->ptr));
    }
    leveldb_write(server.ds_db, server.woptions, wb, &err);
    leveldb_writebatch_clear(wb);
    leveldb_writebatch_destroy(wb);
    wb = NULL;

    if (err != NULL) {
        addReplyError(c, err);
        leveldb_free(err);
        return;
    }

    addReplyLongLong(c, c->argc - 1);

    return;
}

void rl_delete(redisClient *c) {
    int i;
    char *key;
    char *err = NULL;
    leveldb_writebatch_t *wb;

    if (c->argc < 3) {
        key = (char *) c->argv[1]->ptr;
        leveldb_delete(server.ds_db, server.woptions, key, sdslen((sds) key), &err);
        if (err != NULL) {
            addReplyError(c, err);
            leveldb_free(err);
            return;
        }
        delCommand(c);
        return;
    }

    wb = leveldb_writebatch_create();
    for (i = 1; i < c->argc; i++) {
        leveldb_writebatch_delete(wb, (char *) c->argv[i]->ptr, sdslen((sds) c->argv[i]->ptr));
    }
    leveldb_write(server.ds_db, server.woptions, wb, &err);
    leveldb_writebatch_clear(wb);
    leveldb_writebatch_destroy(wb);
    wb = NULL;

    if (err != NULL) {
        addReplyError(c, err);
        leveldb_free(err);
        return;
    }

    delCommand(c);

}

void ds_close() {
    leveldb_readoptions_destroy(server.roptions);
    leveldb_writeoptions_destroy(server.woptions);
    leveldb_options_set_filter_policy(server.ds_options, NULL);
    leveldb_filterpolicy_destroy(server.policy);
    leveldb_close(server.ds_db);
    leveldb_options_destroy(server.ds_options);
    leveldb_cache_destroy(server.ds_cache);
}