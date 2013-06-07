更新说明
=========

<pre>
新增配置项：
rl:ttl 0        ( 单位：秒，大于0时会强制设置redis里key的过期时间，仅对rl系列命令有效) 
rl:ttlcheck 0   (当key的ttl在小于此值时被读取到，则过期期间重设置为rl:ttl ) 

rl系列命令：(同时操作redis和leveldb系列命令)
=======string数据操作======
rl_get key            (从redis或leveldb取值, 优先顺序：redis > leveldb)
rl_getset key         (返回同rl_get, 当leveldb有值，redis无值时，会回写到redis)
rl_mget k1 k2 k3      (取redis和leveldb的并集，优先级：redis>leveldb)
rl_mgetset k1 k2 k3   (返回同rl_mget, 当leveldb有值，redis无值，会回写到redis)
rl_set key val        (往redis和leveldb写值, 优先顺序：leveldb > redis, leveldb如果失败，将中断往redis写，返回错误)
rl_mset k1 v1 k2 v2   (往redis和leveldb批量写值, 优先顺序：leveldb > redis, leveldb如果失败，将中断往redis写，返回错误)
rl_del k1 k2 k3       (往redis和leveldb删值, 优先顺序：leveldb > redis)

========hash数据操作========
rl_hget key hk                (从redis或leveldb取值, 优先顺序：redis > leveldb)
rl_hgetset  key hk            (返回同rl_hget, 当leveldb有值，redis无值时，会回写到redis)
rl_hmget key hk1 hk2          (往redis和leveldb批量写值，优先级：redis>leveldb)
rl_hmgetset k1 k2 k3          (返回同rl_hmget, 当leveldb有值，redis无值，会回写到redis)
rl_hset key hk hv             (往redis和leveldb写值, 优先顺序：leveldb > redis, leveldb如果失败，将中断往redis写，返回错误)
rl_hmset key hk1 hv1 hk2 hv2   (取redis和leveldb的并集，优先级：redis>leveldb)
rl_hdel  key hk1 hk2 hk3      (往redis和leveldb删值, 优先顺序：leveldb > redis)

ds系列命令:
ds_hlen
ds_hvals
ds_hsetnx
使用方法和返回类型跟redis的相关命令一致

新增hash遍历方法
ds_hkeys_asc 正向遍历，默认返回30条key
使用方法
1. ds_hkeys_asc
2. ds_hkeys_asc 50 //返回最多50条
3. ds_hkeys_asc "key1"  //返回从key1位置开始的最多50条key
4. ds_hkeys_asc "key1" 100 //返回从key1位置开始的最多100条key
5. ds_hkeys_asc 0    //0没有意义，返回30条
5. ds_hkeys_asc 0 30 //从"0"这个key开始返回30条
特别说明: "key1"可以是不存在的
ds_hkeys_desc 功能类似，逆向遍历的

ds_hkeys_count startKey endKey // 计算starKey和endKey之间的数量，startKey和endKey可以是不存在的

新增遍历方法
ds_keys_asc
ds_keys_desc
ds_keys_count
使用方法跟 ds_hkeys_** 的类似

修复几个err处理的潜在bug


hash相关的操作除了hincrbyfloat之外已经全部实现

    **特别提醒**
    hash相关的命令，由于内部存储结构已经改变，如果您在老版本上使用ds_h** 存储过数据，新版本上无法支持。

    这个比较重要
    
    感谢skdear 同学提供的 功能补丁.
</pre>    
redis-storage manual
=========
[https://github.com/qiye/redis-storage/wiki/redis-storage-manual](https://github.com/qiye/redis-storage/wiki/redis-storage-manual)

overview
=========
  - 基于最新的redis-2.6.7, leveldb开发的,实现海量、高效数据持久存储
  - 实现redis的string和hashs功能函数,完全兼容redis客户端
  - 用luajit替换LUA,增强lua执行性能
  - author: 七夜, shenzhe
  - QQ: 531020471
  - QQ群: 62116204(已满)
  - QQ群: 154249567 (未满)
  - mail: lijinxing@gmail.com, shenzhe163@gmail.com


Install
=========
<pre>
https://github.com/qiye/redis-storage/archive/master.zip get source code
    
make init
make MALLOC=tcmalloc_minimal

need root
make install PREFIX=/usr/local/redis
</pre>

redis.conf
=========
<pre>
ds:create_if_missing 1                //if the specified database didn't exist will create a new one
ds:error_if_exists 0                  //if the opened database exsits will throw exception
ds:paranoid_checks 0
ds:ds_lru_cache 200                   //单位MB
ds:write_buffer_size 64               //单位MB
ds:block_size 32                      //单位KB
ds:max_open_files 4000
ds:block_restart_interval 16
ds:path ./leveldb
</pre>


cd php-hiredis/
=========
<pre>
//php code 
include "redis.php";
$db = new redis("127.0.0.1", 6379);
$rc = $db->connect();
if(!$rc)
{
   echo "can not connect redis server\r\n";
   exit;
}  
$data = $db->multi(array('DEL test', 'SET test 1', 'GET test'));
print_r($data);
echo $db->set("name", "qiye");
echo $db->get("name");
$db->ds_set("name", "qiye");
$db->ds_set("age", "20");
$data = $db->ds_mget( "name", "age");
print_r($data);
</pre>

php开发者推荐使用 phpredis 加强版 专门针对redis-storage的php扩展
=========

地址： https://github.com/shenzhe/phpredis

<pre>

$redis->dsSet("name", "shenzhe");  								//把数据存到leveldb
$redis->dsGet("name");            						 		//从leveldb取出数据, 输出 shenzhe
$redis->dsMSet(array("daniu"=>"qiye","cainiao"=>"shenzhe"));	//批量把数据存到leveldb; keys结构 array("key1"=>"val1", "key2"=>"val2")       
$redis->dsMGet(array("qiye", "cainiao"));       				//批量从leveldb取出数据
$redis->dsDel("name");               							//从leveldb删除数据， $key可以是字符串，也可是key的数组集合（相当于批量删除）
$redis->dsDel(array("daniu","cainiao"));               			//从leveldb删除数据， $key可以是字符串，也可是key的数组集合（相当于批量删除）
$redis->rlSet("name", "zeze");       							//先把数据存到leveldb，再存到redis
$redis->rlGet("name");
$redis->get("name");
$redis->dsGet("name");
$redis->rlDel("name");

</pre>

Nginx 用户可使用`redis2-nginx-module`或者ngx_lua的`lua-resty-redis`库
=========

https://github.com/agentzh/redis2-nginx-module

https://github.com/agentzh/lua-resty-redis

