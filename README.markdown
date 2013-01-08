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
ds:block_cache_size 10000
ds:write_buffer_size 100000000       //写缓存大小
ds:block_size 4096
ds:max_open_files 8000               //leveldb最多可以使用的檔案數，一個檔案可以儲存 2MB 的資料。
ds:block_restart_interval 16
ds:path /usr/local/redis/db/leveldb  //leveldb save path
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