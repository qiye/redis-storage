redis-storage
=========
  - 基于最新的redis-2.6.7开发的
  - 用luajit替换LUA,增强lua执行性能
  - author: 七夜
  - QQ: 531020471
  - QQ群: 62116204(已满)
  - QQ群: 154249567 (未满)
  - mail: lijinxing@gmail.com


    
安装 redis-storage
=========
<pre>
https://github.com/qiye/redis-storage 获取源码
    
make init
make MALLOC=tcmalloc_minimal

这一步需要root权限
make install PREFIX=/usr/local/redis
</pre>

修改redis配置文件
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

redis new cmd
=========
<pre>
ds_set name qiye
ds_get name
ds_del name 
ds_mset key value age 20
ds_mget key age
ds_del key age
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

由shenzhe同学开发的加强版 phpredis 专门针对redis-storage的php扩展
=========
<pre>
https://github.com/shenzhe/phpredis
    
$redis->dsGet($key): 从leveldb取出数据
$redis->dsMGet(array $keys) : 批量从leveldb取出数据,注：反回的是一个string：key1=val1&key2=val2, 需要用 parse_str 获取数组
$redis->dsSet($key, $value): 把数据存到leveldb
$redis->dsMSet(array $keys) :批量把数据存到leveldb; keys结构 array("key1"=>"val1", "key2"=>"val2")
$redis->dsDel($key):  从leveldb删除数据， $key可以是字符串，也可是key的数组集合（相当于批量删除）
</pre>

由shenzhe同学开发的zphp(专用于社交游戏 && 网页游戏的服务器端开发框架 ).集成了redis-storage 操作
=========

<pre>
https://github.com/shenzhe/zphp
</pre>