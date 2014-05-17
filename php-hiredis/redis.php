<?php

class redis
{
	protected $host;
	protected $port;
	protected $conn;
    protected $seeds = [3079, 6151, 12289, 24593, 49157, 98317, 196613];
	
	public function __construct($host, $port)
	{
		$this->host = $host;
		$this->port = $port;
	}

	public function connect()
	{
		$this->conn = phpiredis_connect($this->host, $this->port);
		return $this->conn ? true : false;
	}
	
	public function pconnect()
	{
		$this->conn = phpiredis_pconnect($this->host, $this->port);
		return $this->conn ? true : false;
	}

    public function multi($recores)
    {
    	$data =  phpiredis_multi_command($this->conn, $recores);
    	return $data;
    }

    public function BloomFilter_add($namespace, $str)
    {
        $data = [['SELECT', '8']];
        foreach ($this->seeds as $value) 
        {
            $hash   = phpiredis_murmur3($str, $value);
            $data[] = ["setBit", $namespace, (string)$hash, "1"];
        }
        $res =  phpiredis_multi_command_bs($this->conn, $data);
        print_r($res);
    }

    public function BloomFilter_check($namespace, $str)
    {
        $data = [['SELECT', '8']];
        foreach ($this->seeds as $value) 
        {
            $hash   = phpiredis_murmur3($str, $value);
            $data[] = ["getBit", $namespace, (string)$hash];
        }
        $res =  phpiredis_multi_command_bs($this->conn, $data);
        if($res[0] != "OK")
            return false;

        array_shift($res);
        foreach ($res as $value) 
        {
            if($value === 0)
                return false;
        }
        return true;
    }

    public function hmset($cmd, $recores)
    {
        if(!is_array($recores))
          return false;
    
        $data  = array("HMSET", $cmd);
        foreach($recores as $key=>$value)
        {
            array_push($data, (string)$key, (string)$value);
        }
    
        $rc =  phpiredis_command_bs($this->conn, $data);
        if($rc == "OK")
          return true;
    
        return false;
    }
    private function array_to_hash($data)
    {
        $len    = count($data);
        $recore = array();    
        
        for($i=0; $i<$len; $i++)
        {
            $recore[$data[$i++]] = $data[$i]; 
        }
        
        return $recore;
    }
    public function __call($method, $params)
    {
        if(is_null($params))
            return false;
    
		array_unshift($params, $method);
        	
        $data =  phpiredis_command_bs($this->conn, $params);	
		if($data == "NULL")
			return NULL;
        if(strcasecmp($method, "ds_mget") == 0 || strcasecmp($method, "ds_hmget") == 0)
		    return $this->array_to_hash($data);
        
        return $data;
	}


	public function __destruct()
	{
		if($this->conn)
			phpiredis_disconnect($this->conn);
	}
}


/*
$db = new redis("127.0.0.1", 6379);
$rc = $db->connect();
if(!$rc)
{
   echo "can not connect redis server\r\n";
   exit;
} 

$db->BloomFilter_add("username", "无语");

$res = $db->BloomFilter_check("username", "无语");
var_dump($res);
*/
/*
$data = array("name"=>"qiye", "age"=>18);
    var_dump($db->hmset("key", $data));
var_dump($db->hgetall("key"));
*/
/*
$value = array("name"=>"qiye", "age"=>18);
$db->hmset("user:1", $value);


$data = $db->multi(array('DEL test', 'SET test 1', 'GET test'));
print_r($data);
//echo $db->set("name", "value");
//echo $db->get("name");
$db->ds_set("name", "qiangjian");
$db->ds_set("nickname", "qiye");
$data = $db->ds_mget( "name", "nickname");
print_r($data);
*/
echo "\r\n";
//echo phpiredis_genid(); 
?>
