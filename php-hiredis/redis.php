<?php

class redis
{
	protected $host;
	protected $port;
	protected $conn;
	
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

	public function __call($method, $params)
	{
		array_unshift($params, $method);
		$str  = implode(" ", $params);
		$data =  phpiredis_command($this->conn, $str);
		
		if($data == "NULL")
			return NULL;
		else if($method == "ds_mget")
		{
			parse_str($data, $out);
			return $out;
		}
		return $data;
	}

	public function __destruct()
	{
		if($this->conn)
			phpiredis_disconnect($this->conn);
	}
}

/*
$db = new redis("192.168.1.102", 6379);
$rc = $db->connect();
if(!$rc)
{
   echo "can not connect redis server\r\n";
   exit;
}  
$data = $db->multi(array('DEL test', 'SET test 1', 'GET test'));
print_r($data);
//echo $db->set("name", "value");
//echo $db->get("name");
//$db->ds_set("tmp", "qiangjian");
$data = $db->ds_mget( "name", "nickname");
print_r($data);
 */
?>
