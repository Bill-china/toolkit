<?php
require_once "cassandra_client_php/xitong/cassandra_client.php";

class CCassandra 
{
    public $cassandra;
    
    public function __construct()
    {
        $dbName = Config::item('cassandra_db');
        $this->cassandra = new CassandraDB($dbName);
    }

    public function init()
    {
    }

    public function insert($key, $value)
    {
        return $this->cassandra->insert($key, $value);
    }

    public function get($key)
    {
        return $this->cassandra->get($key);
    }
}

?>
