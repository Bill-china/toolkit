<?php
//file:webapp/protected/extensions/HRedis.php

class ComRedis
{
    private $redis;

    public $host;
    /**
     * 如果访问redis时抛出Exception时，会尝试再次调用，直到$intTryTime次抛出异常，才返回失败
     * @var int
     */
    public $intTryTime = 3;
    public $intTryInterval = 100; //尝试间隔为100微妙

    public function __construct($name, $sid=false)
    {
        $config = new CConfiguration(Yii::getPathOfAlias('application.config') . '/redis.php');
        $servers = $config->itemAt($name);
        if ( !$servers || ($sid !== false && !isset($servers[$sid])) ) {
            throw new Exception('Redis 配置错误, node:' . $name . ' id:' . $sid);
        }
        $this->redis = new Redis();
        if ($sid !== false) {
            $cf = $servers[$sid];
        }
        else {
            $cf = $servers;
        }
        if ($this->connect($cf['host'], $cf['port'], $cf['timeout'])) {
            if (YII_DEBUG) {
                Yii::log("redis connect to {$name}, id, {$sid}" . print_r($cf, true));
            }
            if(isset($cf['password']) && !empty($cf['password'])){
                $this->redis->auth($cf['password']);
            }
        } else {
            $alert_content =  date('Y-m-d H:i:s',time())."\tredis connect to {$name}, id, {$sid} failed\n";
            Utility::sendAlert(__CLASS__, __FUNCTION__,$alert_content,false);
        }
    }

    public function getInstance()
    {
        return $this->redis;
    }

    public function connect($host, $port=6379, $timeout=false)
    {
        try {
            if ($host{0} == '/') {//unix domain socket
                return $this->redis->connect($host);
            }
            else {
                if ($timeout) {
                    return $this->redis->connect($host, $port, $timeout);
                }
                else {
                    return $this->redis->connect($host, $port);
                }
            }
        }
        catch(Exception $e) {
            return false;
        }
    }

    public function __call($strMethod, $arrParam)
    {
    	if (method_exists($this->redis, $strMethod)) {
    		return Utility::autoTryCall(array($this->redis, $strMethod), $arrParam, $this->intTryTime, $this->intTryInterval);
    	}

    	throw new Exception(get_class($this)."::{$strMethod} not defined");
    }
}


