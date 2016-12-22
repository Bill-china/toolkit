<?php
//file:webapp/protected/extensions/CRedis.php

class CRedis
{
    private $redis;

    public $host;
    /**
     * 如果访问redis时抛出Exception时，会尝试再次调用，直到$intTryTime次抛出异常，才返回失败
     * @var int
     */
    public $intTryTime = 3;
    public $intTryInterval = 100; //尝试间隔为100微妙

    public function __construct()
    {
        $this->redis = new Redis();
    }
    public function init()
    {
        $config = new CConfiguration(Yii::getPathOfAlias('application.config') . '/redis.php');
        if ($conf = $config->itemAt($this->host)) {
            if ($this->connect($conf['host'], $conf['port'], $conf['timeout'])) {
                Yii::log('redis connect to stat:' . print_r($conf, true));
            }
        }
        else {
            $master = $config->itemAt('master');
            if ($this->connect($master['host'], $master['port'], $master['timeout'])) {
                Yii::log('redis connect to master:' . print_r($master, true));
            }
            else {
                $slaves = $config->itemAt('slave');
                foreach($slaves as $slave) {
                    if ($this->connect($slave['host'], $slave['port'], $slave['timeout'])) {
                        Yii::log('redis connect to slave:' . print_r($slave, true));
                    }
                }
            }
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


