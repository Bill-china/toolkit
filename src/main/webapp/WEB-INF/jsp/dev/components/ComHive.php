<?php
/**
 * Author: Kangle.Liu - liukangle@360.cn
 *
 * Last modified: 2013-07-14 18:21
 *
 * Filename: ComHive.php
 *
 * Description: 连接TSocket在抛出Exception时，会尝试再次调用
 *
 * 直到$intTryTime次抛出异常，才返回失败
 *
 */
class ComHive
{
    private $hiveClient;
    private $transport;

    public $host;
    public $port;
    public $intTryTime = 3;
    public $intTryInterval = 1000000; //尝试间隔为1000000微妙, 即1秒

    public function __construct()
    {
        $GLOBALS['THRIFT_ROOT'] = '/usr/local/thrift/lib/hive';  
        require_once $GLOBALS['THRIFT_ROOT'] . '/packages/hive_service/ThriftHive.php';  
        require_once $GLOBALS['THRIFT_ROOT'] . '/transport/TSocket.php';  
        require_once $GLOBALS['THRIFT_ROOT'] . '/protocol/TBinaryProtocol.php';  

        $this->host = Yii::app()->params['hive']['host'];
        $this->port = Yii::app()->params['hive']['port'];
        if ($this->connect($this->host, $this->port)) {
            $this->execute('add jar //home/hdp-guanggao/software/hive-0.8.1-bin/lib/hive-contrib-0.8.1.jar');  
            if (YII_DEBUG) {
                Yii::log("hive client connect connected!", 'info', 'hiveClient');
            }
        } else {
            throw new Exception("connect to hive failre ...\n");
        }
    }

    public function getInstance()
    {
        return $this->hiveClient;
    }

    public function connect($host, $port=10000)
    {
        try {
            $this->transport = new TSocket($host, $port);  
            $protocol = new TBinaryProtocol($this->transport);  
            $this->hiveClient = new ThriftHiveClient($protocol);  
            $this->transport->setSendTimeout(100000000);
            //$this->transport->setRecvTimeout(100000000);

            //tsocket经常会连不上，加上重试机制
            $this->transport->open();  
            Utility::autoTryCall(array($this->transport, 'open'), array(), $this->intTryTime, $this->intTryInterval);
            //echo "-----\n";
            return true;
        } catch (Exception $e) {
            print_r($e->getMessage());
            throw new Exception($e->getMessage());
            //return true;
        }
    }

    public function closeConnect()
    {
        $this->transport->close();
    }

    public function __call($strMethod, $arrParam)
    {
    	if (method_exists($this->hiveClient, $strMethod)) {
    		return Utility::autoTryCall(array($this->hiveClient, $strMethod), $arrParam, $this->intTryTime, $this->intTryInterval);
    	}
    	throw new Exception(get_class($this)."::{$strMethod} not defined");    	
    }
}


