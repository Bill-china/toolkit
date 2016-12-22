<?php
set_time_limit(0);
Yii::setPathOfAlias('Kafka', Yii::getPathOfAlias('application.extensions.Kafka'));
 // 加载kafka命名空间
class ComKafka
{

    private $topic;

    private $partition;
    
    private $partitionNum;

    private $callback;

    public $callbackFunc;

    private $singleOffsetCommit;

    private $client;

    private $tryAgain;
    
    private $currentOffset;
    
    private $category;
 // 调用方法分类，1:emqapi 2:func
    private $max_requests;
 // 单进程最大执行请求数量
    public function __construct($item,$partition=0)
    {
        /*
                       注册信号处理,暂时屏蔽，避免性能问题
        declare(ticks = 1);
        pcntl_signal(SIGTERM, "sig_handler");
        pcntl_signal(SIGHUP,  "sig_handler");
        pcntl_signal(SIGUSR1, "sig_handler");
        */
        // 加载消费配置文件
        \Kafka\Log::$logItem = $item;
        $conf = JConfig::item('kafka.consume.' . $item);
        if ($conf == '') {
            $logMsg = sprintf("no this conf:%s", $item);
            \Kafka\Log::write(\Kafka\Log::ERROR, $logMsg);
            die($logMsg);
        }
        
        // 检测topic
        if (isset($conf['topic']) && trim($conf['topic']) != '') {
            $this->topic = $conf['topic'];
        } else {
            $logMsg = "topic must be set";
            \Kafka\Log::write(\Kafka\Log::ERROR, $logMsg);
            die($logMsg);
        }
        
        // 检测partition
        /*
        if (isset($conf['partition']) && is_numeric($conf['partition']) && $conf['partition'] >= 0) {
            $this->partition = $conf['partition'];
        } else {
            $logMsg = "topic must be integer and >= 0";
            // $this->log($logMsg, ComKafkaLog::ERROR);
            \Kafka\Log::write(\Kafka\Log::ERROR, $logMsg);
            die($logMsg);
        }
        */
        //partition改为从启动参数传入，减少配置文件数量
        if (isset($conf['partitionNum']) && is_numeric($conf['partitionNum']) && $conf['partitionNum'] >= 0) {
            $this->partitionNum = $conf['partitionNum'];
        } else {
            $logMsg = "partitionNum must be integer and >= 0";
            \Kafka\Log::write(\Kafka\Log::ERROR, $logMsg);
            die($logMsg);
        }
        $this->partition = $partition;
        if ($this->partition >= $this->partitionNum) {
            $logMsg = "partition must be integer and <{$this->partitionNum}";
            \Kafka\Log::write(\Kafka\Log::ERROR, $logMsg);
            die($logMsg);
        }
        
        if (isset($conf['category'])) {
            $this->category = $conf['category'];
        } else {
            $this->category = 2;
        }
        
        if (isset($conf['callback']) && is_array($conf['callback'])) {
            $this->callback = $conf['callback'];
            //载入需要使用的类
            Yii::import("application.commands.{$this->callback[0]}");
        } else {
            $logMsg = "callback is error";
            \Kafka\Log::write(\Kafka\Log::ERROR, $logMsg);
            die($logMsg);
        }
        
        // 单进程没执行周期最大处理消息数，避免各种内存泄露问题，默认值1000,(0代表不限制)
        $this->max_requests = isset($conf['max_requests']) ? $conf['max_requests'] : 1000;
        
        // 设置group配置，默认为topic_callback
        $group = (isset($conf['group']) && trim($conf['group']) != '') ? $conf['group'] : $this->topic . "__" . implode('', $this->callback);
        
        // 设置broker_list配置
        $broker_list = JConfig::item('kafka.broker_list');
        if (!$broker_list) {
            $logMsg = "borker_list is error";
            \Kafka\Log::write(\Kafka\Log::ERROR, $logMsg);
            die($logMsg);
        }
        
        // 设置提交方式
        $this->singleOffsetCommit = (isset($conf['singleOffsetCommit']) && $conf['singleOffsetCommit'] === TRUE) ? true : false;
        
        $this->client = new \Kafka\MyClient($broker_list, $this->topic, $this->partition, $group);
    }
    
    /**
     * 获取当前的offset
     */
    public function getCurrentOffset(){
        $errno = 0;
        $offset = $this->client->sendFetchOffsetRequest($errno);
        if ($errno != 0) {
            $logMsg = sprintf("init topic %s, partition %s has an error,errno is:%d", $this->topic, $this->partition, $errno);
            \Kafka\Log::write(\Kafka\Log::ERROR, $logMsg);
            echo $logMsg."\n";
        }
        if (is_numeric($offset)){
            return $offset;
        }else{
            return false;
        }
    }
    
    /**
     * 获取最老的offset
     * @return boolean
     */
    public function getTheOldestOffset(){
        $result = $this->client->sendOffsetRequest(-2);
        if (is_numeric($result[0])){
            return $result[0];
        }else{
            return false;
        }
    }
    
    /**
     * 获取最新的offset
     * @return boolean
     */
    public function getTheLatestOffset(){
        $result = $this->client->sendOffsetRequest(-1);
        if (is_numeric($result[0])){
            return $result[0];
        }else{
            return false;
        }
    }
    
    /**
     * 设置offset
     * @param unknown $offset
     */
    public function setOffset($offset){
        $theOldestOffset  = self::getTheOldestOffset();
        $theLatestOffset  = self::getTheLatestOffset();
        //设置offset>最老可用offset 有效，否则设置成最老可用offset
        if ($offset<$theOldestOffset){
            echo "Invalid offset,auto set to:".$theOldestOffset."\n";
            return $this->client->sendCommitOffsetRequest($theOldestOffset);
        }elseif ($offset>$theLatestOffset){
            echo "Invalid offset,the input offset:({$offset}) > theLatestOffset({$theLatestOffset})\n";
        }else{
            return $this->client->sendCommitOffsetRequest($offset);
        }
    }

    /**
     * 获取当前offset的内容
     */
    public function getCurrentMsg() {
        $offset = self::getCurrentOffset();
        if (! is_numeric($offset)) {
            $logMsg = sprintf("topic:%s, partition:%s get current offset not a number, can't get current Msg", $this->topic, $this->partition);
            \Kafka\Log::write(\Kafka\Log::WARNING, $logMsg);
            return "nooffset";
        } else {
            $logMsg = sprintf("topic:%s, partition:%s,current offset is:%d", $this->topic, $this->partition, $offset);
            \Kafka\Log::write(\Kafka\Log::WARNING, $logMsg);
        }
        $queue = $this->client->sendFetchRequest($offset,$error_code);
        if (!empty($queue)) {
            return $queue[0];
        } else {
            return $queue;
        }
    }
    
    // ------------------------------------ consumer start ------------------------------------
    public function consume()
    {
        $queue = array();
        $request_times = 0;
        while (true) {
            if (! $queue) {
                // 获取当前offset值
                //$offset = $this->client->sendFetchOffsetRequest($errno);
                $offset = self::getCurrentOffset();
                if (! is_numeric($offset)) {
                    //$offset = 0;
                    //$this->client->sendCommitOffsetRequest($offset);
                    //$logMsg = sprintf("topic %s, partition %s rebuild offset", $this->topic, $this->partition);
                    //\Kafka\Log::write(\Kafka\Log::WARNING, $logMsg);
                    //获取最老可用offset
                    $offset = self::getTheOldestOffset();
                    $logMsg = sprintf("topic:%s, partition:%s get the oldestOffset is:%d", $this->topic, $this->partition, $offset);
                    \Kafka\Log::write(\Kafka\Log::WARNING, $logMsg);
                    //获取完最老有效offset后要设置offset
                    self::setOffset($offset);
                } else {
                    $logMsg = sprintf("topic:%s, partition:%s,current begin offset is:%d", $this->topic, $this->partition, $offset);
                    \Kafka\Log::write(\Kafka\Log::WARNING, $logMsg);
                }
                
                // 获取消息
                $queue = $this->client->sendFetchRequest($offset,$error_code);
                if (empty($queue)) {
                    $logMsg = sprintf("topic %s, partition %s no data,sleep 5 second", $this->topic, $this->partition);
                    \Kafka\Log::write(\Kafka\Log::WARNING, $logMsg);
                    sleep(5);
                    continue;
                }
            }
            
            foreach ($queue as $msg) {
                $tryAgain = false;
                $tryTimes = 1;
                do {
                    $begin = Utility::microtime_float();
                    
                    if ($this->category == 1) { // emqapi模式通过统一callbackFunc方法调用,配置文件中的callback作为参数传输
                        $res = call_user_func($this->callbackFunc, $this->callback, $msg['value']);
                    } else {
                        $res = call_user_func($this->callback, $msg['value']);
                    }
                    
                    $costTime = Utility::microtime_float() - $begin;
                    $this->currentOffset = $msg['offset'];
                    
                    // 成功
                    if ($res === 'success') {
                        $logMsg = sprintf("topic:%s, partition:%s,current offset:%d,msg:%s,res:%s,costTime:%8.5f,tryTimes:%d", $this->topic, $this->partition, $this->currentOffset, $msg['value'], $res, $costTime, $tryTimes);
                        \Kafka\Log::write(\Kafka\Log::INFO, $logMsg);
                        // 一次一提
                        if ($this->singleOffsetCommit) {
                            $this->client->sendCommitOffsetRequest($this->currentOffset + 1);
                        }
                        $this->tryAgain = false;
                    } else {
                        $logMsg = sprintf("topic:%s, partition:%s,current offset:%d,msg:%s,res:%s,costTime:%8.2f,tryTimes:%d", $this->topic, $this->partition, $this->currentOffset, $msg['value'], $res, $costTime, $tryTimes);
                        \Kafka\Log::write(\Kafka\Log::WARNING, $logMsg);
                        if ($tryTimes == 1) { // 失败第一次提交当前offset
                            $this->client->sendCommitOffsetRequest($this->currentOffset);
                        }
                        $tryTimes ++;
                        $this->tryAgain = true;
                        sleep(1);
                    }
                    $request_times ++;
                    // 执行超过单进程最大执行条数后退出释放资源。
                    if ($request_times >= $this->max_requests && $this->max_requests != 0) {
                        $logMsg = sprintf("topic:%s, partition:%s,current offset:%d, have reach max_requests:%d", $this->topic, $this->partition, $this->currentOffset+1, $this->max_requests);
                        \Kafka\Log::write(\Kafka\Log::WARNING, $logMsg);
                        // 批量提交且非重试情况下提交offset
                        if (! $this->singleOffsetCommit && $this->tryAgain === false) {
                            $this->client->sendCommitOffsetRequest($this->currentOffset + 1);
                        }
                        exit();
                    }
                    // $this->cleanup(); //没消费一条清理一次
                } while ($msg && $this->tryAgain === true);
            }
            // 一批处理完了，判断批量提交功能
            if (! $this->singleOffsetCommit) {
                $this->client->sendCommitOffsetRequest($this->currentOffset + 1);
            }
            unset($queue); // 清空取出数据
        }
    }

//------------------------------------ consumer end  ------------------------------------



//------------------------------------ public start ------------------------------------

    private function alert($str,$sms=false)
    {
        $this->log("sendAlert:".$str);
        ComAudit::sendAlert(__CLASS__,__FUNCTION__," topic:".$this->topic." partition:".$this->partition." callback:".implode('', $this->callback)." msg:".$str,$sms);
    }

    private function cleanup()
    {
        $components=Yii::app()->getComponents();
        foreach($components as $name =>$com)
        {
            if (is_array($com)){
                if($com['class']=='application.extensions.DbConnection')
                {
                    if(Yii::app()->$name->getActive())
                        Yii::app()->$name->setActive(false);
                    //Yii::app()->$name->close();
                }
            }elseif (is_object($com)){
                //redis不需要释放资源，在扩展中已经实现单例
                /*
                //释放redis资源
                if ($components[$name] instanceof CRedis){
                    Yii::app()->$name->close();
                    $logMsg = sprintf("%s have been release",$name);
                    \Kafka\Log::write(\Kafka\Log::DEBUG, $logMsg);
                }
                */
               
            }
        }
        
    }

    function sig_handler($signo)
    {
    
        switch ($signo) {
            case SIGTERM:
                // 处理SIGTERM信号
                echo "Caught SIGTERM... \n";
                if ($this->tryAgain == true){
                    $this->client->sendCommitOffsetRequest($this->currentOffset);
                }else{
                    $this->client->sendCommitOffsetRequest($this->currentOffset+1);
                }
                exit;
                break;
            case SIGHUP:
                echo "Caught SIGHUP... \n";
                //处理SIGHUP信号
                break;
            case SIGUSR1:
                echo "Caught SIGUSR1... \n";
                break;
            default:
                // 处理所有其他信号
        }
    
    }
    
}