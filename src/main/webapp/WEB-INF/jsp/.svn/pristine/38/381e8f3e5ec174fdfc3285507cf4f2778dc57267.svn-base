<?php
set_time_limit(0);
Yii::setPathOfAlias('Kafka', Yii::getPathOfAlias('application.extensions.Kafka'));
 // 加载kafka命名空间
class ComKafkaHighLevel
{

    private $topic;

    private $partition;

    private $callback;

    public $callbackFunc;

    private $singleOffsetCommit;

    private $client;

    private $tryAgain;
    
    private $currentOffset;
    
    // 调用方法分类，1:emqapi 2:func
    private $category;
     
    // 单进程最大执行请求数量
    private $max_requests;
    
    //提交方式，默认1 按时间间隔提交, 2表示每次fetch之后提交
    private $commit_offset_method;
    
    //检查consumer zk 变动情况间隔
    private $check_consumer_interval;
    
    //单位s,提交offset间隔，当singleOffsetCommit = false 且 commit_offset_method = 1生效
    private $commit_offset_interval;
    
    public function __construct($item)
    {
        // 加载消费配置文件
        
        //设置log项
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
        
        //设置group配置，默认为topic_callback
        $group = (isset($conf['group']) && trim($conf['group']) != '') ? $conf['group'] : $this->topic . "__" . implode('', $this->callback);
        
        //设置单条提交
        $this->singleOffsetCommit = (isset($conf['singleOffsetCommit']) && $conf['singleOffsetCommit'] === TRUE) ? true : false;
        
        //设置提交方式，默认1 按时间间隔提交, 2表示每次fetch之后提交
        $this->commit_offset_method = isset($conf['commit_offset_method']) ? $conf['commit_offset_method'] : 1;
        
        //设置按时间提交间隔，默认10s
        $this->commit_offset_interval = isset($conf['commit_offset_interval']) ? $conf['commit_offset_interval'] : 10;
        
        //检查consumer 消费partition变动情况间隔,默认10s
        $this->check_consumer_interval = isset($conf['check_consumer_interval']) ? $conf['check_consumer_interval'] : 10;
        
        // 设置kafka_broker_list配置，默认为online
        $broker_list = JConfig::item('kafka.broker_list');
        if (empty($broker_list)){
            $logMsg = sprintf("no found broker_list conf");
            \Kafka\Log::write(\Kafka\Log::ERROR, $logMsg);
            die($logMsg); 
        }
        //zk list
        $zookeeper_list = JConfig::item('kafka.zookeeper_list');
        if (empty($zookeeper_list)){
            $logMsg = sprintf("no found zookeeper_list conf");
            \Kafka\Log::write(\Kafka\Log::ERROR, $logMsg);
            die($logMsg);
        }
        
        $this->client = new \Kafka\HighLevelConsumer($broker_list, $zookeeper_list, $this->topic, $group);
    }
    
    // ------------------------------------ consumer start ------------------------------------
    public function highLevelConsume()
    {
        $this->client->setParameter("check_consumer_interval", $this->check_consumer_interval);
        if ($this->singleOffsetCommit === true){
            //手动提交
           $this->client->setParameter("auto_commit_offset", false); 
        } else{
            //自动提交
            $this->client->setParameter("commit_offset_interval", $this->commit_offset_interval);
            $this->client->setParameter("commit_offset_method", $this->commit_offset_method);            
        }
        
        //设置主动退出时，HighLevelConsumer.php的析构函数，不提交offset，避免未处理的数据提交offset造成bug
        $this->client->setWhenExitCommitOffset(false);
        
        $request_times = 0;
        
        while (true) {
            $queue = array();
            $queue = $this->client->consume();
            if (empty($queue)) {
                $logMsg = sprintf("topic %s, partition %s no data,sleep 5 second", $this->topic, $this->partition);
                \Kafka\Log::write(\Kafka\Log::INFO, $logMsg);
                sleep(5);
                continue;
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
                    $this->partition = $msg['partition'];
                    // 成功
                    if ($res === 'success') {
                        $logMsg = sprintf("topic:%s,partition:%s,current offset:%d,msg:%s,res:%s,costTime:%8.5f,tryTimes:%d", $this->topic, $this->partition, $this->currentOffset, $msg['value'], $res, $costTime, $tryTimes);
                        \Kafka\Log::write(\Kafka\Log::NOTICE, $logMsg);
                        // 一次一提
                        if ($this->singleOffsetCommit) {
                            $this->client->sendCommitOffset($this->partition,$this->currentOffset + 1);
                        }
                        $this->tryAgain = false;
                    } else {
                        $logMsg = sprintf("topic:%s, partition:%s,current offset:%d,msg:%s,res:%s,costTime:%8.2f,tryTimes:%d", $this->topic, $this->partition, $this->currentOffset, $msg['value'], $res, $costTime, $tryTimes);
                        \Kafka\Log::write(\Kafka\Log::WARNING, $logMsg);
                        if ($tryTimes == 1) { // 失败第一次提交当前offset
                            $this->client->sendCommitOffset($this->partition,$this->currentOffset);
                        }
                        //重试>1000仍然不成功主要是mysql\redis资源连接不上,主动退出进程重连资源,由supervisor重新拉起
                        if ($tryTimes>1000) {
                            $message = $msg['value'];
                            $alert_content =  date('Y-m-d H:i:s',time())."\tKafka订阅报警\tERROR\tpartition:{$this->partition}\tcurrentOffset:{$this->currentOffset}\tmsg:{$message}\ttryTimes > 1000\n";
                            Utility::sendAlert(__CLASS__, __FUNCTION__,$alert_content,true);
                            \Kafka\Log::write(\Kafka\Log::ERROR, $alert_content);
                            $this->client->sendCommitOffset($this->partition,$this->currentOffset+1);
                            exit();
                        }
                        $tryTimes ++;
                        $this->tryAgain = true;
                    }
                    $request_times ++;
                    // 执行超过单进程最大执行条数后退出释放资源。
                    if ($request_times >= $this->max_requests && $this->max_requests != 0) {
                        $logMsg = sprintf("topic:%s, partition:%s,current offset:%d, have reach max_requests:%d", $this->topic, $this->partition, $this->currentOffset, $this->max_requests);
                        \Kafka\Log::write(\Kafka\Log::WARNING, $logMsg);
                        if (!$this->singleOffsetCommit && $this->tryAgain === false) {
                            $this->client->sendCommitOffset($this->partition,$this->currentOffset + 1);
                        }
                        exit();
                    }
                } while ($msg && $this->tryAgain === true);
            }
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
}
