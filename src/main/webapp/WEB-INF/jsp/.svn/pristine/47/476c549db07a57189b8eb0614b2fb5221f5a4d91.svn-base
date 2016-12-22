<?php
set_time_limit(0);
class ComEMQ
{
    public $callback; //设置回调方法
    public $exchangeName;//设置exchange名称
    public $queueName;//设置队列名称,不设置默认为exchangeName_queue
    public $checkInterval=10;//秒,心跳检查队列剩余数量及服务器连接状态
    public $alarmInterval=30;//分钟,心跳检查这个时间内消息还未被处理的时间间隔
    public $alarmReceiveTime=60;//秒，消息收到和发送时间间隔大于这个时间就报警
    public $exitTime=0;//退出时间,秒，0为不退出，大于0，则超过这个时间就会退出
    public $readTimeout=3600;//连接空闲时间，超过这么长的空闲时间就会重启
    public $priority=9;//0-9 优先级
    public $logid='';

    private $conf;
    private $conn;
    private $channel;
    private $exchange;
    private $queue;
    private $parent_pid;
    private $child_pids=array();
    private $receiveTimeExceedTimes=0;
    private $processStartTime;
    private static $monitor_redis;
    private static $init_param;
	public function __construct($param="emq")
    {
        self::$init_param=$param;
    	$conf=Yii::app()->params[$param];
        if(!$conf)
        {
            $this->log("conf error");
            die();
        }
        $this->conf=$conf;
    }
    public function getConnection()
    {
    	if($this->conn)
    	{
    		return $this->conn;
    	}

    	$conf=$this->conf;
        $conf['login']=$conf['user'];
        $conf['password']=$conf['pass'];
    	$this->conn=new CAMQPConnection($conf);
        $this->conn->setReadTimeout($this->readTimeout);
        $this->conn->setWriteTimeout($this->readTimeout);
    	$this->conn->connect();
    	if(!$this->conn->isConnected())
    	{
    		$this->log("can't connect to server");
            exit();
    	}
    	return $this->conn;
    }

    public function close()
    {
        if($this->conn)
            return $this->conn->disconnect();
        else
            return null;
    }

    public function getChannel()
    {
    	if($this->channel)
        {
            return $this->channel;
        }
    	else
    	{
    		$this->channel=new CAMQPChannel($this->conn);
            $this->channel->setPrefetchCount(1);
            return $this->channel;
    	}
    }
    public function getExchange()
    {
        $this->checkParam('exchangeName');
    	if($this->exchange)
        {
            $this->exchange->setName($this->exchangeName);
            return $this->exchange;
        }
    	else
    	{
    		$this->exchange=new CAMQPExchange($this->channel);
    		$this->exchange->setName($this->exchangeName);
    		$this->exchange->setType(AMQP_EX_TYPE_FANOUT);
    		$this->exchange->setFlags(AMQP_DURABLE);
    		$this->exchange->declare();
    		return $this->exchange;
    	}
    }
    public function getQueue()
    {
        $this->checkParam('exchangeName');

        $this->checkParam('queueName');
    	if($this->queue)
    	{
    		return $this->queue;
    	}
    	else
    	{
    		$this->queue = new CAMQPQueue($this->channel);
	        $this->queue->setName($this->queueName);
	        $this->queue->setFlags(AMQP_DURABLE);
	        $this->queue->declare();
	        $this->queue->bind($this->exchangeName);
    	}
    }

    public function send($data,$redis_monitor=1)
    {
        $start_time=microtime(true);
        $this->checkParam('exchangeName');
    	$this->getConnection();
    	$this->getChannel();
    	$this->getExchange();
        $md5=md5(microtime(true).$this->exchangeName.rand(0,9).getmypid().json_encode($data));
        $send_data=array(
            'mid'=>$md5,
            'logid'=>$this->logid,
            'msg_src'=>'esc',
            'time'=>time(),
            'exchange'=>$this->exchangeName,
            'content'=>$data
        );
        if($redis_monitor==1){
            $this->sendToMonitorRedis($send_data);
        }
    	$res=$this->exchange->publish(json_encode($send_data),null,AMQP_NOPARAM,array('content_type' => 'text/plain', 'delivery_mode' => 2,'priority'=>$this->priority));
        $end_time=microtime(true);
    	return $res?$md5:false;
    }

    public static function receiveSuccess($msg)
    {
        $msg->queue->ack($msg->envelope->getDeliveryTag());
        self::deleteFromMonitorRedis(json_decode($msg->body,true));
    }
    public static function receiveFail($msg,$redelivery=true)
    {
        $msg->queue->reject($msg->envelope->getDeliveryTag(),$redelivery? AMQP_REQUEUE:AMQP_NOPARAM);
    }
    private function sendToMonitorRedis($send_data)
    {
        if(!self::$monitor_redis)
        self::$monitor_redis=new ComRedis('esc_monitor_emq', 0);
        self::$monitor_redis->hset("rmq:".self::$init_param.":".$send_data['exchange'],$send_data['time'].":".$send_data['mid'],json_encode($send_data['content']));
    }
    private static function deleteFromMonitorRedis($send_data)
    {
        if(!self::$monitor_redis)
        self::$monitor_redis=new ComRedis('esc_monitor_emq', 0);
        self::$monitor_redis->hdel("rmq:".self::$init_param.":".$send_data['exchange'],$send_data['time'].":".$send_data['mid']);
    }

    private function checkParam($name)
    {
        if(isset($this->$name) && $this->$name)
            return true;
        else if($name=='queueName' || $name='callback')
        {
            $back=debug_backtrace();
            $back=$back[2];
            $class=$back['class'];
            $func=$back['function'];

            if($name=='queueName')
            {
                $this->$name=$class."_".$func."_queue";
            }
            else
            {
                $this->$name=array($class,'_' . lcfirst(substr($func, 6)));
            }
        }
        else
        {
            $this->log("must set $name");
            exit();
        }
    }

    public function callbackFunc($envelope, $queue)
    {
        try
        {
            $this->checkExitTime();
            $this->checkParent();
            $this->calculateReceiveTime($envelope->getBody());
            call_user_func_array($this->callback, array(new MsgBody($envelope,$queue)));
        }
        catch(Exception $ex)
        {
            $this->alert("callback Exception:".$ex->getMessage());
        }
    }
    private function checkExitTime()
    {
        if($this->exitTime && (time()-$this->processStartTime>$this->exitTime))
        {
            $this->log("exceed exit time,process exited");
            if($this->conn)
                $this->conn->disconnect();
            exit();
        }
    }
    private function calculateReceiveTime($content)
    {
        $data=@json_decode($content,true);
        if($data && isset($data['time']) && isset($data['mid']) && $this->alarmReceiveTime)
        {
            if(($t=(time()-$data['time']))>$this->alarmReceiveTime)
            {
                if($this->receiveTimeExceedTimes%10000==0)
                    $this->alert("msg:{$content} receive time $t sec exceed {$this->alarmReceiveTime}sec");
                $this->receiveTimeExceedTimes++;
            }
        }
    }
    private function checkParent()
    {
        if($this->parent_pid && posix_kill($this->parent_pid,0))
        {
            //说明主进程存在
        }
        else
        {
            $this->log("parent are not exists,child exited");
            exit();
        }
    }

    public function startSubScriber()
    {
        try
        {
            if(!$this->parent_pid)
            $this->parent_pid=getmypid();
            $this->checkParam('exchangeName');
            $this->checkParam('callback');

            if (!is_callable($this->callback)) {
                $this->log("callback can't call:".json_encode($this->callback));
                posix_kill($this->parent_pid,15);
                exit();
            }

            $this->processStartTime=time();
            $this->getConnection();
            $this->getChannel();
            $this->getQueue();
            $this->queue->consume(array($this,"callbackFunc"), AMQP_NOPARAM, $this->queueName."_tag");
        }
        catch(Exception $ex)
        {
            if($ex->getMessage()=='a socket error occurred')
            {
                $this->log("startSubScriber connection timeout ");
            }
            else if(strpos($ex->getMessage(),'no exchange') && strpos($ex->getMessage(),'NOT_FOUND'))
            {
                system('/usr/bin/curl -i -u '.$this->conf['user'].":".$this->conf['pass'].' -H "content-type:application/json" \
                        -XPUT -d\'{"type":"fanout","durable":true}\' \
                        '."http://".$this->conf['host'].":15672/api/".'/exchanges/%2femq/'.$this->exchangeName );
                $this->log("auto create exchange,please restart");
                exit();
            }
            else
            {
                $this->alert("startSubScriber get Exception :".$ex->getMessage());
            }
            exit();
        }
    }

    public function startMultiProcessSubScriber($num=1)
    {
        set_error_handler(array(&$this,'errorHandler'));
        $this->checkParam('exchangeName');
        $this->checkParam('queueName');
        $this->checkParam('callback');
        $this->parent_pid=getmypid();
        if($num<=1) $num=1;
        for($i=1;$i<=$num;$i++)
        {
            $this->createChild();
        }
        foreach($this->child_pids as $pid=>$t)
        {
            pcntl_waitpid($pid, $status,WNOHANG);//立即退出
        }
        $this->checkServer();

    }
    private function createChild()
    {
        $pid=pcntl_fork();
        if($pid<0)
        {
            $this->log("fork error");
            exit();
        }
        else if($pid==0)
        {
            $this->conn=null;
            $this->channel=null;
            $this->exchange=null;
            $this->queue=null;
            $this->startSubScriber();
            $this->log( "child ".getmypid()." exited");
            posix_kill(getmypid(), 9);
            exit();
        }
        else
        {
            $this->log( "create child $pid");
            $this->child_pids[$pid]=time();
        }

    }
    private  function checkServer()
    {
        //declare(ticks = 1);
        //pcntl_signal(SIGCHLD,array($this,"sig_handler"));
        $last_count=array();
        $unAckNum=array();
        while(true)
        {
            try
            {
                $this->conn=null;
                $this->channel=null;
                $this->exchange=null;
                $conn=$this->getConnection();
                $queue = new CAMQPQueue($this->getChannel());
                $queue->setName($this->queueName);
                $queue->setFlags(AMQP_PASSIVE);
                if($conn->isConnected())
                {
                    //检查剩余消息数量
                    $msg_count=intval($queue->declare());
                    $this->log($this->queueName." left msg count:$msg_count");
                    if($msg_count==0)
                    {
                        $last_count=array();
                    }
                    else
                    {
                        if(count($last_count)>($this->alarmInterval*60/$this->checkInterval))
                        {
                            //累计大于0的数量超过了报警时间就需要报警
                            $this->alert($this->alarmInterval ."分钟内检测连续未执行消息数量是:".implode(",", $last_count));
                            $last_count=array();
                        }
                        else
                        {
                            $last_count[]=$msg_count;
                        }
                    }

                    //开始检查未ack数量
                    $unack=$this->getUnAckNum();
                    if($unack>0)
                    {
                        if(count($unAckNum)>($this->alarmInterval*60/$this->checkInterval))
                        {
                            $this->alert($this->alarmInterval ."分钟内,可用消息数为0，检测一直未被确认的消息数量为:".implode(",", $unAckNum));
                            $unAckNum=array();
                        }
                        else
                        $unAckNum[]=$unack;

                    }
                    else
                    {
                        $unAckNum=array();
                    }
                    $conn->disconnect();
                }
                else
                {
                    $this->alert("emq lost connection",true);
                }
                $this->checkChildExitTime();
                $this->restartChild();
            }
            catch(Exception $ex)
            {
                $this->alert("loop check error: ".$ex->getMessage());
            }
            sleep($this->checkInterval);
        }
    }
    private function checkChildExitTime()
    {
        if(!$this->exitTime)
            return;
        foreach($this->child_pids as $pid=>$time)
        {
            //比正常退出时间大1分钟
            if(time()-$time-60>$this->exitTime)
            {
                $this->log("pid:".$pid ." reach exit time ,send kill");
                posix_kill($pid,15);
            }
        }
    }
    private function getUnAckNum()
    {
        $api='queues/%2femq/'.$this->queueName."?columns=messages_ready,messages_unacknowledged";
        $ch = curl_init();
        curl_setopt ( $ch, CURLOPT_URL, "http://".$this->conf['host'].":15672/api/$api" );
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
        curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 10);
        curl_setopt ( $ch , CURLOPT_USERPWD, $this->conf['user'].":".$this->conf['pass'] ) ;
        $ret = curl_exec($ch);
        curl_close($ch);
        if($ret)
        {
            $queue=json_decode($ret,true);
            if($queue['messages_ready']==0)
            {
                //如果没有可用的消息情况下，查看未确认的消息
                if($queue['messages_unacknowledged']>0)
                {
                    return $queue['messages_unacknowledged'];
                }
            }
        }
        return 0;
    }
    public  function sig_handler($signo)
    {
        if($this->parent_pid!=getmypid())
        {
            return ;
        }
        switch ($signo) {
            case SIGCHLD:
                $this->restartChild();
                break;
             default:
                $this->log("get signal:$signo");
                break;
        }
    }
    public  function restartChild()
    {
        $is_killed=array();
        foreach($this->child_pids as $pid=>$time)
        {
            pcntl_waitpid($pid, $status,WNOHANG);//立即退出
            if(!posix_kill($pid,0))
            {
                //如果进程不存在则会到这里
                unset($this->child_pids[$pid]);
                sleep(1);
                $this->createChild();
                $is_killed[]=$pid;
            }
        }
        if($is_killed)
        {
            $this->log("exited pids:".implode(",", $is_killed));
        }
    }
    private function log($str)
    {
        if (defined('YII_CMD') && YII_CMD)
        {
            if(!$this->logid)
            echo "[".date('Y-m-d H:i:s')."] ComEMQ PID_".getmypid()." logid:".$this->logid." $str\n";
        }
        else
        {
    }
    }
    private function alert($str,$sms=false)
    {
        $this->log("sendAlert:".$str);
    }
    public  function errorHandler($errno, $errstr, $errfile, $errline)
    {
        if($errno==E_USER_ERROR || $errno==E_ERROR)
        {
            $msg = 'get ERROR Handler, errno : ' . $errno . ', msg : ' . $errstr . ', file : ' . trim($errfile) . ', line : ' . $errline;
           $this->alert($msg);
           if($errstr=='a socket error occurred' || strpos($errstr,'connection closed'))
           {
                $this->alert("get socket error:$errstr,exiting!");
                die();
           }
        }
    }
}
class MsgBody {
    public $body;
    public $queue;
    public $envelope;

    public function __construct($e, $q)
    {
        $this->envelope = $e;
        $this->queue = $q;
        $this->body = $e->getBody();
    }
}
