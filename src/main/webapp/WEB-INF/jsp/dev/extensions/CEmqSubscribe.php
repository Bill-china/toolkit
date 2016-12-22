<?php

Yii::import('application.extensions.emq.*');

class CEmqSubscribe
{
    protected $subscribe;
    protected $conf;
    protected static $mutexHandle;
    protected static $mutexPidFile;
    protected static $startTime;
    protected static $callbackClass;

    public function __construct($conf)
    {
        $this->conf = $conf;
    }

    public function init($exchange, $queue, $consumerTag, $routingKey, $id=null)
    {
        if (self::getMutex($queue . '_' . $id) == false) {
            echo date("Y-m-d H:i:s") . " task[" . $queue . '_' . $id . "] is running...\n";
            Yii::app()->end();
        }
        $this->subscribe = new EmqSubscribe($exchange, $queue, $consumerTag, $routingKey, $this->conf);
    }

    public function start($class, $action)
    {
        self::$callbackClass = "{$class}_{$action}";
        self::$startTime = time();
        if ($this->subscribe == null) {
            throw new Exception("please call [initSubscribe] first\n", 1);
        }
        $callback = '_' . lcfirst(substr($action, 6));
        if (!is_callable(array($class, $callback))) {
            throw new Exception("[{$class}] is not defined callback function [{$callback}]\n", 1);
        }
        $this->subscribe->start(
            array(
                $class,
                '_' . lcfirst(substr($action, 6))
                )
            );
    }

    /*
     * 消息处理成功
     *
     * $msg
     */
    public static function receiveSuccess($msg)
    {
        $msg->delivery_info['channel']->
            basic_ack($msg->delivery_info['delivery_tag']);
    }

    /*
     * 消息处理失败
     *
     * $msg
     * $retry: 是否重试消息
     */
    public static function receiveFail($msg, $retry = true)
    {
        $msg->delivery_info['channel']->
            basic_reject($msg->delivery_info['delivery_tag'], $retry);
    }

    public static function quit($msg)
    {
        ComAdLog::combineLog(array(date("YmdHis"), 'rabbitmq', json_encode(array('type'=>'sub','className' => self::$callbackClass, 'data'=>$msg->body))));
        if (is_file(self::$mutexPidFile)) {
            touch(self::$mutexPidFile);
        }
        if ((time() - self::$startTime) >= 600) { //十分钟如果没有消息更新则收到消息时先重启
            self::receiveFail($msg);
            $msg->delivery_info['channel']->
                basic_cancel($msg->delivery_info['consumer_tag']);
            echo "warning: [" . date('Y-m-d H:i:s') . "] subscribe exit with max time [600] second\n";
            Yii::app()->end();
        }
        // Send a message with the string "quit" to cancel the consumer.
        if ($msg->body === 'quit') {
            self::receiveSuccess($msg);
            echo "warning: [" . date('Y-m-d H:i:s') . "] subscribe exit with [quit] command\n";
            Yii::app()->end();
        }
        if ( (date("i")%10) == 0 && date("s") > 45 ) { //每00:45 10:45 20:45 ..重启
            self::receiveFail($msg);
            $msg->delivery_info['channel']->
                basic_cancel($msg->delivery_info['consumer_tag']);
            echo "warning: [" . date('Y-m-d H:i:s') . "] subscribe exit with max time [600] second\n";
            Yii::app()->end();
        }

        return false;
    }

    public static function curlPost($url, $msg)
    {
        $res = Yii::app()->curl->run($url,
            false,
            array('msg' => $msg->body)
        );

        if ($res == 'success') {
            self::receiveSuccess($msg);
        }
        else {
            self::receiveFail($msg);
        }
        return $res;
    }


    /**
     * 使用锁机制保证单进程执行任务
     *
     * @param string $strFilename 进行flock操作的文件
     * @return boolean 加锁成功返回true，否则false
     */
    public static function getMutex($strFilename) {
        if ($strFilename == '') {
            return false;
        }
        $strMutexFile = Yii::app()->runtimePath . "/_mutex_{$strFilename}";
        self::$mutexPidFile = $strMutexFile . '.pid';
        try {
            self::$mutexHandle = fopen($strMutexFile, 'w+');
            if (!self::$mutexHandle) {
                return false;
            }
            if (flock(self::$mutexHandle, LOCK_EX | LOCK_NB)) {
                file_put_contents(self::$mutexPidFile, getmypid());
                return true;
            }
            else {
                clearstatcache();
                $mtime = filemtime(self::$mutexPidFile);
                if ((time() - $mtime) > 900) {
                    $pid = file_get_contents(self::$mutexPidFile);
                    $res = exec("kill -9 {$pid} && echo ok");
                    if ($res == 'ok') {
                        echo "warning: [" . date('Y-m-d H:i:s') . "] subscribe exit by killed. max time [900] second\n";
                        $i = 10;
                        while ($i) {
                            if (flock(self::$mutexHandle, LOCK_EX | LOCK_NB)) {
                                file_put_contents(self::$mutexPidFile, getmypid());
                                return true;
                            }
                            sleep(3);
                            $i--;
                        }
                    }
                }
            }
        }
        catch(Exception $e) {
            return false;
        }

        return false;
    }
}

?>
