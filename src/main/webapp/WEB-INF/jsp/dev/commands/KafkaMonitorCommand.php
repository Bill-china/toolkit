
<?php
/**
 * kafka 监控进程
 */
include __DIR__ . '/CommonCommand.php';

class KafkaMonitorCommand extends CommonCommand {
    /*
     * 展示所有item_topic_partition的offset及notconsume信息
     */
    public function actionShowNotConsume()
    {
        $config = JConfig::item('kafka.consume');
        echo date("Y-m-d H:i:s") . "\n";
        echo sprintf("% 20s\t% 20s\t% 20s\t% 20s\t% 20s\t% 20s\t% 20s\t\n","item","topic","partition", "oldest offset", "lastest offset", "current offset","not consume");
        foreach ($config as $item => $conf) {
            if (is_array($conf) && isset($conf['topic']) && isset($conf['partitionNum'])) {
                $partitionNum = $conf['partitionNum'];
                for($i=0;$i<$partitionNum;$i++) {
                    $kafka = new ComKafka($item, $i);
                    $oldestOffset = $kafka->getTheOldestOffset();
                    $currentOffset = $kafka->getCurrentOffset();
                    $lastOffset = $kafka->getTheLatestOffset();
                    $notConsume = $lastOffset - $currentOffset;
                    echo sprintf("% 20s\t% 20s\t% 20s\t% 20s\t% 20s\t% 20s\t% 20s\t\n",$item, $conf['topic'], $i, $oldestOffset, $lastOffset, $currentOffset, ($lastOffset-$currentOffset));
                }
            }
        }
        echo "\n";
    }
    /**
     * 消费堆积情况的监控
     * 列出所有topic的所有partition的各offset情况以及多少条未消费的数据,测试期间
     * 后续可以把结果以邮件形式发送给相关人
     */ 
    public function actionNotConsume()
    {
        $config = JConfig::item('kafka.consume');
        $ip = `/sbin/ifconfig  | grep 'inet addr:'| grep -v '127.0.0.1' | cut -d: -f2 | awk 'NR==1 { print $1}'`;
        $title = "Kafka消费堆积_" . $ip;
        $alertInfo = '';
        foreach ($config as $item => $conf) {
            if (is_array($conf) && isset($conf['topic']) && isset($conf['partitionNum'])) {
                $partitionNum = $conf['partitionNum'];
                for($i=0;$i<$partitionNum;$i++) {
                    $kafka = new ComKafka($item, $i);
                    $oldestOffset = $kafka->getTheOldestOffset();
                    $currentOffset = $kafka->getCurrentOffset();
                    $lastOffset = $kafka->getTheLatestOffset();
                    $notConsume = $lastOffset - $currentOffset;
                    echo __FUNCTION__ ."\t".sprintf("%s\titem:%s, topic:%s, partition:%s, oldestOffset:%s, lastOffset:%s, currentOffset:%s, notConsume:%s\n", date("Y-m-d H:i:s"),$item, $conf['topic'], $i, $oldestOffset, $lastOffset, $currentOffset, $notConsume);

                    if ($notConsume >= 100) {
                        $alertInfo .= "currentOffset:{$currentOffset},topic:{$conf['topic']},partition:{$i}, 堆积了【" . $notConsume . "】条消息;";
                    }
                }
            }
        }
        
        if(!empty($alertInfo)){
            $msgInfo = $title."\t".$alertInfo;
            Utility::sendAlert(__CLASS__, __FUNCTION__,$alertInfo,true);
        }
    }

    /**
     * 消费延迟，超过5分钟报警
     */
    public function actionMsgDelay()
    {
        $config = JConfig::item('kafka.consume');
        $ip = `/sbin/ifconfig  | grep 'inet addr:'| grep -v '127.0.0.1' | cut -d: -f2 | awk 'NR==1 { print $1}'`;
        $title = "Kafka消费延迟_" . $ip;
        $alertInfo = '';
        foreach ($config as $item => $conf) {
            if (is_array($conf) && isset($conf['topic']) && isset($conf['partitionNum'])) {
                $topic = $conf['topic'];
                $partitionNum = $conf['partitionNum'];
                for($i=0;$i<$partitionNum;$i++) {
                    $kafka = new ComKafka($item, $i);
                    $offset = $kafka->getCurrentOffset();
                    $queue = $kafka->getCurrentMsg();
                    $now = time();
                    $nowstr = date("Y-m-d H:i:s", $now);

                    if (empty($queue)) {
                        echo __FUNCTION__ ."\t". "currentOffset:{$offset},topic:{$conf['topic']},partition:{$i} queue is none\n";
                        continue;
                    }
                    if ($queue == "nooffset") {
                        echo __FUNCTION__ ."\t". "currentOffset:{$offset},topic:{$conf['topic']},partition:{$i} queue is nooffset\n";
                        continue;
                    }

                    if($queue && isset($queue['value'])) {
                        $value = json_decode($queue['value'], true);
                        if(isset($value['msg_time'])) {
                            $msgTime = $value['msg_time'];
                            $timestr = date("Y-m-d H:i:s", $time);
                            $dif = $now - $msgTime;
                            echo __FUNCTION__ ."\t". sprintf("%s\ttopic:%s, partition:%s, offset:%s, 消息时间戳:%s, 当前时间戳:%s, 时间差(秒):%s,消息:\n",date("Y-m-d H:i:s"), $topic, $i, $offset , $msgTime ,  $now,  $dif ,$queue['value']);
                            if ($dif > 300) {
                                $alertInfo .= "currentOffset:{$offset},topic:{$conf['topic']},partition:{$i},延迟了【" . $dif . "】秒,msg:{$queue['value']};"; 
                            }                     
                        } else {
                            echo __FUNCTION__ ."\t". "currentOffset:{$offset},topic:{$conf['topic']},partition:{$i} queue msg_time not set\n";
                        }
                    }
                }
            }
        }

        if(!empty($alertInfo)){
            $msgInfo = $title."\t".$alertInfo;
            Utility::sendAlert(__CLASS__, __FUNCTION__,$alertInfo,true);
        }
    }


    /**
     * 消费延迟监控
     * 解析当前offset的json时间
     */
    public function actionShowMsgDelay()
    {
        $config = JConfig::item('kafka.consume');
        $result = array();
        echo date("Y-m-d H:i:s") . "\n";
        echo sprintf("% 30s\t% 30s\t% 10s\t% 10s\t% 20s\t% 20s\t% 20s\t% 20s\t% 20s\t\n", "item", "topic", "partition", "offset" , "消息时间戳" , "消息时间", "当前时间戳", "当前时间", "时间差(秒)");
        foreach ($config as $item => $conf) {
            if (is_array($conf) && isset($conf['topic']) && isset($conf['partitionNum'])) {
                $topic = $conf['topic'];
                $partitionNum = $conf['partitionNum'];
                for($i=0;$i<$partitionNum;$i++) {
                    $kafka = new ComKafka($item, $i);
                    $offset = $kafka->getCurrentOffset();
                    $queue = $kafka->getCurrentMsg();
                    $now = time();
                    $nowstr = date("Y-m-d H:i:s", $now);
                    if ($queue == "nooffset") {
                        echo sprintf("% 30s\t% 30s\t% 10s\t% 10s\t% 20s\t% 20s\t% 20s\t% 20s\t% 20s\t\n", $item ,$topic, $i, "none" , "none" , $now, $nowstr ,$dif);
                        continue;
                    } 
                    if (empty($queue)) {
                        echo sprintf("% 30s\t% 30s\t% 10s\t% 10s\t% 20s\t% 20s\t% 20s\t% 20s\t% 20s\t\n", $item, $topic, $i, $offset , "nomsg", "nomsg",  $now, $nowstr, 0);
                        continue;
                    }
                    if($queue && isset($queue['value'])) {
                        $value = json_decode($queue['value'], true);
                        if(isset($value['time'])) {
                            $time = $value['time'];
                            $timestr = date("Y-m-d H:i:s", $time);
                            $dif = $now - $time;
                            echo sprintf("% 30s\t% 30s\t% 10s\t% 10s\t% 20s\t% 20s\t% 20s\t% 20s\t% 20s\t\n", $item, $topic, $i, $offset , $time , $timestr, $now, $nowstr, $dif);
                        } else {
                            echo sprintf("% 30s\t% 30s\t% 10s\t% 10s\t% 20s\t% 20s\t% 20s\t% 20s\t% 20s\t\n", $item, $topic, $i, $offset , "unknown", "unknown", $now, $nowstr, "unknown");
                        }
                    }
                }
            }
        }
        echo "\n";
    }
}    
