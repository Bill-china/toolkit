<?php
/**
 * kafka
 */
include __DIR__ . '/CommonCommand.php';

class KafkaManageCommand extends CommonCommand {
        
    /**
     * 设置offset，用于回放消息。
     * 回放消息步骤：
     * 1.暂停消费服务
     * 2.设置offset
     * 3.启用消费服务
     * @param unknown $item
     * @param unknown $offset
     */
    public function actionSetOffset($item,$partition,$offset){
        $kafka = new ComKafka($item,$partition);
        $result = $kafka->setOffset($offset);
        if ($result){
            echo "setOffset success\n";
        }else{
            echo "setOffset fail,please cheak log\n";
        }
    }
    /**
     * 批量设置topic所有partition的offset为同一个offset值
     */
    public function actionBatchSetOffset($item, $offset) {
        $conf = JConfig::item('kafka.consume.' . $item);
        if (isset($conf['partitionNum']) && is_numeric($conf['partitionNum']) && $conf['partitionNum'] >= 0) {
            $partitionNum = $conf['partitionNum'];
        } else {
            $logMsg = "partitionNum must be integer and >= 0";
            \Kafka\Log::write(\Kafka\Log::ERROR, $logMsg);
            die($logMsg);
        }
        for($i=0;$i<$partitionNum;$i++){
            $kafka = new ComKafka($item,$i);
            $result = $kafka->setOffset($offset);
            if ($result) {
                echo $item . " Partition ". $i ." Batch SetOffset success\n";
            } else {
                echo $item . " Partition ". $i ." Batch SetOffset fail,Please check log\n";
            }
        }
    }
    
    /**
     * 获取最老有效offset
     * @param unknown $item
     * @param unknown $partition
     */
    public function actionGetTheOldestOffset($item,$partition){
        $kafka = new ComKafka($item,$partition);
        $result = $kafka->getTheOldestOffset();
        echo "The oldest offset is {$result}\n";
    }
    /**
     * 获取所有最老有效offset
     * @param unknown $item
     * @return $offsets
     */
    public function actionGetAllTheOldestOffset($item) {
        $conf = JConfig::item('kafka.consume.' . $item);
        if (isset($conf['partitionNum']) && is_numeric($conf['partitionNum']) && $conf['partitionNum'] >= 0) {
            $partitionNum = $conf['partitionNum'];
        } else {
            $logMsg = "partitionNum must be integer and >= 0";
            \Kafka\Log::write(\Kafka\Log::ERROR, $logMsg);
            die($logMsg);
        }
        for($i=0;$i<$partitionNum;$i++){
            $kafka = new ComKafka($item,$i);
            $result = $kafka->getTheOldestOffset();
            if ($result) {
                echo $item . " Partition ". $i ." oldest Offset is " . $result . "\n";
            } else {
                echo $item . " Partition ". $i ." get Error\n";
            }
        }
    }
    
    /**
     * 获取当前offset
     * @param unknown $item
     * @param unknown $partition
     * @param unknown $offset
     */
    public function actionGetCurrentOffset($item,$partition){
        $kafka = new ComKafka($item,$partition);
        $result = $kafka->getCurrentOffset();
        echo "The current offset is {$result}\n";
    }

    /**
     * 获取当前item下所有partiton的offset
     * @param unknown $item
     * @param unknown $offset
     */
    public function actionGetAllCurrentOffset($item) {
        $conf = JConfig::item('kafka.consume.' . $item);
        if (isset($conf['partitionNum']) && is_numeric($conf['partitionNum']) && $conf['partitionNum'] >= 0) {
            $partitionNum = $conf['partitionNum'];
        } else {
            $logMsg = "partitionNum must be integer and >= 0";
            \Kafka\Log::write(\Kafka\Log::ERROR, $logMsg);
            die($logMsg);
        }
        for($i=0;$i<$partitionNum;$i++){
            $kafka = new ComKafka($item,$i);
            $result = $kafka->getCurrentOffset();
            if ($result) {
                echo $item . " Partition ". $i ." current Offset is " . $result . "\n";
            } else {
                echo $item . " Partition ". $i ." get Error\n";
            }
        }
    }

    /**
     * 首次添加某个订阅时，为从新产生的开始消费
     * 将offset设置为最新
     */
    public function actionInitNewSubscribe($item, $partition) {
        $kafka = new ComKafka($item, $partition);
        $lastOffset = $kafka->getTheLatestOffset();
        $result = $kafka->setOffset($lastOffset);
        if ($result) {
            echo "Init " . $item . " partition ". $partition . " success\n";
        } else {
            echo "Init " . $item . " partition ". $partition . " fail,Please check log\n";
        }
    }
    /**
     * 批量首次添加某个订阅时，为从新产生的开始消费，将offset设置为最新
     */
    public function actionBatchInitNewSubscribe($item) {
        $conf = JConfig::item('kafka.consume.' . $item);
        if (isset($conf['partitionNum']) && is_numeric($conf['partitionNum']) && $conf['partitionNum'] >= 0) {
            $partitionNum = $conf['partitionNum'];
        } else {
            $logMsg = "partitionNum must be integer and >= 0";
            \Kafka\Log::write(\Kafka\Log::ERROR, $logMsg);
            die($logMsg);
        }
        for($i=0;$i<$partitionNum;$i++){
            $kafka = new ComKafka($item, $i);
            $lastOffset = $kafka->getTheLatestOffset();
            $result = $kafka->setOffset($lastOffset);
            if ($result) {
                echo "Init " . $item . " partition ". $i . " success\n";
            } else {
                echo "Init " . $item . " partition ". $i . " fail,Please check log\n";
            }
        }
    }

    /**
     * 重放消息方法
     * @param unknown $item
     * @param int $partition
     * @param unknown $startTime 时间戳
     */
    public function actionReplayMsg($item, $partition, $startTime) {
        $conf = JConfig::item('kafka.consume.' . $item);
        if (isset($conf['partitionNum']) && is_numeric($conf['partitionNum']) && $conf['partitionNum'] >= 0) {
            $partitionNum = $conf['partitionNum'];
        } else {
            $logMsg = "partitionNum must be integer and >= 0";
            \Kafka\Log::write(\Kafka\Log::ERROR, $logMsg);
            die($logMsg);
        }

        $kafka = new ComKafka($item, $partition);
        $oldestOffset = $kafka->getTheOldestOffset();
        $lastOffset = $kafka->getTheLatestOffset();

        $time = (int)($startTime/60);
        $time = $time * 60;
        $key = $conf['topic'] . "_" . $partition . "_" . $time;

        $redis = Yii::app()->redis;
        $redis->connect('10.138.65.229' , 6379);
        
        $replayOffset = $redis->get($key);
        
        if ($replayOffset == false) {
            echo "Can't get offset\n";
            return;
        }
        echo "startTime : " . $time . ", offset = " . $replayOffset . "\n";

        if ($replayOffset<$oldestOffset) {
            echo "offset=" . $replayOffset . " <  oldestOffset=" . $oldestOffset . ", auto set offset to oldest\n";
            $replayOffset = $oldestOffset;
        } elseif ($replayOffset > $lastOffset) {
            echo "offest=" . $replayOffset . " > lastOffset=" . $lastOffset . ", please check it\n";
            return;
        }

        $result = $kafka->setOffset($replayOffset);
        if ($result){
            echo "setOffset success\n";
        } else {
            echo "setOffset fail,please cheak log\n";
        }

    }

    /**
     * 获取topic与item对应关系
     *
     *
     */
    public function actionShowTopicItem() {
        $conf = JConfig::item('kafka.consume');
        $result = array();
        foreach ($conf as $item => $content) {
            $topic = $content['topic'];
            $result[$topic][] = $item;
        }
        foreach ($result as $topic => $items) {
            echo $topic . ": \n";
            foreach ($items as $item) {
                echo  $item . ",";
            }
            echo "\n\n";
        }
    }
}
?>
