<?php
/**
 * kafka
 */
include __DIR__ . '/CommonCommand.php';

class KafkaConsumerCommand extends CommonCommand {
        
    public function actionLowLevel($item,$partition)
    {
        echo date("Y-m-d H:i:s\t") . __METHOD__ . " start\n";
        $strLockFile = "lowLevel".__METHOD__."_{$item}"."_{$partition}";
        if (! $this->_getMutex($strLockFile)) {
            echo date("Y-m-d H:i:s") . "\tok\t" .  $strLockFile . " already running\n";
            return;
        }
        
        echo '开始内存：'.memory_get_usage()."\n";
        $kafka = new ComKafka($item,$partition);
        $kafka->callbackFunc = array($this,'callEmqApi');
        $kafka -> consume();
    }
    
    /**
     * highlevel版本
     * @param unknown $item
     */
    public function actionHighLevel($item)
    {
        echo date("Y-m-d H:i:s\t") . __METHOD__ . " start\n";
        
        echo '开始内存：'.memory_get_usage()."\n";
        $kafka = new ComKafkaHighLevel($item);
        $kafka->callbackFunc = array($this,'callEmqApi');
        $kafka -> highLevelConsume();
    }
    
    public function callEmqApi($callbackParm,$msg){
        $class=ucfirst($callbackParm[0])."Controller";
        $method="action".ucfirst($callbackParm[1]);
        
        //$controller = new $class($class);
        //单例模式避免重复申请内存
        $controller = self::getEmqApiInstance($class);
        $controller->postMsg = json_decode($msg,true);
        $controller->msgId = isset($controller->postMsg['msg_id'])? $controller->postMsg['msg_id'] : $controller->postMsg['mid'];
         
        ob_start();
        try{
            $controller->$method();
        }catch (Exception $e){
            $error_message=$e->getMessage();
            $error_file=$e->getFile();
            $error_line=$e->getLine();
            $error_msg="exception:".$error_message." in ".$error_file." on line ".$error_line;
            echo $error_msg;
        }
        $res = ob_get_contents();
        ob_end_clean();
        echo '现在内存：'.memory_get_usage()."\n";
        return $res;
    }
    
    public static function getEmqApiInstance($class){
        static $obj;
        if(!isset($obj)){
            $obj = new $class($class);
        }
        return $obj;
    }
}
?>
