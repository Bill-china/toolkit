<?php
ini_set('memory_limit', '2048M');
ini_set('max_execution_time', '9000');
ini_set('ignore_user_abort', 'on');

if (!defined('YII_CMD')) define('YII_CMD', true);

/**
 * 后台任务的公共父类
*/
abstract class CommonCommand extends CConsoleCommand {
    protected $_resMutexFile = null; //需要保证单进程执行时flock的文件句柄
    public $logid='';
    public function run($args) {
        global $dbID;
        $dbID = 0;
        foreach ($args as $key => $value) {
            if (preg_match('/^--db=(\d+)$/', $value, $matches)) {
                unset($args[$key]);
                $dbID = $matches[1];
                break;
            }
        }
        $this->logid='DIANJINGCRON_'.rand(1, 9) . microtime(true)*10000 . rand(100,999);
        parent::run($args);
    }

    /**
     * 使用锁机制保证单进程执行任务
     *
     * @param string $strFilename 进行flock操作的文件
     * @return boolean 加锁成功返回true，否则false
     */
    protected function _getMutex($strFilename) {
        $bolRet = false;
        if ($strFilename == '') {
            return $bolRet;
        }
        if (isset($GLOBALS['dbID'])) {
            $strFilename .= "_{$GLOBALS['dbID']}";
        }
        $strMutexFile = Yii::app()->runtimePath . "/crontab_lock_{$strFilename}.txt";
        $resFile = fopen($strMutexFile, 'a');
        if ($resFile) {
            $bolRet = flock($resFile, LOCK_EX | LOCK_NB);
            if ($bolRet) {
                $this->_resMutexFile = $resFile;
            } else {
                fclose($resFile);
            }
        }
         
        return $bolRet;
    }
    
    protected function afterAction($action,$params)
    {
        if ($this->_resMutexFile) {
            flock($this->_resMutexFile, LOCK_UN);
            fclose($this->_resMutexFile);
            $this->_resMutexFile = null;
        }
        parent::afterAction($action, $params);
    }
    
    protected function getDbConnection($dbID) {
        $config = new CConfiguration(Yii::getPathOfAlias('application.config') . '/db.php');
        if ($conf = $config->itemAt('db' . $dbID)) {
            return new CDbConnection($conf['connectionString'], $conf['username'], $conf['password']);
        } else {
            throw new Exception('db config error!');
            return false;
        }
    }

    public function edcApiPost($url, $data)
    {   
        $data['source']     = Yii::app()->params['edcApi']['sys_name'].'_'.Yii::app()->controller->id.'_'.Yii::app()->controller->action->id;
        $res = Yii::app()->curl->run(
            Yii::app()->params['edcApi']['url'] . $url,
            false,
            $this->makeClientSign($data)
        );  

        $res = json_decode($res, true);
        if (!isset($res['errno'])  && $res['errno'] == 1)
            echo 11;

        return $res;
    }   

    //在客户端生成sign值
    public function makeClientSign($params)
    {   
        $edcConfig =  Yii::app()->params['edcApi'];
        $params['appkey'] = Yii::app()->params['edcApi']['appkey'];
        $params['ver'] = $edcConfig['ver'];
        $params['time_stamp'] = microtime(true);
        $res = array();
        foreach($params as $k=>$v){
            if(!in_array($k,array('sign'))){
                if (is_array($v))
                    $v = json_encode($v);
                $res[trim($k)] = trim($v);
            }   
        }   
        $params['sign'] = md5(join('',$res) . Yii::app()->params['edcApi']['token']);
        return $params;
    }   
}
