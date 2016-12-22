<?php
/**
 * Author: Wei.Gong - gongwei-sal@360.cn
 *
 * Last modified: 2014-05-04 10:16
 *
 * Filename: InfoCommand.php
 *
 * Description:
 *
 */
Yii::import('application.extensions.CEmqPublisher');
class InfoCommand extends CConsoleCommand
{
    // 实时消费的redis
    protected $_consume_redis        = null;

    protected $_consume_redis_conf   = array();

    protected $_consume_is_connected = false;

    // 保存用户信息的redis
    protected $_user_redis        = array();

    protected $_user_redis_conf   = array();

    protected $_user_is_connected = array();

    protected $_user_redis_num    = 0;

    // crontab
    protected $_task_name = '';

    /**
     * 将用户的实时消费信息取出，包装再发送给引擎
     */
    public function actionIndex() {
        $this->_task_name = 'info_'.date('YmdHi');
        $beginTime = time();
        $total = $success = $fail = 0;
        while (true) {
            $arrMsg = array();
            // 获取实时消费信息
            $consumeInfo = $this->getConsumeInfo();
            if (false === $consumeInfo) {
                break;
            }
            if (empty($consumeInfo)) {
                printf("task %s, list is empty!\n", $this->_task_name);
                break;
            }
            foreach ($consumeInfo as $oneInfo) {
                $total++;

                $userID = $oneInfo['u'];

                $userInfo = $this->getUserQuotaInfo($userID);
                if ($userInfo === false) {
                    printf("task %s get user[%s] quota info fail!\n", $this->_task_name, $userID);
                    $fail++;
                    continue;
                }

                $costInfo = $this->getUserCostInfo($userID);
                if ($costInfo == false) {
                    printf("task %s get user[%s] cost info fail!\n", $this->_task_name, $userID);
                    $fail++;
                    continue;
                }
                $_oneMessage = array(
                    'userid'            => $userID,
                    'account_budget'    => $userInfo['quota'],
                    'account_cost'      => $costInfo['cost'],
                    'account_balance'   => $userInfo['balance'] - $costInfo['cost'],
                    'plan'              => array(),
                );
                foreach ($oneInfo['p'] as $planID) {
                    if (!isset($costInfo[$planID])) {
                        printf("task %s plan[%s] has no cost record!\n", $this->_task_name, $planID);
                        continue;
                    }
                    if (!isset($userInfo[$planID])) {
                        printf("task %s plan[%s] has no quota record!\n", $this->_task_name, $planID);
                        continue;
                    }
                    $_planTmp = array(
                        'id'        => $planID,
                        'budget'    => $userInfo[$planID],
                        'cost'      => $costInfo[$planID],
                    );
                    $_oneMessage['plan'][] = $_planTmp;
                }
                $arrMsg[] = $_oneMessage;
                $success++;
            }
            // send message 
            $this->sendMsg($arrMsg);
        }
        $endTime = time();
        printf("task %s begin at %s, end %s, total %d, success %d, fail %d\n", 
            $this->_task_name,
            date('Y-m-d H:i:s', $beginTime),
            date('Y-m-d H:i:s', $endTime),
            $total, $success, $fail
        );

    }

    // --------------- 私有函数 -----------------

    // 获取redis链接，成功返回reids实例，失败抛出异常
    protected function getConsumeRedis() {
        if (empty($this->_consume_redis_conf)) {
            $config = new CConfiguration(Yii::getPathOfAlias('application.config') . '/redis.php');
            $this->_consume_redis_conf = $config->itemAt('consume');
        }
        if (is_null($this->_consume_redis)) {
            $this->_consume_redis = new Redis();
        }
        if ($this->_consume_is_connected === false || $this->_consume_redis->ping()!=='+PONG') {
            $ret = $this->_consume_redis->connect(
                $this->_consume_redis_conf['host'],
                $this->_consume_redis_conf['port']
            );
            if ($ret === false) {
                throw new Exception("can not connect redis", 1);
            }
            $this->_consume_is_connected = true;
        }
        return $this->_consume_redis;
    }

    // 获取用户信息的redis
    protected function getUserRedis($userID) {
        // 初始化配置
        if ($this->_user_redis_num == 0)  {
            $config = new CConfiguration(Yii::getPathOfAlias('application.config') . '/redis.php');
            $this->_user_redis_conf = $config->itemAt('limit');
            $this->_user_redis_num = count($this->_user_redis_conf);
        }
        $idx = $userID % $this->_user_redis_num;
        if (!isset($this->_user_redis[$idx])) {
            $this->_user_redis[$idx] = new Redis();
        }
        if (   !isset($this->_user_is_connected[$idx])
            || $this->_user_is_connected[$idx]===false
            || $this->_user_redis[$idx]->ping()!=='+PONG'
        ) {
            $ret = $this->_user_redis[$idx]->connect(
                $this->_user_redis_conf[$idx]['host'],
                $this->_user_redis_conf[$idx]['port']
            );
            if ($ret === false) {
                $msg = sprintf("can not connect redis host[%s] port[%s]", $this->_user_redis_conf[$idx]['host'],
                $this->_user_redis_conf[$idx]['port']);
                throw new Exception($msg, 1);
            }
            $this->_user_is_connected[$idx] = true;
        }
        return $this->_user_redis[$idx];
    }

    // 获取实时消费信息，最多500个
    protected function getConsumeInfo() {
        if (false === ($redis = $this->getConsumeRedis())) {
            printf("task %s error, can not redis!\n", $this->_task_name);
            return false;
        }
        $arrRet = array();
        $i = 0;
        while (true && $i<50) {
            $_ret = $redis->rpop('open_ad_v1:quota:consume');
            if ($_ret===false) {
                break;
            }
            $_arr = json_decode($_ret, true);
            if (false === $_arr) {
                printf("task %s error, invalid json[%s]!\n", $this->_task_name, $_ret);
                continue;
            }
            $i++;
            $arrRet[] = $_arr;
        }
        return $arrRet;
    }

    // 获取用户限额信息
    protected function getUserQuotaInfo($userID) {
        if (false === ($redis = $this->getUserRedis($userID))) {
            printf("task %s error, can not get user redis!\n", $this->_task_name);
            return false;
        }
        $key = "open_ad_v1:quota:user-info:".$userID;
        $info = $redis->get($key);
        if (false == $info) {
            printf("task %s error, can not get user[%s] quota info!\n", $this->_task_name, $userID);
            return false;
        }
        $arrInfo = json_decode($info, true);
        if (!is_array($arrInfo) || empty($arrInfo)) {
            printf("task %s error, can not get user[%s] quota info, invalid json[%s]!\n", $this->_task_name, $userID, $info);
            return false;
        }
        return $arrInfo;
    }
    // 获取用户消费信息
    protected function getUserCostInfo($userID) {
        if (false === ($redis = $this->getUserRedis($userID))) {
            printf("task %s error, can not get user redis!\n", $this->_task_name);
            return false;
        }

        $day = date("d");
        $key = 'open_ad_v1:quota:cost:'.$day.'-'.$userID;
        $info = $redis->get($key);
        if (false == $info) {
            printf("task %s error, can not get user[%s] cost info!\n", $this->_task_name, $userID);
            return false;
        }
        $arrInfo = json_decode($info, true);
        if (!is_array($arrInfo) || empty($arrInfo)) {
            printf("task %s error, can not get user[%s] cost info, invalid json[%s]!\n", $this->_task_name, $userID, $info);
            return false;
        }
        return $arrInfo;
    }

    // 发送消息给引擎
    protected function sendMsg($arrMsg) {
        try {
            foreach ($arrMsg as $oneItem) {
                $mqData = array($oneItem);
                $logID = sprintf("esc_%d_%5d", time(), mt_rand(0, 99999));
                CEmqPublisher::send(
                    Yii::app()->params['exchange']['consumeInfoMsg'],
                    'esc',
                    json_encode($mqData),
                    $logID,
                    Yii::app()->params['emq']
                );
            }
        } catch(Exception $e) {
            echo $e->getMessage();
            return false;
        }
    }
}
