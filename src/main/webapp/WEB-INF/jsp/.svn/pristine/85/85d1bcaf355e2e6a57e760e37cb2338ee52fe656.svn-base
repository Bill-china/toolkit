<?php
/**
 * ComAdQuotaV2
 * 广告限额类 mediav 二期使用
 */
class ComAdQuotaV2 {

    // 点睛消费 key
    public static function getUserDJCostKey ($day, $userID) {
        // open_ad_v1:quota:cost:28-28715380
        return sprintf('open_ad_v1:quota:cost:%02d-%d', $day, $userID);
    }

    // 点睛预算 key
    public static function getUserDJQuotaKey ($userID) {
        // open_ad_v1:quota:user-info:28715380
        return sprintf('open_ad_v1:quota:user-info:%d', $userID);
    }

    // mv预算 key，只有用户预算
    public static function getUserMediavQuotaKey ($userID) {
        // mv:u:quota:28715380
        return sprintf('mv:u:quota:%d', $userID);
    }

    // mv消费 key，只有用户消费
    public static function getUserMediavCostKey ($day, $userID) {
        // mediav:quota:user-info:28715380
        // mv:u:cost:03-28715380
        return sprintf('mv:u:cost:%02d-%d', $day, $userID);
    }

    // 作弊返款
    public static function getRefundKey () {
        return 'open_ad_v1:quota:refundPop';
    }

    // 推送到作弊队列
    public static function refundPush($userID, $planID, $price, $dateTime) {
        $data = array(
            'user_id'   => $userID,
            'plan_id'   => $planID,
            'price'     => $price,
            'time'      => $dateTime
        );
        Utility::log(__CLASS__,__FUNCTION__,$data);
        $key = self::getRefundKey();
        $redis = Yii::app()->loader->limitRedis(0);
        return $redis->rPush($key, json_encode($data));
    }

    // 从队列中取出需要处理的作弊信息
    public static function refundPop() {
        $key = self::getRefundKey();
        $redis = Yii::app()->loader->limitRedis(0);
        return $redis->lPop($key);
    }

    // 作弊反款
    public static function refund($userID, $planID, $price, $dateTime) {
        $refundDay = date('j', $dateTime);
        $costInfo = self::getUserDJCostInfo($refundDay, $userID);
        if (!isset($costInfo['cost']) || !isset($costInfo[$planID])) {
            return ;
        }

        $costInfo['cost']   = number_format($costInfo['cost'] - min($price, $costInfo['cost']),   2, '.', '') ;
        $costInfo[$planID]  = number_format($costInfo[$planID] - min($price, $costInfo[$planID]), 2, '.', '') ;

        return self::setUserDJCostInfo($refundDay, $userID, $costInfo);
    }

    // 获取用户点睛预算信息
    public static function getUserDJQuotaInfo ($userID) {
        $redis = Yii::app()->loader->limitRedis($userID);
        $key = self::getUserDJQuotaKey($userID);
        $data = $redis->get($key);
        if (!empty($data)) {
            $data = json_decode($data, true);
        }
        return is_array($data) ? $data : array();
    }

    // 设置点睛预算
    public static function setDJUserQuota ($userID, $djQuota) {
        //$log = sprintf('%s setDJUserQuota, uid[%d] dj[%f]', date('Y-m-d H:i:s'), $userID, $djQuota);
        //ComAdLog::write($log, 'msg_' . date('ymd').'.log');

        $redis = Yii::app()->loader->limitRedis($userID);

        $cacheTime = strtotime(date('Y-m-d H:59:59')) - time() +900;
        $key = self::getUserDJQuotaKey($userID);
        if ($data = $redis->get($key) ) {
            $data = json_decode($data, true);
        }

        Utility::log(__CLASS__,__FUNCTION__,array($userID, $djQuota,$data));
        if (empty($data)) {
            $data = array(
                'quota' => (float) $djQuota,
            );
        } else {
            $data['quota'] = (float) $djQuota;
        }
        return $redis->setex($key, $cacheTime, json_encode($data));
    }

    // 设置余额
    public static function setDJUserBalance ($userID, $balance) {
        //$log = sprintf('%s setDJUserBalance, uid[%d] balance[%f]', date('Y-m-d H:i:s'), $userID, $balance);
        //ComAdLog::write($log, 'msg_' . date('ymd').'.log');
        $redis = Yii::app()->loader->limitRedis($userID);

        $cacheTime = strtotime(date('Y-m-d H:59:59')) - time() +900;
        $key = self::getUserDJQuotaKey($userID);
        if ($data = $redis->get($key) ) {
            $data = json_decode($data, true);
        }
        Utility::log(__CLASS__,__FUNCTION__,array($userID, $balance,$data));
        if (empty($data)) {
            $data = array(
                'balance' => (float) $balance,
            );
        } else {
            $data['balance'] = (float) $balance;
        }
        return $redis->setex($key, $cacheTime, json_encode($data));
    }

    // 设置点睛预算 用户&计划
    public static function setDJUserQuotaInfo ($userID, $data) {
        //$log = sprintf('%s setDJUserQuotainfo, uid[%d] quota[%f] balance[%f]', date('Y-m-d H:i:s'), $userID, $data['quota'], $data['balance']);
        //ComAdLog::write($log, 'msg_' . date('ymd').'.log');
        Utility::log(__CLASS__,__FUNCTION__,array($userID, $data));
        $redis = Yii::app()->loader->limitRedis($userID);

        //$cacheTime = strtotime(date('Y-m-d H:59:59')) - time() +900;
        //cache时间更改为1天+ jingguangwen@360.cn 2015-03-16
        $cacheTime = strtotime(date('Y-m-d H:59:59')) - time() + 86400;
        $key = self::getUserDJQuotaKey($userID);
        return $redis->setex($key, $cacheTime, json_encode($data));
    }

    // 删除点睛预算
    public static function delDJUserQuota ($userID) {
        $redis = Yii::app()->loader->limitRedis($userID);
        $key = self::getUserDJQuotaKey($userID);
        return $redis->delete($key);
    }

    // 获取用户点睛消费信息
    public static function getUserDJCostInfo ($day, $userID) {
        $key = self::getUserDJCostKey($day, $userID);
        $redis = Yii::app()->loader->limitRedis($userID);
        $data_arr = $redis->mget(array($key));
        Utility::log(__CLASS__,__FUNCTION__,array($day, $userID, $data_arr,$key));
        if($data_arr === false || !is_array($data_arr) || empty($data_arr)){
            //邮件报警
            $name = '点睛消费';
            $alert_content = "uid[$userID];key[$key]获取Redis消费金额失败，请立即查看!";
            Utility::sendAlert("Redis消费获取失败报警",$name,$alert_content,false);
            return false;
        }
        $data = $data_arr[0];
	$json=self::getFile($day,$userID);
	if(!empty($json) && $data!=$json)
	{
		//$data=$json;
		if($data===false){
		$data=$json;
		
		Utility::sendAlert("Redis消费获取失败报警","notsamecost","data=false  ".$json,false);
		}
	}
        if (!empty($data)) {
            $data = json_decode($data, true);
        }

        return is_array($data) ? $data : array();
    }

    // 设置用户点睛消费信息
    public static function setUserDJCostInfo ($day, $userID, $data) {
        $key = self::getUserDJCostKey($day, $userID);
        $redis = Yii::app()->loader->limitRedis($userID);
        $cacheTime = strtotime(date('Y-m-d 23:59:59')) - time() + 86400;
	$t=json_encode($data);
	self::setFile($day,$userID,$t);
        $ret=$redis->setex($key, $cacheTime, $t);
	Utility::log(__CLASS__,__FUNCTION__,array($day, $userID,$t,$cacheTime,$ret));
	return $ret;
    }
public static function getFile($day,$userID)
{
	$dir='/data/log/user_cost/'.$day.'/'.($userID%10).'/';
	if(!file_exists($dir.$userID))
	return "";
	else
	return @file_get_contents($dir.$userID);
}
public static function setFile($day,$userID,$data)
{
	$dir='/data/log/user_cost/'.$day.'/'.($userID%10).'/';
	if(!is_dir($dir))
	system("/bin/mkdir $dir -p");
	@file_put_contents($dir.$userID,$data);
    }
    // 获取用户mediav预算信息，无预算返回false
    public static function getUserMediavQuota ($userID) {
        $key = self::getUserMediavQuotaKey($userID);
        $redis = Yii::app()->loader->limitRedis($userID);
        $mvQuota = $redis->get($key);
        if (false === $mvQuota) {
            return false;
        }
        return $mvQuota;
    }

    // 设置mediav预算
    public static function setMediavQuota ($userID, $mediavQuota) {
        $mediavQuota = (float) $mediavQuota;
        //$log = sprintf('%s setMediavQuota, uid[%d] mediav[%f]', date('Y-m-d H:i:s'), $userID, $mediavQuota);
        //ComAdLog::write($log, 'msg_' . date('ymd').'.log');
        Utility::log(__CLASS__,__FUNCTION__,array($userID, $mediavQuota));

        $redis = Yii::app()->loader->limitRedis($userID);

        //$cacheTime = strtotime(date('Y-m-d H:59:59')) - time() +900;
        //cache时间更改为1天+ jingguangwen@360.cn 2015-03-16
        $cacheTime = strtotime(date('Y-m-d H:59:59')) - time() + 86400;
        $key = self::getUserMediavQuotaKey($userID);
        return $redis->setex($key, $cacheTime, $mediavQuota);
    }

    // 删除mediav预算
    public static function delMediavQuota ($userID) {
        $redis = Yii::app()->loader->limitRedis($userID);
        $key = self::getUserMediavQuotaKey($userID);
        return $redis->delete($key);
    }

    // 获取用户mediav消费信息
    public static function getUserMediavCost ($day, $userID) {
        $key = self::getUserMediavCostKey($day, $userID);
        $redis = Yii::app()->loader->limitRedis($userID);
        $data_arr = $redis->mget(array($key));
        Utility::log(__CLASS__,__FUNCTION__,array($day, $userID, $data_arr));
        if($data_arr === false || !is_array($data_arr) || empty($data_arr)){
            $name = 'MV类消费';
            $alert_content = "uid[$userID];key[$key]获取Redis消费金额失败，请立即查看!";
            Utility::sendAlert("Redis消费获取失败报警",$name,$alert_content,false);
            return false;
        }
        $mvCost = $data_arr[0];
        if (false === $mvCost) {
            return 0;
        }
        return $mvCost;
    }

    // 设置mv消费信息
    public static function setUserMediavCost ($day, $userID, $mvCost) {
        Utility::log(__CLASS__,__FUNCTION__,array($day, $userID, $mvCost));
        $key = self::getUserMediavCostKey($day, $userID);
        $redis = Yii::app()->loader->limitRedis($userID);
        $cacheTime = strtotime(date('Y-m-d 23:59:59')) - time() + 86400;
        return $redis->setex($key, $cacheTime, $mvCost);
    }

    public static function setPlanQuota($userID, $planID, $quota) {
        //$log = sprintf('%s setPlanQuota, uid[%d] pid[%d] quota[%f]', date('Y-m-d H:i:s'), $userID, $planID, $quota);
        //ComAdLog::write($log, 'msg_' . date('ymd').'.log');
        Utility::log(__CLASS__,__FUNCTION__,array($userID, $planID, $quota));
        $redis = Yii::app()->loader->limitRedis($userID);
        $key = self::getUserDJQuotaKey($userID);
        $data = $redis->get($key);
        if (!empty($data)) {
            $data = json_decode($data, true);
        }
        if ($data===false) {
            return false;
        }
        $data[$planID] = (float)$quota;
        $cacheTime = strtotime(date('Y-m-d H:59:59')) - time() + 900;

        return $redis->setex($key, $cacheTime, json_encode($data));
    }

    // 昨天是否已经结算
    public static function isSettledSuccess() {
        $prefixKey = Config::item('redisKey') . 'strategy:';
        $day = intval(date('d', strtotime('-1 day')));
        $key = $prefixKey . "adcost:" . $day;
        $redis = Yii::app()->redis_ad_guess;
        $res = $redis->get($key);
        if ($res == 'success') {
            return true;
        }
        return false;
    }


    private static $lock_fd;
    //基于文件的用户锁，只能在同一台机器中使用
    public static function lockUser($uid)
    {
        if(isset(self::$lock_fd[$uid]))
        {
            $fd=self::$lock_fd[$uid];
        }
        else
        {
            $file='/data/log/lock_user/'.$uid;
            $fd=@fopen($file,'w+');
            self::$lock_fd[$uid]=$fd;
        }
        if(!$fd)
        {
            return false;
        }
        if (!@flock($fd, LOCK_EX))
        {
            return false;
        }
        fwrite($fd, getmypid());
        fflush($fd);

        return true;
    }

    public static function unlockUser($uid)
    {
        if(isset(self::$lock_fd[$uid]))
        {
            $fd=self::$lock_fd[$uid];
            unset(self::$lock_fd[$uid]);
        }
        else
        {
           return ;
        }
        if(!$fd)
        {
            return ;
        }
        @flock($fd, LOCK_UN);
        @fclose($fd);
    }
}
