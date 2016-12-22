<?php
/**
 * ComAdQuota
 * 广告限额类
 * 统一操作广告限额
 * 
 * @package open 360
 * @version v1
 * @copyright 2005-2011 360.CN All Rights Reserved.
 * @author wangguoqiang@360.cn 
 */
class ComAdQuota
{
    protected $prefixKey;
    protected $redis;
    protected $offlineType;
    protected $offlineLog;

    protected $memCache;

    public function __construct()
    {
        //$this->redis = new ComRedis('tongji', $sid);
        $this->prefixKey = Config::item('redisKey') . 'quota:';
        $this->memCache = array();
        $this->offlineType = 0;
        $this->offlineLog = '';
    }

    public function setUserData($userId, $data) {
        $key = $this->getQuotaKey($userId);
        $redis = Yii::app()->loader->limitRedis($userId);
        $cacheTime = strtotime(date('Y-m-d H:59:59')) - time() +900;
        return $redis->setex($key, $cacheTime, json_encode($data));
    }

    public function setUserBalance($userId, $price)
    {
        ComAdLog::write(array(date('Y-m-d H:i:s'), 'components [comAdQuota] setUserBalance called.',$userId, $price), 'msg_' . date('ymd').'.log');
        $key = $this->getQuotaKey($userId);
        $redis = Yii::app()->loader->limitRedis($userId);
        if ($data = $redis->get($key)) {
            $tmp = json_decode($data, true);
            if (!$tmp) {
                $tmp = unserialize($data);
            }
            $data = $tmp;
        }
        $data['balance'] = (float)$price;
        $cacheTime = strtotime(date('Y-m-d H:59:59')) - time() +900;

        return $redis->setex($key, $cacheTime, json_encode($data));
    }

    public function setUserQuota($userId, $price)
    {
        ComAdLog::write(array(date('Y-m-d H:i:s'), 'components [comAdQuota] setUserQuoa called.',$userId, $price), 'msg_' . date('ymd').'.log');
        $key = $this->getQuotaKey($userId);
        $redis = Yii::app()->loader->limitRedis($userId);
        if ($data = $redis->get($key)) {
            $tmp = json_decode($data, true);
            if (!$tmp) {
                $tmp = unserialize($data);
            }
            $data = $tmp;
        }
        $data['quota'] = (float)$price;
        $cacheTime = strtotime(date('Y-m-d H:59:59')) - time() +900;

        return $redis->setex($key, $cacheTime, json_encode($data));
    }
    
    public function setPlanQuota($userId, $planId, $price)
    {
        ComAdLog::write(array(date('Y-m-d H:i:s'), 'components [comAdQuota] setPlanQuota called.',$planId, $price), 'msg_' . date('ymd').'.log');
        $key = $this->getQuotaKey($userId);
        $redis = Yii::app()->loader->limitRedis($userId);
        if ($data = $redis->get($key)) {
            $tmp = json_decode($data, true);
            if (!$tmp) {
                $tmp = unserialize($data);
            }
            $data = $tmp;
        }
        $data[$planId] = (float)$price;
        $cacheTime = strtotime(date('Y-m-d H:59:59')) - time() +900;

        return $redis->setex($key, $cacheTime, json_encode($data));
    
    }

    public function getUserData($userId)
    {
        $key = $this->getQuotaKey($userId);
        $redis = Yii::app()->loader->limitRedis($userId);
        if ($data = $redis->get($key)) {
            $tmp = json_decode($data, true);
            if (!$tmp) {
                $tmp = unserialize($data);
            }
            return $tmp;
        }
        return false;
    }

    public function getQuotaKey($userId)
    {
        return $this->prefixKey . 'user-info:' . $userId;
    }

    /**
     * getAdQuotaByUserId 
     * 用户账户每日消耗费用记录
     * 
     * @param mixed $userId 
     * @return void
     */
    public function getUserCost($userId)
    {
        $redis = Yii::app()->loader->limitRedis($userId);
        $key = $this->getKey($userId);
        if ($data = $redis->get($key)) {
            $tmp = json_decode($data, true);
            if (!$tmp) {
                $tmp = unserialize($data);
            }
            if (isset($tmp['cost'])) {
                return $tmp['cost'];
            }
        }

        return 0;
    }

    public function getYestodayUserCost($userId)
    {
        $redis = Yii::app()->loader->limitRedis($userId);
        $day = date('d', strtotime('-1 day'));
        $key = $this->prefixKey . "cost:{$day}-{$userId}";

        if ($data = $redis->get($key)) {
            $tmp = json_decode($data, true);
            if (!$tmp) {
                $tmp = unserialize($data);
            }
            if (isset($tmp['cost'])) {
                return $tmp['cost'];
            }
        }

        return 0;
    }

    public function getYestodayCost($userId)
    {
        $redis = Yii::app()->loader->limitRedis($userId);
        $day = date('d', strtotime('-1 day'));
        $key = $this->prefixKey . "cost:{$day}-{$userId}";

        if ($data = $redis->get($key)) {
            $tmp = json_decode($data, true);
            if (!$tmp) {
                $tmp = unserialize($data);
            }
            return $tmp;
        }

        return 0;
    }

    /**
     * getPlanCost 
     * 广告计划每日消耗费用
     * 
     * @param mixed $planId 
     * @return void
     */
    public function getPlanCost($userId, $planId)
    {
        $redis = Yii::app()->loader->limitRedis($userId);
        $key = $this->getKey($userId);
        if ($data = $redis->get($key)) {
            $tmp = json_decode($data, true);
            if (!$tmp) {
                $tmp = unserialize($data);
            }
            if (isset($tmp[$planId])) {
                return $tmp[$planId];
            }
        }

        return 0;
    }

    public function getCostData($userId)
    {
        $redis = Yii::app()->loader->limitRedis($userId);
        $key = $this->getKey($userId);
        if ($data = $redis->get($key)) {
            $tmp = json_decode($data, true);
            if (!$tmp) {
                $tmp = unserialize($data);
            }
            return $tmp;
        }

        return false;
    }

    public function setUserCost($userId, $amount)
    {
        $redis = Yii::app()->loader->limitRedis($userId);
        $dateTime = time();
        $key = $this->getKey($userId, $dateTime);
        if ($data = $redis->get($key)) {
            $tmp = json_decode($data, true);
            if (!$tmp) {
                $tmp = unserialize($data);
            }
            $data = $tmp;
        }
        else {
            $data = array('cost' => 0, 'uid' => $userId); 
        }
        $data['cost'] = $amount;

        $cacheTime = strtotime(date('Y-m-d 23:59:59')) - time() + 86400;
        $redis->setex($key, $cacheTime, json_encode($data));
    }

    public function setPlanCost($userId, $planId, $amount)
    {
        $redis = Yii::app()->loader->limitRedis($userId);
        $dateTime = time();
        $key = $this->getKey($userId, $dateTime);
        if ($data = $redis->get($key)) {
            $tmp = json_decode($data, true);
            if (!$tmp) {
                $tmp = unserialize($data);
            }
            $data = $tmp;
        }
        else {
            $data = array('cost' => 0, 'uid' => $userId, $planId => 0); 
        }
        if (!isset($data[$planId])) {
            $data[$planId] = 0;
        }
        $data[$planId] = $amount;

        $cacheTime = strtotime(date('Y-m-d 23:59:59')) - time() + 86400;
        $redis->setex($key, $cacheTime, json_encode($data));
    }

    //$dateTime 统计时间戳
    public function update($userId, $planId, $amount, $dateTime)
    {
        $this->offlineType = 0;
        $this->offlineLog = '';

        $redis = Yii::app()->loader->limitRedis($userId);
        $userInfo = $this->getUserData($userId);
        //if (!$this->lock($redis)) return false;

        $key = $this->getKey($userId, $dateTime);
        if ($data = $redis->get($key)) {
            $tmp = json_decode($data, true);
            if (!$tmp) {
                $tmp = unserialize($data);
            }
            $data = $tmp;
        }
        else {
            $data = array('cost' => 0, 'uid' => $userId, $planId => 0); 
        }

        if (!isset($data[$planId])) {
            $data[$planId] = 0;
        }
        $this->offlineLog = array(
            'cost' => $data,
            'info' => $userInfo
            );
        if ($userInfo) {
            if (!isset($userInfo[$planId])) {
                $this->offlineType = 3;
                return false;
            }
            $cost = number_format($data['cost'], 2, '.', '');
            if ($cost == $userInfo['balance']) {
                $this->offlineType = 1;
                return false;
            }
            if ($userInfo['quota'] > 0 && $cost == $userInfo['quota']) {
                $this->offlineType = 2;
                return false;
            }
            $cost = number_format($data[$planId], 2, '.', '');
            if ($userInfo[$planId] > 0 && $cost == $userInfo[$planId]) {
                $this->offlineType = 3;
                return false;
            }
        } else {
            $this->offlineType = -1;
            return false;
        }
        $amount = $this->getMinPrice($amount, $data['cost'], $data[$planId], $userInfo['balance'], $userInfo['quota'], $userInfo[$planId]);
        if ($amount == 0) return false;

        $data['cost'] += $amount;
        $data[$planId] += $amount;

        $cacheTime = strtotime(date('Y-m-d 23:59:59')) - time() + 86400;
        try {
            $redis->setex($key, $cacheTime, json_encode($data));
        }
        catch(Exception $e) {
            $this->offlineType = 0;
            return false;
        }
        //$this->unlock($redis);

        return $amount;
    }

    public function getMinPrice($cost, $userCost, $planCost, $balance, $userQuota, $planQuota)
    {
        $newUserCost = number_format($userCost+$cost,2,'.','');
        $price = $cost;
        //初始化下线类别以及下线时每种类别对应的金额 jingguangwen 2014-03-05 add
        $offline_type = 0;
        $compare_price = $user_quota_price = $plan_quota_price = 0;
        
        if ($balance <= $newUserCost) {
            $tmp = number_format($balance-$userCost,2,'.',''); 
            if ($tmp < $price)  $price = $tmp;
            $this->offlineType = 1;
            if ($price <= 0) {
                return 0;
            }
            //超过用户余额了 初始化
            $compare_price = $price;
            $offline_type = 1;
            
        }
        if ($userQuota > 0 && $userQuota <= $newUserCost) {
            if ($newUserCost > round($userQuota * 1.1, 2)) {
                $price = round($userQuota * 1.1 - $userCost, 2);
            } else {
                $price = $cost;
            }
            $this->offlineType = 2;
            if ($price <= 0) {
                return 0;
            }
            //超过用户限额，再与超过用户余额时的金额比较，取最小值的
            if (!empty($offline_type)) {
                $user_quota_price = min($compare_price,$price);
                if (round($user_quota_price-$compare_price,2) != 0) {
                    $compare_price = $user_quota_price;
                    $offline_type = 2;
                }
            } else {//初始化
                $compare_price = $price;
                $offline_type = 2;
            }
        }

        $newPlanCost = number_format($planCost+$cost,2,'.','');
        if ($planQuota > 0 && $planQuota <= $newPlanCost) {
            $tmp = number_format($planQuota - $planCost, 2, '.', '');
            if ($tmp < $price)  $price = $tmp;
            $this->offlineType = 3;
            if ($price <= 0) {
                return 0;
            }
            //超过计划限额，再与之前超过用户余额或者用户限额时的金额比较，取最小值的
            if (!empty($offline_type)) {
                $plan_quota_price = min($compare_price,$price);
                if (round($plan_quota_price-$compare_price,2) != 0) {
                    $compare_price = $plan_quota_price;
                    $offline_type = 3;
                }
            } else {//初始化
                $compare_price = $price;
                $offline_type = 3;
            }
            
        }
        //如果超过限额，需要对应的下线
        if(!empty($offline_type)){
            $this->offlineType = $offline_type;
            $price = $compare_price;
        }
        return round($price,2);
    }

    public function getKey($userId, $date = false)
    {
        if ($date === false) {
            $cur_time = time();
        }
        else {
            $cur_time = $date;
        }
        $day = date('d', $cur_time);
        return $this->prefixKey . "cost:{$day}-{$userId}";
    }


    public function lock($redis)
    {
        return true;
        $locked = 0;
        $key = $this->prefixKey . 'lock:';
        $count = 0;
        while ($locked != 1) {
            $count++;
            $time = microtime(true);
            $locked = $redis->setnx($key, $time + 1);
            if ($locked == 1 || ($time > $redis->get($key) && $time > $redis->getset($key, $time + 1))) {
                break;
            }
            else {
                if ($count >= 200) return false;
                usleep(500); //5ms
            }
        }
    
        return true;
    }

    public function unlock($redis)
    {
        $key = $this->prefixKey . 'lock:';
        $time = microtime(true);
        if ($time < $redis->get($key)) {
            $redis->del($key);
        }
    }

    //以下方法为临时作为作弊流量退消费款用 如果实现高效读写锁时可删除，
    //cheatCheck 写入队列，adstastic 统计前 先做退钱处理
    public function refund($userId, $planId, $price, $dateTime)
    {
        $userInfo = $this->getUserData($userId);

        $redis = Yii::app()->loader->limitRedis($userId);
        $key = $this->getKey($userId, $dateTime);
        if ($data = $redis->get($key)) {
            $tmp = json_decode($data, true);
            if (!$tmp) {
                $tmp = unserialize($data);
            }
            $data = $tmp;
            if (!isset($data[$planId])) return;

            $data['cost'] -= min($price, $data['cost']);
            $data[$planId] -= min($price, $data[$planId]);

            $cacheTime = strtotime(date('Y-m-d 23:59:59')) - time() + 86400;
            $redis->setex($key, $cacheTime, json_encode($data));
        }
    }

    public function refundPush($userId, $planId, $price, $dateTime)
    {
        $data = array(
            'user_id' => $userId,
            'plan_id' => $planId,
            'price' => $price,
            'time' => $dateTime
            );
        $key = $this->prefixKey . 'refund:';
        $redis = Yii::app()->loader->limitRedis(0);
        return $redis->rPush($key, json_encode($data));
    }

    public function refundPop()
    {
        $key = $this->prefixKey . 'refund:';
        $redis = Yii::app()->loader->limitRedis(0);

        return $redis->lPop($key);
    }

    public function getOfflineType()
    {
        return $this->offlineType;
    }

    public function getOfflineLog()
    {
        return $this->offlineLog;
    }

    public function setOfflineType()
    {
        $this->offlineType = 0;
    }

    public function isSettledSuccess()
    {
        $prefixKey = Config::item('redisKey') . 'strategy:';
        $day = intval(date('d', strtotime('-1 day')));
        $key = $prefixKey . "adcost:" . $day;
        $redis = Yii::app()->redis_ad_guess;
        $res = $redis->get($key);
        if ($res == 'success') return true;

        return false;
    }
}
