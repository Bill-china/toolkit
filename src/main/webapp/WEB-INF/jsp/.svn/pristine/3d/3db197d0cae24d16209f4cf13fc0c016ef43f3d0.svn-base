<?php
/**
 * ComAdQuotanew
 * 广告限额类 添加mediav后使用
 */
class ComAdQuotanew {


    public static function getUserDJCostKey ($day, $userID) {
        // open_ad_v1:quota:cost:28-28715380
        return sprintf('open_ad_v1:quota:cost:%02d-%d', $day, $userID);
    }

    public static function getUserDJQuotaKey ($userID) {
        // open_ad_v1:quota:user-info:28715380
        return sprintf('open_ad_v1:quota:user-info:%d', $userID);
    }

    public static function getUserQuotaKey ($userID) {
        // user-quota:28715380
        return sprintf('user-quota:%d', $userID);
    }

    public static function getUserMediavQuotaKey ($userID) {
        // mediav:quota:user-info:28715380
        return sprintf('mediav:quota:user-info:%d', $userID);
    }

    public static function getUserMediavCostKey ($day, $userID) {
        // mediav:quota:user-info:28715380
        return sprintf('mediav:quota:cost:%02d-%d', $day, $userID);
    }


    public static function setUserQuota($userID, $totalQuota, $djQuota, $mediavQuota) {
        $log = sprintf('%s setUserQuota, uid[%d] total[%f], dj[%f], mediav[%f]', date('Y-m-d H:i:s'), $userID, $totalQuota, $djQuota, $mediavQuota);
        ComAdLog::write($log, 'msg_' . date('ymd').'.log');
        $redis = Yii::app()->loader->limitRedis($userID);

        // 设置总预算
        $cacheTime = strtotime(date('Y-m-d H:59:59')) - time() +900;
        $data = array(
            'total'         => (float)$totalQuota,
            'dianjing'      => (float)$djQuota,
            'mediav'        => (float)$mediavQuota,
        );
        $key = self::getUserQuotaKey($userID);
        $_ret = $redis->setex($key, $cacheTime, json_encode($data));

        return $_ret && self::setDJUserQuota($userID, $djQuota) && self::setMediavQuota($userID, $mediavQuota);
    }

    // 设置点睛预算
    public static function setDJUserQuota ($userID, $djQuota) {
        $log = sprintf('%s setDJUserQuota, uid[%d] dj[%f]', date('Y-m-d H:i:s'), $userID, $djQuota);
        ComAdLog::write($log, 'msg_' . date('ymd').'.log');
        $redis = Yii::app()->loader->limitRedis($userID);

        $cacheTime = strtotime(date('Y-m-d H:59:59')) - time() +900;
        $key = self::getUserDJQuotaKey($userID);
        if ($data = $redis->get($key) ) {
            $data = json_decode($data, true);
        }
        if (empty($data)) {
            $data = array(
                'quota' => (float) $djQuota,
            );
        } else {
            $data['quota'] = (float) $djQuota;
        }
        return $redis->setex($key, $cacheTime, json_encode($data));
    }

    // 删除点睛预算
    public static function delDJUserQuota ($userID) {
        $redis = Yii::app()->loader->limitRedis($userID);
        $key = self::getUserDJQuotaKey($userID);
        return $redis->delete($key);
    }

    // 设置mediav预算
    public static function setMediavQuota ($userID, $mediavQuota) {
        $log = sprintf('%s setMediavQuota, uid[%d] mediav[%f]', date('Y-m-d H:i:s'), $userID, $mediavQuota);
        ComAdLog::write($log, 'msg_' . date('ymd').'.log');
        $redis = Yii::app()->loader->limitRedis($userID);

        $cacheTime = strtotime(date('Y-m-d H:59:59')) - time() +900;
        $key = self::getUserMediavQuotaKey($userID);
        if ($data = $redis->get($key) ) {
            $data = json_decode($data, true);
        }
        if (empty($data)) {
            $data = array(
                'quota' => (float) $mediavQuota,
            );
        } else {
            $data['quota'] = (float) $mediavQuota;
        }
        return $redis->setex($key, $cacheTime, json_encode($data));
    }

    // 删除mediav预算
    public static function delMediavQuota ($userID) {
        $redis = Yii::app()->loader->limitRedis($userID);
        $key = self::getUserMediavQuotaKey($userID);
        return $redis->delete($key);
    }

    // 获取用户余额信息
    public static function getUserDJQuotaInfo ($userID) {
        $redis = Yii::app()->loader->limitRedis($userID);
        $key = self::getUserDJQuotaKey($userID);
        $data = $redis->get($key);
        if (!empty($data)) {
            $data = json_decode($data, true);
        }
        return is_array($data) ? $data : array();
    }

    // 获取用户点睛消费信息
    public static function getUserDJCostInfo ($day, $userID) {
        $key = self::getUserDJCostKey($day, $userID);
        $redis = Yii::app()->loader->limitRedis($userID);
        $data = $redis->get($key);
        if (!empty($data)) {
            $data = json_decode($data, true);
        }
        return is_array($data) ? $data : array();
    }

    // 获取用户mediav消费信息
    public static function getUserMediavCostInfo ($day, $userID) {
        $key = self::getUserMediavCostKey($day, $userID);
        $redis = Yii::app()->loader->limitRedis($userID);
        $data = $redis->get($key);
        if (!empty($data)) {
            $data = json_decode($data, true);
        }
        return is_array($data) ? $data : array();
    }

    public static function setUserMediavCostInfo ($day, $userID, $data) {
        $key = self::getUserMediavCostKey($day, $userID);
        $redis = Yii::app()->loader->limitRedis($userID);
        $cacheTime = strtotime(date('Y-m-d 23:59:59')) - time() + 86400;
        return $redis->setex($key, $cacheTime, json_encode($data));
    }

    // 获取用户mediav预算信息
    public static function getUserMediavQuotaInfo ($userID) {
        $key = self::getUserMediavQuotaKey($userID);
        $redis = Yii::app()->loader->limitRedis($userID);
        $data = $redis->get($key);
        if (!empty($data)) {
            $data = json_decode($data, true);
        }
        return is_array($data) ? $data : array();
    }
    // 获取用户实时余额信息
    public static function getUserCurBalance($userID) {
        $redis = Yii::app()->loader->limitRedis($userID);

        $key = self::getUserDJQuotaKey($userID);
        $djQuotaInfo = $redis->get($key);
        if (!empty($djQuotaInfo)) {
            $djQuotaInfo = json_decode($djQuotaInfo, true);
        }
        $balance = isset($djQuotaInfo['balance']) ? $djQuotaInfo['balance'] : 0;

        $key = self::getUserMediavCostKey(date('j'), $userID);
        $mediavCostInfo = $redis->get($key);
        if (!empty($mediavCostInfo)) {
            $mediavCostInfo = json_decode($mediavCostInfo, true);
        }
        $mediavCost = isset($mediavCostInfo['cost']) ? $mediavCostInfo['cost'] : 0;

        $key = self::getUserDJCostKey(date('j'), $userID);
        $djCostInfo = $redis->get($key);
        if (!empty($djCostInfo)) {
            $djCostInfo = json_decode($djCostInfo, true);
        }
        $djCost = isset($djCostInfo['cost']) ? $djCostInfo['cost'] : 0;

        $curBalance = $balance - $mediavCost - $djCost;
        return $curBalance>=0 ? $curBalance : 0;
    }

    public static function getMediavYesterdayCost ($userID) {
        $redis = Yii::app()->loader->limitRedis($userID);
        $yesterday = date('j', strtotime('-1 day'));

        $key = self::getUserMediavCostKey($yesterday, $userID);
        $tmp = array();
        if ($data = $redis->get($key)) {
            $tmp = json_decode($data, true);
        }
        $mediavCost = isset($tmp['cost']) ? $tmp['cost'] : 0;
        return $mediavCost;
    }

    // 获取用户昨天的余额信息
    public static function getYestodayUserCost ($userID) {
        $redis = Yii::app()->loader->limitRedis($userID);
        $yesterday = date('j', strtotime('-1 day'));

        $key = self::getUserDJCostKey($yesterday, $userID);
        $tmp = array();
        if ($data = $redis->get($key)) {
            $tmp = json_decode($data, true);
        }
        $djCost = isset($tmp['cost']) ? $tmp['cost'] : 0;

        $key = self::getUserMediavCostKey($yesterday, $userID);
        $tmp = array();
        if ($data = $redis->get($key)) {
            $tmp = json_decode($data, true);
        }
        $mediavCost = isset($tmp['cost']) ? $tmp['cost'] : 0;
        return $djCost + $mediavCost;
    }

    // 设置总预算
    public function setUserQuotaTotal ($userID, $totalQuota, $djQuota, $mediavQuota) {
        $log = sprintf('%s setUserQuota, uid[%d] total[%f], dj[%f], mediav[%f]', date('Y-m-d H:i:s'), $userID, $totalQuota, $djQuota, $mediavQuota);
        ComAdLog::write($log, 'msg_' . date('ymd').'.log');
        $redis = Yii::app()->loader->limitRedis($userID);

        // 设置总预算
        $cacheTime = strtotime(date('Y-m-d H:59:59')) - time() +900;
        $data = array(
            'total'         => (float)$totalQuota,
            'dianjing'      => (float)$djQuota,
            'mediav'        => (float)$mediavQuota,
        );
        $key = self::getUserQuotaKey($userID);
        return $redis->setex($key, $cacheTime, json_encode($data));
    }

    // 设置点睛预算 用户&计划
    public static function setDJUserQuotaInfo ($userID, $data) {
        $log = sprintf('%s setDJUserQuotainfo, uid[%d] quota[%f] balance[%f]', date('Y-m-d H:i:s'), $userID, $data['quota'], $data['balance']);
        ComAdLog::write($log, 'msg_' . date('ymd').'.log');
        $redis = Yii::app()->loader->limitRedis($userID);

        $cacheTime = strtotime(date('Y-m-d H:59:59')) - time() +900;
        $key = self::getUserDJQuotaKey($userID);
        return $redis->setex($key, $cacheTime, json_encode($data));
    }

    // 设置点睛预算 用户&计划
    public static function setMediavUserQuotaInfo ($userID, $data) {
        $log = sprintf('%s setMediavUserQuotainfo, uid[%d] quota[%f]', date('Y-m-d H:i:s'), $userID, $data['quota']);
        ComAdLog::write($log, 'msg_' . date('ymd').'.log');
        $redis = Yii::app()->loader->limitRedis($userID);

        $cacheTime = strtotime(date('Y-m-d H:59:59')) - time() +900;
        $key = self::getUserMediavQuotaKey($userID);
        return $redis->setex($key, $cacheTime, json_encode($data));
    }

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

}

