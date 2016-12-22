<?php
/**
 * 在一个计费周期内存储用户的消费记录
 * 即某个用户的有哪些计划有消费
 * 计费周期结束后，将这些信息计入redis
 */
class ComConsume {

    // 保存实时消费信息 uid => array( pid list)
    protected static $_info = array();

    // redis 使用 phpredis 扩展
    // 参见 https://github.com/nicolasff/phpredis
    protected static $_consume_redis            = null;
    protected static $_consume_is_connected     = false;
    protected static $_consume_redis_conf       = array();


    public static function addInfo ($uid, $pid) {
        $uid = intval($uid);
        $pid = intval($pid);
        if (isset(self::$_info[$uid])) {
            self::$_info[$uid][] = $pid;
        } else {
            self::$_info[$uid] = array(
                $pid,
            );
        }
        return true;
    }

    public static function save() {
        if (empty(self::$_info)) {
            return true;
        }
        try {
            $redis = self::_getRedis();
            foreach (self::$_info as $uid => $pids) {
                // redis 里存储的格式为
                // {"u":1000,"p":[109901,109902,109903,109904,109905,109906,109907,109908,109909,109910,109911,109912,109913,109914,109915,109916,109917,109918,109919,109920,109921,109922,109923,109924,109925,109926,109927,109928,109929,109930,109931,109932,109933,109934,109935,109936,109937,109938,109939,109940,109941,109942,109943,109944,109945,109946,109947,109948,109949,109950,109951,109952,109953,109954,109955,109956,109957,109958,109959,109960,109961,109962,109963,109964,109965,109966,109967,109968,109969,109970,109971,109972,109973,109974,109975,109976,109977,109978,109979,109980,109981,109982,109983,109984,109985,109986,109987,109988,109989,109990,109991,109992,109993,109994,109995,109996,109997,109998,109999,110000]}
                $arrMsg = array(
                    'u' => $uid,
                    'p' => array_unique($pids),
                );
                $redis->lpush('open_ad_v1:quota:consume', json_encode($arrMsg));
            }
        } catch (Exception $e) {
            echo $e->getMessage()."\n";
            return false;
        }
        return true;
    }

    public static function pop($limit) {
        try {
            $redis = self::_getRedis();
        } catch (Exception $e) {
            echo $e->getMessage()."\n";
            return false;
        }

        $limit = intval($limit);
        $arrRet = array();
        $i = 0;
        while (true && $i<$limit) {
            $_ret = $redis->rpop('open_ad_v1:quota:consume');
            if ($_ret===false) {
                break;
            }
            $arrRet[] = $_ret;
            $i++;
        }
        return $arrRet;
    }

    protected static function _getRedis () {
        if (is_null(self::$_consume_redis)) {
            self::$_consume_redis = new Redis();
        }
        if (self::$_consume_is_connected===false || self::$_consume_redis->ping()!=='+PONG') {
            if (empty(self::$_consume_redis_conf)) {
                $config = new CConfiguration(Yii::getPathOfAlias('application.config') . '/redis.php');
                self::$_consume_redis_conf = $config->itemAt('consume');
            }
            if (!is_array(self::$_consume_redis_conf)
                || !isset(self::$_consume_redis_conf['host'])
                || !isset(self::$_consume_redis_conf['port'])
            ) {
                throw new Exception('can not get redis config of consume');
            }
            if (isset(self::$_consume_redis_conf['timeout']) && self::$_consume_redis_conf['timeout']==true) {
                $ret = self::$_consume_redis->connect(self::$_consume_redis_conf['host'], self::$_consume_redis_conf['port'], true);
            } else {
                $ret = self::$_consume_redis->connect(self::$_consume_redis_conf['host'], self::$_consume_redis_conf['port']);
            }
            if ($ret === false) {
                $msg = sprintf("can not connet redis of consume, host[%s], port[%s]",
                    self::$_consume_redis_conf['host'],
                    self::$_consume_redis_conf['port']
                );
                throw new Exception($msg);
            }
            self::$_consume_is_connected = true;
        }
        return self::$_consume_redis;
    }

    public static function addDataToRedis ($uid, $pid, $cid=0, $amount =0) {
        $uid = intval($uid);
        $pid = intval($pid);
        $cid = intval($cid);
        $amount = (float)$amount;

        $redis = self::_getRedis();

        $arrMsg = array(
            'u' => $uid,
            'p' => array($pid),
            't' => time(),
            'cid' => $cid,
            'clickprice' => $amount,
        );
        $redis->lpush('open_ad_v1:quota:consume', json_encode($arrMsg));
        return true;
    }
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */