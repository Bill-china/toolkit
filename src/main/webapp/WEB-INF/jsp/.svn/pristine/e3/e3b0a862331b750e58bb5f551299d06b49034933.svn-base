<?php
/**
 * Author: Kangle.Liu - liukangle@360.cn
 *
 * Last modified: 2013-08-12 17:40
 *
 * Filename: ComStatsCheckCost.php
 *
 * Description: 
 *
 */

class ComStatsCheckCost
{
    protected $limitRedis;
    protected $statsRedis;
    protected $lockFile;
    //过期时间为290秒
    protected $_timeout = 290;
    protected $date;

    public function __construct($date=false, $sid=0)
    {
        $this->lockFile = Config::item('statsLog') . "checkCost.lock";
        $this->statsRedis = Yii::app()->loader->statRedis($sid, true);
        //$redis = Yii::app()->loader->limitRedis($userId);
        if ($date)
            $this->date=$date;
        else
            $this->date=date('Y-m-d');
    }

    public function lock()
    {
        $wh = fopen($this->lockFile, 'w');
        fwrite($wh, time());
        fclose($wh);
        if (file_exists($this->lockFile))
            return true;
        else
            return false;
    }

    public function unLock()
    {
        @unlink($this->lockFile);
        return ;
    }

    public function checkLock()
    {
        if (file_exists($this->lockFile)) {
            $content = file_get_contents($this->lockFile);
            if (time() - $content > $this->_timeout) {
                $this->unLock();
                return false;
            } else
                return true;

        } else
            return false;
    }

    public function isEmptyDbQueue()
    {
        $keys = "*db_queure*";
        if ($this->statsRedis->keys($keys))
            return false;
        else
            return true;
    }

    public function getDbData()
    {
        $sql = "select ad_user_id, sum(total_cost) as sum_costs from " . EdcStats::model()->tableName()
            . " where create_date='{$this->date}' group by ad_user_id";
        return EdcStats::model()->getDbConnection()->createCommand($sql)->queryAll();
    }

    public function getRedisCostKey($uid)
    {
        return Config::item('redisKey') . 'quota:cost:' . date('d', strtotime($this->date)) . '-' . $uid;
    }

    public function getRedisDataByUid($uid)
    {
        $redis = Yii::app()->loader->limitRedis($uid);
        $key = $this->getRedisCostKey($uid);
        return $redis->get($key);
    }
}
