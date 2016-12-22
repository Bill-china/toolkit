<?php
/**
 * ComAdStats
 *
 * 
 * @package open 360
 * @version v1
 * @copyright 2005-2011 360.CN All Rights Reserved.
 * @author wangguoqiang@360.cn 
 */
class ComAdStats
{
    protected $prefixKey;
    protected $redis;
    protected $serverId;

    public function __construct($sid = false)
    {
        if ($sid !== false) {
            //$this->redis = new ComRedis('tongji', $sid);
            $this->redis = Yii::app()->loader->statRedis($sid, true);
        }
        $this->serverId = $sid;
        $this->prefixKey = Config::item('redisKey') . 'stats:';
        //$this->prefixKey = Config::item('redisKey') . 'stats:quotaError';
        //设置database
        //$this->redis->select(0); // 缓存数据库
    }

    /**
     * push 
     * 广告统计数据入队列
     * 
     * @param mixed $arr 
     * @return void
     */
    public function push($arr, $no = 0)
    {
        $key = $this->prefixKey;
        if (YII_DEBUG) {
            Yii::log('AdStats:Push:' . print_r($arr, true));
        }

        if ($this->serverId === false) {
            $this->redis = Yii::app()->loader->statRedis($no);
        }
        return $this->redis->rPush($key, json_encode($arr));
    }

    /**
     * pop 
     * 广告统计数据出队列
     * 
     * @return void
     */
    public function pop()
    {
        $key = $this->prefixKey;
        if (YII_DEBUG) {
            Yii::log('AdStats:Pop:' . $key);
        }
        if ($this->serverId === false) {
            $this->redis = Yii::app()->redis;
        }

        return $this->redis->lPop($key);
    }
    
    public function popMulti($num)
    {
        $key = $this->prefixKey;
        try {
            if ($this->serverId === false) {
                $this->redis = Yii::app()->redis;
            }

            if ($num < 0) $num = 1;
            $multi = $this->redis->multi();
            if (!$multi)
                throw new Exception("connect redis error!");
            for($i=0; $i<$num; $i++) {
                $multi->lPop($key);
            }
            return $multi->exec();
        } catch (Exception $e) {
            return false;
        }
    }
}
