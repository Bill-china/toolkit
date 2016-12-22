<?php

/**
 * ComAdStrategy
 * 广告策略类
 *
 * @package open 360
 * @version v1
 * @copyright 2005-2011 360.CN All Rights Reserved.
 * @author wangguoqiang@360.cn
 */
class ComAdStrategy
{

    public $prefixKey;
    public $mcKey; //机房代码

    protected $redis;


    public function __construct()
    {
        $this->redis = Yii::app()->redis_ad_guess;
        $this->prefixKey = Config::item('redisKey') . 'strategy:';
    }

    public function cronPushPlaceDefault()
    {
        $mPlace = new AdPlace();
        $rows = $mPlace->getDefaultAdList();
        if (!$rows) return ;
        foreach($rows as $row) {
            if (!$row['image_url']) continue;
            $this->addDefaultService($row);
        }
    }

    public function addDefaultService($data)
    {
        $key = $this->prefixKey . "place:" . $data['id_hash'];
        $cacheTime = strtotime(date('Y-m-d') . " 23:59:59") - time() + 180;
        return $this->redis->setex($key, $cacheTime, serialize($data));
    }

    public function setUnSettlement()
    {
        $day = intval(date('d', strtotime('-1 day')));
        $key = $this->prefixKey . "adcost:" . $day;
        if ($this->redis->get($key)) return true;
        $this->redis->setex($key, 86400*2, 'fail');
    }

    public function setSettlement()
    {
        $day = intval(date('d', strtotime('-1 day')));
        $key = $this->prefixKey . "adcost:" . $day;
        $this->redis->setex($key, 86400*2, 'success');
    }

    public function settled()
    {
        $day = intval(date('d', strtotime('-1 day')));
        $key = $this->prefixKey . "adcost:" . $day;
        $res = $this->redis->get($key);
        if ($res == 'fail') {
            return false;
        }
        return true;
    }

    /**
     * 判断昨天的用户广告消费是否已经完成结算
     *
     * @return boolean
     */
    public function isSettledSuccess()
    {
        $day = intval(date('d', strtotime('-1 day')));
        $key = $this->prefixKey . "adcost:" . $day;
        $res = $this->redis->get($key);
        if ($res == 'success') return true;

        return false;
    }

    public function updateChannelPlace()
    {
        $mPlace = new AdPlace();
        $placeRows = $mPlace->getAllPlace();

        if (!is_array($placeRows)) {
            return false;
        }
        $mImageSize = new EdcImageSize();
        $sizeRows = $mImageSize->getList();
        foreach($sizeRows as $row) {
            $sizeRows[$row['width'] . 'x' . $row['height']] = $row;
        }
        $key = Config::item('redisKey') . 'place:';
        foreach($placeRows as $row) {
            $row['pid'] = '';
            if (isset($sizeRows[$row['width'] . 'x' . $row['height']])) {
                $row['pid'] = 'img_' . $sizeRows[$row['width'] . 'x' . $row['height']]['id'];
            }
            $this->redis->setex($key . $row['id_hash'], 86000, serialize($row));
        }
        return true;
    }
}
