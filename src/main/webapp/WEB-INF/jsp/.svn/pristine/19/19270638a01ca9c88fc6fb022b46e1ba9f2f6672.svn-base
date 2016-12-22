<?php
/**
 * ComAdStatistic
 * 广告浏览/点击统计
 *
 * @package open 360
 * @version v1
 * @copyright 2005-2011 360.CN All Rights Reserved.
 * @author wangguoqiang@360.cn
 */
class ComAdStatistic
{
    protected $prefixKey;
    protected $redis;
    protected $serverId;
    protected $memCache;
    protected $comQuota;
    protected $nowTime;
    protected $nowHour;
    protected $nowMinute;
    protected $nowDate;
    protected $collectArr;
    protected $type; //type=view or click

    public function __construct($sid, $type='view')
    {
        $this->comQuota = Yii::app()->loader->component('ComAdQuota');
        $this->redis = Yii::app()->loader->statRedis($sid, true);
        $this->serverId = $sid;
        $this->prefixKey = Config::item('redisKey') . 'ad_statistic:';

        $this->memCache = array();
        $this->nowTime = time();
        $this->nowHour = date('H', $this->nowTime);
        $this->nowMinute = date('i', $this->nowTime);
        $this->nowDate = date('ymd', $this->nowTime);
        $this->collectArr = array();
        $this->type = $type;
    }

    public function update(&$arr, $mInter)
    {
        $aid = $arr['aid']; //广告ID
        $type = $arr['type']; //统计类型 点击/展示
        $cur_time = $this->nowTime;
        $price = 0;
        $adDate = isset($arr['now']) ? date('ymd', $arr['now']) : date('ymd', $arr['view_time']);
        $adHour = isset($arr['now']) ? date('H', $arr['now']) : date('H', $arr['view_time']);
        if (!isset($arr['channel_id'])) $arr['channel_id'] = 0;
        if (!isset($arr['place_id'])) $arr['place_id'] = 0;
        $key = $this->prefixKey . $adDate . $adHour . "_" . date('H', $cur_time) . "-{$mInter}-{$aid}-{$arr['channel_id']}-{$arr['place_id']}";

        $logData = $arr;
        $logData['key'] = $key;
        $logData['clickPrice'] = 0;

        //如果0点15分以后还有昨天的数据 则需要停止自动结算
        if ($this->nowHour  == 0
            && $this->nowDate != $adDate
            && $this->nowMinute > 30
           ) {
            $cStrategy = Yii::app()->loader->component('ComAdStrategy');
            $cStrategy->setUnSettlement();
        }
        if ($type == 'click') {
            $tmp_arr = array(
                'amount' => 0,
            );
            if ($price = $this->calCost($arr, $tmp_arr)) {
                $logData['clickPrice'] = $price;
                $this->writeQuotaFail($logData, $arr);
            } else {
                $this->writeQuotaFail($logData, $arr);
                return;
            }
            $comBineLog = date('YmdHis') . "\t" . "esc_click" . "\t" . json_encode($logData);
            ComAdLog::write($comBineLog, '/dev/shm/combineLog');
            ComAdLog::write($comBineLog, '/dev/shm/stats/statsLog');
        } else {
            $statsLog = date('YmdHis') . "\t" . "esc_view" . "\t" . json_encode($logData);
            ComAdLog::write($statsLog, '/dev/shm/stats/statsLog');
        }

    }

    /**
     * initData
     * 初始化数据
     *
     * @param mixed $arr
     * @param mixed $cur_time
     * @return void
     */
    private function initData($arr, $cur_time)
    {
        $version = 0;
        if (isset($arr['ver'])) $version = $arr['ver'];
        $dataTime = isset($arr['now']) ? $arr['now'] : $arr['view_time'];
        return array(
            'aid' => $arr['aid'],
            'pid' => $arr['pid'],
            'gid' => $arr['gid'],
            'uid' => $arr['uid'],
            'view' => 0,
            'click' => 0,
            'trans' => 0,
            'amount' => 0,
            'data_time' => $dataTime,
            'last_time' => $cur_time,
            'area' => array(),
            'inter' => array(),
            'kw' => array(),
            'saved' => array('stats' => 0,'area' => 0,'inter' => 0, 'kw' => 0),
            'errors' => array('stats' => 0,'area' => 0,'inter' => 0, 'kw' => 0),
            'channel_id' => isset($arr['channel_id'])? $arr['channel_id']:0,
            'place_id' => isset($arr['place_id'])? $arr['place_id']:0,
            'ver' => $version,
            );
    }

    /**
     * calCost
     * 计算费用 点击/展示
     *
     * @param mixed $arr
     * @param mixed $data
     * @return void
     */
    private function calCost(&$arr, &$data)
    {
        $viewTime = isset($arr['now']) ? $arr['now'] : $arr['view_time'];
        if ($price = $this->comQuota->update($arr['uid'], $arr['pid'], $arr['price'], $viewTime)) {
            $data['amount'] = round($data['amount'] + $price,2);
            return $price;
        }

        $logData = array(
            date('Y-m-d H:i:s'),
        );
        $logData = array_merge($logData, $arr);

        $comBineLog = date('YmdHis') . "\t" . "esc_quotaError" . "\t" . json_encode($logData);
        ComAdLog::write($comBineLog, '/dev/shm/combineLog');
        return false;
    }

    public function save($mInter = 0, $forceSave = false)
    {
        $second = intval(date('s'));
        if (YII_DEBUG) {
            Yii::log('ad save statistic:' . $second);
        }
        if ( $forceSave || $second >= 55 || ($second%10) == 0 ) {
            $dbQueue = Yii::app()->loader->component('ComDbQueue', $this->serverId);
            $dbQueue->setType($this->type);
            $data = $this->memCache;
            $this->memCache = array();
            foreach($data as $k => $v) {
                //$this->redis->setex($k, 86400, serialize($v)); //设置3 days
                $this->redis->setex($k, 7200, serialize($v)); //设置3 days
                //放入同步数据库队列
                $dbQueue->push($mInter, $k, 'ad');
            }
        }
    }

    public function saveToCollect($flagKey)
    {
        $collect = Yii::app()->loader->component('ComCollectQueue');
        //$collect->pushData($this->collectArr, $this->serverId);
        $collect->collectStatistic($this->collectArr, $flagKey);
    }

    public function updateToCollect($key, &$data) {
        if ($data = $this->redis->get($key)) {
            //这个地方本来就是往文件里写，不再记日志了
            //$logData = array(
                //date('Y-m-d H:i:s'),
                //$key,
            //);
            //ComAdLog::write($logData, 'statstocollect.log');
            return true;
        } else
            return false;
    }

    /**
     *
     * 更新统计数据到数据库
     *
     * @param mixed $key
     * @param mixed $arr
     * @param mixed $views
     * @param mixed $clicks
     * @param mixed $time
     * @return void
     *
     */
    public function updateToDb($key)
    {
        $db = Yii::app()->db_stats;
        if ($data = $this->redis->get($key)) {
            $data = unserialize($data);
            if (isset($data['saved']) && $data['saved']['stats'] == 1 && $data['saved']['area'] == 1) {
                if ((isset($data['saved']['inter']) && $data['saved']['inter'] == 1) || (isset($data['saved']['kw']) && $data['saved']['kw'] == 1)) {
                    return true;
                }
            }
            $row = array(
                'ad_advert_id' => $data['aid'],
                'ad_plan_id' => $data['pid'],
                'ad_group_id' => $data['gid'],
                'ad_user_id' => $data['uid'],
                'create_date' => date('Y-m-d', $data['data_time']),
                'last_update_time' => $data['last_time'],
                'views' => intval($data['view']),
                'clicks' => intval($data['click']),
                'trans' => isset($data['trans'])? intval($data['trans']):0,
                'total_cost' => round(floatval($data['amount']), 2),
                'ad_channel_id' => intval($data['channel_id']),
                'data_source' => 0,
                'admin_user_id' => 0,
                'status' => 0,
                'ad_place_id' => intval($data['place_id']),
                'create_time' => date('Y-m-d H:i:s'),
                'update_time' => date('Y-m-d H:i:s'),
            );
            if ('click' == $this->type)
                $row['type'] = 1;
            elseif ('view' == $this->type)
                $row['type'] = 2;
            else
                $row['type'] = 0;

            //展示切换新统计
            if ($row['clicks'] == 0) return true;

            $logData = array(
                date('Y-m-d H:i:s'),
                $key,
                );
            $logData = array_merge($logData, $row);

            /*
            if (($row['create_date'] == date("Y-m-d", strtotime("-1 day"))) && date("H") >= 2) {
                $data['key'] = $key;
                ComAdLog::write(serialize($data), 'statstodbFail.log');
                return true;
            }
            */

            if ($row['views'] > 0 || $row['clicks'] > 0) {
                if (!$this->verifyData($key, $row)) {
                    return true;
                }
                $stats = new EdcStats();
                $res = 0;
                if ($data['saved']['stats'] == 1) {
                    return true;
                }
                $trans = $db->beginTransaction();
                try {
                    $res = $stats->addIncr($row);
                    if ($res != -1) {
                        if (isset($data['area'])) {
                            $this->updateArea($data, $key);
                        }
                        if (isset($data['inter'])) {
                            $this->updateInterest($data, $key);
                        }
                        if (isset($data['kw'])) {
                            $this->updateKeyword($data, $key);
                        }
                    }
                    $data['saved']['stats'] = 1;
                    //$this->redis->setex($key, 86400, serialize($data));
                    $this->redis->setex($key, 7200, serialize($data));
                    ComAdLog::write($logData, 'statstodb');
                    $trans->commit();
                } catch (Exception $e) {
                    var_dump($e->getMessage());
                    $trans->rollback();
                    if (!isset($data['retry'])) {
                        $data['retry'] = 1;
                    }
                    else {
                        $data['retry'] += 1;
                    }
                    $data['errors']['stats'] = $e->getMessage();
                }

                if (isset($data['retry']) && $data['retry'] <= 5) {
                    return false;
                }
                if ($data['saved']['stats'] == 0) {
                    ComAdLog::write($logData, 'statstodbError');
                }
            }
        }
        return true; //删除该键值
    }

    /**
     * updateToDb
     * 更新统计数据到数据库
     *
     * @param mixed $key
     * @param mixed $arr
     * @param mixed $views
     * @param mixed $clicks
     * @param mixed $time
     * @return void
     */
    public function collectToDb($arr)
    {
        $stats = new AdStats();
        $res = 0;
        $trans = Yii::app()->db_center->beginTransaction();
        try {
            $res = $stats->addIncr($row);
            if ($res != -1) {
                //if (isset($data['area'])) {
                    //$this->updateArea($data, $key);
                //}
                //if (isset($data['inter'])) {
                    //$this->updateInterest($data, $key);
                //}
                //if (isset($data['kw'])) {
                    //$this->updateKeyword($data, $key);
                //}
            }
            $trans->commit();
        }
        catch(Exception $e) {
            $trans->rollback();
            //记日志
            return false;
        }

        return true; //删除该键值
    }

    /**
     * verifyData
     * 保存到数据库时做数据验证
     *
     * @param mixed $key
     * @param mixed $row
     * @return void
     */
    private function verifyData($key, $row)
    {
        if (!isset($row['ad_advert_id']) || $row['ad_advert_id'] == 0) {
           return false;
        }
        if (!isset($row['ad_plan_id']) || $row['ad_plan_id'] == 0) {
           return false;
        }
        if (!isset($row['ad_group_id']) || $row['ad_group_id'] == 0) {
           return false;
        }
        if (!isset($row['ad_user_id']) || $row['ad_user_id'] == 0) {
           return false;
        }
        return true;
    }

    public function updateArea($data, $key)
    {
        $areaStats = new EdcStatsArea();
        foreach($data['area'] as $k => $value) {
            $list = explode(',', $k);
            $row = array(
                'ad_group_id' => $data['gid'],
                'ad_user_id' => $data['uid'],
                'ad_plan_id' => $data['pid'],
                'area_key' => $k,
                'area_fid' => 0,
                'area_id' => 0,
                'clicks' => isset($value['click']) ? $value['click'] : 0,
                'views' => isset($value['view']) ? $value['view'] : 0,
                'trans' => isset($data['trans']) ? intval($data['trans']) : 0,
                'costs' => isset($value['cost']) ? round($value['cost'], 2) : 0,
                'create_date' => date('Y-m-d', $data['data_time'] ? $data['data_time'] : $data['last_time']),
                'create_time' => date('Y-m-d H:i:s'),
                'update_time' => date('Y-m-d H:i:s'),
            );
            if ('click' == $this->type)
                $row['type'] = 1;
            elseif ('view' == $this->type)
                $row['type'] = 2;
            else
                $row['type'] = 0;
            if (count($list) == 2) {
                if ($list[0] == 10001 && $list[1] == 10001) {
                    $row['area_fid'] = 0;
                    $row['area_id'] = 0;
                } elseif ($list[0] != 10001 && $list[1] == 10001) {
                    $row['area_fid'] = $list[0];
                    $row['area_id'] = 0;
                } else {
                    $row['area_fid'] = $list[0];
                    $row['area_id'] = $list[1];
                }
            }
            $res = $areaStats->updateIncr($row);

            if (!$res) {
                ComAdLog::write($row, Yii::app()->runtimePath . '/area_db_' . date('ymd') . '.log');
            }
            //$logData = array(
                //date('Y-m-d H:i:s'),
                //$key,
                //);
            //$logData = array_merge($logData, $row);
            //ComAdLog::write($logData, 'areatodb.log');
        }
    }

    public function updateInterest($data, $key)
    {
        $interStats = new EdcStatsInterest();
        foreach($data['inter'] as $k => $value) {
            $row = array(
                'ad_group_id' => $data['gid'],
                'ad_user_id' => $data['uid'],
                'ad_plan_id' => $data['pid'],
                'inter_id' => $k,
                'clicks' => isset($value['click'])? $value['click']:0,
                'views' => isset($value['view'])? $value['view']:0,
                'costs' => isset($value['cost'])? round($value['cost'], 2):0,
                'trans' => isset($data['trans'])? intval($data['trans']):0,
                'create_date' => date('Y-m-d', $data['data_time']? $data['data_time']:$data['last_time']),
                'create_time' => date('Y-m-d H:i:s'),
                'update_time' => date('Y-m-d H:i:s'),
            );
            if ('click' == $this->type)
                $row['type'] = 1;
            elseif ('view' == $this->type)
                $row['type'] = 2;
            else
                $row['type'] = 0;
            if (!$interStats->updateIncr($row)) {
                ComAdLog::write($row, Yii::app()->runtimePath . '/interest_db_' . date('ymd') . '.log');
            }
            //$logData = array(
                //date('Y-m-d H:i:s'),
                //$key,
                //);
            //$logData = array_merge($logData, $row);
            //ComAdLog::write($logData, 'interesttodb.log');
        }
    }

    public function updateKeyword($data, $key)
    {
        $mKeyword = new EdcStatsKeyword();
        foreach($data['kw'] as $k => $value) {
            $row = array(
                'ad_group_id' => $data['gid'],
                'ad_user_id' => $data['uid'],
                'ad_plan_id' => $data['pid'],
                'keyword' => $k,
                'clicks' => isset($value['click'])? $value['click']:0,
                'views' => isset($value['view'])? $value['view']:0,
                'costs' => isset($value['cost'])? round($value['cost'],2) : 0,
                'trans' => isset($data['trans'])? intval($data['trans']):0,
                'create_date' => date('Y-m-d', $data['data_time']? $data['data_time']:$data['last_time']),
                'create_time' => date('Y-m-d H:i:s'),
                'update_time' => date('Y-m-d H:i:s'),
            );
            if ('click' == $this->type)
                $row['type'] = 1;
            elseif ('view' == $this->type)
                $row['type'] = 2;
            else
                $row['type'] = 0;
            if (!$mKeyword->updateIncr($row)) {
                ComAdLog::write($row, Yii::app()->runtimePath . '/keyword_db_' . date('ymd') . '.log');
            }
            //$logData = array(
                //date('Y-m-d H:i:s'),
                //$key,
                //);
            //$logData = array_merge($logData, $row);
            //ComAdLog::write($logData, 'keywordtodb.log');
        }

    }

    public function areaStats(&$data, $type, $cityId, $price = 0)
    {
        if (!isset($data['area'])) {
            $data['area'] = array();
        }
        $res = $data['area'];
        if (!isset($res[$cityId][$type])) {
            $res[$cityId][$type] = 0;
            $res[$cityId]['cost'] = 0;
        }

        $res[$cityId][$type]++;
        $res[$cityId]['cost'] += $price;

        return $res;
    }

    public function interestStats(&$data, $type, $tagId, $price = 0)
    {
        if (!isset($data['inter'])) {
            $data['inter'] = array();
        }
        $res = $data['inter'];
        if (!isset($res[$tagId][$type])) {
            $res[$tagId][$type] = 0;
            $res[$tagId]['cost'] = 0;
        }

        $res[$tagId][$type]++;
        $res[$tagId]['cost'] += $price;

        return $res;
    }

    public function keywordStats(&$data, $type, $keyword, $price = 0)
    {
        if (!isset($data['kw'])) {
            $data['kw'] = array();
        }
        $res = $data['kw'];
        if (!isset($res[$keyword][$type])) {
            $res[$keyword][$type] = 0;
            $res[$keyword]['cost'] = 0;
        }

        $res[$keyword][$type]++;
        $res[$keyword]['cost'] += $price;

        return $res;

    }

    public function writeLog($data)
    {
        $data['logTime'] = date("Y-m-d H:i:s");
        //ComAdLog::write($data, 'statisticLog');
    }

    public function writeQuotaFail($data, &$arr)
    {
        if ($this->comQuota->getOfflineType() > 0) {
            $redis = Yii::app()->loader->redis('queue_offline');
            $key = "dj:offline:queue";
            $offlineData = array(
                'advert_id' => $data['aid'],
                'group_id' => $data['gid'],
                'plan_id' => $data['pid'],
                'user_id' => $data['uid'],
                'offline_type' => $this->comQuota->getOfflineType(),
                'ad_type' => $data['ver'],
                'time' => time(),
                'key' => $data['key'],
                'log' => $this->comQuota->getOfflineLog(),
                );
            if ($redis) {
                $redis->lPush($key, json_encode($offlineData));
            }
        } elseif ($this->comQuota->getOfflineType() == -1) {
            $redis = Yii::app()->loader->statRedis(0, true);
            $key = Config::item('redisKey') . 'stats:quotaError';
            if ($redis) {
                $redis->rPush($key, json_encode($arr));
            }
        }
    }
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
