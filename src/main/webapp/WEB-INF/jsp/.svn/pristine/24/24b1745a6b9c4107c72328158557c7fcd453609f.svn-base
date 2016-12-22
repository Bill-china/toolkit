<?php
/**
 * Author: Kangle.Liu - liukangle@360.cn
 *
 * Last modified: 2013-09-24 14:49
 *
 * Filename: ComAdView.php
 *
 * Description: 
 *
 */
class ComAdView
{
    protected $redis = array();
    protected $prefixKey;
    protected $modelArr;
    public $num;
    public $count;

    public function __construct()
    {
        $this->prefixKey = Config::item('redisKey') . 'view:';
        $this->num = 0;
        $config = new CConfiguration(Yii::getPathOfAlias('application.config') . '/redis.php');
        $this->count = count($conf = $config->itemAt('view_todb_queue'));
        for ($i=0; $i<$this->count; $i++) {
            $this->redis[$i] = Yii::app()->loader->viewRedis($i);
        }
    }

    public function setModel()
    {
        $this->modelArr['stats'] = new EdcStats();
        $this->modelArr['statsArea'] = new EdcStatsArea();
        $this->modelArr['statsInterest'] = new EdcStatsInterest();
        $this->modelArr['statsKeyword'] = new EdcStatsKeyword();
    }

    public function push($data)
    {
        if (!isset($data['gid'])) {
            //todo log
            return ;
        }
        $id = $data['gid'] % $this->count;

        if ((time() - $data['data_time'] > 7200) && (date('Y-m-d') != date('Y-m-d', $data['data_time']))) {
            return ;
        }

        //stats
        $statsKey = $this->prefixKey . 'stats:';
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
            'total_cost' => floatval($data['amount']),
            'ad_channel_id' => intval($data['channel_id']),
            'ad_place_id' => intval($data['place_id']),
            'data_source' => 0,
            'admin_user_id' => 0,
            'status' => 0,
            'type' => EdcStats::TYPE_VIEW,
            'create_time' => date('Y-m-d H:i:s'),
            'update_time' => date('Y-m-d H:i:s'),
        );
        $this->redis[$id]->rpush($statsKey, serialize($row));
        $this->num++;

        //statsArea
        /*
        if (isset($data['area'])) {
            $statsAreaKey = $this->prefixKey . 'statsArea:';
            foreach($data['area'] as $k => $value) {
                $list = explode(',', $k);
                $row = array(
                    'ad_group_id' => $data['gid'],
                    'ad_user_id' => $data['uid'],
                    'ad_plan_id' => $data['pid'],
                    'area_key' => addslashes($k),
                    'clicks' => isset($value['click'])? $value['click']:0,
                    'views' => isset($value['view'])? $value['view']:0,
                    'trans' => isset($data['trans'])? intval($data['trans']):0,
                    'costs' => isset($value['cost'])? number_format($value['cost'],2,'.',''):0,
                    'create_date' => date('Y-m-d', $data['data_time']),
                    'create_time' => date('Y-m-d H:i:s'),
                    'update_time' => date('Y-m-d H:i:s'),
                    'type' => EdcStats::TYPE_VIEW,
                );
                if (count($list) == 2) {
                    if ($list[0] == 10001 && $list[1] == 10001) {
                        $row['area_id'] = 0;
                        $row['area_fid'] = 0;
                    } elseif ($list[0] != 10001 && $list[1] == 10001) {
                        $row['area_id'] = 0;
                        $row['area_fid'] = (int)$list[0];
                    } else {
                        $row['area_id'] = (int)$list[1];
                        $row['area_fid'] = (int)$list[0];
                    }
                } else {
                    $row['area_id'] = 0;
                    $row['area_fid'] = 0;
                }
                $this->redis[$id]->rpush($statsAreaKey, serialize($row));
                $this->num++;
            }
        }
        */

        //statsInterest
        if (isset($data['inter'])) {
            $statsInterestKey = $this->prefixKey . 'statsInterest:';
            foreach($data['inter'] as $k => $value) {
                $row = array(
                    'ad_group_id' => $data['gid'],
                    'ad_user_id' => $data['uid'],
                    'ad_plan_id' => $data['pid'],
                    'inter_id' => addslashes($k),
                    'clicks' => isset($value['click'])? $value['click']:0,
                    'views' => isset($value['view'])? $value['view']:0,
                    'costs' => isset($value['cost'])? number_format($value['cost'],2,'.',''):0,
                    'trans' => isset($data['trans'])? intval($data['trans']):0,
                    'create_date' => date('Y-m-d', $data['data_time']),
                    'create_time' => date('Y-m-d H:i:s'),
                    'update_time' => date('Y-m-d H:i:s'),
                    'type' => EdcStats::TYPE_VIEW,
                );
                $this->redis[$id]->rpush($statsInterestKey, serialize($row));
                $this->num++;
            }
        }
        if (isset($data['kw'])) {
            $statsKeywordKey = $this->prefixKey . 'statsKeyword:';
            foreach($data['kw'] as $k => $value) {
                $row = array(
                    'ad_group_id' => $data['gid'],
                    'ad_user_id' => $data['uid'],
                    'ad_plan_id' => $data['pid'],
                    // 'keyword' => addslashes($k),
                    'keyword' => $k,
                    'clicks' => isset($value['click'])? $value['click']:0,
                    'views' => isset($value['view'])? $value['view']:0,
                    'costs' => isset($value['cost'])? number_format($value['cost'],2,'.',''):0,
                    'trans' => isset($data['trans'])? intval($data['trans']):0,
                    'create_date' => date('Y-m-d', $data['data_time']? $data['data_time']:$data['last_time']),
                    'ad_keyword_id' => 0,
                    'create_time' => date('Y-m-d H:i:s'),
                    'update_time' => date('Y-m-d H:i:s'),
                    'type' => EdcStats::TYPE_VIEW,
                );
                $this->redis[$id]->rpush($statsKeywordKey, serialize($row));
                $this->num++;
            }
        }
        return;
    }

    public function popOpt($id, $opt)
    {
        $key = $this->prefixKey . $opt . ':';
        return $this->redis[$id]->lpop($key);
    }

    public function pushOpt($data, $id, $opt)
    {
        $key = $this->prefixKey . $opt . ':'; 
        $this->redis[$id]->rpush($key, serialize($data));
    }

    public function dataToDb($opt, $row)
    {
        if ($opt == 'stats') {
            $res = $this->modelArr['stats']->addIncr($row);
        } elseif ($opt == 'statsArea') {
            $this->modelArr['statsArea']->updateIncr($row);
        } elseif ($opt == 'statsInterest') {
            $res = $this->modelArr['statsInterest']->updateIncr($row);
        } elseif ($opt == 'statsKeyword') {
            $res = $this->modelArr['statsKeyword']->updateIncr($row);
        }
        return ;
    }

    //public function dataToDb($data)
    //{
        //$row = array(
            //'ad_advert_id' => $data['aid'],
            //'ad_plan_id' => $data['pid'],
            //'ad_group_id' => $data['gid'],
            //'ad_user_id' => $data['uid'],
            //'create_date' => date('Y-m-d', $data['data_time']),
            //'last_update_time' => $data['last_time'],
            //'views' => intval($data['view']),
            //'clicks' => intval($data['click']),
            //'trans' => isset($data['trans'])? intval($data['trans']):0,
            //'total_cost' => floatval($data['amount']),
            //'ad_channel_id' => intval($data['channel_id']),
            //'ad_place_id' => intval($data['place_id']),
            //'data_source' => 0,
            //'admin_user_id' => 0,
            //'status' => 0,
            //'type' => EdcStats::TYPE_VIEW,
            //'create_time' => date('Y-m-d H:i:s'),
            //'update_time' => date('Y-m-d H:i:s'),
        //);
        //$res = $this->modelArr['stats']->addIncr($row);
        //if ($res != -1) {
            //if (isset($data['area'])) {
                //foreach($data['area'] as $k => $value) {
                    //$list = explode(',', $k);
                    //$row = array(
                        //'ad_group_id' => $data['gid'],
                        //'ad_user_id' => $data['uid'],
                        //'ad_plan_id' => $data['pid'],
                        //'area_key' => addslashes($k),
                        //'clicks' => isset($value['click'])? $value['click']:0,
                        //'views' => isset($value['view'])? $value['view']:0,
                        //'trans' => isset($data['trans'])? intval($data['trans']):0,
                        //'costs' => isset($value['cost'])? number_format($value['cost'],2,'.',''):0,
                        //'create_date' => date('Y-m-d', $data['data_time']),
                        //'create_time' => date('Y-m-d H:i:s'),
                        //'update_time' => date('Y-m-d H:i:s'),
                        //'type' => EdcStats::TYPE_VIEW,
                    //);
                    //if (count($list) == 2) {
                        //if ($list[0] == 10001 && $list[1] == 10001) {
                            //$row['area_id'] = 0;
                            //$row['area_fid'] = 0;
                        //} elseif ($list[0] != 10001 && $list[1] == 10001) {
                            //$row['area_id'] = 0;
                            //$row['area_fid'] = (int)$list[0];
                        //} else {
                            //$row['area_id'] = (int)$list[1];
                            //$row['area_fid'] = (int)$list[0];
                        //}
                    //} else {
                        //$row['area_id'] = 0;
                        //$row['area_fid'] = 0;
                    //}
                    //$this->modelArr['statsArea']->updateIncr($row);
                //}
            //}
            //if (isset($data['inter'])) {
                //foreach($data['inter'] as $k => $value) {
                    //$row = array(
                        //'ad_group_id' => $data['gid'],
                        //'ad_user_id' => $data['uid'],
                        //'ad_plan_id' => $data['pid'],
                        //'inter_id' => addslashes($k),
                        //'clicks' => isset($value['click'])? $value['click']:0,
                        //'views' => isset($value['view'])? $value['view']:0,
                        //'costs' => isset($value['cost'])? number_format($value['cost'],2,'.',''):0,
                        //'trans' => isset($data['trans'])? intval($data['trans']):0,
                        //'create_date' => date('Y-m-d', $data['data_time']),
                        //'create_time' => date('Y-m-d H:i:s'),
                        //'update_time' => date('Y-m-d H:i:s'),
                        //'type' => EdcStats::TYPE_VIEW,
                    //);
                    //$this->modelArr['statsInterest']->updateIncr($row);
                //}
            //}
            //if (isset($data['kw'])) {
                //foreach($data['kw'] as $k => $value) {
                    //$row = array(
                        //'ad_group_id' => $data['gid'],
                        //'ad_user_id' => $data['uid'],
                        //'ad_plan_id' => $data['pid'],
                        //'keyword' => addslashes($k),
                        //'clicks' => isset($value['click'])? $value['click']:0,
                        //'views' => isset($value['view'])? $value['view']:0,
                        //'costs' => isset($value['cost'])? number_format($value['cost'],2,'.',''):0,
                        //'trans' => isset($data['trans'])? intval($data['trans']):0,
                        //'create_date' => date('Y-m-d', $data['data_time']? $data['data_time']:$data['last_time']),
                        //'ad_keyword_id' => 0,
                        //'create_time' => date('Y-m-d H:i:s'),
                        //'update_time' => date('Y-m-d H:i:s'),
                        //'type' => EdcStats::TYPE_VIEW,
                    //);
                    //$this->modelArr['statsKeyword']->updateIncr($row);
                //}
            //}
        //}
        //return ;
    //}

    public function getLockFile()
    {
        return Config::item('statsCollectLog') . 'assignCollectTask.lock';
    }

    public function setLock()
    {
        $file = $this->getLockFile();
        file_put_contents($file, time());
        return ;
    }

    public function setUnlock()
    {
        $file = $this->getLockFile();
        unlink($file);
        return ;
    }

    /**
     * check lock
     * lock return true
     * unlock return false
     *
     */
    public function checkLock()
    {
        $file = $this->getLockFile();
        if (!file_exists($file))
            return false;
        $content = file_get_contents($file);
        if (time() - (int)$content > 300) {
            unlink($file);
            return false;
        } else
            return true;
    }

}
