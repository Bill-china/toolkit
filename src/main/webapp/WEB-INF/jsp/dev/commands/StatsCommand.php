<?php
class StatsCommand extends CConsoleCommand {

    protected $_task_name = '';

    protected $_resMutexFile = null;

    const PAGE_SIZE = 500;
    const COST_TIMES_GAME = 100;
    const TIMEOUT = 30;
    const QUEST_END_NO = 0;
    const QUEST_END_YES = 1;
    const RET_GAME_UNION_SUC = '0';
    /**
     * 同步历史点击，消费数据到游戏联盟
     * @param $date 数据日期
     */
    public function actionGameUnionUpdate($date=null) {
        ini_set('memory_limit', '10240M');
        printf("%s begin, date [%s]\n", __FUNCTION__, date('Y-m-d H:i:s'));

        $limitTimeStamp = strtotime(date("Y-m-d")." -1 second");
        if(is_null($date)) {
            $timeStamp = $limitTimeStamp;
        } else {
            $timeStamp = strtotime($date);
        }

        //只能获取历史数据
        if($timeStamp > $limitTimeStamp) {
            printf("%s error[invalid date], date [%s]\n", __FUNCTION__, $date);
            return;
        }

        //获取接口地址
        $url = Config::item('game_union_url');
        if(empty($url)) {
            printf("%s error[invalid conf], url [%s]\n", __FUNCTION__, $url);
            return;
        }

        $requestDate = date('Y-m-d', $timeStamp);

        $ch = curl_init();
        curl_setopt($ch, CURLOPT_POST, true);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_TIMEOUT, self::TIMEOUT);

        $isError = false;
        $page = 0;
        $gameUnionData = $this->_getGameUnion($timeStamp);//计算游戏联盟消费情况
        $pages = count($gameUnionData);
        foreach($gameUnionData as $requestData) {
            $austin = rand(0,10000);
            $str = 'POST'.$austin.'2.0MTIzNDU2Nzg5MA==/app/report?rev=2.0&austin='.$austin;
            $sig = substr(base64_encode(md5(urlencode($str))), 10, 30);
            $requestDataArr = array(
                'ctime'=>time(),
                'date'=>$requestDate,
                'page'=>$page++,
                'end'=>($page<$pages?self::QUEST_END_NO:self::QUEST_END_YES),
                'data'=>$requestData,
            );
            $requestDataStr = json_encode($requestDataArr);
            curl_setopt($ch, CURLOPT_URL, $url."?rev=2.0&austin=$austin&ssig=$sig");
            curl_setopt($ch, CURLOPT_POSTFIELDS, $requestDataStr);
            $response = curl_exec($ch);
            $responseArr = json_decode($response, true);
            if(is_array($responseArr) &&
                array_key_exists('ret', $responseArr) &&
                $responseArr['ret'] === self::RET_GAME_UNION_SUC) {
                printf("%s request suc, page 【%s】, response 【%s】\n", __FUNCTION__, ($page-1), $response);
            } else {
                $isError = true;
                printf("%s request error, data 【%s】, response 【%s】\n", __FUNCTION__, $requestDataStr, $response);
            }
        }
        curl_close($ch);

        if($isError) {
            Utility::sendAlert(__CLASS__,__FUNCTION__,'游戏联盟消费数据同步出现异常',true);
        }

        printf("%s end, date [%s]\n", __FUNCTION__, date('Y-m-d H:i:s'));
        return;
    }

    /**
     * 获取游戏联盟数据
     * @param int $timeStamp    数据时间戳
     *
     * @return array    数据结果集
     */
    private function _getGameUnion($timeStamp) {
        $res = array();
        $sql = "select location place,count(*) click,sum(price-reduce_price) cost from click_detail where status not in (-1,2) and deal_status=1 and ver ='shouzhu'  and cheat_type not in (2,3) and price != reduce_price and source_system=7 group by location";
        $queryInfo = ComAdDetail::queryBySql($sql, $timeStamp);

        $count = 0;
        $tmp = array();
        foreach($queryInfo as $row) {
            $tmp[] = array(
                'place'=>$row['place'],
                'click'=>intval($row['click']),
                'cost'=> round($row['cost'],2) * self::COST_TIMES_GAME,
            );
            $count ++;
            if($count % self::PAGE_SIZE == 0) {
                $res[] = $tmp;
                $tmp = array();
            }
        }
        if(!empty($tmp)) {
            $res[] = $tmp;
        }
        unset($queryInfo);

        return $res;
    }
    /**
     * 新的计费任务，mediav 二期上线后使用
     * todo
     */
    public function actionAdstatisticV2 ($sid = false, $master = false) {
        ini_set('memory_limit','10240M');
        $beginTime = time();
        $this->_task_name = $taskName = sprintf("[AdstatisticV2 %s]", date('Y-m-d H:i:s', $beginTime));

        $redisConfig = new CConfiguration(Yii::getPathOfAlias('application.config') . '/redis.php');
        $redisList = $redisConfig->itemAt('tongji');
        if (!isset($redisList[$sid])) {
            printf("%s redis node [%d] node is not exists\n", $taskName, $sid);
            return;
        }

        $queue      = new ComAdStats($sid);
        $statistic  = new ComAdStatisticV2($sid);
        $cheat      = new ComAdCheat($sid);

        $start_time = time();
        $count = $recordCount = $clickCount = $viewCount = $failCount = 0;
        $mInter = Utility::minuteInter(date('i', $start_time));
        if ($sid != 0) {
            $mInter = intval(date('i', $start_time) / 10);
        }
        while (true) {
            $arr = $queue->popMulti(500);
            if (is_array($arr)) {
                $arr = array_filter($arr);
                if (count($arr) == 0) {
                    usleep(100000); //100毫秒
                }
                $cheat_clickids=array();
                $clicks=array();
                $zhuangxian=array();
                foreach($arr as $k => $value) {
			//Utility::log(__CLASS__,"popMulti",$value);
                    if ($value === false)
                        continue;
                    $list = json_decode($value, true);
                    if (!is_array($list))
                        continue;
                    $recordCount++;
                    if (!(isset($list['isArray']) && $list['isArray'] == 1)) {
                        $tmp = array('isArray' => 1);
                        $tmp['data'][] = $list;
                        $list = $tmp;
                    }
                    foreach ($list['data'] as $v) {

                        if ($cheat->check($v)) {
                            $statistic->update($v, $mInter);
                            if ($v['type'] == 'click') {

                            } else {
                                $viewCount++;
                            }
                        } else {

                            $failCount++;
                        }
                        $count++;
                    }
                }

            }
            $end_time = time();
            $spendTime = $end_time - $start_time;
            $second = intval(date('s'));
            if ($spendTime > 56 || $second > 56) {
                break;
            }
        }

        // 保存实时消费数据入库
        ComConsume::save();

        // if ($sid==0 && $master) {
        //     $this->_unlock('task', 'click');
        // }
        echo date('Y-m-d H:i:s') . "\trecords:{$recordCount}\tview:{$viewCount}\tclick:{$clickCount}\tfail:{$failCount}\ttotal:{$count}\n";
    }

    protected function updateCheatClick($clickid, $click_time)
    {
        if(!$clickid)
        {
            return;
        }
        try
        {
            $ids = array();
            $sql = "update click_detail set cheat_type=2,reduce_price=price,update_time=".time()." where click_id='$clickid'";
            ComAdDetail::queryBySql($sql, $click_time, 'exec');
            $sql = "insert into operate_log (click_id,create_time,operate_type,operate_from,extension) values ('" . $clickid . "'," . time() . ",2,1,2".")";
            $ret = Yii::app()->db_click_log->createCommand($sql)->execute();
        }
        catch(Exception $ex)
        {
            Utility::sendAlert(__CLASS__,__FUNCTION__,$ex->getMessage());
            sleep(1);
        }
    }

    protected function addClick($list)
    {
        if(empty($list))
        {
            return;
        }
        try{
            $list2 = array();
            $src = $list['src'];
            if($list['ver'] == 'shouzhu'){
                $src = $list['reqsrc'];
            }

            $list2['click_id']=$list['click_id'];
            $list2['get_sign']=$list['get_sign'];
            $list2['click_time']=$list['now'];
            $list2['view_id']=$list['view_id'];
            $list2['view_time']=$list['view_time'];
            $list2['ip']=$list['ip'];
            $list2['mid']=$list['mid'];
            $list2['ad_user_id']=$list['uid'];
            $list2['ad_advert_id']=$list['aid'];
            $list2['ad_group_id']=$list['gid'];
            $list2['ad_plan_id']=$list['pid'];
            $list2['keyword']=$list['keyword'];
            $list2['query']=$list['query'];
            $list2['ls']=$list['ls'];
            $list2['src']=$src;
            $area=explode(",",$list['city_id']);
            $list2['area_fid']=$area[0];
            $list2['area_id']=$area[1];
            $list2['price']=$list['price'];
            $list2['bidprice']=$list['bidprice'];
            $list2['create_date']=date('Y-m-d',$list['now']);
            $list2['cid']=$list['channel_id'];
            $list2['pid']=$list['place_id'];
            $list2['ver']=$list['ver'];
            $list2['create_time']=time();
            $list2['update_time']=time();
            $list2['sub_ver']=$list['subver'];
            $list2['sub_data']=$list['subdata'];
            $list2['sub_ad_info']=$list['sub_ad_info'];
            $list2['pos']=$list['pos'];
            $list2['location']=$list['place'];
            $list2['tag_id']=$list['tag_id'];
            $list2['apitype']=$list['apitype'];
            $list2['type']=$list['type']=='click'?1:2;
            $list2['cheat_type']=0;
            $list2['source_type']=$list['source_type'];
            $list2['source_system']=1;
            $list2['status']=0;
            $list2['app_cid']= intval($list['app_cid']);

            //调用统一的添加函数
            ComAdDetail::insertDetail($list2, $list['now']);
        } catch(Exception $ex) {
            Utility::sendAlert(__CLASS__,__FUNCTION__,$ex->getMessage());
            sleep(1);
        }
    }

    protected function _lock($prefix, $fname) {
        $lockPath = Config::item('mediav_lock_path');
        $strMutexFile = sprintf('%s%s_%s.lock', $lockPath, $prefix, $fname);
        $resFile = fopen($strMutexFile, 'a');
        $bolRet = false;
        if ($resFile) {
            $bolRet = flock($resFile, LOCK_EX | LOCK_NB);
            if ($bolRet) {
                $this->_resMutexFile = $resFile;
            } else {
                fclose($resFile);
            }
        }

        return $bolRet;
    }

    protected function _unlock($prefix, $fname) {
        fclose($this->_resMutexFile);
        $lockPath = Config::item('mediav_lock_path');
        $strMutexFile = sprintf('%s%s_%s.lock', $lockPath, $prefix, $fname);
        @unlink($strMutexFile);
    }
}

