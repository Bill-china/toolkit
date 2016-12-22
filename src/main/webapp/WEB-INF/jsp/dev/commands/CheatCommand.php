<?php
include __DIR__ . '/CommonCommand.php';
Yii::import('application.extensions.CEmqPublisher');
class CheatCommand extends CommonCommand {
	public function actionClickIdSubscriber($count = 1)
	{
		$emq = new ComEMQ('emq_esc');
		$emq->exchangeName = 'cheat_click_id';
		$emq->checkInterval = 20; //多少秒检查一次
		$emq->alarmReceiveTime = 60 * 60 * 2; //超过多少秒发送时间和接受时间差就报警
		$emq->startMultiProcessSubscriber($count);
	}

	public static function _clickIdSubscriber($msg)
	{
		$content =  "";
		$msgId = 0;
        //时间判断  0:00-0:05之间反作弊不处理
        if( time() <= (strtotime(date('Ymd'))+300) ){

            ComEMQ::receiveFail($msg, true);
            Utility::log(__CLASS__,"CHEAT_CLICKID_DEAL_DELAY",$msg->body);
            sleep(30);
            return;
        }

		$cnn = Yii::app()->db_click_log;
		try
		{
			$content = json_decode($msg->body, true);
			if (!$content)
			{
				sleep(2);
				//throw new Exception("json_decode error");
			}
			else
			{
    			Utility::$logid = isset($content['logid']) ? $content['logid'] : "";
    			Utility::log(__CLASS__, __FUNCTION__, $msg->body);
    			$msgId=$click_id = mysql_escape_string($content['content']);
    			$date=date("Y-m-d");

	        	$trs = $cnn->beginTransaction();
                $has_commit=0;
				$click=ComAdDetail::queryBySql("select * from click_detail where click_id='".$click_id."' for update",time(),'row');
    			if(!$click)
    			{

					$date=date("Y-m-d",strtotime("-1 days"));
					$click=ComAdDetail::queryBySql("select * from click_detail where click_id='".$click_id."' for update",strtotime("-1 days"),'row');
				}
				if(!$click)
				{
					Utility::log(__CLASS__,"_clickIdSubscriber_no_clickid",$click_id);
				}
				else
				{
                    //esc计费产品线支持 20160412 jingguangwen add
                    $product_line_info = Utility::getProductLineInfo($click,1);
                    if(empty($product_line_info)){

                        Utility::log(__CLASS__,"ERROR",$click['click_id']." get product_line_info err");
                        @$trs->commit();
                        ComEMQ::receiveSuccess($msg);
                        return;
                    }
                    $cheat_price = 0;
                    $ret=array();
                    $need_search_quota = 0;//传递引擎数据是否需要重新查询quota

					if($click['cheat_type']!=0  || $click['deal_status'] !=0 ||  $click['price'] <= $click['reduce_price'])
        			{
        				//说明已经处理完成，或者没必要处理
        				Utility::log(__CLASS__,"_clickIdSubscriber_no_need",$click);
                        $need_search_quota = 1;
        			}
        			else
        			{

						//开始计算费用,需退款金额
            	       $cheat_price = $amount = round($click['price']-$click['reduce_price'], 2);

						$ret=ComQuota::cost((int)$click['ad_user_id'],(int)$click['ad_plan_id'],(0-$amount),(int)$click['click_time'],$click['click_id'],($click['ver']=='mediav'?'mv':'dj'),$click['ver'],$product_line_info);
						Utility::log(__CLASS__,"CHEAT_RET",array($click['click_id'],$ret));

						$reduce_price=$click['reduce_price']-$ret['real_cost'];//之前减少的，加上现在减少的金额
		            	ComAdDetail::updateDetail(array('status'=>2,'update_time' => time(),'reduce_price'=>$reduce_price,'cheat_type'=>3),$click['click_id'], strtotime($date));

                        $mqData = array(
                            'ad_user_id'    => $click['ad_user_id'],
                            'data'          => array(
                                'ad_user_id'      => $click['ad_user_id'],
                                'ad_plan_id'      => $click['ad_plan_id'],
    		                            'amount'          => $reduce_price,
                                'ver'             => $click['ver'],
                                'create_time'     => $click['click_time'],
                                'click_id'        => $click['click_id'],
                                'cheat_exec_time' => date('Y-m-d H:i:s'),
                                'product_line'    => $product_line_info['product_line'],
                            ),
                        );

                        /*下线rmq消息
                        CEmqPublisher::send(
                            Yii::app()->params['exchange']['cheatRefund'],
                            'esc_cheatnew',
                            json_encode($mqData),
                            Utility::$logid,
                            Yii::app()->params['emq']
                        );

                        CEmqPublisher::send(
                            'cron_cheat_check',
                            'esc_cheat_new',
                            json_encode($mqData),
                            Utility::$logid,
                            Yii::app()->params['emq']
                        );
                        */

                        //给消息基线发上下线消息

                        //消息优化需求 20160620
                        //充值和反作弊上线判断消费和限额，达到限额的产品线不发上线
                        $costInfo = ComQuota::getUserCost($click['ad_user_id']);
                        if (empty($costInfo)) {
                            $errorInfo = "uid:{$click['ad_user_id']} getUserCost failed\n";
                            Utility::log(__CLASS__,__FUNCTION__,$errorInfo.$msg->body);
                        }else{
                            $productLine = ComMsg::setProductLineOnlineData($costInfo,$mqData['data']['product_line']);
                        }

                        //如果查库异常，默认发上线
                        if(false === $productLine){
                            $productLine = array(1,2,3,4);
                        }
                        //事务提交
                        @$trs->commit();
                        $has_commit=1;

                        $mqData['ad_user_id'] = $click['ad_user_id'];
                        $mqData['product_line_arr'] = $productLine;
                        $sData = ComMsg::setCheatMsgData($mqData,1,$ret,$click);
                        ComMsg::sendMsg(1,$ret,$sData,$click['ad_user_id']);
                        //给消息基线发上下线消息结束
					}
                    //写redis
                    //ComConsume::addDataToRedis($click['ad_user_id'], $click['ad_plan_id'], $click['cid'], $cheat_price);

                    $click['cost_info']=$ret;

                    $click['cheat_info']= array(
                        'cheat_price'       => $cheat_price,
                        'cheat_deal_time'   => time(),
                        'need_search_quota' => $need_search_quota,
                    );
                    $click['product_line_info']= $product_line_info;
                    //发送处理各维度消息
                    static $emq2;
                    $emq2 = new ComEMQ('emq_esc');
                    $emq2->exchangeName = "cheat_dimension_reduce";
                    $emq2->logid = Utility::$logid;
                    $id=$emq2->send($click);
                    Utility::log(__CLASS__,"_clickIdSubscriber_done",array($click['click_id'],$id));
                    //QBUS数据源
                    $comBineLog = date('YmdHis') . "\t" . "esc_cheat" . "\t" . json_encode($click);
                    $qbus_file_name = "esc_cheat.combineLog.".date('YmdH');
                    ComAdLog::write($comBineLog, '/data/log/e/qbus/'.$qbus_file_name);

			     }
                 if($has_commit==0){
                    @$trs->commit();
                 }

			}
			ComEMQ::receiveSuccess($msg);
			echo date('Y-m-d H:i:s') . "\t" . $click_id . "\tsuccess\n";

		}
		catch (Exception $ex)
		{
			@$trs->rollBack();
			ComEMQ::receiveFail($msg, true);
			echo date('Y-m-d H:i:s') . "\t" . $ex->getMessage(). " fail\n";
			Utility::sendAlert(__CLASS__,__FUNCTION__,"fail clickId:{$msgId} ".$ex->getMessage());
			sleep(1);
		}
	}


    public function actionDimensionReduceNewSubscriber($count = 1)
    {
        $emq = new ComEMQ('emq_esc');
        $emq->exchangeName = 'cheat_dimension_reduce';
        $emq->checkInterval = 20; //多少秒检查一次
        $emq->alarmReceiveTime = 60 * 60 * 2; //超过多少秒发送时间和接受时间差就报警
        $emq->startMultiProcessSubscriber($count);
    }

    public static function _dimensionReduceNewSubscriber($msg)
    {
        $content = "";
        $msgId = 0;
        try {
            $content = json_decode($msg->body, true);
            if (!$content) {
                sleep(2);
                throw new Exception("json_decode error");
            }
            Utility::$logid = isset($content['logid']) ? $content['logid'] : "";
            Utility::log(__CLASS__, __FUNCTION__, $msg->body);

            $clickLogDB = Yii::app()->db_click_log;
            $adStats            = new EdcStats();
            $adStatsArea        = new EdcStatsArea();
            $adStatsKeyword     = new EdcStatsKeyword();
            $adStatsInterest    = new EdcStatsInterest();
            $adApp              = new AdApp();
            $adStatsBiyi        = new EdcStatsBiYi();
            //$adClickLog = new AdClickLog();
            //$adClickLog->setDB($clickLogDB);
            $dbSize = Yii::app()->params['db_stat_num'];
            $oneCheatData= $content['content'];
            if(isset($oneCheatData['cheat_info']['cheat_price']) && $oneCheatData['cheat_info']['cheat_price']==0 ){
                ComEMQ::receiveSuccess($msg);
                return;
            }
            $amount = round($oneCheatData['price']-$oneCheatData['reduce_price'], 2);

            $dbIndex = $oneCheatData['ad_user_id'] % $dbSize + 1;
            $dbConfName = 'db_stat_'.$dbIndex;
            $dbStat = Yii::app()->$dbConfName;
            $adStats->setDB($dbStat);
            $adStatsArea->setDB($dbStat);
            $adStatsKeyword->setDB($dbStat);
            $adStatsInterest->setDB($dbStat);
            $adApp->setDB($dbStat);
            $adStatsBiyi->setDB($dbStat);

            $oneCheatData['ad_channel_id'] = $oneCheatData['cid'];
            $oneCheatData['ad_place_id'] = $oneCheatData['pid'];
            $oneCheatData['area_key'] = $oneCheatData['area_fid'].','.$oneCheatData['area_id'];
            $oneCheatData['inter_id'] = $oneCheatData['tag_id'];

            $ver = AdClickLog::VER_SOU;
            if (!empty($oneCheatData['ver'])) {
                $ver = trim($oneCheatData['ver']);
            }

            do {

                if ($ver == 'shouzhu') {
                    //查询 ad_app 表
                    $ad_app_arr = $adApp->getByDateAndAreaKey(
                        $oneCheatData['ad_group_id'],
                        $oneCheatData['area_key'],
                        $oneCheatData['create_date'],
                        $oneCheatData['cid'],
                        $oneCheatData['app_cid'],
                        $oneCheatData['pid'],
                        $oneCheatData['ad_plan_id'],
                        $oneCheatData['ad_advert_id'],
                        $oneCheatData['type']
                    );
                    if (empty($ad_app_arr)) {
                        Utility::log(__CLASS__, __FUNCTION__,sprintf("task %s, can not get data from ad_app, gid[%s], areakey[%s], create_date[%s], cid[%s], app_cid[%s], pid[%s],ad_plan_id[%d],ad_advert_id[%d],type[%d]\n",
                            $taskName,
                            $oneCheatData['ad_group_id'],
                            $oneCheatData['area_key'],
                            $oneCheatData['create_date'],
                            $oneCheatData['cid'],
                            $oneCheatData['app_cid'],
                            $oneCheatData['pid'],
                            $oneCheatData['ad_plan_id'],
                            $oneCheatData['ad_advert_id'],
                            $oneCheatData['type']
                        ));


                    } else {

                        $adApp->cheatClickRefund($ad_app_arr['id'], $amount, $oneCheatData['create_date']);
                        //$adClickLog->updateStatusByClickId($oneCheatData['click_id'], AdClickLog::STATUS_DONE);
                    }

                    //查询 ad_app_statistic 表
                    $adAppStatistic     = new AdAppStatistic();
                    $adAppStatistic->setDB($dbStat);
                    $ad_app_arr = $adAppStatistic->getByDateAndPlanKey(
                        $oneCheatData['create_date'],
                        $oneCheatData['ad_plan_id'],
                        $oneCheatData['cid'],
                        $oneCheatData['pid'],
                        $oneCheatData['src']
                    );
                    if (empty($ad_app_arr)) {
                        Utility::log(__CLASS__, __FUNCTION__,sprintf("task %s, can not get data from ad_app_statistic, ad_plan_id[%s], cid[%s], pid[%s], req_src[%s]\n",
                            'CheatRefund',
                            $oneCheatData['ad_plan_id'],
                            $oneCheatData['cid'],
                            $oneCheatData['pid'],
                            $oneCheatData['src']
                        ));
                    } else {
                        $adAppStatistic->cheatClickRefund($ad_app_arr['id'], $amount, $oneCheatData['create_date']);
                    }
                    break;

                }

                // 从 ad_stats 表里取数据
                $ad_stats_arr = $adStats->getByAdvertIdAndDateAndChannelAndPlace($oneCheatData['ad_advert_id'],$oneCheatData['create_date'],$oneCheatData['ad_channel_id'],$oneCheatData['ad_place_id']);
                if (empty($ad_stats_arr)) {
                    Utility::log(__CLASS__, __FUNCTION__,sprintf("task %s, can not get data from ad_stats, aid[%s], create_date[%s], channel_id[%s], place_id[%s]\n",
                        $taskName,
                        $oneCheatData['ad_advert_id'],
                        $oneCheatData['create_date'],
                        $oneCheatData['ad_channel_id'],
                        $oneCheatData['ad_place_id']
                    ));

                    break;
                }


                if (($ad_stats_arr['total_cost'] - $amount) < 0 || ($ad_stats_arr['clicks']-1) < 0 ) {
                    Utility::log(__CLASS__, __FUNCTION__,sprintf("task %s, check amount fail, total_cost[%s], clicks[%s], click_id[%s]\n",
                        $taskName,
                        $ad_stats_arr['total_cost'],
                        $ad_stats_arr['clicks'],
                        $oneCheatData['click_id']
                    ));

                    break;
                }

                //查询ad_stats_area表对应的数据
                $ad_stats_area_arr = $adStatsArea->getByDateAndAreaKey(
                    $oneCheatData['ad_group_id'],
                    $oneCheatData['area_key'],
                    $oneCheatData['create_date']
                );
                if (empty($ad_stats_area_arr)) {
                    Utility::log(__CLASS__, __FUNCTION__,sprintf("task %s, can not get data from ad_stats_area, gid[%s], area_key[%s], create_date[%s]\n",
                        $taskName,
                        $oneCheatData['ad_group_id'],
                        $oneCheatData['area_key'],
                        $oneCheatData['create_date']
                    ));

                    break;
                }
                //查询ad_stats_keyword表、ad_stats_interest表、ad_app表 对应的数据
                $ad_stats_keyword_arr = $ad_stats_interest_arr = $ad_app_arr = array();
                //区分sou与guess
                if ($ver == AdClickLog::VER_SOU) {
                    //查询ad_stats_keyword统计表
                    $ad_stats_keyword_arr = $adStatsKeyword->getByDateAndKeyword(
                        $oneCheatData['ad_group_id'],
                        $oneCheatData['keyword'],
                        $oneCheatData['create_date']
                    );

                    if (empty($ad_stats_keyword_arr)) {
                        Utility::log(__CLASS__, __FUNCTION__,sprintf("task %s, can not get data from ad_stats_keyword, gid[%s], keyword[%s], create_date[%s]\n",
                            $taskName,
                            $oneCheatData['ad_group_id'],
                            $oneCheatData['keyword'],
                            $oneCheatData['create_date']
                        ));

                        break;
                    }

                }elseif ($ver == AdClickLog::VER_GUESS) {
                    //查询ad_stats_interest表
                    $ad_stats_interest_arr = $adStatsInterest->getByDateAndInterId(
                        $oneCheatData['ad_group_id'],
                        $oneCheatData['inter_id'],
                        $oneCheatData['create_date']
                    );
                    if (empty($ad_stats_interest_arr)) {
                        Utility::log(__CLASS__, __FUNCTION__,sprintf("task %s, can not get data from ad_stats_interest, gid[%s], inter_id[%s], create_date[%s]\n",
                            $taskName,
                            $oneCheatData['ad_group_id'],
                            $oneCheatData['inter_id'],
                            $oneCheatData['create_date']
                        ));

                        break;
                    }
                }
                // 比翼
                $ad_stats_biyi_arr = array();
                if ($oneCheatData['sub_ver']=='biyi') {
                     //解析sub_data 2015-04-20 jingguangwen@360.cn  add
                     $subdata = '';
                    //解析新加属性 sub_ad_info
                    if (isset($oneCheatData['sub_ad_info']) && !empty($oneCheatData['sub_ad_info'])) {
                        $sub_ad_info = json_decode($oneCheatData['sub_ad_info'],true);
                        if(is_array($sub_ad_info) &&!empty($sub_ad_info)){
                            foreach ($sub_ad_info as $sub_ad_info_arr) {
                                $subdata = $sub_ad_info_arr['id'];
                            }
                        }
                    } else {
                        if (isset($oneCheatData['subdata']) && !empty($oneCheatData['subdata'])) {
                            $subdata_arr = json_decode($oneCheatData['subdata'],true);
                            if(is_array($subdata_arr) &&!empty($subdata_arr)){
                                $subdata = implode(',', $c['subdata']);
                            }
                        }
                    }
                    $oneCheatData['sub_data'] = $subdata;
                    //add  end
                    $ad_stats_biyi_arr = $adStatsBiyi->getByGroupIDSubID(
                        $oneCheatData['ad_group_id'],
                        $oneCheatData['sub_data'],
                        $oneCheatData['create_date']
                    );
                    if (empty($ad_stats_biyi_arr)) {
                        Utility::log(__CLASS__, __FUNCTION__,sprintf("task %s, can not get data from ad_stats_biyi, gid[%s], sub_id[%s], create_date[%s]\n",
                            $taskName,
                            $oneCheatData['ad_group_id'],
                            $oneCheatData['sub_data'],
                            $oneCheatData['create_date']
                        ));

                        break;
                    }
                }


                //使用事物逻辑处理
                $trans = $dbStat->beginTransaction();
                $adStats->cheatClickRefund($ad_stats_arr['id'], $amount, $oneCheatData['create_date']);
                $adStatsArea->cheatClickRefund($ad_stats_area_arr['id'],$amount, $oneCheatData['create_date']);
                //sou  ad_stats_keyword
                if ($ad_stats_keyword_arr) {
                    $adStatsKeyword->cheatClickRefund($ad_stats_keyword_arr['id'], $amount, $oneCheatData['create_date']);
                }
                //guess ad_stats_interest
                if ($ad_stats_interest_arr) {
                    $adStatsInterest->cheatClickRefund($ad_stats_interest_arr['id'],$amount, $oneCheatData['create_date']);
                }
                // shouzhu
                if ($ad_app_arr) {
                    //$adApp->cheatClickRefund($ad_app_arr['id'], $amount, $oneCheatData['create_date']);
                }

                // biyi
                if ($ad_stats_biyi_arr) {
                    $adStatsBiyi->cheatClickRefund($ad_stats_biyi_arr['id'], $amount, $oneCheatData['create_date']);
                }

                //$adClickLog->updateStatusByClickId($oneCheatData['click_id'], AdClickLog::STATUS_DONE);
                $trans->commit();
                $trans=null;

            } while (false);
            ComEMQ::receiveSuccess($msg);
            echo date('Y-m-d H:i:s') . "\t" . $oneCheatData['click_id'] . "\tsuccess\n";
        }
        catch (Exception $ex)
        {
            if($trans)
            {
                @$trans->rollback();
            }
            //ComEMQ::receiveFail($msg, true);
            ComEMQ::receiveSuccess($msg);
            Utility::sendAlert(__CLASS__,__FUNCTION__,"fail :{$oneCheatData['click_id']} ".$ex->getMessage());
        }
    }

    public function actionClickInfoSubScriber($count=1)
    {
        $emq = new ComEMQ('emq_esc');

        $emq->exchangeName = 'click_info';
        $emq->checkInterval = 20;           //多少秒检查一次
        $emq->alarmReceiveTime = 60*60*2;   //超过多少秒发送时间和接受时间差就报警
        $emq->exitTime = 30*60;             //30分钟
        $emq->startMultiProcessSubscriber($count);
    }

    //推送给反作弊
    public static function _clickInfoSubScriber($msg)
    {
        $sendData = array();
        $data = json_decode($msg->body,true);
        $content = $data['content'];
        $bussIds = array('pc_search','pc_guess','mobile_search','mobile_app');
        if($content['clickPrice'] <= 0 || $content['ver'] =='mediav' || (isset($content['need_send_to_cheat']) && $content['need_send_to_cheat']==false)){
            echo 'real_cost le 0 or mediav or noneed:'.$msg->body."\n";
            ComEMQ::receiveSuccess($msg);
        } else {
            if(in_array($content['bussId'],$bussIds)){
                $sendData = self::setField($content);
                $arr = '['.json_encode($sendData).']';
                $res = Utility::cheatApiPost($arr);
                if(0 == $res['errno']){
                    echo 'Sent to the cheating success '.$arr."\n";
                    ComEMQ::receiveSuccess($msg);
                }else{
                    ComEMQ::receiveFail($msg, true);
                    echo 'Sent to the cheating fail '.$arr."\n";
                }
            }else{
                echo 'bussid error'.$arr."\n";
                ComEMQ::receiveSuccess($msg);
            }
        }
    }

    //整理数据字段与格式给反作弊用(front与反作弊字段不一致)
    private static function setField(&$content){
        $sendData['bussId'] = $content['bussId'];
        $sendData['recordDate'] = date('Y-m-d H:i:s',$content['now']);
        $sendData['clickId'] = isset($content['click_id'])?$content['click_id']:'';
        $sendData['mid'] = isset($content['mid'])?$content['mid']:'';
        $sendData['viewIp'] = isset($content['viewIp'])?$content['viewIp']:'';//展现IP
        $sendData['adId'] = isset($content['aid'])?$content['aid']:'';
        $sendData['userId'] = isset($content['uid'])?$content['uid']:'';
        $sendData['upTime'] = isset($content['upTime'])?$content['upTime']:0;
        $sendData['showTime'] = isset($content['showTime'])?$content['showTime']:0;
        $sendData['clickTime'] = isset($content['clickTime'])?$content['clickTime']:0;
        $sendData['agent'] = isset($content['agent'])?$content['agent']:'';
        $sendData['price'] = isset($content['price'])?$content['price']:'';
        $sendData['pvid'] = isset($content['view_id'])?$content['view_id']:'';
        $sendData['planId'] = isset($content['pid'])?$content['pid']:'';
        $sendData['gid'] = isset($content['gid'])?$content['gid']:'';
        $sendData['cityId'] = isset($content['city_id'])?$content['city_id']:'';
        $sendData['matchType'] = isset($content['matchtype'])?$content['matchtype']:'';
        $sendData['sid'] = isset($content['sid'])?$content['sid']:'';
        $sendData['lmId'] = isset($content['lmid'])?$content['lmid']:'';
        $sendData['clickIp'] = isset($content['ip'])?$content['ip']:'';//点击IP
        $sendData['pos'] = isset($content['pos'])?$content['pos']:'';
        $sendData['refer'] = isset($content['refer'])?$content['refer']:'';
        $sendData['adPosition'] = isset($content['adPosition'])?$content['adPosition']:'';
        $sendData['mobileType'] = isset($content['mobileType'])?$content['mobileType']:'';
        $sendData['adCatId'] = isset($content['adCatId'])?$content['adCatId']:'';
        $sendData['location'] = isset($content['place'])?$content['place']:'';
        $sendData['guid'] = isset($content['guid'])?$content['guid']:'';
        $sendData['ls'] = isset($content['ls'])?$content['ls']:'';
        $sendData['chanId'] = isset($content['channel_id'])?$content['channel_id']:'';
        $sendData['placeId'] = isset($content['place_id'])?$content['place_id']:'';
        $sendData['leftClick'] = isset($content['leftClick'])?$content['leftClick']:'';
        $sendData['rightClick'] = isset($content['rightClick'])?$content['rightClick']:'';
        $sendData['divWidth'] = isset($content['divWidth'])?$content['divWidth']:'';
        $sendData['divHeight'] = isset($content['divHeight'])?$content['divHeight']:'';
        $sendData['positionX'] = isset($content['positionX'])?$content['positionX']:'';
        $sendData['positionY'] = isset($content['positionY'])?$content['positionY']:'';
        $sendData['keyDown'] = isset($content['keyDown'])?$content['keyDown']:'';
        $sendData['position1'] = isset($content['position1'])?$content['position1']:'';
        $sendData['position2'] = isset($content['position2'])?$content['position2']:'';
        $sendData['position3'] = isset($content['position3'])?$content['position3']:'';
        $sendData['position4'] = isset($content['position4'])?$content['position4']:'';
        $sendData['adIndex'] = isset($content['ad_index'])?$content['ad_index']:'';
        $sendData['searchKeyword'] = isset($content['keyword'])?$content['keyword']:'';
        $sendData['trigerKeyword'] = isset($content['query'])?$content['query']:'';

        switch ($content['bussId']) {
            case "pc_guess":
                $sendData['searchKeyword'] = isset($content['searchKeyword'])?$content['searchKeyword']:'';
                $sendData['trigerKeyword'] = isset($content['trigerKeyword'])?$content['trigerKeyword']:'';
                break;
            case "mobile_app":
                if((!empty($content['m2'])) && (!empty($content['mid']))){
                    $sendData['mid'] = $content['m2'] .'-'.$content['mid'];
                }elseif((!empty($content['m2'])) && (empty($content['mid']))){
                    $sendData['mid'] = $content['m2'];
                }elseif((empty($content['m2'])) && (!empty($content['mid']))){
                    $sendData['mid'] = $content['mid'];
                }elseif((empty($content['m2'])) && (empty($content['mid']))){
                    $sendData['mid'] = '';
                }
                $sendData['adPosition'] = isset($content['adPosition'])?$content['adPosition']:0;
                $sendData['adIndex'] = isset($content['ad_index'])?$content['ad_index']:0;

                //布尔四期增加
                $sendData['adType'] = $content['ad_style'];
                $sendData['fp']     = $content['fp'];
                $sendData['sh']     = $content['sh'];
                $sendData['sw']     = $content['sw'];
                $sendData['sdip']   = $content['sdip'];
                $sendData['offset'] = $content['offset'];
                $sendData['appKey'] = $content['appKey'];
                $sendData['os']     = $content['os'];
                $sendData['sdkVer'] = $content['sdkVer'];
                $sendData['rf']     = $content['rf'];
                $sendData['imei']   = $content['imei'];
                $sendData['lon']    = isset($content['lon']) ? $content['lon'] : 0;
                $sendData['lat']    = isset($content['lat']) ? $content['lat'] : 0;
                break;
            case "pc_search":
                if((!empty($content['linkNo'])) && (!empty($content['aid']))){
                  $sendData['adId'] = $content['aid'] ."-" . $content['linkNo'];
                }
                //联盟ctype
                $sendData['lsAdType'] = isset($content['tag_id'])?$content['tag_id']:'';
                break;

            case "mobile_search":
                $sendData['adPosition'] = isset($content['pos'])?$content['pos']:0;
                //联盟ctype
                $sendData['lsAdType'] = isset($content['tag_id'])?$content['tag_id']:'';
                break;
        }

        return $sendData;

    }
}
