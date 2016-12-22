<?php

/**
 * 增量同步Kafka消息
 */
class IncrementCommand
{
    /**
     * 布尔新建计划
     * @param json $msg
     * @return string
     */
    public function boolAddPlan($msg)
    {
        try{
            $content = json_decode($msg, true);

            $res = 'success';
            if (!$content) {
                throw new Exception("json_decode error");
            }
            //增加消息日志便于排查
            Utility::log(__CLASS__,__FUNCTION__,$msg);
            $uid = $content['ad_user_id'];

            //获取计划的限额参数 exp_amt:0不限
            if(isset($content['id']) && isset($content['exp_amt'])){

                $planQuota = array($content['id']=>$content['exp_amt']);

                //获取用户限额及余额
                $userArr = ComQuota::getUserBalance($uid);

                //更新用户、计划限额、余额
                if (!empty($userArr)) {
                    $ret = ComQuota::updateIncrementQuota($userArr,$planQuota);
                    $info = $ret?'success':'failed';
                    $info = "uid:{$uid} refreshOneUser {$info}";
                    Utility::log(__CLASS__,__FUNCTION__,$info);
                    echo date("Y-m-d H:i:s")."\tplan_id:{$content['id']}\t{$info} \n";
                }else{
                    $errorInfo = "uid:{$uid} getUserQuota failed";
                    Utility::log(__CLASS__,__FUNCTION__,$errorInfo);
                }
            }else{
                $errorInfo = "uid:{$uid} Get Add a plan message parameter error\n";
                Utility::log(__CLASS__,__FUNCTION__,$errorInfo.$msg);
            }
            Yii::app()->db_quota->setActive(false);
        } catch (Exception $ex) {
            Utility::sendAlert(__CLASS__, __FUNCTION__, $ex->getMessage());
            return $ex->getMessage();
        }

        return $res;
    }

    /**
     * 布尔更新计划
     * @param json $msg
     * @return string
     */
    public function boolModPlan($msg) {

        $flag = true;
        $res = 'success';
        $planQuota = array();
        try{
            $content = json_decode($msg,true);
            if (!$content) {
                throw new Exception("json_decode error");
            }
            //增加消息日志便于排查
            Utility::log(__CLASS__,__FUNCTION__,$msg);
            $before = $content['before'];
            $uid = $before['ad_user_id'];
            $after = $content['data'];
            $ad_plan_id = 0;
            //判断是否更新计划限额 exp_amt:0是不限
            if(isset($after['exp_amt'])){

                if(isset($before['id'])){

                    $ad_plan_id = $before['id'];
                    $exp_amt = $after['exp_amt'];
                    //获取计划的限额参数
                    $planQuota = array($ad_plan_id=>$exp_amt);

                    //获取用户限额及余额
                    $userArr = ComQuota::getUserBalance($uid);

                    //更新用户、计划限额、余额
                    if (!empty($userArr)) {
                        $ret = ComQuota::updateIncrementQuota($userArr,$planQuota);
                        $info = $ret?'success':'failed';
                        $info = "uid:{$uid} refreshOneUser {$info}";
                        Utility::log(__CLASS__,__FUNCTION__,$info);
                        echo date("Y-m-d H:i:s")."\tplan_id:{$ad_plan_id}\t{$info} \n";

                        //给消息基线发消息
                        if($ret){
                            $new_before[] = $before;
                            $topic = Yii::app()->params['topic'];
                            $content['exp_amt'] = $exp_amt;
                            $content['plat_type'] = isset($content['plat_type'])?$content['plat_type']:2;//产品线默认2为布尔
                            //self::sendKafka($uid, $topic, $content, $new_before); 统一使用新账户通消息，下线此消息

                            //给消息基线发上下线消息
                            if ($before['exp_amt'] > 0 && ($after['exp_amt'] == 0 || $after['exp_amt'] > $before['exp_amt'])){
                                $sData = ComMsg::setBoolPlanMsgData($content,1,$ret);
                                ComMsg::sendMsg(1,$ret,$sData,$uid);
                            }
                        }
                    }else{
                        $errorInfo = "uid:{$uid} getUserQuota failed";
                        Utility::log(__CLASS__,__FUNCTION__,$errorInfo);
                    }
                }else{
                    $errorInfo = "uid:{$uid} Get update a plan message parameter error";
                    Utility::log(__CLASS__,__FUNCTION__,$errorInfo.$msg);
                }
            }else{
                $errorInfo = "uid:{$uid} There is no change in the plan quota";
                Utility::log(__CLASS__,__FUNCTION__,$errorInfo.$msg);
            }
            //增加给引擎发送消息 jingguangwen 20150812 add
            // $user_quota_arr = ComQuota::getUserQuotaByUserId($uid);

            // if (!empty($user_quota_arr) && $ad_plan_id != 0) {

            //     $dj_quota = $user_quota_arr['dj_quota'];
            //     $real_time_balance = max(round($user_quota_arr['balance'] - $user_quota_arr['dj_cost'] - $user_quota_arr['mv_cost'] - $user_quota_arr['yesterday_dj_cost'] - $user_quota_arr['yesterday_mv_cost'],2),0);
            //     $user_today_cost = max(round($user_quota_arr['app_cost'],2),0);
            //     $plan_quota = $exp_amt;
            //     $product_budget = $user_quota_arr['app_quota'];
            //     $user_quota_data = array(
            //             'quota'     => 0,
            //             'balance'   => $real_time_balance,
            //             'product_budget'   => $product_budget,
            //             );
            //     $productLine = isset($content['plat_type'])?$content['plat_type']:2;//产品线默认2为布尔
            //     ComBudgetData::sendPlanQuota($uid, $ad_plan_id, $plan_quota, $user_quota_data, $user_today_cost,$productLine);
            // } else{
            //     $errorInfo = "uid:{$uid} plan_id{$ad_plan_id} get ad_user_quota  failed\n";
            //     Utility::log(__CLASS__,__FUNCTION__,$errorInfo.$msg->body);
            // }
            Yii::app()->db_quota->setActive(false);
            //add  end
        } catch (Exception $ex) {
            Utility::sendAlert(__CLASS__, __FUNCTION__, $ex->getMessage());
            return $ex->getMessage();
        }

        return $res;
    }

    /*
     * 布尔批量修改计划预算
     */
    public function boolBatchUpdatePlan($msg)
    {
       $res = 'success';
       $planQuota = array();

       try{
            $content = json_decode($msg,true);
            if (!$content) {
                throw new Exception("json_decode error");
            }

            //增加消息日志便于排查
            Utility::log(__CLASS__,__FUNCTION__,$msg);
            $uid = $content['ad_user_id'];
            $planStr = $content['planIds'];
            $planIds = explode(',',$planStr);

            //判断是否更新计划限额 exp_amt:0是不限
            if(isset($content['exp_amt'])){
                $expAmt = $content['exp_amt'];
                //获取用户限额及余额
                $userArr = ComQuota::getUserBalance($uid);
                if (empty($userArr)) {
                    $errorInfo = "uid:{$uid} getUserBalance failed\n";
                    Utility::log(__CLASS__,__FUNCTION__,$errorInfo.$msg);
                }else{
                    foreach($planIds as $planId){
                        $planQuota = array($planId=>$expAmt);
                        $ret = ComQuota::updateIncrementQuota($userArr,$planQuota);
                        $info = $ret?'success':'failed';
                        $info = "uid:{$uid} refreshOneUser {$info}";
                        Utility::log(__CLASS__,__FUNCTION__,$info);
                        echo date("Y-m-d H:i:s")."\tplan_id:{$planId}\t{$info} \n";
                        if($ret){
                            $beforePlanId[] = $planId;
                        }
                    }

                    //给消息基线发消息
                    if(!empty($beforePlanId)){
                        foreach($beforePlanId as $planId){
                            foreach($content['before'] as $v){
                                if($planId == $v['id']){
                                    $before[] = array("id"=>$planId, "exp_amt"=>$v['exp_amt']);
                                    //需要发给消息基线发上下线消息的计划
                                    if ($v['exp_amt'] > 0 && ($exp_amt == 0 || $exp_amt > $v['exp_amt'])){
                                        $sPlanids[] = $planId;
                                        $sExpamts[] = $v['exp_amt'];
                                    }
                                }
                            }
                        }
                    }
                    $topic = Yii::app()->params['topic'];
                    $content['plat_type'] = isset($content['before']['plat_type'])?$content['before']['plat_type']:2;//产品线默认2为布尔
                    //self::sendKafka($uid, $topic, $content, $before);统一使用新账户通消息，下线此消息

                    //给消息基线发上下线消息
                    if(!empty($sPlanids)){
                        $sData = ComMsg::setBatchBoolPlanMsgData($content,1,$ret,$sPlanids,$sExpamts);
                        ComMsg::sendMsg(1,$ret,$sData,$uid);
                    }
                }
            }
            //增加给引擎发送消息 jingguangwen 20150812 add
            // $user_quota_arr = ComQuota::getUserQuotaByUserId($uid);
            // if (!empty($user_quota_arr)) {

            //     $dj_quota = $user_quota_arr['dj_quota'];
            //     $real_time_balance = max(round($user_quota_arr['balance'] - $user_quota_arr['dj_cost'] - $user_quota_arr['mv_cost'] - $user_quota_arr['yesterday_dj_cost'] - $user_quota_arr['yesterday_mv_cost'],2),0);
            //     $user_today_cost = max(round($user_quota_arr['app_cost'],2),0);
            //     $product_budget = $user_quota_arr['app_quota'];
            //     $plan_quota = $expAmt;
            //     $user_quota_data = array(
            //         'quota'     => 0,
            //         'balance'   => $real_time_balance,
            //         'product_budget'   => $product_budget,
            //     );
            //     $productLine = isset($content['plat_type'])?$content['plat_type']:2;//产品线默认2为布尔
            //     ComBudgetData::sendPlanQuotaBatch($uid, $planIds, $plan_quota, $user_quota_data, $user_today_cost,$productLine);
            // } else{
            //     $errorInfo = "uid:{$uid}  get ad_user_quota failed\n";
            //     Utility::log(__CLASS__,__FUNCTION__,$errorInfo.$msg);
            // }
            Yii::app()->db_quota->setActive(false);
            //add  end

        } catch (Exception $ex) {
            Utility::sendAlert(__CLASS__, __FUNCTION__, $ex->getMessage());
            return $ex->getMessage();
        }
        return $res;
    }

    public static function sendKafka ($uid, $topic, $mq, $before=array())
    {
        $after_logid = Utility::getLoggerID('ESC');
        $mq['msg_time'] = time();
        $mq['topic'] = $topic;
        $mq['after_logid'] = $after_logid;
        $mq['before'] = $before;
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, Yii::app()->params['kafka'] . '?key=' . $uid . '&uid=' . $uid . '&topic=' . $topic);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($mq));
        curl_setopt($ch, CURLOPT_TIMEOUT, 3);
        curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 3);
        $ret = curl_exec($ch);
        curl_close($ch);
        $msg = json_encode($mq);
        if ($ret == 'ok') {
            $status = 'success';
        } else {
            $status = 'fail';
            Utility::sendAlert(__CLASS__, __FUNCTION__, '更新预算给消息模块发消息失败:'.$msg,true);
        }
        $escLog = date('YmdHis') . "\t" . "esc_update_budget_send_info"."\t" .$status."\t". $msg;
        Utility::log(__CLASS__,__FUNCTION__,$escLog);
    }
}
