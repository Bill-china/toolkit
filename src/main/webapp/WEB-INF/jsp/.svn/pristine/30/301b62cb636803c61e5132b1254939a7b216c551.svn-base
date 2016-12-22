<?php
/**
 * ComMsg
 * 新消息类
 * @package open 360
 * @copyright 2005-2016 360.CN All Rights Reserved.
 * @author dongdapeng@360.cn
 */
class ComMsg
{

     /**
     * 设置反作弊返款发送消息数据
     * @param  array $data
     * @param  int $msg_type
     * @param  array $ret
     * @return array
     */
    public static function setCheatMsgData($data,$msg_type,$ret,$click){

        $sData['ad_user_id'] = intval($data['ad_user_id']);
        $sData['online_time'] = $click['click_time'];
        $sData['ad_plan_id']  = $click['ad_plan_id'];
        $sData['product_line'] = $data['data']['product_line'];
        $sData['product_line_arr'] = $data['product_line_arr'];
        $sData['online_type'] = 0;
        $sData['quota_before'] = '';
        $sData['quota_after'] = '';

        return self::setData($sData,$msg_type,$ret);
    }

     /**
     * 设置产品线预算发送消息数据
     * @param  array $data
     * @param  int $msg_type
     * @return array
     */
    public static function setProductQuotaMsgData($data,$msg_type){

        if(isset($data['content']['data']['quota_type'])){
            $sData['quota_before'] = isset($data['content']['data']['quota_before'])?$data['content']['data']['quota_before']:'';
            $sData['quota_after'] = isset($data['content']['data']['quota_after'])?$data['content']['data']['quota_after']:'';
            $sData['product_line'] = $data['content']['data']['quota_type'];
        }else{
            $sData['quota_before'] = isset($data['content']['data']['before'])?$data['content']['data']['before']:'';
            $sData['quota_after'] = isset($data['content']['data']['day_quota'])?$data['content']['data']['day_quota']:$data['content']['data']['mv_quota'];
            $sData['product_line'] = '';
        }
        $sData['ad_user_id'] = intval($data['content']['data']['ad_user_id']);
        $sData['online_time'] = $data['time'];
        $sData['ad_plan_id']  = '';
        $sData['online_type'] = 4;

        return self::setData($sData,$msg_type);
    }

     /**
     * 设置充值发送消息数据
     * @param  array $data
     * @param  int $msg_type
     * @return array
     */
    public static function setPayMsgData($data,$msg_type){

        $sData['ad_user_id'] = intval($data['content']['ad_user_id']);
        $sData['product_line_arr'] = $data['product_line_arr'];
        $sData['online_time'] = $data['time'];
        $sData['quota_before'] = '';
        $sData['quota_after'] = '';
        $sData['ad_plan_id']  = '';
        $sData['product_line'] = '';
        $sData['online_type'] = 1;

        return self::setData($sData,$msg_type);
    }

     /**
     * 设置计划发送消息数据
     * @param  array $data
     * @param  int $msg_type
     * @param  array $ret
     * @return array
     */
    public static function setPlanMsgData($data,$msg_type,$ret){

        $sData['quota_before'] = array($data['content']['before']['exp_amt']);
        $sData['quota_after'] = $data['content']['data']['exp_amt'];
        $sData['ad_user_id'] = intval($data['content']['before']['ad_user_id']);
        $sData['online_time'] = $data['time'];
        $sData['ad_plan_id']  = array($data['content']['before']['id']);
        $sData['product_line'] = $data['content']['plat_type'];
        $sData['online_type'] = 3;

        return self::setData($sData,$msg_type,$ret);
    }


    /**
     * 设置批量计划发送消息数据
     * @param  array $data
     * @param  int $msg_type
     * @param  array $ret
     * @param  array $planIds
     * @param  array $expAmts
     * @return array
     */
    public static function setBatchPlanMsgData($data,$msg_type,$ret,$planIds,$expAmts){

        $sData['quota_before'] = $expAmts;
        $sData['quota_after'] = $data['content']['exp_amt'];
        $sData['ad_user_id'] = intval($data['content']['user_id']);
        $sData['online_time'] = $data['time'];
        $sData['ad_plan_id']  = $planIds;
        $sData['product_line'] = $data['content']['plat_type'];
        $sData['online_type'] = 3;

        return self::setData($sData,$msg_type,$ret);
    }

    /**
     * 设置布尔计划发送消息数据
     * @param  array $data
     * @param  int $msg_type
     * @param  array $ret
     * @return array
     */
    public static function setBoolPlanMsgData($data,$msg_type,$ret){

        $sData['quota_before'] = array($data['before']['exp_amt']);
        $sData['quota_after'] = $data['data']['exp_amt'];
        $sData['ad_user_id'] = intval($data['ad_user_id']);
        $sData['online_time'] = $data['msg_time'];
        $sData['ad_plan_id']  = array($data['before']['id']);
        $sData['product_line'] = 2;
        $sData['online_type'] = 3;

        return self::setData($sData,$msg_type,$ret);
    }

    /**
     * 设置布尔批量计划发送消息数据
     * @param  array $data
     * @param  int $msg_type
     * @param  array $ret
     * @param  array $planIds
     * @param  array $expAmts
     * @return array
     */
    public static function setBatchBoolPlanMsgData($data,$msg_type,$ret,$planIds,$expAmts){

        $sData['quota_before'] = $expAmts;
        $sData['quota_after'] = $data['exp_amt'];
        $sData['ad_user_id'] = intval($data['ad_user_id']);
        $sData['online_time'] = $data['msg_time'];
        $sData['ad_plan_id']  = $planIds;
        $sData['product_line'] = 2;
        $sData['online_type'] = 3;

        return self::setData($sData,$msg_type,$ret);
    }

    /**
     * 设置消息数据
     * @param  array $data
     * @param  int $msg_type
     * @param  array $ret
     * @return array
     */
    public static function setData($data, $msg_type, $ret = null){

        $mqData = array(
                'ad_user_id'        => $data['ad_user_id'],
                'msg_type'          => $msg_type, //1 online;2 offline
                'time'              => time(),
                );

        if(1 == $msg_type){

            $online_type = isset($data['online_type'])?$data['online_type']:'';
            $plat_type = isset($data['product_line'])?$data['product_line']:'';
            $quota_before = isset($data['quota_before'])?$data['quota_before']:'';
            $quota_after = isset($data['quota_after'])?$data['quota_after']:'';
            $online_time = isset($data['online_time'])?$data['online_time']:'';
            $ad_plan_id = isset($data['ad_plan_id'])?$data['ad_plan_id']:'';
            $plat_type_arr = isset($data['product_line_arr'])?$data['product_line_arr']:array();

            $msgData = array(
                    'online' => array(
                        'online_type' => $online_type,//0反作弊，1充值，2账户限额调整，3计划限额调整 4产品线预算下线
                        'plat_type'=> $plat_type,//产品线，1-搜索 2-布尔 3-如意 4-mv
                        'quota_before' => $quota_before,//online_type=2,3时使用
                        'quota_after' => $quota_after,//online_type=2,3时使用
                        'online_time' => $online_time,//上线数据对应的时间: 1比如隔天的反作弊时间则为点击时间 or 2充值为当前时间 or etc.
                        'ad_plan_id' => $ad_plan_id,//online_type=3时使用,online_type=0时反作弊退款也需要
                        'plat_type_arr'=> $plat_type_arr, //0反作弊，1充值: 以上两值时读取此字段，产品上线类型
                        )
                    );

        }elseif(2 == $msg_type){

            $pid = isset($data['pid'])?$data['pid']:'';
            $balance = isset($ret['balance'])?$ret['balance']:'';
            $userQuota = isset($ret['userQuota'])?$ret['userQuota']:'';
            $planQuota = isset($ret['planQuota'])?$ret['planQuota']:'';
            $productQuota = isset($ret['productQuota'])?$ret['productQuota']:'';
            $needOfflineLog = isset($ret['needOfflineLog'])?$ret['needOfflineLog']:'';
            $plat_type = isset($data['product_line'])?$data['product_line']:'';
            $offLineType = isset($data['offLineType'])?$data['offLineType']:'';
            $adType = isset($data['ver'])?$data['ver']:'';

            $msgData = array(
                    'ad_user_id'        => $data['uid'],
                    'offline' =>array(
                        'ad_plan_id' => $pid,
                        'offline_type' => $offLineType,//1账户余额下线 2账户限额下线 3计划限额下线 4 产品线预算下线 
                        'balance' => $balance,
                        'userQuota' => $userQuota,
                        'planQuota' => $planQuota,
                        'productQuota'=> $productQuota,
                        'needOfflineLog' => $needOfflineLog,
                        'plat_type'=> $plat_type, //业务线
                        'ad_type' => $adType,
                        )
                    );
        }
        return array_merge($mqData,$msgData);
    }

    /**
     * 发送上下线消息通用方法
     * @param  array $data
     * @param  int $msg_type
     * @param  array $ret
     * @param  int $uid
     * @param  strint $topic
     * @return bool
     */
    public static function sendMsg($msg_type, $ret, $data, $uid, $topic = null) {
        if(empty($topic)){
            $topic = Yii::app()->params['onOffLineTopic'];
        }
        $return = true;
        $info = 'success';
        $logid = Utility::getLoggerID('ESC');
        $data['topic'] = $topic;
        $data['logid'] = $logid;

        $retryTimes = 0;
        if (empty($key)) {
            $key = $uid;
        }
        $msg = json_encode($data);
        while($retryTimes < 3) {
            $retryTimes++;
            //各模块根据自己配置获取url，trunk下为 http://10.138.65.216:19527/
            $baseUrl = Yii::app()->params['kafka'];
            $url = $baseUrl . "?key={$key}&uid={$uid}&topic={$topic}";
            $ch = curl_init();
            curl_setopt($ch,CURLOPT_URL,$url);
            curl_setopt($ch, CURLOPT_POSTFIELDS, $msg);
            curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
            curl_setopt( $ch, CURLOPT_TIMEOUT, 2 );  //接收数据时超时设置，如果2秒内数据未接收完，直接退出
            curl_setopt( $ch, CURLOPT_CONNECTTIMEOUT, 3 );  //连接超时，这个数值如果设置太短可能导致数据请求不到就断开了
            $res = curl_exec($ch);
            if ($res == "ok") {
                break;
            } else {
                usleep(1000 * 200);
            }
            if ($retryTimes == 3) { //重试三次
                Utility::sendAlert(__CLASS__, __FUNCTION__, "发送上下线消息失败\t".$msg."\t".$res,false);
                Utility::writeLog($msg,'resend_kafka');
                $info = 'fail';
                $return = false;
            }
        }
        $escLog = date('YmdHis') . "\t" . "发送上下线消息\t"."topic:\t$topic\t$info\t". $msg;
        Utility::log(__CLASS__,__FUNCTION__,$escLog);
        return $return;
    }

    /**
     * 设置充值和反作弊产品线上线类型
     * @param  array $quotaInfo
     * @param  int $platType 
     * @return array
     */
    public static function setProductLineOnlineData($quotaInfo,$platType=null){

        $productLine = array();

        if(empty($quotaInfo)){
            return false;
        }
       
        if(($quotaInfo['sou_cost'] < $quotaInfo['sou_quota']) || ($quotaInfo['sou_quota'] == 0) ){
            $productLine[] = 1;
        }
        if($quotaInfo['app_cost'] < $quotaInfo['app_quota'] || ($quotaInfo['app_quota'] == 0)){
            $productLine[] = 2;
        }
        if($quotaInfo['ruyi_cost'] < $quotaInfo['ruyi_quota'] || ($quotaInfo['ruyi_quota'] == 0)){
            $productLine[] = 3;
        }
        if($quotaInfo['mv_cost'] < $quotaInfo['mv_quota'] || ($quotaInfo['mv_quota'] == 0)){
            $productLine[] = 4;
        }

        if($platType !== null){
            if (false === in_array($platType, $productLine)) {
                array_push($productLine, $platType);
            }
        }
        return $productLine;
    }
}
