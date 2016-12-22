<?php
/**
 *
 */
Yii::import('application.extensions.CEmqPublisher');
class ComBudgetData {

    const MSG_TYPE_CONSUME  = 1; // 消费
    const MSG_TYPE_RECHARGE = 2; // 充值
    const MSG_TYPE_QUOTA    = 3; // 修改预算
    const MSG_TYPE_CHEAT    = 4; // 反作弊返钱
    const MSG_TYPE_USER_PLAN= 5; // 用户或者计划消费
    const MSG_TYPE_USER_MV  = 6; // 用户mv消费后更新余额信息

    const SRC_UNION_TYPE_360    = 0; // 联盟id 360自己正常的搜索
    const SRC_UNION_TYPE_58     = 1; // 联盟id 58
    const SRC_UNION_TYPE_DUGUHU = 2; // 联盟id 独孤虎

    const UNION_CHANNEL_ID      = 48;
    const PLACE_ID_DUGUHU       = 197;
    const PLACE_ID_58           = 198;
    const PLACE_ID_LIMIT           = 365;

    // 单条点击消费数据
    /**
     * $data = array(
     *     'query'         => '这是个测试',            // todo
     *     'src'           => $data['src'],            // 来源
     *     'keyword'       => $data['keyword'],
     *     'clickprice'    => $data['clickPrice'],     // 点击价格(扣费)
     *     'matchtype'     => $data['matchtype'],
     *     'channel_id'    => $data['channel_id'],
     *     'place_id'      => $data['place_id'],
     *     'clicktime'     => $data['clicktime'],
     *     'charge_time'   => $saveTime,               // 结算时间
     * );
     */
    public static function sendOneConsumeData($userID, $planID, $data) {
        $BGData = array(
            'type'       => self::MSG_TYPE_CONSUME,
            'query'      => $data['query'],                 //查询串
            'src'        => $data['src'],                   //流量来源
            'keyword'    => $data['keyword'],               // 关键词
            'clickprice' => (float)$data['clickprice'],     //点击价格
            'matchtype'  => $data['matchtype'],             //query与bidword的匹配类型,1,精确，2，短语，3,宽泛
            'lsid'       => 0,                              // 具体标识是哪个来源的联盟id，1表示58, 2表示独孤虎，0 表示360自己的正常搜索
            'clicktime'  => $data['clicktime'],             // 点击时间
            'chargetime' => self::_getTimeMs(),
            'dealtime'  => isset($data['settletime'])?$data['settletime']:0,
            'gspprice'  => isset($data['gspprice'])? (float) $data['gspprice']:0,
            'bucket_id' => isset($data['bucket_id'])? $data['bucket_id']:0,
            'bidprice'  => isset($data['bidprice'])? (float) $data['bidprice']:0,
            'lsid'      => isset($data['lsid'])? (int) $data['lsid']:0,
            'plat_type' => isset($data['product_line'])? (int) $data['product_line']:0,
        );
        // if ($data['channel_id']==self::UNION_CHANNEL_ID) {
        //     // if ($data['place_id']==self::PLACE_ID_DUGUHU) {
        //     //     $BGData['lsid'] = 2;
        //     // } else if ($data['place_id']==self::PLACE_ID_58) {
        //     //     $BGData['lsid'] = 1;
        //     // } else if ($data['place_id']==self::PLACE_ID_LIMIT) {
        //     //     $BGData['lsid'] = 3;
        //     // }
        //     $BGData['lsid'] = 3;//联盟的lsid全部为3
        // }

        $mqData = array(
            'type'          => 'BUDGET_UPDATE',
            'userid'        => (int)$userID,
            'planid'        => (int)$planID,
            'budgetdata'    => $BGData,
        );
        $logID = sprintf("esc_%d_%05d", time(), mt_rand(0, 99999));
        CEmqPublisher::send(
            Yii::app()->params['exchange']['budgetData'],
            'esc',
            json_encode($mqData),
            $logID,
            Yii::app()->params['esc_emq_proxy']
        );
        Utility::log(__CLASS__,__FUNCTION__,array($logID,$mqData));
    }

    // 单个计划的消费数据
    public static function sendOnePlanConsumeData($userID, $planID, $data, $type) {
        $BGData = array(
            'type'          => $type,
            'planbudget'    => (float)$data['planbudget'],
            'plancost'      => (float)$data['plancost'],
            'chargetime'    => self::_getTimeMs(),
            'plat_type'  => isset($data['product_line'])? (int) $data['product_line']:0,
        );
        if($type == self::MSG_TYPE_CHEAT){
             $BGData['dealtime'] = isset($data['cheatdealtime'])?$data['cheatdealtime']:0;

             $BGData['lsid'] = isset($data['lsid'])? (int) $data['lsid']:0;

             $BGData['clickprice'] = isset($data['clickprice'])?$data['clickprice']:0;
        }
        $mqData = array(
            'type'          => 'BUDGET_UPDATE',
            'userid'        => (int)$userID,
            'planid'        => (int)$planID,
            'budgetdata'    => $BGData,
        );
        $logID = sprintf("esc_%d_%05d", time(), mt_rand(0, 99999));
        CEmqPublisher::send(
            Yii::app()->params['exchange']['budgetData'],
            'esc',
            json_encode($mqData),
            $logID,
            Yii::app()->params['esc_emq_proxy']
        );
        Utility::log(__CLASS__,__FUNCTION__,array($logID,$mqData));
    }

    // 单个用户的消费数据
    public static function sendOneUserConsumeData ($userID, $data, $type) {
        $BGData = array(
            'type'          => $type,
            'balance'       => (float)$data['balance'],
            'userbudget'    => (float)$data['userbudget'],
            'usercost'      => (float)$data['usercost'],
            'chargetime'    => self::_getTimeMs(),
            'product_budget'=> isset($data['product_budget'])? (float) $data['product_budget']:0,
            'plat_type'  => isset($data['product_line'])? (int) $data['product_line']:0,
        );
        if($type == self::MSG_TYPE_CHEAT){
             $BGData['dealtime'] = isset($data['cheatdealtime'])?$data['cheatdealtime']:0;
             $BGData['lsid'] = isset($data['lsid'])? (int) $data['lsid']:0;
             //$BGData['clickprice'] = isset($data['clickprice'])?$data['clickprice']:0;
        }
        $mqData = array(
            'type'          => 'BUDGET_UPDATE',
            'userid'        => (int)$userID,
            'budgetdata'    => $BGData,
        );
        $logID = sprintf("esc_%d_%05d", time(), mt_rand(0, 99999));
        CEmqPublisher::send(
            Yii::app()->params['exchange']['budgetData'],
            'esc',
            json_encode($mqData),
            $logID,
            Yii::app()->params['esc_emq_proxy']
        );
        Utility::log(__CLASS__,__FUNCTION__,array($logID,$mqData));
    }

    // 用户mv消费后更新余额信息
    public static function sendOneUserMVConsumeData ($userID, $balance) {
        $BGData = array(
            'type'          => (int)self::MSG_TYPE_USER_MV,
            'balance'       => (float)$balance,
            'chargetime'    => self::_getTimeMs(),
        );

        $mqData = array(
            'type'          => 'BUDGET_UPDATE',
            'userid'        => (int)$userID,
            'budgetdata'    => $BGData,
        );
        $logID = sprintf("esc_%d_%05d", time(), mt_rand(0, 99999));
        CEmqPublisher::send(
            Yii::app()->params['exchange']['budgetData'],
            'esc',
            json_encode($mqData),
            $logID,
            Yii::app()->params['esc_emq_proxy']
        );
        Utility::log(__CLASS__,__FUNCTION__,array($logID,$mqData));
    }

    protected static function _getTimeMs() {
        $ret = gettimeofday();
        return $ret['sec']*1000 + intval($ret['usec']/1000);
    }
    // 发送用户余额变化
    public static function sendUserBalance ($userID, $balance) {
        $BGData = array(
            'type'          => self::MSG_TYPE_RECHARGE,
            'balance'       => (float)$balance,
            'chargetime'    => self::_getTimeMs(),
            //'plat_type'     => 0 //用户充值无产品线信息
        );

        $mqData = array(
            'type'          => 'BUDGET_UPDATE',
            'userid'        => (int)$userID,
            'budgetdata'    => $BGData,
        );

        $logID = sprintf("esc_%d_%05d", time(), mt_rand(0, 99999));

        try {
            CEmqPublisher::send(
                Yii::app()->params['exchange']['budgetData'],
                'esc',
                json_encode($mqData),
                $logID,
                Yii::app()->params['esc_emq_proxy']
            );
            Utility::log(__CLASS__,__FUNCTION__,array($logID,$mqData));
        } catch(Exception $e) {
            // echo $e->getMessage();
            return false;
        }
        return true;
    }
    // 发送用户限额变化
    public static function sendUserQuota ($userID, $quota, $balance, $product_budget, $platType) {
        $BGData = array(
            'type'          => self::MSG_TYPE_QUOTA,
            'userbudget'    => (float)$quota,
            'balance'       => (float)$balance,
            'chargetime'    => self::_getTimeMs(),
            'product_budget'    => (float)$product_budget,
            'plat_type'     => isset($platType) ? (int) $platType : 0
        );

        $mqData = array(
            'type'          => 'BUDGET_UPDATE',
            'userid'        => (int)$userID,
            'budgetdata'    => $BGData,
        );

        $logID = sprintf("esc_%d_%5d", time(), mt_rand(0, 99999));

        try {
            CEmqPublisher::send(
                Yii::app()->params['exchange']['budgetData'],
                'esc',
                json_encode($mqData),
                $logID,
                Yii::app()->params['esc_emq_proxy']
            );
            Utility::log(__CLASS__,__FUNCTION__,array($logID,$mqData));
        } catch(Exception $e) {
            // echo $e->getMessage();
            return false;
        }
        return true;
    }

    // 发送用户单个计划限额变化
    public static function sendPlanQuota ($userID, $planID, $quota, $userQuotaData, $product_today_cost, $platType) {
        $BGData = array(
            'type'          => self::MSG_TYPE_QUOTA,
            'userbudget'    => (float)$userQuotaData['quota'],
            'balance'       => (float)$userQuotaData['balance'],
            'usercost'      => (float)$product_today_cost,
            'planbudget'    => (float)$quota,
            'chargetime'    => self::_getTimeMs(),
            'product_budget'    => (float)$userQuotaData['product_budget'],
            'plat_type'     => isset($platType)? (int) $platType:0,
        );

        $mqData = array(
            'type'          => 'BUDGET_UPDATE',
            'userid'        => (int)$userID,
            'planid'        => (int)$planID,
            'budgetdata'    => $BGData,
        );

        $logID = sprintf("esc_%d_%5d", time(), mt_rand(0, 99999));

        try {
            CEmqPublisher::send(
                Yii::app()->params['exchange']['budgetData'],
                'esc',
                json_encode($mqData),
                $logID,
                Yii::app()->params['esc_emq_proxy']
            );
            Utility::log(__CLASS__,__FUNCTION__,array($logID,$mqData));
        } catch(Exception $e) {
            // echo $e->getMessage();
            return false;
        }
        return true;
    }

    // 发送用户多个计划限额变化
    public static function sendPlanQuotaBatch ($userID, $planIDs, $quota, $userQuotaData, $product_today_cost, $platType) {
        $BGData = array(
            'type'          => self::MSG_TYPE_QUOTA,
            'userbudget'    => (float)$userQuotaData['quota'],
            'balance'       => (float)$userQuotaData['balance'],
            'usercost'      => (float)$product_today_cost,
            'planbudget'    => (float)$quota,
            'chargetime'    => self::_getTimeMs(),
            'product_budget'    => (float)$userQuotaData['product_budget'],
            'plat_type'     => isset($platType)? (int) $platType:0,
        );

        $mqData = array(
            'type'          => 'BUDGET_UPDATE',
            'userid'        => (int)$userID,
            'planid'        => 0,
            'budgetdata'    => $BGData,
        );
        foreach ($planIDs as $planID) {
            $mqData['planid'] = (int)$planID;
            $mqData['budgetdata']['chargetime'] = self::_getTimeMs();
            $logID = sprintf("esc_%d_%5d", time(), mt_rand(0, 99999));
            try {
                CEmqPublisher::send(
                    Yii::app()->params['exchange']['budgetData'],
                    'esc',
                    json_encode($mqData),
                    $logID,
                    Yii::app()->params['esc_emq_proxy']
                );
                Utility::log(__CLASS__,__FUNCTION__,array($logID,$mqData));
            } catch(Exception $e) {
                // echo $e->getMessage();
                // return false;
				//
            }
        }
        return true;
    }
}

/*
消息格式
BudgetData
{
    optional uint32 type = 1; //1:消费，2:充值；3:修改预算；4:反作弊返钱 5 用户或者计划消费 6 用户mv消费后更新余额信息
    optional float balance = 2; // 账户余额
    optional float userbudget = 3; //账户预算
    optional float usercost = 4; // 账户已经消费
    optional float planbudget = 5; //计划预算
    optional float plancost = 6; //计划已经消费
    optional bytes query = 7; //查询串
    optional bytes src = 8; //流量来源
    optional bytes keyword = 9; // 关键词
    optional float clickprice = 10; //点击价格
    optional uint32 matchtype = 11; //query与bidword的匹配类型,1,精确，2，短语，3,宽泛
    optional uint64 lsid = 12; // 具体标识是哪个来源的联盟id，1表示58, 2表示独孤虎 0 表示360自己的正常搜索
    optional uint32 clicktime = 13; // 点击时间
    optional uint32 chargetime = 14; //具体的结算时间
}

// 这里只列出budget相关字段
AdMessage
{
    optional bytes type ; //消息类型，这里是BUDGET_UPDATE
    optional uint64 userid; //必选
    optional uint32 planid; //可选
    optional BudgetData budgetdata; //消费数据，可选。
}
*/

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
