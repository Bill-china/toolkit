<?php
Yii::import('application.extensions.CEmqPublisher');
class ComAdStatisticV2 {
    protected $prefixKey;
    protected $redis;
    protected $serverId;
    protected $nowTime;
    protected $nowHour;
    protected $nowMinute;
    protected $nowDate;
    protected $type; //type=view or click
    protected $offLineType;
    protected $offlineLog;
    protected $needOfflineLog;
    public function __construct($sid, $type='view')
    {
        $this->comQuota     = Yii::app()->loader->component('ComAdQuota');
        $this->redis        = Yii::app()->loader->statRedis($sid, true);
        $this->serverId     = $sid;
        $this->prefixKey    = Config::item('redisKey') . 'ad_statistic:';

        $this->nowTime      = time();
        $this->nowHour      = date('H', $this->nowTime);
        $this->nowMinute    = date('i', $this->nowTime);
        $this->nowDate      = date('ymd', $this->nowTime);
        $this->type         = $type;
        $this->offLineType  = 0;
        $this->offlineLog   = array();
        $this->needOfflineLog=0;
    }

    public function update(&$arr, $mInter) {
        $aid        = $arr['aid']; //广告ID
        $type       = $arr['type']; //统计类型 点击/展示
        $cur_time   = $this->nowTime;
        $price      = 0;
        if( (!isset($arr['now']) && !isset($arr['view_time']) ) || (empty($arr['now']) && empty($arr['view_time']) ) || (intval($arr['now']) <=0 && intval($arr['view_time']) <=0 ) ) {
            $adDate     = date('ymd');
            $adHour     = date('H');
            //报警
            $alert_content=array(
                'content' => $arr,
                'err_msg' => 'now  or  time error!'
            );
            Utility::sendAlert(__CLASS__,__FUNCTION__,json_encode($alert_content));
        } else{
            $adDate     = isset($arr['now']) ? date('ymd', $arr['now']) : date('ymd', $arr['view_time']);
            $adHour     = isset($arr['now']) ? date('H', $arr['now']) : date('H', $arr['view_time']);
        }

        if (!isset($arr['channel_id']))  {
            $arr['channel_id'] = 0;
        }
        if (!isset($arr['place_id'])) {
            $arr['place_id'] = 0;
        }
        $key = $this->prefixKey . $adDate . $adHour . "_" . date('H', $cur_time) . "-{$mInter}-{$aid}-{$arr['channel_id']}-{$arr['place_id']}";

        $logData = $arr;
        $logData['key']         = $key;
        $logData['clickPrice']  = 0;

        if ($type == 'click') {

        } else {
            $statsLog = date('YmdHis') . "\t" . "esc_view" . "\t" . json_encode($logData);
            ComAdLog::write($statsLog, '/dev/shm/stats/statsLog');
        }


    }

    private function calCost(&$arr) {
        $userID = $arr['uid'];
        $planID = $arr['pid'];
        $price  = $arr['price'];
        $this->offLineType = 0;
        $this->offlineLog = array();
        // 获取用户信息
        $userInfo = ComAdQuotaV2::getUserDJQuotaInfo($userID);
        Utility::log(__CLASS__,"userInfo",$userInfo);
        if (empty($userInfo)) {
            $this->offLineType = -1;
            return false;
        }
        if (!isset($userInfo[$planID])) {
            $this->offLineType = 3;
            return false;
        }

        // 获取用户消费信息
        $viewTime = isset($arr['now']) ? $arr['now'] : $arr['view_time'];
        $djCostInfo = ComAdQuotaV2::getUserDJCostInfo(date('j', $viewTime), $userID);
        Utility::log(__CLASS__,"djCostInfo",$djCostInfo);
        //连接失败
        if($djCostInfo === false){
            $this->offLineType = -2;
            return false;
        }
        if (empty($djCostInfo)) {
            $djCostInfo = array(
                'cost'  => 0,
                'uid'   => $userID,
                $planID => 0,
            );
            //echo date('Y-m-d H:i:s',time())."\tdjCostInfoEmpty\tuid[$userID];arr:".json_encode($arr).";pid[".getmypid()."]\n";
            Utility::log(__CLASS__,"emptydjCostInfo",$djCostInfo);
        }
        if (!isset($djCostInfo[$planID])) {
            $djCostInfo[$planID] = 0;
        }

        // 消费不能超余额
        $mvCost = ComAdQuotaV2::getUserMediavCost(date('j', $viewTime), $userID);
        if($mvCost===false){
            $mvCost = 0;
        }

        $totalCost = number_format($djCostInfo['cost'] + $mvCost, 2, '.', '');
        $this->offlineLog = array(
            'cost' => $totalCost,
            'info' => $userInfo
        );
        if ($totalCost >= $userInfo['balance']) {
            $this->offLineType = 1;
            return false;
        }

        $cost = number_format($djCostInfo['cost'], 2, '.', '');
        if ($userInfo['quota']>0  && $cost>=number_format($userInfo['quota'] * 1.1, 2, '.', '')) {
            $this->offLineType = 2;
            return false;
        }

        // 不能超计划预算
        $cost = number_format($djCostInfo[$planID], 2, '.', '');
        if ($userInfo[$planID]>0 && $cost >= $userInfo[$planID]) {
            $this->offLineType = 3;
            return false;
        }

        // 计算花费
        $price = $this->_getMinPrice(
            $price,                 // 请求计费
            $mvCost,                // mv消费
            $djCostInfo['cost'],    // 点睛总消费
            $djCostInfo[$planID],   // 点睛此计划消费
            $userInfo['balance'],   // 用户余额
            $userInfo['quota'],     // 点睛用户预算
            $userInfo[$planID],     // 点睛用户此计划预算
            $arr['ver']             // 计费广告类型
        );

        if ($price>0) { // 更新
            $djCostInfo[$planID]    += $price;
            $djCostInfo['cost']     += $price;
            ComAdQuotaV2::setUserDJCostInfo(date('j', $viewTime), $userID, $djCostInfo);
            $djCostInfo['click_id']=$arr['click_id'];
            $djCostInfo['click_price']=$price;
            $djCostInfo['process_id']=getmypid();
            //$comBineLog = date('YmdHis') . "\t" . "esc_user_cost_log_dj" . "\t" . json_encode($djCostInfo);
            //ComAdLog::write($comBineLog, '/dev/shm/user_cost_log');
            Utility::log(__CLASS__,"setUserDJCostInfo",$djCostInfo);

            return $price;
        } else {
            $logData = array(
                date('Y-m-d H:i:s'),
            );
            $logData = array_merge($logData, $arr);
            $comBineLog = date('YmdHis') . "\t" . "esc_quotaError" . "\t" . json_encode($logData);
            ComAdLog::write($comBineLog, '/dev/shm/combineLog');
            return false;
        }

    }


    public function writeQuotaFail($data, &$arr) {
         Utility::log(__CLASS__,__FUNCTION__,array($data,$arr));

        if ($this->offLineType == 0) {
            // 正常
            return ;
        }
        //点击时间判断，昨天的点击限额撞线今天不再下线
        $need_offline = 1;
        if (in_array($this->offLineType, array(2,3))) {
            $click_time = $data['now'];
            $today_unixtime = strtotime(date('Y-m-d 00:00:00'));
            if($click_time < $today_unixtime){
                $need_offline = 0;
            }
        }
        //_offline 日志记录 jingguangwen 20150304 add
        $offline_log_data = $data;
        $offline_log_data['need_offline'] = $need_offline;
        $offline_log_data['offlineLog'] = $this->offlineLog;
        $offline_log_data['offline_type'] = $this->offLineType;
        $comBineLog = date('YmdHis') . "\t" . "esc_offline" . "\t" . json_encode($offline_log_data);
        ComAdLog::write($comBineLog, '/dev/shm/combineLog');
        if($need_offline == 0){
            return;
        }
        if (in_array($this->offLineType, array(-1,-2))) {
            // todo 记录日志
            // printf ("%s : %s\n", __FILE__, __LINE__);
            return ;
        }
        // 发送下线消息
        // 暂时 redis 和 rmq 都发送
        // 稳定后迁移至 rmq 去掉 redis
        $redis = Yii::app()->loader->redis('queue_offline');
        $key = "dj:offline:queue";
        $offlineData = array(
            'advert_id'     => $data['aid'],
            'group_id'      => $data['gid'],
            'plan_id'       => $data['pid'],
            'user_id'       => $data['uid'],
            'offline_type'  => $this->offLineType,
            'ad_type'       => $data['ver'],
            'time'          => time(),
            'key'           => $data['key'],
            'log'           => $this->offlineLog,
            'needOfflineLog' =>$this->needOfflineLog,
        );
        if ($redis) {
            $redis->lPush($key, json_encode($offlineData));
        }

        // rmq
        // {"mid":"d34258ffd5ba32e81ce7a5d04d1ec68f","msg_src":"esc_comadstatisticv2","time":1414563683,"logid":"ESC_141456368326548924","exchange":"ex_overlimit_offline","content":{"advert_id":195425161,"ad_group_id":27732251,"ad_plan_id":7242513,"ad_user_id":817040046,"offline_type":3,"ad_type":"sou","time":1414563683}}
        if ($this->offLineType==1) {
            $curTime = gettimeofday();
            $logID = sprintf("ESC_TO_EMQAPI_DJ_%d%06d%02d", $curTime['sec'], $curTime['usec'], mt_rand(0, 99));
            $mqData = array(
                'advert_id'         => $data['aid'],
                'ad_group_id'       => $data['gid'],
                'ad_plan_id'        => $data['pid'],
                'ad_user_id'        => $data['uid'],
                'offline_type'      => $this->offLineType,
                'quota'             => $this->offlineLog['info']['balance'],
                'ad_type'           => $data['ver'],
                'time'              => time(),
            );

            // CEmqPublisher::send(
            //     Yii::app()->params['exchange']['offline'],
            //     'esc_comadstatisticv2',
            //     json_encode($mqData),
            //     $logID,
            //     Yii::app()->params['emq']
            // );
            $param_conternt = array(
                'mid' => 'esc_emqapi_dianjing',
                'msg_src' => 'esc',
                'time' => time(),
                'logid' => $logID,
                'exchange' => Yii::app()->params['exchange']['offline'],
                'content' => $mqData,
            );

            $data = array(
                'msg' => json_encode($param_conternt)
            );
            static   $emq;
            $emq=new ComEMQ('emq_audit');
            $emq->exchangeName='offline';
            $emq->logid=$logID;
            $emq->send($data);

        }
    }

    // 返回此次可扣金额
    protected function _getMinPrice ($price, $mvCost, $djCost, $planCost, $balance, $userQuota, $planQuota, $ver) {
        $price = number_format($price, 2, '.', '');
        $balancePrice = $quotaPrice = $planPrice = $price;
        // 计划限额
        $newPlanCost = number_format($planCost + $price, 2, '.', '');
        if ($planQuota>0 && $newPlanCost>=$planQuota) {
            $planPrice = number_format($planQuota - $planCost, 2, '.', '');
            $this->offLineType = 3;
            if ($planPrice<=0) {
                return 0;
            }
        }

        // 余额
        $newUserCost = number_format($price + $mvCost + $djCost, 2, '.', '');
        if ($newUserCost>= $balance) {
            $this->offLineType = 1;
            $balancePrice = number_format($balance - $mvCost - $djCost, 2, '.', '');
            if ($balancePrice<=0) {
                return 0;
            }
        }

        // 用户预算 搜索类的下线预算阀值是 105% 其余是 100%
        if ($ver == 'sou') {
            $overQuota = number_format($userQuota * 1.05, 2, '.', '');
        } else {
            $overQuota = number_format($userQuota, 2, '.', '');
        }
        $quotaPrice = min($planPrice, $balancePrice);

        $newUserCost = number_format($quotaPrice + $djCost, 2, '.', '');
        if ($userQuota>0 && $newUserCost >= $overQuota) {
            if ($newUserCost >= number_format($userQuota*1.1, 2, '.', '')) {
                $quotaPrice = number_format($userQuota*1.1 - $djCost, 2, '.', '');
            }
            $this->offLineType = 2;
            if ($quotaPrice<=0) {
                return 0;
            }
        }

        $price = min($balancePrice, $quotaPrice, $planPrice);
        // 处理下线类型
        if (number_format($price + $mvCost + $djCost - $balance, 2, '.', '') >= 0) {
            // 余额
            $this->offLineType = 1;
        } elseif ($userQuota>0 && number_format($price + $djCost - $overQuota, 2, '.', '')>=0) {
            // 帐户预算
            $this->offLineType = 2;
        } elseif ($planQuota>0 && number_format($planCost + $price - $planQuota, 2, '.', '')>=0) {
            // 计划预算
            $this->offLineType = 3;
        }

        if ($this->offLineType<1 && $ver == 'sou' && $userQuota>0 && $newUserCost >= number_format($userQuota, 2, '.', '')) {
           $this->offLineType = 2;
           $this->needOfflineLog=1;
        }

        return $price;
    }
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
