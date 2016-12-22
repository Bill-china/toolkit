<?php
class ClickCommand extends CConsoleCommand {
	public function actionClick()
	{
		ini_set('memory_limit','1024M');
		set_time_limit(0);
        $queue = new ComAdStats(0);
        $cheat = new ComAdCheat(0);
        do
        {
        	try
            {
	            $datastr=$queue->pop();
	            if(!$datastr)
	            {
	            	sleep(1);
	            	continue;
	            }
	            Utility::log(__CLASS__,"POP",$datastr);
                    $this->pingDB();

	            $data = json_decode($datastr, true);
	            if(!$data || !is_array($data) || $data['type'] != 'click' || $data['price']<0 || intval($data['now'])<=0 || intval($data['uid'])<=0)
	        	{
	        		Utility::log(__CLASS__,"ERROR1",$datastr);
	        		continue;
	        	}

	        	$ret=$this->addClick($data);
	        	if($ret!=1 && !isset($data['try_times']))
	        	{
	        		Utility::log(__CLASS__,"ERROR2",array($ret,$data));
	        		continue;
	        	}

               // 注意下如果常驻了，一分钟过滤就要稍微改下
	        	$ret=$cheat->check($data);
	        	if($ret==false)
	        	{
	        		//是一分钟过滤的
	        		$sql = "update click_detail set cheat_type=2,reduce_price=price,deal_status=0,update_time=".time()." where click_id='{$data['click_id']}'";
	            	ComAdDetail::queryBySql($sql, $data['now'], 'exec');
                    Utility::log(__CLASS__,"ERROR3",$data['click_id']." is 1min-cheat");
	        		continue;
	        	}
                //esc计费产品线支持 20160412 jingguangwen add
                $product_line_info = Utility::getProductLineInfo($data);
                if(empty($product_line_info)){

                    Utility::log(__CLASS__,"ERROR4",$data['click_id']." get product_line_info err");
                    continue;
                }
                $data['product_line'] = $product_line_info['product_line'];
	        	//开始计算费用
				$ret=ComQuota::cost($data['uid'],$data['pid'],$data['price'],$data['now'],$data['click_id'],($data['ver']=='mediav'?'mv':'dj'),$data['ver'],$product_line_info);
				Utility::log(__CLASS__,"COST_RET",array($data['click_id'],$ret));

				//异常判断
				if(!($ret['result']===true))
				{
					//需要重新执行的
					$data['try_times']=(int)$data['try_times'] +1;
					$queue->push($data,0);
					Utility::log(__CLASS__,"REQUEUE",array($data,$ret));
					if($data['try_times']%100==0)
					Utility::sendAlert(__CLASS__,"REQUEUE",json_encode(array($data,$ret)));
					sleep(1);
					continue;
				}

                $sql = "update click_detail set reduce_price=".round($data['price']-$ret['real_cost'],2).",update_time=".time().",extension=".$ret['offline_type'].",deal_status=0 where click_id='{$data['click_id']}'";
                $row=ComAdDetail::queryBySql($sql, $data['now'], 'exec');

				//发送下游相关消息日志
		        //$this->sendOfflineMsg($data,$ret);
                $this->sendOnOfflineMsg($data,$ret);
		        $this->writeCombileLog($data,$ret['real_cost']);
		        $this->sendClickInfo($data,$ret);
		        echo date('Y-m-d H:i:s').' '.date('Y-m-d H:i:s',$data['now']).' '.$data['click_id'].' '.$ret['real_cost']." {$row} done\n";

	    	}
	    	catch(Exception $ex)
	    	{
	    		Utility::sendAlert(__CLASS__, __FUNCTION__, $ex->getMessage()."\t".$datastr, false);

	    		sleep(1);
	    	}

        }while(true);

	}


    private $last_conn_db_time=null;
    protected function pingDB()
    {
        if($this->last_conn_db_time==null)
            $this->last_conn_db_time=time();
        if(time()-$this->last_conn_db_time<60*15)
            return;
        try
        {
            $this->last_conn_db_time=time();
            $r1=Yii::app()->db_click_log->createCommand("select 1")->queryScalar();
            $r2=Yii::app()->db_quota->createCommand("select 1")->queryScalar();
            if(!$r1  || !$r2)
                throw new Exception("empty result");
        }
        catch(Exception $ex)
        {
            Yii::app()->db_click_log->setActive(false);
            Yii::app()->db_quota->setActive(false);
            Utility::log(__CLASS__,__FUNCTION__,$ex->getMessage());
            sleep(1);
        }
    }
	protected function sendClickInfo($data,$ret)
	{
        static $emq;
        $emq=new ComEMQ('emq_esc');
        $emq->exchangeName='click_info';
        $emq->logid=Utility::getLoggerID('ESC');
        $data['clickPrice']=$ret['real_cost'];
        $data['cost_info']=$ret;
        $data['settleTime']  = time();
        $res = $emq->send($data);
        Utility::log(__CLASS__,__FUNCTION__,$res);
	}

	protected function sendOfflineMsg($data,$ret)
	{

		$offLineType=$ret['offline_type'];
        if (in_array($offLineType, array(-1,-2,0))) {
            return ;
        }
        if(isset($ret['no_need_offline']))
            return;

        $curTime = gettimeofday();
        $logID = sprintf("ESC_TO_EMQAPI_DJ_%d%06d%02d", $curTime['sec'], $curTime['usec'], mt_rand(0, 99));
        $mqData = array(
            'advert_id'         => intval($data['aid']),
            'ad_group_id'       => intval($data['gid']),
            'ad_plan_id'        => intval($data['pid']),
            'ad_user_id'        => intval($data['uid']),
            'plat_type'        => intval($data['product_line']),//产品线
            'offline_type'      => $offLineType,
            'ad_type'           => $data['ver'],
            'time'              => time(),
            'balance'           => $ret['balance'],
            //'userQuota'         => $ret['userQuota'],
            'productQuota'         => $ret['productQuota'],//产品线账户限额

            'planQuota'         => $ret['planQuota'],
            'needOfflineLog'    => $ret['needOfflineLog'],

        );

        static   $emq;
        $emq=new ComEMQ('emq_audit');
        $emq->exchangeName='ex_overlimit_offline';
        $emq->logid=$logID;
        $emq->send($mqData,0);
        Utility::log(__CLASS__,__FUNCTION__,$mqData);

    }
	protected function writeCombileLog($arr,$price)
	{
		$prefixKey    = Config::item('redisKey') . 'ad_statistic:';

		$aid        = $arr['aid']; //广告ID
        $type       = $arr['type']; //统计类型 点击/展示
        $cur_time   = time();

        $adDate     = isset($arr['now']) ? date('ymd', $arr['now']) : date('ymd', $arr['view_time']);
        $adHour     = isset($arr['now']) ? date('H', $arr['now']) : date('H', $arr['view_time']);
        if (!isset($arr['channel_id']))  {
            $arr['channel_id'] = 0;
        }
        if (!isset($arr['place_id'])) {
            $arr['place_id'] = 0;
        }
        $key = $prefixKey . $adDate . $adHour . "_" . date('H', $cur_time) . "-{$mInter}-{$aid}-{$arr['channel_id']}-{$arr['place_id']}";

        $logData = $arr;
        $logData['key']         = $key;
        $logData['clickPrice']  = $price;
        $logData['settleTime']  = time();

        if($price)
        {
	        $comBineLog = date('YmdHis') . "\t" . "esc_click" . "\t" . json_encode($logData);
	        ComAdLog::write($comBineLog, '/dev/shm/combineLog');
	        ComAdLog::write($comBineLog, '/dev/shm/stats/statsLog');
            //QBUS数据源
            $qbus_file_name = "esc_click.combineLog.".date('YmdH');
            ComAdLog::write($comBineLog, '/data/log/e/qbus/'.$qbus_file_name);
    	}

	}

    protected function sendOnOfflineMsg($data,$ret)
    {
        $offLineType=$ret['offline_type'];
        if (in_array($offLineType, array(-1,-2,0))) {
            return ;
        }
        if(isset($ret['no_need_offline']))
            return;

        $uid = intval($data['uid']);
        $data['offLineType'] = $offLineType;
        $sData = ComMsg::setData($data,2,$ret);
        ComMsg::sendMsg(2,$ret,$sData,$uid);
    }

    protected function addClick($list)
    {
        if(empty($list))
        {
            return 0;
        }
        try{
            $list2 = array();
            $src = $list['src'];
            if($list['ver'] == 'shouzhu'){
                $src = $list['reqsrc'];
                $list2['mid']=$list['m2'];
            }else{
                $list2['mid']=$list['mid'];
            }

            $list2['click_id']=$list['click_id'];
            $list2['get_sign']=$list['get_sign'];
            $list2['click_time']=$list['now'];
            $list2['view_id']=$list['view_id'];
            $list2['view_time']=$list['view_time'];
            $list2['ip']=$list['ip'];
            $list2['ad_user_id']=$list['uid'];
            $list2['ad_advert_id']=$list['aid'];
            $list2['ad_group_id']=isset($list['gid'])?$list['gid']:0;
            $list2['ad_plan_id']=$list['pid'];
            $list2['keyword']=$list['keyword'];
            $list2['query']=$list['query'];
            $list2['ls']=$list['ls'];
            $list2['lsid']= (int) $list['lsid'];
            $list2['src']=$src;
            $area=explode(",",$list['city_id']);
            $list2['area_fid']=$area[0];
            $list2['area_id']=$area[1];
            $list2['price']=$list['price'];
            $list2['bidprice']=$list['bidprice'];
            $list2['create_date']=date('Y-m-d',$list['now']);
            $list2['cid']=(int)$list['channel_id'];
            $list2['pid']=(int)$list['place_id'];
            $list2['ver']=$list['ver'];
            $list2['create_time']=time();
            $list2['update_time']=time();
            $list2['sub_ver']=$list['subver'];
            $list2['sub_data']=$list['subdata'];
            $list2['sub_ad_info']=$list['sub_ad_info'];
            $list2['pos']=$list['pos'];
            $list2['location']=$list['place'];
            $list2['tag_id']=(int)$list['tag_id'];
            $list2['apitype']=$list['apitype'];
            $list2['type']=$list['type']=='click'?1:2;
            $list2['cheat_type']=0;
            $list2['source_type']=(int)$list['source_type'];
            $list2['source_system']=intval(isset($list['source_system'])?$list['source_system']:1);
            $list2['status']=0;
            $list2['deal_status']=-1;//默认状态,表示没有计费。0是已计费，1是已结算
            $list2['ad_keyword_id']=(int)$list['ad_keyword_id'];
            $list2['app_cid']= intval($list['app_cid']);

            $list2['style_id']=intval(isset($list['style_id'])?$list['style_id']:0);
            //shouzhu  '1-下载2-打开'
            if( $list2['ver']=='shouzhu'){
                //1-下载;2-打开;3-移动网站推广
                if(isset($list['promote_type'])){
                    $list2['type'] = intval($list['promote_type']);

                }
                //1表示内部，2表示外部
                if(isset($list['rf'])){
                    if($list['rf']==2||$list['rf']==3) {
                        $list2['source_system'] = 5;
                    } else if($list['rf']==5) {
                        $list2['source_system'] = 6;
                    } else if($list['rf']==6) {
                        $list2['source_system'] = 7;
                    }
                }
            } else {
                if(isset($list['is_intent']) && $list['is_intent'] > 0) {
                    $list2['type'] = 2;
                } else {
                    $list2['type'] = 1;
                }
            }

            //调用统一的添加函数
            return ComAdDetail::insertDetail($list2, $list['now']);
        } catch(Exception $ex) {
            Utility::sendAlert(__CLASS__,__FUNCTION__,$ex->getMessage());
            sleep(1);
            return 0;
        }
    }

    //切换日期脚本
    //这个脚本必须在0点后开始执行
    public function actionSwitchCurDate()
    {
        set_time_limit(0);
        ini_set('memory_limit', '5048M');
        $cur_date=date('Y-m-d');
        $count=0;
        $centerDB = DbConnectionManager::getDjCenterDB();
        $daoUser        = new User();
        $daoDbRouter    = new DbRouter();
        $daoUser->setDB($centerDB);
        $daoDbRouter->setDB($centerDB);
        $materialDB = DbConnectionManager::getMaterialDB();
        echo date('Y-m-d H:i:s')." start \n";
        //切换用户，按照有消费的先切换的原则，尽快切换
        $cost_users=ComAdDetail::queryBySql("select distinct(ad_user_id) uid from click_detail where ad_user_id>0",strtotime($cur_date)-60*24*24,'all');
        $cost_users_kv=array();
        while ($user_arr=array_pop($cost_users)) {
            try
            {

                $uid = $user_arr['uid'];
                $cost_users_kv[$uid]=1;
                $user=$daoUser->getInfoByID($uid);
                if(!$user)
                    continue;
                $dbID = $daoDbRouter->getRouter($uid);

                if(!$dbID)
                    continue;
                $djBranchDB = DbConnectionManager::getDjBranchDB($dbID);
                if(!$djBranchDB)
                    continue;

                $sql = sprintf("select id, exp_amt from ad_plan where ad_user_id=%d", $uid);
                $plans=$djBranchDB->createCommand($sql)->queryAll();
                $plan_quotas=array();
                foreach($plans as $p)
                {
                    $plan_quotas[$p['id']]=$p['exp_amt'];
                }
                //添加布尔的计划限额数据
                $sql_app = sprintf("select id, exp_amt from app_plan where ad_user_id=%d", $uid);

                $app_plans=$materialDB->createCommand($sql_app)->queryAll();

                if(!empty($app_plans)){

                    foreach($app_plans as $p)
                    {
                        $plan_quotas[$p['id']]=$p['exp_amt'];
                    }
                }

                $ret=ComQuota::switchUserCurDate($cur_date,$uid,$user['day_quota'],$user['mv_quota'],$plan_quotas,$user['quota_dianjing'],$user['quota_ruyi'],$user['quota_app']);
                if($ret)
                {
                    $count++;
                    echo date('Y-m-d H:i:s')." {$uid} success \n";
                }
                else
                    echo date('Y-m-d H:i:s')." {$uid} failed \n";

            }
            catch(Exception $ex)
            {
                echo $ex->getMessage();
            }
        }

        echo date('Y-m-d H:i:s')." cost user ok  {$count} \n";
        $lastUserID     = 0;
        $userListLimit  = 10000;

        while (true) {
            try
            {
                // 获取所有用户
                $user_arrs = $daoUser->getQuotaUser($lastUserID, $userListLimit);
                if (empty($user_arrs)) {
                    break;
                }
                foreach ($user_arrs as $user_arr)
                { // 遍历每一个用户

                    $lastUserID = $uid = $user_arr['id'];
                    if(isset($cost_users_kv[$uid]))
                        continue;
                    $user=$daoUser->getInfoByID($uid);
                    if(!$user)
                        continue;
                    $dbID = $daoDbRouter->getRouter($uid);

                    if(!$dbID)
                        continue;
                    $djBranchDB = DbConnectionManager::getDjBranchDB($dbID);
                    if(!$djBranchDB)
                        continue;

                    $sql = sprintf("select id, exp_amt from ad_plan where ad_user_id=%d", $uid);
                    $plans=$djBranchDB->createCommand($sql)->queryAll();
                    $plan_quotas=array();
                    foreach($plans as $p)
                    {
                        $plan_quotas[$p['id']]=$p['exp_amt'];
                    }
                    //添加布尔的计划限额数据
                    $sql_app = sprintf("select id, exp_amt from app_plan where ad_user_id=%d", $uid);

                    $app_plans=$materialDB->createCommand($sql_app)->queryAll();

                    if(!empty($app_plans)){

                        foreach($app_plans as $p)
                        {
                            $plan_quotas[$p['id']]=$p['exp_amt'];
                        }
                    }
                    $ret=ComQuota::switchUserCurDate($cur_date,$uid,$user['day_quota'],$user['mv_quota'],$plan_quotas,$user['quota_dianjing'],$user['quota_ruyi'],$user['quota_app']);
                    if($ret)
                    {
                        $count++;
                        echo date('Y-m-d H:i:s')." {$uid} success \n";
                    }
                    else
                        echo date('Y-m-d H:i:s')." {$uid} failed \n";

                }
            }
            catch(Exception $ex)
            {
                echo $ex->getMessage();
            }
        }


        echo date('Y-m-d H:i:s').' done '.$count."\n";
    }

    /**
     * 更新用户余额以及用户与计划的限额
     * @author jingguangwen@360.cn  20150713
     * user_status 空：全部用户,不空，则用status 状态字段逗号分隔
     */
    public function actionRefreshBalanceAndBudget($user_status="",$plan_status="") {
        set_time_limit(0);
        $cur_date=date('Y-m-d');
        $count=0;
        $centerDB = DbConnectionManager::getDjCenterDB();
        $daoUser        = new User();
        $daoDbRouter    = new DbRouter();
        $daoUser->setDB($centerDB);
        $daoDbRouter->setDB($centerDB);

        $materialDB = DbConnectionManager::getMaterialDB();
        $lastUserID     = 0;
        $userListLimit  = 1;//这个暂时不能改成多个，因为读到用户信息之后，可能很久才会更新到限额库，中间数据可能会发生变化，产生脏读。
        echo date('Y-m-d H:i:s')." start \n";
        while (true) {
            try
            {
                // 获取所有用户
                $user_arrs = $daoUser->getQuotaUser($lastUserID, $userListLimit,($user_status?explode(",", $user_status):array()));
                if (empty($user_arrs)) {
                    break;
                }
                foreach ($user_arrs as $user_arr) { // 遍历每一个用户

                    $lastUserID = $uid = $user_arr['id'];
                    //$user_status = $user_arr['status'];
                    //如果有用户状态判断，可以在此处
                    $dbID = $daoDbRouter->getRouter($uid);
                    if(!$dbID)
                        continue;
                    $djBranchDB = DbConnectionManager::getDjBranchDB($dbID);
                    if(!$djBranchDB)
                        continue;
                    //获取计划的限额信息
                    $sql = sprintf("select id, exp_amt from ad_plan where ad_user_id=%d", $uid);
                    if($plan_status)
                        $sql = sprintf("select id, exp_amt from ad_plan where ad_user_id=%d and status in (%s)", $uid,$plan_status);
                    $plans=$djBranchDB->createCommand($sql)->queryAll();
                    $plan_quotas=array();
                    foreach($plans as $p)
                    {
                        $plan_quotas[$p['id']]=$p['exp_amt'];
                    }
                    //添加布尔的计划限额数据
                    $sql_app = sprintf("select id, exp_amt from app_plan where ad_user_id=%d", $uid);
                    if($plan_status)
                        $sql_app = sprintf("select id, exp_amt from app_plan where ad_user_id=%d and status in (%s)", $uid,$plan_status);

                    $app_plans=$materialDB->createCommand($sql_app)->queryAll();

                    if(!empty($app_plans)){

                        foreach($app_plans as $p)
                        {
                            $plan_quotas[$p['id']]=$p['exp_amt'];
                        }
                    }


                    //更新余额以及限额
                    $ret=ComQuota::refreshOneUser($uid,$user_arr['balance'],$user_arr['day_quota'],$user_arr['mv_quota'],$plan_quotas,$cur_date,$user_arr['quota_dianjing'],$user_arr['quota_ruyi'],$user_arr['quota_app']);
                    if($ret)
                    {
                        $count++;
                        echo date('Y-m-d H:i:s')." {$uid} ok \n";
                    }
                    else
                        echo date('Y-m-d H:i:s')." {$uid} failed \n";
                }
            }
            catch(Exception $ex)
            {
                echo date('Y-m-d H:i:s')." {$uid} ".$ex->getMessage()."\n";
            }
        }

        echo date('Y-m-d H:i:s').' done '.$count."\n";

    }
    /**
     * 消费结算
     * @author jingguangwen@360.cn  20150714
     * uid="" 表示所有用户
     */
    public function actionSettleCost($uid="") {
        $task_name = sprintf('[Settlecost %s]', date('Y-m-d H:i:s'));
        $beginTime = date('Y-m-d H:i:s');
        printf("%s begin\n", $task_name);

        $yesterday = date('Y-m-d', strtotime('-1 day'));

        $cur_date=date('Y-m-d');
        // 获取中心库
        try {
            $centerDB = DbConnectionManager::getDjCenterDB();
            if (false === $centerDB) {
                printf("%s get dj center db fail\n", $task_name);
            }
        } catch (Exception $e) {
            printf("%s get dj center db fail, error [%s]!\n", $task_name, $e->getMessage());
            $centerDB = false;
        }
        if (!$centerDB) {
            $content = '点睛系统自动结算无法获取点睛中心库连接。请尽快手工结算。'.$yesterday;
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            return ;
        }

        $daoUser            = new User();
        $daoUserCL          = new UserChargeLog();
		$comAdDetail        =new ComAdDetail();

        $daoUser->setDB($centerDB);
        $daoUserCL->setDB($centerDB);

        $total = $fail = 0;
        $arrUserCostInfo = array();
        $settle_error_users = array();
        $validate_error_users = array();

        $arrUserCostInfo = ComAdDetail::getUserCostByDay($yesterday, 0,$uid);
        foreach ($arrUserCostInfo as $oneUserCostInfo) {
            $total++;

            $userID = $oneUserCostInfo['uid'];
            // 获取用户信息
            $userInfo = $daoUser->getInfoByID($userID);
            if (!$userInfo) {
                $fail++;
                printf("%s get user info of id[%d] fail!\n", $task_name, $userID);
                continue;
            }
            $max_click_id = $oneUserCostInfo['max_click_id'];
            $userCost = $oneUserCostInfo['cost'] > $userInfo['balance'] ? $userInfo['balance'] : $oneUserCostInfo['cost'];
            $valiad_user_cost = round($oneUserCostInfo['cost']-$userCost,2);
            // 更新
            try {

				$sql_detail = sprintf('update click_detail set deal_status=1 where create_date="%s" and ad_user_id=%d and deal_status=0 and id<=%d', $yesterday,$userID,$max_click_id);
                $res = $comAdDetail->queryBySql($sql_detail, strtotime($yesterday), 'exec');

                //jingguangwen add 20150617
                // $sql = sprintf('update ad_click_log set deal_status=1 where create_date="%s" and ad_user_id=%d and deal_status=0 ', $yesterday,$userID);
                // $res_click_log = Yii::app()->db_click_log->createCommand($sql)->execute();

                // if(!$res_click_log){
                //     //失败或者没有数据
                //     Utility::log(__CLASS__,"settleCostAdClickLogByUserId",$userID);
                // }
                //jingguangwen add 20150617
                if ($userCost>0 && $res) {
                    // 更新用户的charge log表
                    $daoUserCL->updateCostIncr($userID, $userCost, $userInfo['balance'], $yesterday);

                    //结算完更新ad_quota库账户与计划消费以及余额信息
                    $settle_return_arr = ComQuota::settleOneUser($cur_date,$userID);
                    //结算处理失败报警
                    if(empty($settle_return_arr) || !is_array($settle_return_arr) || $settle_return_arr['result'] === false ){
                        $settle_error_users[] = $userID;
                    }
                    // 更新用户表的 balance
                    $daoUser->updateCostByUserId($userID, $userCost);
                    //校验更新账户状态
                    $daoUser->checkUserStatus($userID);

                }

            } catch (Exception $e) {
                printf ("%s update fail, use id [%d], msg[%s]", $task_name, $userID, $e->getMessage());
                $fail++;
                $settle_error_users[] = $userID;
                continue;
            }
            //需要校对
            if($valiad_user_cost>0){
                $validate_return = $this->validateClikDetail($userID,$valiad_user_cost,$yesterday);
                if(!$validate_return){
                    $validate_error_users[] = $userID;
                }
            }
        }
        if(!empty($settle_error_users)){
            $settle_error_user_ids = implode(',', $settle_error_users);
            Utility::sendAlert(__CLASS__,__FUNCTION__,$cur_date.'结算异常账户id:'.$settle_error_user_ids,true);
        }
        if(!empty($validate_error_users)){
            $validate_error_user_ids = implode(',', $validate_error_users);
            Utility::sendAlert(__CLASS__,__FUNCTION__,$yesterday.'校对click_detail异常账户id:'.$validate_error_user_ids,true);
        }
        //启动其它各个维度生成脚本 20150714 add
        $importStatsDataShell = dirname(__FILE__)."/../yiic settle importStatsData >> /data/log/dj_importStatsData.log &";
        $importStatsInterestDataShell = dirname(__FILE__)."/../yiic settle importStatsInterestData >> /data/log/dj_importStatsInterestData.log &";
        $importStatsKeywordDataShell = dirname(__FILE__)."/../yiic settle importStatsKeywordData >> /data/log/dj_importStatsKeywordData.log &";
        $importStatsBiyiDataShell = dirname(__FILE__)."/../yiic settle importStatsBiyiData >> /data/log/dj_importStatsBiyiData.log &";
        $importStatsMvDataShell = dirname(__FILE__)."/../yiic settle updateStatsMvData >> /data/log/dj_updateStatsMvData.log &";
        $updateUserCacheShell = dirname(__FILE__)."/../yiic settle updateUserCache >> /data/log/dj_updateUserCache.log &";
        $importShouZhuDataShell = dirname(__FILE__)."/../yiic settle importShouZhuData >> /data/log/dj_importShouZhuData.log &";
        $importShouZhuSspDataShell = dirname(__FILE__)."/../yiic settle importShouZhuSspData >> /data/log/dj_importShouZhuSspData.log &";

        $updateProductUserUserChargeLogShell = dirname(__FILE__)."/../yiic settle updateProductUserUserChargeLog >> /data/log/dj_updateProductUserUserChargeLog.log &";

        exec($importStatsDataShell);
        exec($importStatsInterestDataShell);
        exec($importStatsKeywordDataShell);
        exec($importStatsBiyiDataShell);
        exec($importStatsMvDataShell);
        exec($updateUserCacheShell);
        exec($importShouZhuDataShell);
        exec($importShouZhuSspDataShell);
        exec($updateProductUserUserChargeLogShell);
        //写结算完标记状态
        $sql = "insert into click_tag_status (business,status,create_date,create_time,update_time) values ('1','1','".$yesterday."',".time().",".time().")";
        $ret = Yii::app()->db_click_log->createCommand($sql)->execute();


        //update ad_click_log deal_status
        //jingguangwen add 20150617
        // $sql = sprintf('update ad_click_log set deal_status=1 where create_date="%s" and deal_status=0 ', $yesterday);
        // $res_click_log = Yii::app()->db_click_log->createCommand($sql)->execute();

        //update  ad_click_log status
        // $updateClickLogShell = dirname(__FILE__)."/../yiic monitor updateClickLog >> /data/log/updateClickLogStatus.log &";
        // exec($updateClickLogShell);
        //////
        $endTime = date('Y-m-d H:i:s');
        printf("%s begin at %s, end at %s\n",
            $task_name, $beginTime, $endTime
        );
    }

    /**
     * 结算完毕校验点击数据（超账户余额花费问题）
     * @param  [int] $ad_user_id     [账户id]
     * @param  [double] $validate_money [校验金额差额]
     * @param  [string] $validate_date  [校验日期2015-07-14]
     * @return [bole]
     * @author jingguangwen@360.cn 20150714
     */
    protected  function validateClikDetail($ad_user_id,$validate_money,$validate_date)
    {
        // 校正数据
        $sumCost = round($validate_money - 0, 2);
        if($sumCost <= 0){
            return true;
        }
        $tableName = ComAdDetail::getTableName(strtotime($validate_date));
        $clickLogDB = DbConnectionManager::getClickLogDB();
        $sql = sprintf("select * from $tableName where ad_user_id=%d and create_date='%s' and status not in (-1,2) and deal_status=1  and cheat_type not in (2,3) and price != reduce_price  order by id desc", $ad_user_id,$validate_date);
        $clickLogRows = $clickLogDB->createCommand($sql)->queryAll();
        // 使用事物校正数据
        $trans = $clickLogDB->beginTransaction();
        try {
            // 校正 stats 表
            foreach ($clickLogRows as $oneRow) {
                $id = $oneRow['id'];
                $status = $oneRow['status'];
                $price = round($oneRow['price']-$oneRow['reduce_price'],2);
                if (round($price - $sumCost,2)>0) {
                    //$tmpCost = $sumCost;
                    $reducePriceAdd = $sumCost;
                    $sumCost = 0;
                } else {
                    //$tmpCost = $oneRow['price'];
                    $sumCost = round($sumCost - $price,2);
                    $reducePriceAdd = $price;
                    $status = -1;//全额扣减
                }
                $sql = sprintf('update '.$tableName.' set reduce_price=reduce_price+%s,status=%d where id=%s',
                    $reducePriceAdd, $status, $id
                );
                $clickLogDB->createCommand($sql)->execute();

                printf("%s\n", $sql);
                if ($sumCost==0) {
                    break;
                }
            }
            $trans->commit();
        } catch (Exception $e) {
            printf("%d commit fail, msg[%s]\n", $ad_user_id, $e->getMessage());
            $trans->rollback();
            return false;
        }
        return true;
    }


    //重新计费的流程，慎用该功能，参数是需要重跑的点击ID的文件地址，一行一个点击ID
    //需要注意的是：先执行step1，等待反作弊全部处理完成之后，然后再修数据(如果有必要)，然后再执行step2
    public function actionReCostClickIdStep1($click_id_file_path)
    {
        set_time_limit(0);
        ini_set("memory_limit","4096M");
        $file=file($click_id_file_path);
        if(count($file)==0)
        {
            echo "no content";
            return;
        }

        $queue = new ComAdStats(0);
        $emq=new ComEMQ('emq_esc');
        $emq->exchangeName='cheat_click_id';
        $emq->logid=Utility::$logid;

        foreach($file as $line)
        {

            $click_id=trim($line);
            $date=date("Y-m-d");

            Utility::log(__CLASS__,__FUNCTION__,$click_id);
            $click=ComAdDetail::queryBySql("select * from click_detail where click_id='".$click_id."'",time(),'row');
            if(!$click)
            {

                $date=date("Y-m-d",strtotime("-1 days"));
                $click=ComAdDetail::queryBySql("select * from click_detail where click_id='".$click_id."'",strtotime("-1 days"),'row');
            }
            if(!$click)
            {
                echo "no click_id {$click_id} \n";
                Utility::log(__CLASS__,__FUNCTION__,"no ".$click_id);
                continue;
            }

            if($click['deal_status'] ==1)
            {
                //说明已经处理完成，或者没必要处理
                echo "click_id has dealed {$click_id} \n";
                Utility::log(__CLASS__,__FUNCTION__,"dealed ".$click_id);
                continue;
            }
            if($click['deal_status']==-1)
            {
                ComAdDetail::queryBySql("update click_detail set extension=-2,update_time=".time().",reduce_price=price where click_id='".$click_id."'",strtotime($date),'exec');
                echo "click_id has not costed {$click_id} \n";
                Utility::log(__CLASS__,__FUNCTION__,"not costed ".$click_id);
                continue;
            }
            if($click['extension']==-2||$click['extension']==-3)
            {
                //说明已经处理完成，或者没必要处理
                echo "click_id has recosted or posted {$click_id} \n";
                Utility::log(__CLASS__,__FUNCTION__,"recosted or posted  ".$click_id);
                continue;
            }

            ComAdDetail::queryBySql("update click_detail set extension=-2,update_time=".time()." where click_id='".$click_id."'",strtotime($date),'exec');

            //发送反作弊处理信息
            $emq->send($click_id);

            echo date("Y-m-d H:i:s")." {$click_id} aborted\n";
            Utility::log(__CLASS__,__FUNCTION__," {$click_id} aborted");

        }
        echo "done\n";
    }

    //文件路径，或者是指定的clickid，多个用逗号隔开
    public function actionReCostClickIdStep2($click_id_file_path="",$click_ids="",$need_send_to_cheat=true)
    {
        set_time_limit(0);
        ini_set("memory_limit","4096M");
        if(empty($click_id_file_path) && empty($click_ids))
        {
            echo 'must have file path or click_ids';
            return;
        }
        if($click_id_file_path)
        {
            $file=file($click_id_file_path);
        }
        if($click_ids)
        {
            $file=explode(",",$click_ids);
        }
        if(count($file)==0)
        {
            echo "find no clickid";
            return;
        }
        //echo implode(",", $file)."\n";

        $queue = new ComAdStats(0);
        $emq=new ComEMQ('emq_esc');
        $emq->exchangeName='cheat_click_id';
        $emq->logid=Utility::$logid;
        $click_ids=array();
        $sql="select count(*) from click_detail where click_id in (";
        $sql_click_ids=array();
        foreach($file as $line)
        {
            $click_ids[]=trim($line);
            $sql_click_ids [] ="'".trim($line)."'";
        }

        $sql .= implode(",",$sql_click_ids).") and reduce_price!=price and deal_status in (0,-1)";

        $wait=true;
        while($wait)
        {
            $left_count=ComAdDetail::queryBySql($sql,time(),'scalar');
            if($left_count==0)
            {
                $yesterday_left_count=ComAdDetail::queryBySql($sql,strtotime("-1 days"),'scalar');
                if($yesterday_left_count==0)
                    {
                        $false=true;
                        break;
                    }
                else
                {
                    echo "yesterday's click_id has not execute completed,must wait ...\n";
                    sleep(20);
                }
            }
            else
            {
                echo "today's click_id has not execute completed,must wait ...\n";
                sleep(20);
            }
        }
        unset($sql_click_ids);
        foreach($click_ids as $click_id)
        {
            Utility::log(__CLASS__,__FUNCTION__,$click_id);
            $date=date("Y-m-d");
            $click=ComAdDetail::queryBySql("select * from click_detail where click_id='".$click_id."'",time(),'row');
            if(!$click)
            {
                $date=date("Y-m-d",strtotime("-1 days"));
                $click=ComAdDetail::queryBySql("select * from click_detail where click_id='".$click_id."'",strtotime("-1 days"),'row');
            }
            if(!$click)
            {
                echo "no click_id {$click_id} \n";
                Utility::log(__CLASS__,__FUNCTION__,"no ".$click_id);
                continue;
            }
            if($click['extension']==-3)
            {
                //说明已经处理完成，或者没必要处理
                echo "click_id has pushed {$click_id} \n";
                Utility::log(__CLASS__,__FUNCTION__,"pushed ".$click_id);
                continue;
            }
            if($click['deal_status'] ==1)
            {
                //说明已经处理完成，或者没必要处理
                echo "click_id has dealed {$click_id} \n";
                Utility::log(__CLASS__,__FUNCTION__,"dealed ".$click_id);
                continue;
            }

            //开始push新的数据
            $arr=array(
                'get_sign'=> $click['get_sign'],
                'view_id' => $click['view_id'],
                'ip' => $click['ip'],
                'type' => $click['type']==1?'click':'view',
                'now' => $click['click_time'],
                'view_time' => $click['view_time'],
                'apitype' =>  $click['apitype'],
                'pid' => (int) $click['ad_plan_id'],
                'place' => $click['location'],
                'pos' => (int) $click['pos'],
                'gid' => (int) $click['ad_group_id'],
                'aid' => (int) $click['ad_advert_id'],
                'uid' => (int) $click['ad_user_id'],
                'price' => (float) $click['price'],
                'bidprice' => (float) $click['bidprice'],
                'mid' => (string) $click_id,//请注意这个是用来新老关联的ID，原先的MID已经没用，这个暂时用来存储老的clickid
                'tag_id'=>$click['tag_id'],
                'city_id' => $click['area_fid'] . ',' . $click['area_id'],
                'keyword' => $click['keyword'],
                'query' => $click['query'],
                'matchtype' => 0,//这个暂时没有
                'click_id' => substr(md5($click['ip'].$click['view_id']. $click['ad_advert_id'] . php_uname('n').posix_getpid() . microtime(true)), 8, 16),
                'channel_id' => (int) $click['cid'],
                'place_id' => (int) $click['pid'],
                'ls' => (string) $click['ls'],
                'src' => (string) $click['src'],
                'reqsrc' => (string) $click['src'],//shouzhu专用
                'guid' => '',
                'site' => '',
                'ver' => $click['ver'],
                'subver' => $click['sub_ver'],
                'subdata' => $click['sub_data'],
                'sub_ad_info' =>$click['sub_ad_info'],
                'buckettest' => 0,
                'source_type'=>(int) $click['source_type'],
                'source_system'=>$click['source_system'],
                'app_cid'=>$click['app_cid'],
                'ad_keyword_id'=>date('j').getmypid(),//这个用日期表示今天添加的数据
                'need_send_to_cheat'=>$need_send_to_cheat,//表示是否需要发送至反作弊
            );

            $queue->push($arr);
            ComAdDetail::queryBySql("update click_detail set extension=-3,update_time=".time()." where click_id='".$click_id."'",strtotime($date),'exec');

            echo date("Y-m-d H:i:s")." {$click_id} aborted {$arr['click_id']} created  uid {$arr['uid']} price {$arr['price']}\n";
            Utility::log(__CLASS__,__FUNCTION__," {$click_id} aborted {$arr['click_id']} created");
        }

    }

    //这个方法的功能是抽取部分反作弊的点击，当成正常点击计费，必须是在当天24点之前完成。
    //$account_min_cost 选取的用户，取昨天消费金额大于这么多的用户,$percent_total_cheat抽取反作弊多少的百分比,$need_earn_money总共需要捞回多少钱
    public function actionReCostCheatClick($account_min_cost,$percent_total_cheat,$need_earn_money,$email='renyajun@360.cn')
    {
        set_time_limit(0);
        ini_set("memory_limit","4096M");
        echo date('Y-m-d H:i:s')." start\n";
        //第一步，获取昨天符合消费的用户，随机分散
        $user_cost=ComAdDetail::queryBySql("select ad_user_id,sum(price-reduce_price) su from click_detail where deal_status=1  group by ad_user_id having su>{$account_min_cost}",strtotime("-1 days"),'all');
        shuffle($user_cost);
        $has_earn_money=0;
        $has_earn_clicks=0;
        $today_time=time();
        $today_date=date('j').getmypid();

        foreach($user_cost as $u)
        {
            if($has_earn_money>=$need_earn_money)
            {
                break;
            }
            $uid=$u['ad_user_id'];
            $uid_cost=$u['su'];
            $quota=ComQuota::getUserQuotaByUserId($uid);
            if($quota['balance']<=0 ||($quota['dj_cost']>=$quota['dj_quota'] && $quota['dj_quota']>0)||$quota['balance']-$quota['dj_cost']-$quota['mv_cost']-$quota['yesterday_dj_cost']-$quota['yesterday_mv_cost']<=0)
                continue;

            //第二步,获取这些账户的所有算法反作弊金额
            $uid_cheat=ComAdDetail::queryBySql("select sum(price) su,count(*) cn from click_detail where deal_status=0 and cheat_type=3 and ad_user_id={$uid} and price=reduce_price ",$today_time,'row');
            if($uid_cheat['cn']==0 || $uid_cheat['cn']*$percent_total_cheat<1)
                continue;

            $cheat_clicks=ComAdDetail::queryBySql("select click_id,price,ad_plan_id from click_detail where deal_status=0 and cheat_type=3 and ad_user_id={$uid} and price=reduce_price ",$today_time,'all');
            shuffle($cheat_clicks);
            $uid_cheat_money=0;
            $uid_click_ids=array();
            $uid_plan_money=array();
            foreach($cheat_clicks as $clk)
            {
                //判断是否已经大于反作弊总比例了
                if(($uid_cheat_money/$uid_cheat['su']) >$percent_total_cheat)
                    break;
                //看下账户预算这个点击是否够扣
                if($quota['dj_cost']+$uid_cheat_money>=$quota['dj_quota'] && $quota['dj_quota']>0)
                {
                    break;
                }
                //用户余额是否够扣
                if($quota['balance']-$quota['dj_cost']-$quota['mv_cost']-$quota['yesterday_dj_cost']-$quota['yesterday_mv_cost']-$uid_cheat_money<=0)
                {
                    break;
                }

                //获取计划消耗情况,暂时不需要大于110%
                $sql = sprintf("select  *  from  %s where ad_user_id=%d and ad_plan_id=%d", "ad_plan_quota_".$uid%10,$uid,$clk['ad_plan_id']);
                $plan_quota = Yii::app()->db_quota->createCommand($sql)->queryRow();
                if($plan_quota['plan_quota']>0 && $plan_quota['plan_cost']+(float)$uid_plan_money[$clk['ad_plan_id']]>=$plan_quota['plan_quota'])
                {
                    //计划没钱可以消耗了
                   continue;
                }

                $uid_click_ids[]=$clk['click_id'];
                $uid_cheat_money+=$clk['price'];
                $has_earn_money+=$clk['price'];
                $uid_plan_money[$clk['ad_plan_id']]=(float)$uid_plan_money[$clk['ad_plan_id']] + $clk['price'];
                $has_earn_clicks++;
                if($has_earn_money>=$need_earn_money)
                {
                    break;
                }
            }

            $this->actionReCostClickIdStep2("",implode(",",$uid_click_ids),false);
        }
        //等待计费完成
        sleep(600);
        $percent_total_cheat=intval($percent_total_cheat*100);
        $real_earned=ComAdDetail::queryBySql("select sum(price-reduce_price) as su from  click_detail  where ad_keyword_id={$today_date}",$today_time,'scalar');

        $detail=ComAdDetail::queryBySql("select sum(price-reduce_price) as su,ad_user_id,count(*) as cnt from  click_detail  where ad_keyword_id={$today_date} group by ad_user_id",$today_time,'all');
        $table="<table border=1><tr><td>用户UID</td><td>计费金额</td><td>计费数量</td></tr>";
        foreach($detail as $d)
        {
            $table .= "<tr><td>{$d['ad_user_id']}</td><td>{$d['su']}</td><td>{$d['cnt']}</td></tr>";
        }
        $table .="</table>";
        $body="重新计费目标设定增加收入:{$need_earn_money}元<br/>账户最低日消耗:{$account_min_cost}元<br/>抽取反作弊点击百分比{$percent_total_cheat}%<br/><br/>重新计费后实际收回收入：{$real_earned}元<br/>总共重新计算{$has_earn_clicks}个反作弊点击 <br/><br/>".$table;
        Utility::sendEmail("renyajun@360.cn", $email,date('Y-m-d',$today_time)."反作弊重新计费收入提醒", $body);

        echo date('Y-m-d H:i:s')." done {$body} ad_keyword_id={$today_date}\n";
    }

    //增量同步-添加计划
    public function actionIncrementAddPlanSubScriber($count=1)
    {
        $emq = new ComEMQ('emq_wuliao');
        $emq->exchangeName = 'ex_new_plan';
        $emq->checkInterval = 20;           //多少秒检查一次
        $emq->alarmReceiveTime = 60*60*2;   //超过多少秒发送时间和接受时间差就报警
        $emq->exitTime = 30*60;             //30分钟
        $emq->startMultiProcessSubscriber($count);
    }

    //增量同步-添加计划
    public static function _incrementAddPlanSubScriber($msg)
    {
        $planQuota = array();
        try{
            $data = json_decode($msg->body,true);
            if (!$data) {
                sleep(2);
                throw new Exception("json_decode error");
            }
            //增加消息日志便于排查
            Utility::log(__CLASS__,__FUNCTION__,$msg->body);
            $content = $data['content'];
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
                    $info = date("Y-m-d H:i:s")."\tuid:{$uid} refreshOneUser {$info}\t plan_id:{$content['id']}";
                    Utility::log(__CLASS__,__FUNCTION__,$info);
                }else{
                    $errorInfo = "uid:{$uid} getUserQuota failed";
                    Utility::log(__CLASS__,__FUNCTION__,$errorInfo);
                }
            }else{
                $errorInfo = "uid:{$uid} Get Add a plan message parameter error\n";
                Utility::log(__CLASS__,__FUNCTION__,$errorInfo.$msg->body);
            }

            ComEMQ::receiveSuccess($msg);
            echo date("Y-m-d H:i:s")." {$content['id']} ok\n";
        } catch (Exception $ex) {
            Utility::sendAlert(__CLASS__, __FUNCTION__, $ex->getMessage());
            ComEMQ::receiveFail($msg, true);
            sleep(1);
        }
    }

    //增量同步-更新计划
    public function actionIncrementModPlanSubScriber($count=1)
    {
        $emq = new ComEMQ('emq_wuliao');
        $emq->exchangeName = 'ex_plan_update';
        $emq->checkInterval = 20;           //多少秒检查一次
        $emq->alarmReceiveTime = 60*60*2;   //超过多少秒发送时间和接受时间差就报警
        $emq->exitTime = 30*60;             //30分钟
        $emq->startMultiProcessSubscriber($count);
    }

    //增量同步-更新计划
    public static function _incrementModPlanSubScriber($msg)
    {
        $flag = true;
        $planQuota = array();
        try{
        $data = json_decode($msg->body,true);
        if (!$data) {
            sleep(2);
            throw new Exception("json_decode error");
        }
        //增加消息日志便于排查
        Utility::log(__CLASS__,__FUNCTION__,$msg->body);
        $content = $data['content']['before'];
        $uid = $content['ad_user_id'];
        $after = $data['content']['data'];
        $ad_plan_id = 0;
        //判断是否更新计划限额 exp_amt:0是不限
        if(isset($after['exp_amt'])){

            if(isset($content['id'])){

                $ad_plan_id = $content['id'];
                $exp_amt = $after['exp_amt'];
                //获取计划的限额参数
                $planQuota = array($content['id']=>$after['exp_amt']);

                //获取用户限额及余额
                $userArr = ComQuota::getUserBalance($uid);

                //更新用户、计划限额、余额
                if (!empty($userArr)) {
                    $ret = ComQuota::updateIncrementQuota($userArr,$planQuota);
                    $info = $ret?'success':'failed';
                    $info = date("Y-m-d H:i:s")."\tuid:{$uid} refreshOneUser {$info}\t plan_id:{$content['id']}";
                    Utility::log(__CLASS__,__FUNCTION__,$info);

                    //给消息基线发上下线消息
                    if ($content['exp_amt'] > 0 && ($after['exp_amt'] == 0 || $after['exp_amt'] > $content['exp_amt'])){
                        $data['uid'] = $uid;
                        $sData = ComMsg::setPlanMsgData($data,1,$ret);
                        ComMsg::sendMsg(1,$ret,$sData,$uid);
                    }
                }else{
                    $errorInfo = "uid:{$uid} getUserQuota failed";
                    Utility::log(__CLASS__,__FUNCTION__,$errorInfo);
                }
            }else{
                $errorInfo = "uid:{$uid} Get update a plan message parameter error";
                Utility::log(__CLASS__,__FUNCTION__,$errorInfo.$msg->body);
            }
        }else{
            $errorInfo = "uid:{$uid} There is no change in the plan quota";
            Utility::log(__CLASS__,__FUNCTION__,$errorInfo.$msg->body);
        }

        //增加产品线
        $plat_type = $data['content']['plat_type'];
        if(in_array($plat_type, array(1,3))){
            //增加给引擎发送消息 jingguangwen 20150812 add
            $user_quota_arr = ComQuota::getUserQuotaByUserId($uid);
            if (!empty($user_quota_arr) && $ad_plan_id != 0) {

                $dj_quota = $user_quota_arr['dj_quota'];
                $real_time_balance = max(round($user_quota_arr['balance'] - $user_quota_arr['dj_cost'] - $user_quota_arr['mv_cost'] - $user_quota_arr['yesterday_dj_cost'] - $user_quota_arr['yesterday_mv_cost'],2),0);
                //$user_today_cost = max(round($user_quota_arr['dj_cost'],2),0);
                $plan_quota = $exp_amt;
                if($plat_type==1){
                    $product_budget = $user_quota_arr['sou_quota'];
                    $product_today_cost = $user_quota_arr['sou_cost'];
                } else {
                    $product_budget = $user_quota_arr['ruyi_quota'];
                    $product_today_cost = $user_quota_arr['ruyi_cost'];
                }

                $user_quota_data = array(
                    'quota'     => 0,//
                    'balance'   => $real_time_balance,
                    'product_budget'   => $product_budget,
                    'product_today_cost'   => $product_today_cost,
                );
                ComBudgetData::sendPlanQuota($uid, $ad_plan_id, $plan_quota, $user_quota_data, $product_today_cost,$plat_type);


            } else{
                $errorInfo = "uid:{$uid} plan_id{$ad_plan_id} get ad_user_quota  failed\n";
                Utility::log(__CLASS__,__FUNCTION__,$errorInfo.$msg->body);
            }
        }
        Yii::app()->db_quota->setActive(false);
        //add  end
        ComEMQ::receiveSuccess($msg);
        echo date("Y-m-d H:i:s")." {$content['id']} ok\n";
        } catch (Exception $ex) {
            Utility::sendAlert(__CLASS__, __FUNCTION__, $ex->getMessage());
            ComEMQ::receiveFail($msg, true);
            sleep(1);
        }
    }

    //增量同步-用户限额
    public function actionIncrementUserQuotaSubScriber($count=1)
    {
        $emq = new ComEMQ('emq_wuliao');
        $emq->exchangeName = 'ex_update_user_quota';
        $emq->checkInterval = 20;           //多少秒检查一次
        $emq->alarmReceiveTime = 60*60*2;   //超过多少秒发送时间和接受时间差就报警
        $emq->exitTime = 30*60;             //30分钟
        $emq->startMultiProcessSubscriber($count);
    }

    //增量同步-用户限额
    //exp_amt、day_quota等限额,0不限
    public static function _incrementUserQuotaSubScriber($msg)
    {
        $flag = 0;
        $planQuota = array();
        try{
            $data = json_decode($msg->body,true);
            if (!$data) {
                sleep(2);
                throw new Exception("json_decode error");
            }
            //增加消息日志便于排查
            Utility::log(__CLASS__,__FUNCTION__,$msg->body);
            $content = $data['content']['before'];
            $after = $data['content']['data'];
            $uid = $after['ad_user_id'];

            //获取产品线限额及余额
            $userArr = ComQuota::getUserBalance($uid);
            if (empty($userArr)) {
                $errorInfo = "uid:{$uid} getUserQuota failed\n";
                Utility::log(__CLASS__,__FUNCTION__,$errorInfo.$msg->body);
            }else{
                //兼容新老不同消息内容和格式
                if(isset($after['quota_type'])){
                    //sou
                    if(1 == $after['quota_type']){
                        $userArr['quota_dianjing'] = $after['quota_after'];
                    }elseif(2 == $after['quota_type']){
                        $userArr['quota_app'] = $after['quota_after'];
                    }elseif(3 == $after['quota_type']){
                        $userArr['quota_ruyi'] = $after['quota_after'];
                    }elseif(4 == $after['quota_type']){
                        $userArr['mv_quota'] = $after['quota_after'];
                    }
                    if(($after['quota_before'] > 0 && $after['quota_after'] > $after['quota_before']) || ($after['quota_after'] == 0 && $after['quota_before'] >0 )){
                        $flag = 1;
                    }

                }else{
                    //判断是否day_quota 或mv_quota限额
                    if(isset($after['day_quota'])){
                        $userArr['day_quota'] = $after['day_quota'];
                        if(($after['before'] > 0 && $after['day_quota'] > $after['before']) || ($after['day_quota'] == 0 && $after['before'] >0 )){
                            $flag = 1;
                        }
                    }elseif(isset($after['mv_quota'])){
                        $userArr['mv_quota'] = $after['mv_quota'];
                        if(($after['before'] > 0 && $after['mv_quota'] > $after['before']) || ($after['mv_quota'] == 0 && $after['before'] >0 )){
                            $flag = 1;
                        }
                    }
                }
                $ret = ComQuota::updateIncrementQuota($userArr,$planQuota);
                $info = $ret?'success':'failed';
                $info = date("Y-m-d H:i:s")."\tuid:{$uid} refreshOneUser {$info}";
                Utility::log(__CLASS__,__FUNCTION__,$info);

                //给消息基线发上下线消息
                if (1 == $flag) {
                    $data['uid'] = $uid;
                    $sData = ComMsg::setProductQuotaMsgData($data,1);
                    ComMsg::sendMsg(1,$ret,$sData,$uid);
                }
            }

            //增加产品线
            $plat_type = $data['content']['data']['quota_type'];
            if(in_array($plat_type, array(1,3))){

                //增加给引擎发送消息 jingguangwen 20150812 add
                 $user_quota_arr = ComQuota::getUserQuotaByUserId($uid);
                if (!empty($user_quota_arr)) {

                    //$dj_quota = $user_quota_arr['dj_quota'];
                    $dj_quota = 0;
                    $real_time_balance = max(round($user_quota_arr['balance'] - $user_quota_arr['dj_cost'] - $user_quota_arr['mv_cost'] - $user_quota_arr['yesterday_dj_cost'] - $user_quota_arr['yesterday_mv_cost'],2),0);

                    if($plat_type==1){
                        $product_budget = $user_quota_arr['sou_quota'];
                    } else {
                        $product_budget = $user_quota_arr['ruyi_quota'];
                    }

                    ComBudgetData::sendUserQuota($uid, $dj_quota, $real_time_balance, $product_budget, $plat_type);
                } else{
                    $errorInfo = "uid:{$uid} get ad_user_quota_ failed\n";
                    Utility::log(__CLASS__,__FUNCTION__,$errorInfo.$msg->body);
                }
            }
            Yii::app()->db_quota->setActive(false);
            //add  end
            ComEMQ::receiveSuccess($msg);
            echo date("Y-m-d H:i:s")." {$uid} ok\n";
        } catch (Exception $ex) {
            Utility::sendAlert(__CLASS__, __FUNCTION__, $ex->getMessage());
            ComEMQ::receiveFail($msg, true);
            sleep(1);
        }
    }

    //增量同步-用户余额
    public function actionIncrementUserBalanceSubScriber($count=1)
    {
        $emq = new ComEMQ('emq_esc');
        $emq->exchangeName = 'ex_pay_success_deal';
        $emq->checkInterval = 20;           //多少秒检查一次
        $emq->alarmReceiveTime = 60*60*2;   //超过多少秒发送时间和接受时间差就报警
        $emq->exitTime = 30*60;             //30分钟
        $emq->startMultiProcessSubscriber($count);
    }

    //增量同步-用户余额
    public static function _incrementUserBalanceSubScriber($msg)
    {
        $planQuota = array();
        try{
            $data = json_decode($msg->body,true);
            if (!$data) {
                sleep(2);
                throw new Exception("json_decode error");
            }
            //增加消息日志便于排查
            Utility::log(__CLASS__,__FUNCTION__,$msg->body);
            $uid = $data['content']['ad_user_id'];

            //获取用户限额及余额
            $userArr = ComQuota::getUserBalance($uid);
            if (empty($userArr)) {
                $errorInfo = "uid:{$uid} getUserBalance failed\n";
                Utility::log(__CLASS__,__FUNCTION__,$errorInfo.$msg->body);
            }else{
                $ret = ComQuota::updateIncrementQuota($userArr,$planQuota);
                $info = $ret?'success':'failed';
                $info = date("Y-m-d H:i:s")."\tuid:{$uid} refreshOneUser {$info}";
                Utility::log(__CLASS__,__FUNCTION__,$info);
            }

            //如果是1代表是正值发上线消息，不是1说明为撤销充值或部分撤销(金额小于0或是负数)
            if( 1 == $data['content']['data']['set_ad_online']){
                //给消息基线发上下线消息

                //消息优化需求 20160620
                //充值和反作弊上线判断消费和限额，达到限额的产品线不发上线
                $costInfo = ComQuota::getUserCost($uid);
                if (empty($costInfo)) {
                    $errorInfo = "uid:{$uid} getUserCost failed\n";
                    Utility::log(__CLASS__,__FUNCTION__,$errorInfo.$msg->body);
                }else{
                    $productLine = ComMsg::setProductLineOnlineData($costInfo); 
                }

                //如果查库异常，默认发上线
                if(false === $productLine){
                    $productLine = array(1,2,3,4);
                }

                $data['uid'] = $uid;
                $data['product_line_arr'] = $productLine;
                $sData = ComMsg::setPayMsgData($data,1);
                ComMsg::sendMsg(1,'',$sData,$uid);
                //给消息基线发上下线消息结束
            }

            //增加给引擎发送消息 jingguangwen 20150812 add
            $user_quota_arr = ComQuota::getUserQuotaByUserId($uid);
            if (!empty($user_quota_arr)) {

                $real_time_balance = max(round($user_quota_arr['balance'] - $user_quota_arr['dj_cost'] - $user_quota_arr['mv_cost'] - $user_quota_arr['yesterday_dj_cost'] - $user_quota_arr['yesterday_mv_cost'],2),0);
                ComBudgetData::sendUserBalance($uid, $real_time_balance);
            } else{
                $errorInfo = "uid:{$uid} get ad_user_quota_ failed\n";
                Utility::log(__CLASS__,__FUNCTION__,$errorInfo.$msg->body);
            }
            Yii::app()->db_quota->setActive(false);
            //add  end
            ComEMQ::receiveSuccess($msg);
            echo date("Y-m-d H:i:s")." {$uid} ok\n";
        } catch (Exception $ex) {
            Utility::sendAlert(__CLASS__, __FUNCTION__, $ex->getMessage());
            ComEMQ::receiveFail($msg, true);
            sleep(1);
        }
    }

    //增量同步-新增用户
    public function actionIncrementUpUserSubScriber($count=1)
    {
        $emq = new ComEMQ('emq_wuliao');
        $emq->exchangeName = 'ex_user_status_change';
        $emq->checkInterval = 20;           //多少秒检查一次
        $emq->alarmReceiveTime = 60*60*2;   //超过多少秒发送时间和接受时间差就报警
        $emq->exitTime = 30*60;             //30分钟
        $emq->startMultiProcessSubscriber($count);
    }

    //增量同步-新增用户
    public static function _incrementUpUserSubScriber($msg)
    {
        $planQuota = array();
        try{
            $data = json_decode($msg->body,true);
            if (!$data) {
                sleep(2);
                throw new Exception("json_decode error");
            }

            //增加消息日志便于排查
            Utility::log(__CLASS__,__FUNCTION__,$msg->body);
            $uid = $data['content']['ad_user_id'];


            //获取用户限额及余额
            $userArr = ComQuota::getUserBalance($uid);
            if (empty($userArr)) {
                $errorInfo = "uid:{$uid} getUserBalance failed\n";
                Utility::log(__CLASS__,__FUNCTION__,$errorInfo.$msg->body);
            }else{
                $ret = ComQuota::updateIncrementQuota($userArr,$planQuota);
                $info = $ret?'success':'failed';
                $info = date("Y-m-d H:i:s")."\tuid:{$uid} refreshOneUser {$info}";
                Utility::log(__CLASS__,__FUNCTION__,$info);
            }

            ComEMQ::receiveSuccess($msg);
            echo date("Y-m-d H:i:s")." {$uid} ok\n";

        } catch (Exception $ex) {
            Utility::sendAlert(__CLASS__, __FUNCTION__, $ex->getMessage());
            ComEMQ::receiveFail($msg, true);
            sleep(1);
        }
    }

    //增量同步-批量修改计划预算
    public function actionIncrementBatchUpPlanSubScriber($count=1)
    {
        $emq = new ComEMQ('emq_wuliao');
        $emq->exchangeName = 'ex_plan_update_exp_all';
        $emq->checkInterval = 20;           //多少秒检查一次
        $emq->alarmReceiveTime = 60*60*2;   //超过多少秒发送时间和接受时间差就报警
        $emq->exitTime = 30*60;             //30分钟
        $emq->startMultiProcessSubscriber($count);
    }

    //增量同步-批量修改计划预算
    public static function _incrementBatchUpPlanSubScriber($msg)
    {
        $planQuota = array();
        try{
            $data = json_decode($msg->body,true);
            if (!$data) {
                sleep(2);
                throw new Exception("json_decode error");
            }

            //增加消息日志便于排查
            Utility::log(__CLASS__,__FUNCTION__,$msg->body);
            $uid = $data['content']['user_id'];
            $before = $data['content']['before'];
            $planStr = $data['content']['planIds'];
            $planIds = explode(',',$planStr);
            $expAmt = $data['content']['exp_amt'];

            //获取用户限额及余额
            $userArr = ComQuota::getUserBalance($uid);
            if (empty($userArr)) {
                $errorInfo = "uid:{$uid} getUserBalance failed\n";
                Utility::log(__CLASS__,__FUNCTION__,$errorInfo.$msg->body);
            }else{
                foreach($planIds as $planId){
                    $planQuota = array($planId=>$expAmt);
                    $ret = ComQuota::updateIncrementQuota($userArr,$planQuota);
                    $info = $ret?'success':'failed';
                    $info = date("Y-m-d H:i:s")."\tuid:{$uid} refreshOneUser {$info}\tplan_id:{$planId}";
                    Utility::log(__CLASS__,__FUNCTION__,$info);


                }

                //给消息基线发上下线消息
                foreach($before as $v){
                    //需要发给消息基线发上下线消息的计划
                    if ($v['exp_amt'] > 0 && ($exp_amt == 0 || $exp_amt > $v['exp_amt'])){
                        $sPlanids[] = $v['id'];
                        $sExpamts[] = $v['exp_amt'];
                    }
                }
                if(!empty($sPlanids)){
                    $sData = ComMsg::setBatchPlanMsgData($data,1,$ret,$sPlanids,$sExpamts);
                    ComMsg::sendMsg(1,$ret,$sData,$uid);
                }
            }

            //增加产品线
            $plat_type = $data['content']['plat_type'];
            if(in_array($plat_type, array(1,3))){

                //增加给引擎发送消息 jingguangwen 20150812 add
                $user_quota_arr = ComQuota::getUserQuotaByUserId($uid);
                if (!empty($user_quota_arr)) {

                    //$dj_quota = $user_quota_arr['dj_quota'];
                    $dj_quota = 0;
                    $real_time_balance = max(round($user_quota_arr['balance'] - $user_quota_arr['dj_cost'] - $user_quota_arr['mv_cost'] - $user_quota_arr['yesterday_dj_cost'] - $user_quota_arr['yesterday_mv_cost'],2),0);
                    //$user_today_cost = max(round($user_quota_arr['dj_cost'],2),0);

                    $plan_quota = $expAmt;

                    if($plat_type==1){
                        $product_budget = $user_quota_arr['sou_quota'];
                        $product_today_cost = $user_quota_arr['sou_cost'];
                    } else {
                        $product_budget = $user_quota_arr['ruyi_quota'];
                        $product_today_cost = $user_quota_arr['ruyi_cost'];
                    }

                    $user_quota_data = array(
                        'quota'     => 0,//
                        'balance'   => $real_time_balance,
                        'product_budget'   => $product_budget,
                        'product_today_cost'   => $product_today_cost,
                    );


                    ComBudgetData::sendPlanQuotaBatch($uid, $planIds, $plan_quota, $user_quota_data, $product_today_cost, $plat_type);
                } else{
                    $errorInfo = "uid:{$uid}  get ad_user_quota failed\n";
                    Utility::log(__CLASS__,__FUNCTION__,$errorInfo.$msg->body);
                }
            }
            Yii::app()->db_quota->setActive(false);
            //add  end
            ComEMQ::receiveSuccess($msg);
            echo date("Y-m-d H:i:s")." {$planStr} ok\n";
        } catch (Exception $ex) {
            Utility::sendAlert(__CLASS__, __FUNCTION__, $ex->getMessage());
            ComEMQ::receiveFail($msg, true);
            sleep(1);
        }
    }

    private $child_pids=array();//子进程id
    //批量切换日期脚本
    //这个脚本必须在0点后开始执行
    public function actionMultiSwitchCurDate()
    {
        set_time_limit(0);
        ini_set('memory_limit', '5048M');
        $count=0;

        $centerDB = Yii::app()->db_center;
        $daoUser        = new User();
        $daoUser->setDB($centerDB);

        echo date('Y-m-d H:i:s')." start \n";
        //切换用户，按照有消费的先切换的原则，尽快切换
        $cost_users=ComAdDetail::queryBySql("select distinct(ad_user_id) uid from click_detail where ad_user_id>0",strtotime($cur_date)-60*24*24,'all');
        $cost_users_kv=array();
        $switch_user_ids =array();
        $count = $count_all = 0;
        while ($user_arr=array_pop($cost_users)) {

            $uid = $user_arr['uid'];
            $cost_users_kv[$uid]=1;
            $switch_user_ids[$uid%10][] = $uid;
            $count++;
            $count_all++;
        }

        $lastUserID     = 0;
        $userListLimit  = 10000;
        //查询昨天未消费的账户
        while (true) {

            // 获取所有用户
            $user_arrs = $daoUser->getQuotaUser($lastUserID, $userListLimit);
            if (empty($user_arrs)) {
                break;
            }
            foreach ($user_arrs as $user_arr)
            { // 遍历每一个用户

                $lastUserID = $uid = $user_arr['id'];
                if(isset($cost_users_kv[$uid]))
                    continue;
                $switch_user_ids[$uid%10][] = $uid;
                $count_all++;

            }

        }
        //断开db连接
        Yii::app()->db_center->setActive(false);
        if(!empty($switch_user_ids)){
            foreach ($switch_user_ids as $child_id => $user_ids_arr) {
                $this->childSwitchCurDate($user_ids_arr);
            }
        }
        foreach($this->child_pids as $pid=>$t)
        {
            pcntl_waitpid($pid, $status,WUNTRACED);//等待退出
        }
        echo date('Y-m-d H:i:s')." cost user ok  {$count} done{$count_all}\n";
    }
    protected function childSwitchCurDate($user_ids_arr)
    {
        $pid=pcntl_fork();
        if($pid<0)
        {
            echo "[".date('Y-m-d H:i:s')."] Click::MultiSwitchCurDate PID_".getmypid()." fork error\n";
            exit();
        }
        else if($pid==0)
        {

            if(empty($user_ids_arr)){
                exit();
            }
            $cur_date=date('Y-m-d');
            $centerDB = Yii::app()->db_center;
            $daoUser        = new User();
            $daoDbRouter    = new DbRouter();
            $daoUser->setDB($centerDB);
            $daoDbRouter->setDB($centerDB);
            $materialDB = DbConnectionManager::getMaterialDB();
            $count = 0;
            foreach ($user_ids_arr as $uid) {
                try
                {
                    $user=$daoUser->getInfoByID($uid);
                    if(!$user)
                        continue;
                    $dbID = $daoDbRouter->getRouter($uid);

                    if(!$dbID)
                        continue;
                    $djBranchDB = DbConnectionManager::getDjBranchDB($dbID);
                    if(!$djBranchDB)
                        continue;

                    $sql = sprintf("select id, exp_amt from ad_plan where ad_user_id=%d", $uid);
                    $plans=$djBranchDB->createCommand($sql)->queryAll();
                    $plan_quotas=array();
                    foreach($plans as $p)
                    {
                        $plan_quotas[$p['id']]=$p['exp_amt'];
                    }

                    //添加布尔的计划限额数据
                    $sql_app = sprintf("select id, exp_amt from app_plan where ad_user_id=%d", $uid);

                    $app_plans=$materialDB->createCommand($sql_app)->queryAll();

                    if(!empty($app_plans)){

                        foreach($app_plans as $p)
                        {
                            $plan_quotas[$p['id']]=$p['exp_amt'];
                        }
                    }

                    $ret=ComQuota::switchUserCurDate($cur_date,$uid,$user['day_quota'],$user['mv_quota'],$plan_quotas,$user['quota_dianjing'],$user['quota_ruyi'],$user['quota_app']);
                    if($ret)
                    {
                        $count++;
                        echo date('Y-m-d H:i:s')." {$uid} success \n";
                    }
                    else
                        echo date('Y-m-d H:i:s')." {$uid} failed \n";

                }
                catch(Exception $ex)
                {
                    echo $ex->getMessage();
                }
            }
            echo "[".date('Y-m-d H:i:s')."] Click::MultiSwitchCurDate PID_".getmypid()." child ".getmypid()." exited;Success deal users {$count}\n";
            //posix_kill(getmypid(), 9);
            exit();
        }
        else
        {

            $this->child_pids[$pid]=time();
            echo "[".date('Y-m-d H:i:s')."] Click::MultiSwitchCurDate PID_".getmypid()." create child $pid \n";
        }
    }

    //MV超投数据
    public function actionMediavDataSubScriber($count=1)
    {
        $emq = new ComEMQ('esc_emq');
        $emq->exchangeName = 'click_info';
        $emq->checkInterval = 20;           //多少秒检查一次
        $emq->alarmReceiveTime = 60*60*2;   //超过多少秒发送时间和接受时间差就报警
        $emq->exitTime = 30*60;             //30分钟
        $emq->startMultiProcessSubscriber($count);
    }

    //MV超投数据
    public static function _mediavDataSubScriber($msg)
    {
        $sendData = array();
        Utility::log(__CLASS__,__FUNCTION__,$msg->body);
        $data = json_decode($msg->body,true);
        $content = $data['content'];

        if($content['ver'] == 'mediav' && !(isset($content['file_click']) && $content['file_click']==1) ){

            if(round($content['clickPrice'],2) != round($content['price'],2)){

                static $emq;
                $emq=new ComEMQ('emq_esc');
                $emq->exchangeName='mediav_over_limit_data';
                $emq->logid=Utility::getLoggerID('ESC');

                if($content['type'] == 'click'){

                    $sendData['clickId'] = $content['click_id'];
                    $sendData['uid'] = $content['uid'];
                    $sendData['cost'] = $content['price']*100;
                    $sendData['realCost'] = $content['clickPrice']*100;
                    $sendData['reason'] = $content['cost_info']['offline_type'];
                    list($parentId,$geoId) = explode(",", $content['city_id']);
                    $sendData['geoId']=$geoId;
                    $sendData['parentId']=$parentId;
                    $sendData['timestamp']=$content['now'];
                    $res = $emq->send($sendData,0);
                    Utility::log(__CLASS__,__FUNCTION__,$res);

                    if($res){
                        Utility::log(__CLASS__,"SENDMVSUCCESS",$sendData);
                    }else{
                        ComEMQ::receiveFail($msg, true);
                        Utility::log(__CLASS__,"SENDMVFAIL",$sendData);
                        return;
                    }
                }
            }

        }else{
            Utility::log(__CLASS__,"NOTMVDATA",$content['click_id'].':NOT MV DATA');
        }
        ComEMQ::receiveSuccess($msg);
    }


    /**
     * 更新布尔账户计划余额以及用户与计划的限额
     * @author jingguangwen@360.cn  20150713
     * user_status 空：全部用户,不空，则用status 状态字段逗号分隔
     */
    public function actionRefreshBoleBalanceAndBudget($user_status="",$plan_status="") {
        set_time_limit(0);
        $cur_date=date('Y-m-d');
        $count=0;
        $centerDB = DbConnectionManager::getDjCenterDB();
        $daoUser        = new User();
        $daoUser->setDB($centerDB);

        $materialDB = DbConnectionManager::getMaterialDB();
        $lastUserID     = 0;
        $userListLimit  = 1;//这个暂时不能改成多个，因为读到用户信息之后，可能很久才会更新到限额库，中间数据可能会发生变化，产生脏读。
        echo date('Y-m-d H:i:s')." start \n";
        while (true) {
            try
            {
                // 获取所有用户
                $user_arrs = $daoUser->getQuotaUser($lastUserID, $userListLimit,($user_status?explode(",", $user_status):array()));
                if (empty($user_arrs)) {
                    break;
                }
                foreach ($user_arrs as $user_arr) { // 遍历每一个用户

                    $lastUserID = $uid = $user_arr['id'];

                    $plan_quotas=array();

                    //添加布尔的计划限额数据
                    $sql_app = sprintf("select id, exp_amt from app_plan where ad_user_id=%d", $uid);
                    if($plan_status)
                        $sql_app = sprintf("select id, exp_amt from app_plan where ad_user_id=%d and status in (%s)", $uid,$plan_status);

                    $app_plans=$materialDB->createCommand($sql_app)->queryAll();

                    if(!empty($app_plans)){

                        foreach($app_plans as $p)
                        {
                            $plan_quotas[$p['id']]=$p['exp_amt'];
                        }
                    }


                    //更新余额以及限额
                    $ret=ComQuota::refreshOneUser($uid,$user_arr['balance'],$user_arr['day_quota'],$user_arr['mv_quota'],$plan_quotas,$cur_date,$user_arr['quota_dianjing'],$user_arr['quota_ruyi'],$user_arr['quota_app']);
                    if($ret)
                    {
                        $count++;
                        echo date('Y-m-d H:i:s')." {$uid} ok \n";
                    }
                    else
                        echo date('Y-m-d H:i:s')." {$uid} failed \n";
                }
            }
            catch(Exception $ex)
            {
                echo date('Y-m-d H:i:s')." {$uid} ".$ex->getMessage()."\n";
            }
        }

        echo date('Y-m-d H:i:s').' done '.$count."\n";

    }

    public function actionPushBool2Os($date=null) {
        ini_set('memory_limit', '20480M');
        $taskName = __CLASS__ . '_' . __FUNCTION__;
        $beginTime = date('Y-m-d H:i:s');
        printf("%s begin at %s\n", $taskName, $beginTime);

        $path = Config::item('os_click_detail');
        if(empty($path)) {
            printf("%s error[invalid conf : os_click_detail ]\n", $taskName);
            return;
        }

        //禁止多进程
        $strMutexFile = sprintf('%s%s.lock', $path, __FUNCTION__);
        $resFd = fopen($strMutexFile, 'a');
        if($resFd === FALSE) {
            printf("%s error, when open mutex file \n", $taskName);
            return ;
        }
        $flock = flock($resFd, LOCK_EX | LOCK_NB);
        if($flock === FALSE) {
            printf("%s has been running \n", $taskName);
            fclose($resFd);
            return ;
        }

        $lastTime = strtotime(date('Ymd').' -1 second');
        if (is_null($date)) {
            $date = date('Ymd', strtotime('-1 day'));
        } else {
            $date = date('Ymd', strtotime($date));
        }
        $dateTimeStamp = strtotime($date);
        if ($dateTimeStamp>$lastTime) {
            printf("%s can not run\n", $taskName);
            return ;
        }

        $clickDetailFd = fopen($path.$date,'w');
        if($clickDetailFd === FALSE) {
            printf("%s has been running \n", $taskName);
            return ;
        }

        $db = DbConnectionManager::getDB('click_log_slave');
        if (false === $db) {
            printf("%s get click_log  db fail\n", $taskName);
            return;
        }
        $time = strtotime($date);
        $clickDetailTable = 'click_detail_' . date('Ymd', $time);

        $sql="SELECT cid,mid m2,price-reduce_price cost, src FROM $clickDetailTable where status NOT IN (-1,2) AND deal_status=1 AND ver='shouzhu' AND cheat_type NOT IN (2,3) AND price != reduce_price AND cid IN (59,65,89)";
        $data=$db->createCommand($sql)->queryAll();
        foreach($data as $row) {
            fwrite($clickDetailFd, json_encode($row)."\n");
        }
        fclose($clickDetailFd);

        //文件传输
        $yesterday = date('Ymd', strtotime($date . ' -1 day'));
        @unlink($path . $yesterday);

        flock($resFd, LOCK_UN);
        fclose($resFd);
        @unlink($strMutexFile);

        $endTime = date('Y-m-d H:i:s');
        printf("%s begin at %s, end at %s\n", $taskName, $beginTime, $endTime);
    }
}
