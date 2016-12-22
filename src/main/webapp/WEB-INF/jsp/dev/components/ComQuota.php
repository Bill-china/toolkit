<?php
//计费核心逻辑
class ComQuota{

    /**
    * 注意：$price>0表示扣费，$price<0表示退款
    * $price 点击价格,退款的时候价格为负,$cost_from='dj/mv'
    * 返回值：result:正确计费（包含撞线的）返回true,系统异常、参数错误等返回false，用来区分是否需要重试; real_cost 当退款的时候也就是负的
    *
    */
    public static function cost($ad_user_id,$ad_plan_id,$price,$click_time,$click_id,$cost_from='dj',$ver='sou',$product_line_info)
    {
    	Utility::log(__CLASS__,"COST_START",func_get_args());
    	//返回值的初始默认值
    	$ret=array(
    		'result'=>false,
    		'msg'=>'',
    		'real_cost'=>0,
    		'offline_type'=>0,
    		'needOfflineLog'=>0,
    	);

    	//验证输入信息
    	if(($click_time=((int)$click_time))<=0||!is_numeric($price) ||!in_array($cost_from,array('dj','mv')) || empty($click_id))
    	{
    		$ret['msg']='params error';
    		return $ret;
    	}

    	$ret['result']=true;
    	if(!in_array(date('Y-m-d',$click_time),array(date('Y-m-d',time()),date('Y-m-d',strtotime('-1  day')))))
    	{
    		$ret['msg']='click_time must be today or yesterday';
    		return $ret;
    	}

    	$cnn = Yii::app()->db_quota;
        $trs = $cnn->beginTransaction();
        try {

        	//第一步获取用户信息
        	$sql = sprintf("select  *  from  %s where ad_user_id=%d  for update", self::getUserTableName($ad_user_id),$ad_user_id);
        	$user = $cnn->createCommand($sql)->queryRow();
            Utility::log(__CLASS__,"USERINFO",$user);
        	if($price>0 && empty($user))
        	{
        		//说明库里还没有用户记录,或者状态不对
        		$ret['offline_type']=-1;
        		$ret['msg']='no user quota error';
        		@$trs->commit();
        		return $ret;
        	}
        	//判断点击时间和结算时间相比，已经过了结算时间就放弃掉点击
        	if($click_time<strtotime($user['settle_date']))
        	{
        		$ret['msg']='exceed settle_date';
        		$ret['offline_type']=-1;
        		@$trs->commit();
        		return $ret;
        	}

        	$plan=array();
        	if($cost_from=='dj')
        	{
        		//点睛的需要检查计划状态
        		$sql=sprintf("select  *  from  %s where ad_plan_id=%d and ad_user_id=%d for update", self::getPlanTableName($ad_user_id),$ad_plan_id,$ad_user_id);
        		$plan=$cnn->createCommand($sql)->queryRow();
                Utility::log(__CLASS__,"PLANINFO",$plan);
        		if($price>0)
        		{
	        		if(empty($plan))
		        	{
		        		$ret['offline_type']=-1;
		        		$ret['msg']='no valid plan';
		        		@$trs->commit();
		        		return $ret;
		        	}
		        	//再判断下日期是否一致
		        	if($plan['cur_date'] !=$user['cur_date'] )
		        	{
		        		$ret['offline_type']=-1;
		        		$ret['msg']='user & plan cur_date not eq';
		        		Utility::sendAlert(__CLASS__,__FUNCTION__,$ret['msg']." {$ad_user_id} : {$user['cur_date']} {$ad_plan_id} :{$plan['cur_date']}");
		        		@$trs->commit();
		        		return $ret;
		        	}
	        	}
        	}

        	//判断点击时间和表中的日期 必须是前一天或者后天一天或者当天
        	if(!in_array(date('Y-m-d',$click_time),array($user['cur_date'],date("Y-m-d",strtotime($user['cur_date'])+60*60*24),date("Y-m-d",strtotime($user['cur_date'])-60*60*24))))
        	{
        		//日期不在范围，放弃点击
        		$ret['offline_type']=-1;
	        	$ret['msg']='invalid click_time';
	        	@$trs->commit();
	        	return $ret;
        	}

        	//判断切换时间是否是当前时间，必须等切换完成后才能处理.//如果是0:05分，还没切，得放到暂存区
        	if($click_time>strtotime($user['cur_date']." 23:59:50"))
        	{
        		$ret['result']=false;
        		$ret['msg']='click_time>cur_date ,must wait switch curdate';
        		@$trs->commit();
        		return $ret;
        	}


        	//判断是今天的消费，还是昨天的
        	$is_today=(date('Y-m-d',$click_time) == $user['cur_date']);

            $is_today_offline = ( (date('Y-m-d',$click_time) == $user['cur_date']) && (date('Y-m-d',$click_time)==date('Y-m-d')) );

			$priceInfo = self::getClickPrice($user,$plan,$price,$click_time,$cost_from,$ver,$product_line_info);

            if(($priceInfo['offline_type']== 2 || $priceInfo['offline_type']== 3 || $priceInfo['offline_type']== 4 ) && !$is_today_offline)
            {
                $priceInfo['no_need_offline']=1;
            }
            $priceInfo['balance']=$user['balance'];
            if($cost_from=='dj')
            {
                $priceInfo['userQuota']=$is_today?$user['dj_quota']:$user['yesterday_dj_quota'];
                $priceInfo['productQuota']=$is_today?$user[$product_line_info['product_quota']]:$user[$product_line_info['product_yesterday_quota']];
                $priceInfo['planQuota']=$is_today?$plan['plan_quota']:$plan['yesterday_plan_quota'];
            }
            else {

                $priceInfo['userQuota']=$is_today?$user['mv_quota']:$user['yesterday_mv_quota'];
                $priceInfo['productQuota']=$is_today?$user['mv_quota']:$user['yesterday_mv_quota'];
            }
	        Utility::log(__CLASS__,"PRICEINFO",$priceInfo);
	        $real_cost=$priceInfo['real_cost'];

            if(round($real_cost,2)!=0)
            {
                //更新用户与计划消费数据
                if($cost_from == 'dj'){
                	//如果是昨天消费记录到昨天里，如果是今天消费，记录到今天的里
                	if($is_today)
                	{
						$sql = sprintf("update %s set plan_cost=plan_cost+%.2f,update_time=%d where ad_plan_id=%d", self::getPlanTableName($ad_user_id),$real_cost,time(),$ad_plan_id);
                	}
                	else
                	{
						$sql = sprintf("update %s set yesterday_plan_cost=yesterday_plan_cost+%.2f,update_time=%d where ad_plan_id=%d", self::getPlanTableName($ad_user_id),$real_cost,time(),$ad_plan_id);
                	}
                	//echo $sql."\n";
                	$rows1=$cnn->createCommand($sql)->execute();

                	if($is_today)
                	{
						$sql = sprintf("update %s set dj_cost=dj_cost+%.2f,%s=%s+%.2f,update_time=%d where ad_user_id=%d",self::getUserTableName($ad_user_id),$real_cost,$product_line_info['product_cost'],$product_line_info['product_cost'],$real_cost,time(),$ad_user_id);
                	}
                	else
                	{
                        $sql = sprintf("update %s set yesterday_dj_cost=yesterday_dj_cost+%.2f,%s=%s+%.2f,update_time=%d where ad_user_id=%d",self::getUserTableName($ad_user_id),$real_cost,$product_line_info['product_yesterday_cost'],$product_line_info['product_yesterday_cost'],$real_cost,time(),$ad_user_id);
                	}
                	//echo $sql."\n";
                	$rows2=$cnn->createCommand($sql)->execute();
                	Utility::log(__CLASS__,"DJUPDATE",array($rows1,$rows2));
                    //user实时消费信息
                    $user['dj_cost'] = $is_today?round($user['dj_cost']+$real_cost,2):$user['dj_cost'];
                    $user['yesterday_dj_cost'] = $is_today?$user['yesterday_dj_cost']:round($user['yesterday_dj_cost']+$real_cost,2);

                    $user['product_cost'] = $is_today?round($user[$product_line_info['product_cost']]+$real_cost,2):$user[$product_line_info['product_cost']];
                    $user['product_yesterday_cost'] = $is_today?$user[$product_line_info['product_yesterday_cost']]:round($user[$product_line_info['product_yesterday_cost']]+$real_cost,2);
                    //add
                    $user[$product_line_info['product_cost']] = $is_today?round($user[$product_line_info['product_cost']]+$real_cost,2):$user[$product_line_info['product_cost']];
                    $user[$product_line_info['product_yesterday_cost']] = $is_today?$user[$product_line_info['product_yesterday_cost']]:round($user[$product_line_info['product_yesterday_cost']]+$real_cost,2);
                    //add  end
                    $user['product_quota'] = $user[$product_line_info['product_quota']];
                    $user['product_yesterday_quota'] = $user[$product_line_info['product_yesterday_quota']];

                    //plan实时消费信息
                    $plan['plan_cost'] = $is_today?round($plan['plan_cost']+$real_cost,2):$plan['plan_cost'];
                    $plan['yesterday_plan_cost'] = $is_today?$plan['yesterday_plan_cost']:round($plan['yesterday_plan_cost']+$real_cost,2);
                }
                else
                {
                	if($is_today)
                	{
                		$sql = sprintf("update %s set mv_cost=mv_cost+%.2f,update_time=%d where ad_user_id=%d",  self::getUserTableName($ad_user_id),$real_cost,time(),$ad_user_id);
                	}
                	else
                	{
                		$sql = sprintf("update %s set yesterday_mv_cost=yesterday_mv_cost+%.2f,update_time=%d where ad_user_id=%d",  self::getUserTableName($ad_user_id),$real_cost,time(),$ad_user_id);
                	}
                	$rows=$cnn->createCommand($sql)->execute();
                	Utility::log(__CLASS__,"MVUPDATE",$rows);
                    //user实时消费信息
                    $user['mv_cost'] = $is_today?round($user['mv_cost']+$real_cost,2):$user['mv_cost'];
                    $user['yesterday_mv_cost'] = $is_today?$user['yesterday_mv_cost']:round($user['yesterday_mv_cost']+$real_cost,2);
                    $user['product_cost'] = $is_today?round($user['mv_cost']+$real_cost,2):$user['mv_cost'];
                    $user['product_yesterday_cost'] = $is_today?$user['yesterday_mv_cost']:round($user['yesterday_mv_cost']+$real_cost,2);
                }
                //添加消费记录
                self::addCostLog($ad_user_id,$ad_plan_id,$price,$real_cost,$click_id,($price>0?1:2),($cost_from=='dj'?1:2),$priceInfo['offline_type'],$click_time);
            } else {
                //初始化数据
                $user['product_cost'] = $user[$product_line_info['product_cost']];
                $user['product_yesterday_cost'] = $user[$product_line_info['product_yesterday_cost']];
                $user['product_quota'] = $user[$product_line_info['product_quota']];
                $user['product_yesterday_quota'] = $user[$product_line_info['product_yesterday_quota']];
            }

            $priceInfo['user_quota_info']=$user;
            $priceInfo['plan_quota_info']=$plan;

            $trs->commit();
            Utility::log(__CLASS__,"COST_END",$click_id);
            return $priceInfo;

        } catch (Exception $e) {
            @$trs->rollBack();
            Utility::sendAlert(__CLASS__,__FUNCTION__,$e->getMessage());
            $ret['result']=false;
            $ret['msg']=$e->getMessage();
            if(!(strpos($e->getMessage(),'Duplicate')===false))
            {
                $ret['result']=true;
                $ret['msg']=$e->getMessage();
                $ret['real_cost']=0;
                $ret['offline_type']=0;
            }

            return $ret;
        }
    }
    private static function getClickPrice($userInfo,$planInfo,$price,$click_time,$cost_from,$ver,$product_line_info)
    {
       $ret=array(
    		'result'=>true,
    		'msg'=>'',
    		'real_cost'=>0,
    		'offline_type'=>0,
    		'needOfflineLog'=>0,
    	);

        $is_today=date('Y-m-d',$click_time)==$userInfo['cur_date'];
        //结算之后，yesterday的cost会被请0
        $mvCost = number_format($is_today?$userInfo['mv_cost']:$userInfo['yesterday_mv_cost'], 2, '.', '');
        $djCost = number_format($is_today?$userInfo['dj_cost']:$userInfo['yesterday_dj_cost'], 2, '.', '');

        $product_cost = number_format($is_today?$userInfo[$product_line_info['product_cost']]:$userInfo[$product_line_info['product_yesterday_cost']], 2, '.', '');


        $mvQuota = number_format($is_today?$userInfo['mv_quota']:$userInfo['yesterday_mv_quota'], 2, '.', '');
        $djQuota = number_format($is_today?$userInfo['dj_quota']:$userInfo['yesterday_dj_quota'], 2, '.', '');
        $product_quota = number_format($is_today?$userInfo[$product_line_info['product_quota']]:$userInfo[$product_line_info['product_yesterday_quota']], 2, '.', '');

        if($cost_from=='dj')
        {
        	$planCost=number_format($is_today?$planInfo['plan_cost']:$planInfo['yesterday_plan_cost'], 2, '.', '');
        	$planQuota=number_format($is_today?$planInfo['plan_quota']:$planInfo['yesterday_plan_quota'], 2, '.', '');
		}

		//如果是反作弊的退款，就算一个退款价格就行
		if($price<0)
		{
			//$ret['real_cost']=$cost_from=='dj'? (0 - min(abs($price),min($userInfo['dj_cost']+$userInfo['yesterday_dj_cost'],$planCost))):(0 - min(abs($price),$userInfo['mv_cost']+$userInfo['yesterday_mv_cost']));
            $ret['real_cost']=$cost_from=='dj'? (0 - min(abs($price),min($userInfo[$product_line_info['product_cost']]+$userInfo[$product_line_info['product_yesterday_cost']],$planCost),min($userInfo['dj_cost']+$userInfo['yesterday_dj_cost'],$planCost))):(0 - min(abs($price),$userInfo['mv_cost']+$userInfo['yesterday_mv_cost']));
			return $ret;
		}
        //计算之前的是否已经撞线
        if (number_format($userInfo['dj_cost']+$userInfo['yesterday_dj_cost']+$userInfo['mv_cost']+$userInfo['yesterday_mv_cost'] , 2, '.', '') >= $userInfo['balance']) {
            $ret['offline_type']=1;
            $ret['msg']='balance now exceed 1';
            return $ret;
        }
        if($cost_from=='dj')
        {
        	if ($product_quota>0  && $product_cost>=number_format($product_quota * 1.1, 2, '.', '')) {
				$ret['offline_type']=4;
            	$ret['msg']='account quota overload 1';
            	return $ret;
        	}
        	if ($planQuota>0 && $planCost >= number_format($planQuota * 1.1 , 2, '.', '')) {
				$ret['offline_type']=3;
            	$ret['msg']='plan quota overload 1';
            	return $ret;
        	}
        }
        else if($cost_from=='mv')
        {
        	if ($mvQuota>0  && $mvCost>=number_format($mvQuota*1.1 , 2, '.', '')) {
				$ret['offline_type']=4;
            	$ret['msg']='account quota overload 1';
            	return $ret;
        	}
        }

        $reduce_balance=$userInfo['balance']-number_format($is_today?$userInfo['yesterday_mv_cost']:$userInfo['mv_cost'], 2, '.', '')-number_format($is_today?$userInfo['yesterday_dj_cost']:$userInfo['dj_cost'], 2, '.', '');


        if($cost_from=='dj')
        {
	        $priceInfo = self::_getMinPrice($price, $mvCost,$djCost,$planCost,$reduce_balance,$djQuota,$planQuota,$ver,$product_quota, $product_cost);
    	}
    	else
    	{
    		$priceInfo=self::_getMinMVPrice ($price,$mvQuota,$mvCost,$djCost,$reduce_balance);
    	}

        return $priceInfo;
    }
     // 返回此次可扣金额
    public static function _getMinPrice ($price, $mvCost, $djCost, $planCost, $balance, $userQuota, $planQuota, $ver, $product_quota, $product_cost) {

    	$ret=array(
    		'result'=>true,
    		'msg'=>'',
    		'real_cost'=>0,
    		'offline_type'=>0,
    		'needOfflineLog'=>0,
    	);

        $price = number_format($price, 2, '.', '');
        $balancePrice = $quotaPrice = $planPrice = $price;
        // 计划限额
        $newPlanCost = number_format($planCost + $price, 2, '.', '');
        if ($planQuota>0 && $newPlanCost>=$planQuota) {
            if($newPlanCost >=number_format($planQuota*1.1,2,'.',''))
            {
                $planPrice = number_format($planQuota * 1.1 - $planCost, 2, '.', '');
            }
            $ret['offline_type']=3;

            if ($planPrice<=0) {
                $ret['msg']='plan quota overload';
                $ret['real_cost']=0;
            	return $ret;
            }
        }

        // 余额
        $newUserCost = number_format($price + $mvCost + $djCost, 2, '.', '');
        if ($newUserCost>= $balance) {
            $ret['offline_type']= 1;
            $balancePrice = number_format($balance - $mvCost - $djCost, 2, '.', '');
            if ($balancePrice<=0) {
                $ret['msg']='balance overload';
                $ret['real_cost']=0;
            	return $ret;
            }
        }

        // 用户预算 搜索类的下线预算阀值是 105% 其余是 100%
        if ($ver == 'sou') {
            $overQuota = number_format($product_quota * 1.05, 2, '.', '');
        } else {
            $overQuota = number_format($product_quota, 2, '.', '');
        }
        $quotaPrice = min($planPrice, $balancePrice);

        $newProductCost = number_format($quotaPrice + $product_cost, 2, '.', '');
        if ($product_quota>0 && $newProductCost >= $overQuota) {
            if ($newProductCost >= number_format($product_quota*1.1, 2, '.', '')) {
                $quotaPrice = number_format($product_quota*1.1 - $product_cost, 2, '.', '');
            }
            $ret['offline_type'] = 4;
            if ($quotaPrice<=0) {
                $ret['msg']='account quota overload';
                $ret['real_cost']=0;
            	return $ret;
            }
        }

        $price = min($balancePrice, $quotaPrice, $planPrice);
        // 处理下线类型
        if (number_format($price + $mvCost + $djCost - $balance, 2, '.', '') >= 0) {
            // 余额
            $ret['offline_type'] = 1;
        } elseif ($product_quota>0 && number_format($price + $product_cost - $overQuota, 2, '.', '')>=0) {
            // 帐户预算
            $ret['offline_type']=4;
        } elseif ($planQuota>0 && number_format($planCost + $price - $planQuota, 2, '.', '')>=0) {
            // 计划预算
            $ret['offline_type']=3;
        }

        if ($ret['offline_type']<1 && $ver == 'sou' && $product_quota>0 && $newProductCost >= number_format($product_quota, 2, '.', '')) {
           $ret['offline_type'] = 4;
           $ret['needOfflineLog']=1;
        }

        $ret['msg']='ok';
        $ret['real_cost']=$price;
        $ret['result']=true;
        return $ret;
    }

    // 计费 true 成功 false 失败
    private static function _getMinMVPrice ($clickPrice,$userMVQuota,$userMVCost,$userDJCost,$userBalance) {
        $ret=array(
    		'result'=>true,
    		'msg'=>'',
    		'real_cost'=>0,
    		'offline_type'=>0,
    		'needOfflineLog'=>0,
    	);

        if ($userMVQuota!=0 && round($userMVCost- $userMVQuota * 1.1, 2)>=0) {
            $ret['offline_type']=4;
            $ret['msg']='account quota overload';
            return $ret;
        }
        // 检查余额

        if ($userBalance - $userDJCost - $userMVCost <= 0) {
            $clickPrice = round($userBalance - $userDJCost - $userMVCost, 2);
            $ret['offline_type']=1;
            $ret['msg']='balance overload';
            return $ret;
        }

        // 计费应扣费用
        $clickPrice = number_format($clickPrice, 2, '.', '');

        if ($userMVQuota>0 && $clickPrice >= number_format($userMVQuota - $userMVCost, 2, '.', '')) {
            if ($clickPrice >= number_format($userMVQuota*1.1 - $userMVCost, 2, '.', '')) {
                $clickPrice = number_format($userMVQuota*1.1 - $userMVCost, 2, '.', '');
            }

            $ret['offline_type']=4;

            if ($clickPrice<=0) {
                return $ret;
            }
        }
        if ($clickPrice >= $userBalance - $userDJCost - $userMVCost) {
            $clickPrice = number_format($userBalance - $userDJCost - $userMVCost, 2, '.', '');
            //这个是原来没有的
            $ret['offline_type']=1;
            $ret['msg']='balance overload';
        }

        $ret['real_cost']=$clickPrice>0?$clickPrice:0;
        $ret['msg']='ok';

        return $ret;
    }


    //$plan_quotas kv结构,key是planid，value是限额
    public static function switchUserCurDate($cur_date,$ad_user_id,$dj_quota,$mv_quota,$plan_quotas,$sou_quota,$ruyi_quota,$app_quota)
    {
        Utility::log(__CLASS__,__FUNCTION__,func_get_args());
        $cnn = Yii::app()->db_quota;
        $trs = $cnn->beginTransaction();
        $sql = sprintf("select  *  from  %s where cur_date!='%s' and ad_user_id=%d  for update", self::getUserTableName($ad_user_id),$cur_date,$ad_user_id);
        $user = $cnn->createCommand($sql)->queryRow();
        Utility::log(__CLASS__,__FUNCTION__,$user);
        if($user)
        {
            $sql=sprintf("update  %s set cur_date='%s',update_time=%d,yesterday_mv_cost=mv_cost,yesterday_dj_cost=dj_cost,yesterday_sou_cost=sou_cost,yesterday_ruyi_cost=ruyi_cost,yesterday_app_cost=app_cost,yesterday_dj_quota=dj_quota,yesterday_mv_quota=mv_quota,yesterday_sou_quota=sou_quota,yesterday_ruyi_quota=ruyi_quota,yesterday_app_quota=app_quota,mv_cost=0,dj_cost=0,sou_cost=0,ruyi_cost=0,app_cost=0,dj_quota=%.2f,mv_quota=%.2f,sou_quota=%.2f,ruyi_quota=%.2f,app_quota=%.2f where ad_user_id=%d", self::getUserTableName($ad_user_id),$cur_date,time(),$dj_quota,$mv_quota,$sou_quota,$ruyi_quota,$app_quota,$user['ad_user_id']);
            $ret=$cnn->createCommand($sql)->execute();
            Utility::log(__CLASS__,__FUNCTION__."1",$ret);

            if($plan_quotas)
            {
                foreach($plan_quotas as $plan_id => $plan_quota)
                {
                    $sql=sprintf("update %s set cur_date='%s',update_time=%d,yesterday_plan_cost=plan_cost,plan_cost=0,yesterday_plan_quota=plan_quota,plan_quota=%.2f where ad_user_id=%d and ad_plan_id=%d and cur_date!='%s'", self::getPlanTableName($user['ad_user_id']),$cur_date,time(),$plan_quota,$user['ad_user_id'],$plan_id,$cur_date);
                    $ret=$cnn->createCommand($sql)->execute();

                }
                Utility::log(__CLASS__,__FUNCTION__."2",$ret);
            }
            $trs->commit();
            return true;
        }
        else
        {
            $trs->commit();
            return false;
        }
    }

    public static function settleOneUser($settle_date,$ad_user_id)
    {
        $cnn = Yii::app()->db_quota;
        $trs = $cnn->beginTransaction();
        $yesterday_settle_date=date('Y-m-d',strtotime($settle_date)-60*24*60);
        $sql = sprintf("select  *  from  %s where settle_date<='%s' and ad_user_id=%d for update", self::getUserTableName($ad_user_id),$yesterday_settle_date,$ad_user_id);
        $user = $cnn->createCommand($sql)->queryRow();
        Utility::log(__CLASS__,__FUNCTION__,array($sql ,$user));
        $ret=array('msg'=>'','result'=>true,'user'=>array());
        $user_center=Yii::app()->db_center->createCommand("select balance from ad_user where id=$ad_user_id")->queryRow();

        if($user)
        {
            $ret['user']=$user;
            if($user['cur_date']!=$settle_date)
            {
                $ret['result']=false;
                $ret['msg']='user.cur_date !=settle_date';
                $trs->commit();
                return $ret;
            }
            if($user_center)
            {
                if(round($user['balance'],2)!=round($user_center['balance'],2))
                {
                    Utility::sendAlert(__CLASS__, __FUNCTION__, "ad_quota user balance({$user['balance']}) not equal user_center balance({$user_center['balance']})", false);
                }
                //为了防止数据不一致的情况，去中心库实时取余额
                $user['balance']=$user_center['balance'];
            }
            $new_balance=max($user['balance']-$user['yesterday_mv_cost']-$user['yesterday_dj_cost'],0);

            $sql=sprintf("update  %s set settle_date='%s',update_time=%d , balance=%.2f ,yesterday_mv_cost=0,yesterday_dj_cost=0,yesterday_sou_cost=0,yesterday_ruyi_cost=0,yesterday_app_cost=0 where ad_user_id=%d", self::getUserTableName($ad_user_id),$settle_date,time(),$new_balance,$user['ad_user_id']);
            $affect=$cnn->createCommand($sql)->execute();
            Utility::log(__CLASS__,__FUNCTION__."1",$affect);


            //计划因为需要把yesterday清0，所以需要更新
            $sql=sprintf("select  *  from  %s where ad_user_id=%d  for update", self::getPlanTableName($user['ad_user_id']),$user['ad_user_id']);
            $plan=$cnn->createCommand($sql)->queryAll();
            if($plan)
            {
                $sql=sprintf("update  %s set yesterday_plan_cost=0,update_time=%d where ad_user_id=%d", self::getPlanTableName($user['ad_user_id']),time(),$user['ad_user_id']);
                $affect=$cnn->createCommand($sql)->execute();
                Utility::log(__CLASS__,__FUNCTION__."2",$affect);
            }

            $ret['user']=$user;
            $ret['user']['new_balance']=$new_balance;
            $ret['user']['settle_money']=min($user['balance'],$user['yesterday_mv_cost']+$user['yesterday_dj_cost']);
            $trs->commit();
            Utility::log(__CLASS__,__FUNCTION__."3",$ret);
            return $ret;
        }
        else
        {
            $trs->commit();
            $ret['result']=false;
            $ret['msg']='no settle user ';
            return $ret;
        }
    }
    private static function getUserTableName($ad_user_id)
    {

        return 'ad_user_quota_'.$ad_user_id%10;

    }
    private static function getPlanTableName($ad_user_id,$table_index=-1)
    {
        return 'ad_plan_quota_'.$ad_user_id%10;

    }
    private static function getCostLogTableName($time)
    {
    	return 'ad_cost_log_'.date('Ymd',$time);
    }

    //$cost_from 1dj   2mv
    //$req_cost原始请求 计费价格，$real_cost 实际扣费价格
    //$type 1:click  2：cheat
    private static function addCostLog($ad_user_id,$ad_plan_id,$req_cost,$real_cost,$click_id,$type,$cost_from,$offline_type,$click_time)
    {
        $data=array(
        		'ad_user_id'=>$ad_user_id,
        		'ad_plan_id'=>$ad_plan_id,
        		'real_cost'=>$real_cost,
        		'req_cost'=>$req_cost,
        		'create_time'=>time(),
        		'click_id'=>$click_id,
        		'type'=>$type,
        		'cost_from'=>$cost_from,
        		'offline_type'=>$offline_type,
        		);

        return Yii::app()->db_quota->createCommand()->insert(self::getCostLogTableName($click_time),$data);

    }
    /**
     * 更新账户余额以及账户与计划的限额
     * @param  [int] $ad_user_id [账户id]
     * @param  [double] $user_balance [账户余额]
     * @param  [double] $dj_quota [账户点睛限额]
     * @param  [double] $mv_quota [账户mv限额]
     * @param  [array] $plan_quotas [计划限额数组]
     * @param  [date] $cur_date   [当前时间]
     * @return [bole]             [执行的结果]
     * @author jingguangwen@360.cn 20150713
     */
    public static function refreshOneUser($ad_user_id,$user_balance,$dj_quota,$mv_quota,$plan_quotas,$cur_date,$sou_quota,$ruyi_quota,$app_quota)
    {
        Utility::log(__CLASS__,__FUNCTION__,func_get_args());
        $cnn = Yii::app()->db_quota;
        $trs = $cnn->beginTransaction();
        try
        {
        $sql = sprintf("select  *  from  %s where  ad_user_id=%d for update", self::getUserTableName($ad_user_id),$ad_user_id);
        $user = $cnn->createCommand($sql)->queryRow();
        Utility::log(__CLASS__,__FUNCTION__,$user);
        $ret_user = $ret_plan =0;
        if($user)
        {
            if($user['cur_date'] == $cur_date){//当天有数据
                //判断限额余额信息是都跟之前一致，不一致才需要更新
                if(!(round($user['balance']-$user_balance,2) == 0 && round($user['dj_quota']-$dj_quota,2) == 0 && round($user['mv_quota']-$mv_quota,2) == 0 && round($user['sou_quota']-$sou_quota,2) == 0 && round($user['ruyi_quota']-$ruyi_quota,2) == 0 && round($user['app_quota']-$app_quota,2) == 0)){

                    $sql=sprintf("update  %s set update_time=%d, balance=%.2f,dj_quota=%.2f,mv_quota=%.2f,sou_quota=%.2f,ruyi_quota=%.2f,app_quota=%.2f where ad_user_id=%d", self::getUserTableName($ad_user_id),time(),$user_balance,$dj_quota,$mv_quota,$sou_quota,$ruyi_quota,$app_quota,$ad_user_id);
                    $ret_user=$cnn->createCommand($sql)->execute();
                }

            } else{//当天无数据

                //其它程序监控处理
            }

        }
        else //没有则insert
        {
            $sql=sprintf("insert into  %s (ad_user_id,dj_quota,mv_quota,sou_quota,ruyi_quota,app_quota,balance,cur_date,create_time) values (%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,'%s',%d)", self::getUserTableName($ad_user_id),$ad_user_id,$dj_quota,$mv_quota,$sou_quota,$ruyi_quota,$app_quota,$user_balance,$cur_date,time());
            $ret_user = $cnn->createCommand($sql)->execute();
        }
        Utility::log(__CLASS__,__FUNCTION__."1",$ret_user);
        //计划对应的处理
        if($plan_quotas)
        {
            foreach($plan_quotas as $plan_id => $plan_quota)
            {

                $sql = sprintf("select  *  from  %s where  ad_user_id=%d and ad_plan_id=%d for update", self::getPlanTableName($ad_user_id),$ad_user_id,$plan_id);
                $plan_arr = $cnn->createCommand($sql)->queryRow();
                if(!empty($plan_arr)){
                    if($plan_arr['cur_date'] == $cur_date){
                        $sql=sprintf("update %s set update_time=%d,plan_quota=%.2f where ad_user_id=%d and ad_plan_id=%d and cur_date='%s'", self::getPlanTableName($ad_user_id),time(),$plan_quota,$ad_user_id,$plan_id,$cur_date);
                        $ret_plan =$cnn->createCommand($sql)->execute();
                    } else{//当天无数据
                        //其它程序监控处理
                    }
                } else{
                    //没有数据，则插入
                    $sql=sprintf("insert into  %s (ad_plan_id,ad_user_id,plan_quota,cur_date,create_time) values (%d,%d,%.2f,'%s',%d)", self::getPlanTableName($ad_user_id),$plan_id,$ad_user_id,$plan_quota,$cur_date,time());
                    $ret_plan = $cnn->createCommand($sql)->execute();
                }

                $ret_plan .= $plan_id.':'.$ret_plan.";";
            }
            Utility::log(__CLASS__,__FUNCTION__."2",$ret_plan);
        }
        $trs->commit();
        return true;
    }
        catch(Exception $ex)
        {
            @$trs->rollBack();
            Utility::sendAlert(__CLASS__, __FUNCTION__, $ex->getMessage(),false);
            return false;
        }
    }
    /**
     * 根据账户id查询限额信息
     * @param  [int] $ad_user_id [账户od]
     * @return [array]
     * @author jingguangwen@360.cn
     */
     public static function getUserQuotaByUserId($ad_user_id)
     {
        $table_name = self::getUserTableName($ad_user_id);
        $sql = sprintf("select  *  from  %s where ad_user_id=%d ", $table_name,$ad_user_id);
        $user_quota_arr = Yii::app()->db_quota->createCommand($sql)->queryRow();
        return $user_quota_arr;
     }

    //增量同步 - 更新用户、计划限额、余额
    public static function updateIncrementQuota($user,$plan)
    {
        $uid = $user['id'];
        $cur_date = date('Y-m-d');
        return self::refreshOneUser($uid,$user['balance'],$user['day_quota'],$user['mv_quota'],$plan,$cur_date,$user['quota_dianjing'],$user['quota_ruyi'],$user['quota_app']);
    }

    //增量同步 - 获取用户限额及余额
    public static function getUserBalance($uid)
    {
        $uid = intval($uid);
        if(0 >= $uid){
            return false;
        }

        $userTable= 'ad_user';
        $centerDB = DbConnectionManager::getDjCenterDB();
        $sql = sprintf("select * from %s where id =%d",$userTable,$uid);
        $res = $centerDB->createCommand($sql)->queryRow();
        $centerDB->setActive(false);//显示关闭连接
        return $res;
    }

    //获取消费和产品线限额
    public static function getUserCost($uid)
    {
        $uid = intval($uid);
        if(0 >= $uid){
            return false;
        }

        $cnn = Yii::app()->db_quota;
        $sql = sprintf("select  *  from  %s where  ad_user_id=%d", self::getUserTableName($uid),$uid);
        $res = $cnn->createCommand($sql)->queryRow();
        $cnn->setActive(false);//显示关闭连接
        return $res;
    }
}
