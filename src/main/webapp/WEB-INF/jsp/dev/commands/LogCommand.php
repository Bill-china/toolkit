<?php
ini_set('memory_limit', '2048M');
ini_set('max_execution_time', '9000');
ini_set('ignore_user_abort', 'on');
error_reporting(E_ALL ^ E_NOTICE);
set_time_limit(9000);
class LogCommand extends CConsoleCommand
{
    //默认时间间隔
    const FRONT_TIME_LEN = 10;
    static $front2esc = array(
        'adshow' => 'guess',
        'guess' => 'guess',
        'booler' => 'shouzhu',
        'mobile' => 'mobile',
        'search' => 'sou',
        'union' => 'union',
    );

    /**
     * 获取front召回广告数据，存储到redis
     * @param string $end   获取数据的起始时间
     * @param string $date  指定获取的日期
     */
    public function actionStatisticFrontRecallLog($end = "")
    {
        //计算起止时间
        $endTime = strtotime('-2 minutes');//front最长2分钟可以生成好数据
        $endTime = $this->getPreTimeSection($endTime);

        if(empty($end)) {
            $beginTime = $endTime - (self::FRONT_TIME_LEN * 60);
        } else {
            $beginTime = strtotime($end);
            $beginTime = $this->getPreTimeSection($beginTime);
        }
        if($beginTime>=$endTime) {
            return;
        }

        //获取front各api召回情况
        $dbFront=Yii::app()->db_front_log;
        $sql = sprintf("SELECT (UNIX_TIMESTAMP(time)-UNIX_TIMESTAMP(time)%%%d) st,name,sum(api_total) requests from business_summary where UNIX_TIMESTAMP(time) >= :beginTime and UNIX_TIMESTAMP(time)< :endTime group by name,st",self::FRONT_TIME_LEN * 60);
        $command = $dbFront->createCommand($sql);
        $command->bindParam(':beginTime', $beginTime);
        $command->bindParam(':endTime', $endTime);
        $res = $command->queryAll();
        if(empty($res))
        {
            return;
        }

        //合并front不同name数据，如front的adshow,guess对应esc的guess
        $data = $this->mergeFrontData($res);

        //入redis
        $redis = new ComRedis('esc_monitor_redis', 0);
        foreach($data as $nk => $nv)
        {
            foreach($nv as $tk => $tv)
            {
                $date = date('YmdHis',$tk);
                $redis->setex('esc:alarm-front-'.$nk.'-'.$date, 60*60*24*15, $tv);
            }
        }
    }

    //将front的来源转换成esc的来源
    private function mergeFrontData($data)
    {
        $res = array();

        foreach($data as $row)
        {
            if(array_key_exists($row['name'], self::$front2esc))
            {
                if(!isset($res[self::$front2esc[$row['name']]][$row['st']]))
                {
                    $res[self::$front2esc[$row['name']]][$row['st']] = $row['requests'];
                }
                else
                {
                    $res[self::$front2esc[$row['name']]][$row['st']] += $row['requests'];
                }
            }
        }
        return $res;
    }
    //获取整点时间的时间戳：5分钟
    private function getPreTimeSection($timestamp = 0)
    {
        if(empty($timestamp))
        {
            $timestamp = time();
        }
        else
        {
            $timestamp = intval($timestamp);
        }
        return $timestamp - ($timestamp%(self::FRONT_TIME_LEN * 60));
    }
	public function actionInsertDjLog()
	{
		$fd = fopen('/tmp/esc_log_lock', "w+");
        $fl = flock($fd, LOCK_EX | LOCK_NB);
        if($fl == false) {
            echo "服务是单例模式运行，不支持多开!\n";
            exit();
        }

		$redis = new ComRedis('esc_monitor_redis', 0);
		$key=Config::item('redisKey') . 'stats:';
		$len=$redis->llen($key);
		if($len<10000)
		{echo $len."\n";exit();}
		while(true)
		{
			try{
			$data=$redis->lPop($key);
			if(!$data)
			{
				usleep(100000);
				continue;
			}
			Utility::log(__CLASS__,"OrginalInsertDjLog",$data);
			$list = @json_decode($data, true);
			if(!$list || !is_array($list))
			{
				Utility::log(__CLASS__,__FUNCTION__,"$data decode empty");
				continue;
			}
			if (isset($list['isArray']) && $list['isArray'] == 1)
			{
				Utility::log(__CLASS__,__FUNCTION__,$data. " is array");
				//说明是个PV打点
				continue;
			}
			if($list['type']!='click')
			{
				Utility::log(__CLASS__,__FUNCTION__,$data. " not click");
				continue;
			}
			if($list['click_id']==null)
                continue;
			$click_id=$list['click_id'];
			$sql="update click_detail set get_sign='{$list['get_sign']}' where click_id='".$click_id."'";
			$ret=Yii::app()->db_click_log->createCommand($sql)->execute();
			echo " $ret \n";
			}
			catch(Exception $ex)
			{
				echo $sql." ".$ex->getMessage()."\n";
			//	Utility::sendAlert(__CLASS__,__FUNCTION__,$ex->getMessage());
			}
		}
	}

	public function actionReadNginxLog($logDir='/data/logs/nginx_click_log/')
	{
		$fd = fopen('/tmp/esc_nginx_log_lock', "w+");
        $fl = flock($fd, LOCK_EX | LOCK_NB);
        if($fl == false) {
            echo "服务是单例模式运行，不支持多开!\n";
            exit();
        }
		$files = array();
        $d = dir($logDir);
        while (false !== ($entry = $d->read())) {
            if ($entry == '.' || $entry == '..') {
                continue;
            }
            $file = $logDir . $entry; // 数据文件
            //这个时间判断不严谨
            if (is_file($file) ) {
            	$fs=explode(".",$file);
            	if(count($fs)==3)
                $files[$fs[2]] = $file;
            }
        }
        $d->close();

		ksort($files);
		$keys=array_keys($files);
		//往前推迟2*60分钟
		if(count($keys)<120)
			return;
        for($i=1;$i<120;$i++)
        {
            unset($files[$keys[count($keys)-$i]]);
        }
		foreach($files as $key=>$file)
		{
			$handle = @fopen($file, "r");
			if ($handle) {
			    while (($buffer = fgets($handle, 10*1024)) !== false) {
			    	try
			    	{
			        	$this->parseNginxLog(trim($buffer));
			    	}
			    	catch(Exception $ex)
			    	{
			    		Utility::sendAlert(__CLASS__,__FUNCTION__,$ex->getMessage());
			    	}
			    }
			    fclose($handle);
			}
			//@unlink($file);
			rename($file,'/data/logs/nginx_click_log_his/'.basename($file));
		}

	}
	private function parseNginxLog($logContent)
	{
		if(empty($logContent) || strpos($logContent,'eagle_eye')>0 || strpos($logContent,'webscan')>0 || date('H:i')=='01:59')
		{
			return;
		}
		$db=Yii::app()->db_click_log;
		$system=0;
		//这里要判断 来源系统
		$t=explode("\"",$logContent);
		$p=explode(" ",$t[1]);
		$h=parse_url($p[1]);
		$path=$h['path'];
		if($path=='/search/eclk' || $path=='/guess/clk.js' || $path=='/sz/click' || $path=='/search/mclick')
			$system=1;
		if($path=='/search/rclk')
			$system=2;
		if($system==0)
			return;
		parse_str($h['query'],$ps);
		if(empty($ps))
			return;
		ksort($ps);
		$ip=explode(" ",$t[0]);
	    $ip=$ip[0];
		$now=explode("-",$t[0]);
		$now=strtotime(trim(trim(trim($now[2]),']'),'['));
		if(!$now)
		$now=time();
		$http_code=explode(" ",trim($t[2]));
		$http_code=$http_code[0];
		if($http_code>=400 || $http_code==204)
			return;
        if($ps['ls'] && !strpos($ps['ls'],'\x')===false)
            return;
        if($ps['bt'] && !strpos($ps['bt'],'\x')===false)
            return;
		$get_sign=md5(http_build_query($ps));
		// $click=$db->createCommand("select count(*) as cnt from click_detail where get_sign='".$get_sign."'")->queryScalar();
        $click = ComAdDetail::queryBySql("select count(*) as cnt from click_detail where get_sign='".$get_sign."'", $now, 'scalar');
		if($click>0)
		{
			//说明记录存在，无需再进行插入
		}
		else {
			$alert=false;
			//不存在需要插入记录
			if($path=='/search/eclk')
			{
				$des_p=dj_auth_decode($ps['p'], 'ad_dianjing', false);
				$list=json_decode($des_p,true);
				if(!$list)
					return;
				$alert=true;
				$list2=array();
				$list2['click_id']=substr(md5($list[8].$list[15]. $list[0] . php_uname('n').posix_getpid() . microtime(true)), 8, 16);
				$list2['get_sign']=$get_sign;
				$list2['click_time']=$now;
				$list2['view_id']=$list[15];
				$list2['view_time']=$list[13];
				$list2['ip']=$list[8]?$list[8]:$ip;
				$list2['mid']=$list[9];
				$list2['ad_user_id']=$list[3];
				$list2['ad_advert_id']=$list[0];
				$list2['ad_group_id']=$list[2];
				$list2['ad_plan_id']=$list[1];
				$list2['keyword']=$list[4];
				$list2['query']=$list[5];
				$list2['ls']=$list[18];
				$list2['src']=$list[17];
				$list2['area_fid']=$list[10];
				$list2['area_id']=$list[11];
				$list2['price']=$list[12];
				$list2['create_date']=date('Y-m-d',$list2['click_time']);
				$list2['cid']=$list[19];
				$list2['pid']=$list[20];
				$list2['ver']='sou';
				$list2['create_time']=time();
				$list2['update_time']=time();
				$et = $ps['et'];
		        if (isset($et)){   //et参数格式   et=biyicomb_123456
		            $etArr=explode("_",$et);
		            $list2['sub_ver'] = isset($etArr[0])?"biyi":'';
		            $list2['sub_data'] = array(isset($etArr[1])?$etArr[1]:'');
		        }else{
		            $list2['sub_ver']='';
		            $list2['sub_data']=array('');
		        }
				$list2['pos']=$list[7];
				$list2['location']=$list[6];
				$list2['apitype']=0;
				$list2['type']=1;
				$list2['cheat_type']=0;
				$list2['source_type']=3;//固定
				$list2['source_system']=1;	//固定
				$list2['matchtype']=(int)$list[16];
				//$list2['extension']=@json_encode(array('matchtype'=>$list[16],'guid'=>$list[14]));
				$list2['status']=0;
				$this->push_click_redis($list2);
				ComAdDetail::insertOperateLog($list2['click_id'],5,'',1);
				//这里还需要写operate_log 记录下
				echo "inserted search $system $get_sign {$list2['click_id']} $ret\n";
			}
			else if($path=='/guess/clk.js')
        	{
                $asgin=$ps['asin'];
                $asginList = explode('-', $asgin, 5);
                if (count($asginList) != 5) {
                        return;
                }
                $asgin = $asginList[0];
                $tid = $asginList[1];   #兴趣ID
                        $cityId = $asginList[2];
                $arr = explode(',', $asginList[3]);
                $channelId = (int) $arr[0];
                $placeId = (int) $arr[1];
                $extraInfo = $asginList[4];
                $ip=$ps['ip']?$ps['ip']:$ip;
                $mid=$ps['mid'];
                $arrExtraInfo = json_decode(dj_auth_decode($extraInfo, 'ad_dianjing', false), true);
                if (empty($arrExtraInfo) || ! isset($arrExtraInfo['p'])) {
                        return;
                }
                $adData = explode('-', $arrExtraInfo['p']);
                $pvid = substr($asgin, -16);
                $intCurTime = time();
                $intViewTime = (int) substr($asgin, 0, strlen($asgin)-16);
                $intViewTime += ($intCurTime & (~ 0x7FFFF));
                if ($intViewTime > ($intCurTime + 300)) {
                        $intViewTime -=0x7FFFF;
                        if ($intViewTime > ($intCurTime + 300)) {
                                return;
                        }
                }
                $alert=true;
                $strMachineInfo = php_uname('n').posix_getpid();
                $clickId = substr(md5($ip.$mid.$pvid.$strMachineInfo.$adData[1].microtime(true)), 8, 16);
                $list=array();
                $list['click_id']=$clickId;
                $list['get_sign']=$get_sign;
                $list['click_time']=$now;
                $list['view_id']=$pvid;
                $list['view_time']=$intViewTime;
                $list['ip']=$ip;
                $list['mid']=(string) $mid;
                $list['ad_user_id']=(int) $adData[4];
                $list['ad_advert_id']=(int) $adData[1];
                $list['ad_group_id']= (int) $adData[2];
                $list['ad_plan_id']=(int) $adData[3];
                $list['ls']=$ls;
                $list['src']='';
                $area=explode(",",$cityId);
                $list['area_fid']=$area[0];
                $list['area_id']=$area[1];
                $list['price']=(float) $adData[0];
                $list['bidprice']=(float) $adData[5];
                $list['create_date']=date('Y-m-d',$list['click_time']);
                $list['cid']=(int) $channelId;
                $list['pid']=(int) $placeId;
                $list['ver']='guess';
                $list['create_time']=time();
                $list['update_time']=time();
                $list['sub_ver']=isset($adData[6]) ? $adData[6] : '';
                $list['sub_data']=isset($adData[7]) ? array($adData[7]) : '';
                $list['tag_id']=$tid;
                $list['apitype']=0;
                $list['type']=1;
                $list['cheat_type']=0;
                $list['source_type']=1;
                $list['source_system']=1;
                $list['status']=0;

                $this->push_click_redis($list);
                ComAdDetail::insertOperateLog($list['click_id'],5,'',1);
                //这里还需要写operate_log 记录下
                echo "inserted guess $system $get_sign {$list['click_id']} $ret\n";
			}
			else if($path=='/search/rclk')
			{
				return ;//暂时不自动添加
				$des_p=dj_auth_decode($ps['p'], 'ad_dianjing', false);
				$ps['lkn']=$ps['lkn']?$ps['lkn']:1;
				$list=json_decode($des_p,true);
				if(!$list)
					return;
				$alert=true;
				$list2=array();
				$list2['click_id']=substr(md5($list[8].$list[15]. $list[0] . php_uname('n').posix_getpid() . microtime(true)), 8, 16);
				$list2['get_sign']=$get_sign;
				$list2['click_time']=$now;
				$list2['view_id']=$list[15];
				$list2['view_time']=$list[13];
				$list2['ip']=$list[8]?$list[8]:$ip;
				$list2['mid']=$list[9];
				$list2['ad_user_id']=$list[3];
				$list2['ad_advert_id']=$list[0];
				$list2['ad_group_id']=$list[2];
				$list2['ad_plan_id']=$list[1];
				$list2['keyword']=$list[4];
				$list2['query']=$list[5];
				$list2['ls']=$list[18];
				$list2['src']=$list[17];
				$list2['area_fid']=$list[10];
				$list2['area_id']=$list[11];
				$list2['price']=$list[12];
				$list2['create_date']=date('Y-m-d',$list2['click_time']);
				$list2['cid']=$list[19];
				$list2['pid']=$list[20];
				$list2['ver']='sou';
				$list2['create_time']=time();
				$list2['update_time']=time();
				$list2['pos']=$list[7];
				$list2['location']=$list[6];
				$list2['apitype']=2;//无需反作弊
				$list2['type']=1;
				$list2['cheat_type']=0;
				$list2['source_type']=3;//固定
				$list2['source_system']=2;	//固定
				$list2['matchtype']=(int)$list[16];
				//$list2['extension']=@json_encode(array('matchtype'=>$list[16],'guid'=>$list[14],'linkNo'=>$ps['lkn']));
				$list2['status']=0;
				$this->push_click_redis($list2);
				ComAdDetail::insertOperateLog($list2['click_id'],5,'',1);
				echo "inserted ruyi $system $get_sign {$list2['click_id']} $ret\n";
			}
			else if ($path=='/sz/click')
			{
				//手助暂时不处理
			}
			else if ($path=='/search/mclick')
			{

				$des_p=dj_auth_decode($ps['asin'], 'ad_dianjing', false);
				$list=json_decode($des_p,true);
				if(!$list)
					return;
				$alert=true;
				$list2=array();
				$list2['click_id']=substr(md5($ps['ip'].$list['pvid']. $list['adId'] . php_uname('n').posix_getpid() . microtime(true)), 8, 16);
				$list2['get_sign']=$get_sign;
				$list2['click_time']=$now;
				$list2['view_id']=$list['pvid'];
				$list2['view_time']=$list['viewTime'];
				$list2['ip']=$ps['ip']?$ps['ip']:$ip;
				$list2['mid']=$ps['mid']?$ps['mid']:$ps['m2'];
				$list2['ad_user_id']=$list['userId'];
				$list2['ad_advert_id']=$list['adId'];
				$list2['ad_group_id']=$list['groupId'];
				$list2['ad_plan_id']=$list['planId'];
				$list2['keyword']=$list['keyword'];
				$list2['query']=$list['trigerKeyword'];
				$list2['ls']=$list['ls'];
				$list2['src']=$list['src'];				//暂时没有
				$list2['area_fid']=$list['provinceId'];
				$list2['area_id']=$list['cityId'];
				$list2['price']=$list['price'];
				$list2['create_date']=date('Y-m-d',$list2['click_time']);
				$list2['cid']=$list['channelId'];
				$list2['pid']=$list['placeId'];
				$list2['ver']='sou';
				$list2['create_time']=time();
				$list2['update_time']=time();
		        $list2['sub_ver'] = $list['subver'];
		        $list2['sub_data'] = $list['subdata']?$list['subdata']:array();
		        $list2['sub_ad_info']=$list['subAdInfo'];
				$list2['pos']=$list['pos'];
				$list2['location']=$list['location'];
				$list2['matchtype']=(int)$list[16];
				$list2['apitype']=0;
				$list2['type']=1;
				$list2['cheat_type']=0;
				$list2['source_type']=4;//固定
				$list2['source_system']=1;	//固定
				$list2['status']=0;
				$this->push_click_redis($list2);
				ComAdDetail::insertOperateLog($list2['click_id'],5,'',1);
				//这里还需要写operate_log 记录下
				echo "inserted mobile $system $get_sign {$list2['click_id']} $ret\n";
			}
			//说明记录不存在，先报警即可.
			if($alert)
			{
				Utility::sendAlert(__CLASS__,__FUNCTION__,"find no click in nginx log get_sign:$get_sign, params:".$logContent);
				usleep(10000);
			}
		}
	}

	//使用sql语句 把detail表里的数据插入到redis中继续处理
	public function actionRecoveryDataFromClickDetailToRedis($sql_where)
	{
		ini_set('memory_limit','4000m');
		$db=Yii::app()->db_click_log;


        $limit  = 10000;
        $lastID     = 0;
        $total_deal_num = 0;
        $time = time();
        while (true) {

            $sql="select * from click_detail where 1=1 $sql_where"." and  id >".$lastID." limit ".$limit;
            $data = ComAdDetail::queryBySql($sql, $time, 'all');
            if(empty($data)){
                break;
            }
    		foreach($data as $list)
    		{
                $lastID = $list['id'];
    			$this->push_click_redis($list);
                echo 'id:'.$list['id']."--deal over\n";
                $total_deal_num++;
    		}
            echo 'done:'.count($data)."\n";
        }
        echo "total_deal_num:".$total_deal_num."\n";
	}

	private function push_click_redis($list)
	{
		static $redis;
		$redis = new ComAdStats(0);
		$arr=array(
			'get_sign'=> $list['get_sign'],
            'view_id' => $list['view_id'],
            'ip' => $list['ip'],
            'type' => $list['type']==1?'click':'view',
            'now' => $list['click_time'],
            'view_time' => $list['view_time'],
            'apitype' =>  $list['apitype'],
            'pid' => (int) $list['ad_plan_id'],
            'place' => $list['location'],
            'pos' => (int) $list['pos'],
            'gid' => (int) $list['ad_group_id'],
            'aid' => (int) $list['ad_advert_id'],
            'uid' => (int) $list['ad_user_id'],
            'price' => (float) $list['price'],
            'mid' => (string) $list['mid'],
            'city_id' => $list['area_fid'] . ',' . $list['area_id'],
            'keyword' => $list['keyword'],
            'query' => $list['query'],
            'matchtype' => $list['matchtype'],
            'click_id' => $list['click_id'],
            'channel_id' => (int) $list['cid'],
            'place_id' => (int) $list['pid'],
            'ls' => (string) $list['ls'],
            'src' => (string) $list['src'],
            'guid' => '',
            'site' => '',
            'ver' => $list['ver'],
            'subver' => $list['sub_ver'],
            'subdata' => $list['sub_data'],
            'sub_ad_info' =>$list['sub_ad_info'],
            'buckettest' => 0,
            'source_type'=>(int) $list['source_type'],
			);

		$redis->push($arr);
	}

    public function actionInsertAlarmByDuration($end="-30 minutes",$date='')
    {
        //每5分钟计算一下从库收入，放入redis
        $db = DbConnectionManager::getDB('click_log_slave');
        if($date=='')
        $date=date('Y-m-d');

        $now=date('YmdHis');
        $endtime=0;
        if($end)
            $endtime=strtotime($end);

        $time = strtotime($date);
        $clickDetailTable = 'click_detail_' . date('Ymd', $time);
    	//搜索   pid 238 如意onebox source_type=4是移动
        $sql="select sum(price-reduce_price) as p,source_system,ver,floor(from_unixtime(click_time)/500)*500 as fen from $clickDetailTable where create_date='{$date}' and deal_status in (0,1) and click_time >".($endtime)." and click_time < ".strtotime("-5 minutes")." and pid!=238 and ls not like 'n%' and ls not like 's%' group by source_system,ver,fen";
        $data=$db->createCommand($sql)->queryAll();
        $redis = new ComRedis('esc_monitor_redis', 0);
        foreach($data as $d)
        {
            $redis->setex("alarm-price-".$d['source_system']."-".$d['ver']."-".$d['fen'],60*60*24*15,$d['p']);
        }
        //移动
        $sql="select sum(price-reduce_price) as p,source_system,ver,floor(from_unixtime(click_time)/500)*500 as fen,source_type from $clickDetailTable where create_date='{$date}'  and deal_status in (0,1) and click_time >".($endtime)." and click_time < ".strtotime("-5 minutes")." and source_type=4 group by source_system,ver,fen";
        $data=$db->createCommand($sql)->queryAll();
        foreach($data as $d)
        {
            $redis->setex("alarm-price-".$d['source_type']."-".$d['ver']."-".$d['fen'],60*60*24*15,$d['p']);
        }
        //如意onebox
        $sql="select sum(price-reduce_price) as p,source_system,ver,floor(from_unixtime(click_time)/500)*500 as fen,source_type from $clickDetailTable where create_date='{$date}' and deal_status in (0,1) and click_time >".($endtime)." and click_time < ".strtotime("-5 minutes")." and pid=238 group by source_system,ver,fen";
        $data=$db->createCommand($sql)->queryAll();
        foreach($data as $d)
        {
            $redis->setex("alarm-price-".$d['source_system']."-".$d['source_type']."-onebox-".$d['fen'],60*60*24*15,$d['p']);
        }

        $sql="select count(*) as p,cheat_type,floor(from_unixtime(click_time)/500)*500 as fen from $clickDetailTable where create_date='{$date}' and deal_status in (0,1) and click_time >".($endtime)." and click_time < ".strtotime("-10 minutes")." group by cheat_type,fen";
        $data=$db->createCommand($sql)->queryAll();
        foreach($data as $d)
        {
        	$redis->setex("alarm-cheat-".$d['cheat_type']."-".$d['fen'],60*60*24*15,$d['p']);
        }

		$sql="select sum(reduce_price) as p,floor(from_unixtime(click_time)/500)*500 as fen from $clickDetailTable where create_date='{$date}' and deal_status in (0,1) and click_time >".($endtime)." and click_time < ".strtotime("-5 minutes")." and extension in (1,2,3) group by fen";
        $data=$db->createCommand($sql)->queryAll();
        foreach($data as $d)
        {
            $redis->setex("alarm-zx-".$d['fen'],60*60*24*15,$d['p']);
        }
        //添加联盟
        $sql="select sum(price-reduce_price) as p,source_system,floor(from_unixtime(click_time)/500)*500 as fen from $clickDetailTable where create_date='{$date}' and deal_status in (0,1) and click_time >".($endtime)." and click_time < ".strtotime("-20 minutes")."  and  (ls like 'n%' or ls like 's%')  group by source_system,fen";
        $data=$db->createCommand($sql)->queryAll();
        $redis = new ComRedis('esc_monitor_redis', 0);
        foreach($data as $d)
        {
            $redis->setex("alarm-price-".$d['source_system']."-lm-".$d['fen'],60*60*24*15,$d['p']);
        }

    }
	public function actionDoAlarm()
	{
        $this->alarm_module();
        //计费锐减告警
        $this->alarmModuleV2("搜索","alarm-price-1-sou",-0.10);
        $this->alarmModuleV2("移动","alarm-price-4-sou",-0.20);
        $this->alarmModuleV2("手助广告","alarm-price-1-shouzhu",-0.30);
        $this->alarmModuleV2("MV广告","alarm-price-4-mediav",-0.30);
        //$this->alarmModuleV2("联盟","alarm-price-1-lm",-0.40);


        //计费突增告警
        $this->alarmModuleV2("搜索","alarm-price-1-sou",0.70);
        $this->alarmModuleV2("移动","alarm-price-4-sou",0.40);
        $this->alarmModuleV2("MV广告","alarm-price-4-mediav",0.80);
        $this->alarmModuleV2("手助广告","alarm-price-1-shouzhu",1.00);
        //$this->alarmModuleV2("联盟","alarm-price-1-lm",0.50);
	}
	private function alarm_module()
	{
        $redis = new ComRedis('esc_monitor_redis', 0);
		//算法反作弊报警
        $key_patten="alarm-cheat-3";
        $date=date('Ymd');
        $keys=$redis->keys($key_patten."-{$date}*");
        $values=$redis->mget($keys);
        $zuobi_3=array();
        foreach($keys as $index=>$k)
        {
            $kk=explode("-",$k);
            $zuobi_3[strtotime($kk[3])]=(int)$values[$index];
        }
        ksort($zuobi_3);

        $key_patten = "alarm-cheat-0";
        $date = date('Ymd');
        $keys = $redis->keys($key_patten . "-{$date}*");
        $values = $redis->mget($keys);
        $zuobi_0 = array();
        foreach($keys as $index => $k)
        {
            $kk = explode("-", $k);
            $zuobi_0[strtotime($kk[3])] = (int)$values[$index];
        }
        ksort($zuobi_0);

        if(count($zuobi_3) > 12)
        {
            //需要有1小时的反作弊日志
            $now = $this->getLastValue($zuobi_3, 2) + $this->getLastValue($zuobi_3, 3) + $this->getLastValue($zuobi_3, 4) + $this->getLastValue($zuobi_3, 5) + $this->getLastValue($zuobi_3, 6) + $this->getLastValue($zuobi_3, 7);
            $now_normal = $this->getLastValue($zuobi_0, 2) + $this->getLastValue($zuobi_0, 3) + $this->getLastValue($zuobi_0, 4) + $this->getLastValue($zuobi_0, 5) + $this->getLastValue($zuobi_0, 6) + $this->getLastValue($zuobi_0, 7);
            $last = $this->getLastValue($zuobi_3, 8) + $this->getLastValue($zuobi_3, 9) + $this->getLastValue($zuobi_3, 10) + $this->getLastValue($zuobi_3, 11) + $this->getLastValue($zuobi_3, 12) + $this->getLastValue($zuobi_3, 13);
            $last_normal = $this->getLastValue($zuobi_0, 8) + $this->getLastValue($zuobi_0, 9) + $this->getLastValue($zuobi_0, 10) + $this->getLastValue($zuobi_0, 11) + $this->getLastValue($zuobi_0, 12) + $this->getLastValue($zuobi_0, 13);

            if((($now - $last) / max($now, $last)) > (($now_normal - $last_normal) / max($now_normal, $last_normal)) + 0.4)
            {
                $content = "作弊点击数量异常，当前数量：$now, 半小时之前数量：$last,增加" . (int)((($now - $last) / max($now, $last)) * 100) . "%";
                Utility::sendAlert("报警", "搜索", $content, false);
                $alarm_content['content'] = $content;
                $redis->rpush("alarm-list", json_encode($alarm_content));
                $this->sendToOurs("搜索", $content);

                return;
            }
        }
		echo date('Y-m-d H:i:s')." 搜索 反作弊检测\n";
	}

    /**
     * 根据键值前缀，获取每特定时间段的消费数据
     *
     * @param ComRedis $redis
     * @param string $keyPatten
     *
     * @return array
     */
    private function getCostFromRedis(ComRedis $redis, $keyPatten)
    {
        $values = array();

        if(!is_object($redis) || empty($keyPatten))
        {
            return $values;
        }

        //获取对应key
        $keyPatten = trim($keyPatten, '*') . '*';
        $keys = $redis->keys($keyPatten);

        //取消费数据
        foreach($keys as $key)
        {
            $value = $redis->get($key);
            if(false === $value)
            {
                continue;
            }
            $split = explode('-', $key);
            $values[strtotime($split[4])] = $value;
        }

        return $values;
    }

    /**
     * 支持增加的告警
     *
     * @param $name
     * @param $key_patten
     * @param $alarm_percent
     *
     * @throws Exception
     */
    private function alarmModuleV2($name, $key_patten, $alarm_percent)
    {
        $hour = (int)date('H');
        if($hour == 0)
            return;
        if($hour < 8)
        {
            $alarm_percent = $alarm_percent * 2.5;
        }
        if($alarm_percent <= -1)
        {
            return;
        }
        $redis = new ComRedis('esc_monitor_redis', 0);

        //获取今天的收入
        $date = date('Ymd');
        $today_price = $this->getCostFromRedis($redis, $key_patten . "-{$date}");
        if(count($today_price) < 3)
        {
            return;
        }
        ksort($today_price);

        //获取上一周末或工作日收入
        $weekth = date("w");
        if($weekth == 6)//周六取上周日
        {
            $date = date('Ymd', strtotime('-6 days'));
        }
        else if ($weekth == 1)//周一上周五
        {
            $date = date('Ymd', strtotime('-3 days'));
        }
        else
            $date = date('Ymd', strtotime('-1 days'));
        $yesterday_price = $this->getCostFromRedis($redis, $key_patten . "-{$date}");
        if(count($yesterday_price) < 1)
        {
            return;
        }
        ksort($yesterday_price);

        //获取上周今天的收入
        $date = date('Ymd', strtotime('-7 days'));
        $last_week_price = $this->getCostFromRedis($redis, $key_patten . "-{$date}");
        ksort($last_week_price);

        //计算今天5分钟之前和之后的差异
        $today_percent = 0;
        $today_price_keys = array_keys($today_price);
        $today_this_time = (float)$today_price[$today_price_keys[count($today_price_keys) - 2]];
        $today_last_time = (float)$today_price[$today_price_keys[count($today_price_keys) - 3]];
        if((!($today_this_time == 0 && $today_last_time == 0)))
        {
            $today_percent = ($today_this_time - $today_last_time) / max($today_last_time, $today_this_time);
        }

        //获取对比计费的时间段
        $today_time_key = $today_price_keys[count($today_price_keys) - 2];

        //计算昨天现在的情况
        if($weekth == 6)//周六取上周日
        {
            $yesterday_time_key = $today_time_key - 60 * 60 * 24 * 6;
        }
        else if ($weekth == 1)//周一上周五
        {
            $yesterday_time_key = $today_time_key - 60 * 60 * 24 * 3;
        }
        else
            $yesterday_time_key = $today_time_key - 60 * 60 * 24;
        //获取yesterday_last_time_key;
        $yesterday_last_time_key = '';
        foreach($yesterday_price as $k => $v)
        {
            if($k < $yesterday_time_key)
            {
                $yesterday_last_time_key = $k;
            }
            else
            {
                break;
            }
        }
        $yesterday_p = (float)($yesterday_price[$yesterday_time_key] ? $yesterday_price[$yesterday_time_key] : 0);
        if($yesterday_p == 0 && $today_this_time == 0)
        {
            return;
        }
        if($alarm_percent < 0)
            $yesterday_percent = (float)($yesterday_p - $today_this_time) / max($yesterday_p, $today_this_time);
        else
            $yesterday_percent = (float)($yesterday_p - $today_this_time) / min($yesterday_p, $today_this_time);
        $yesterday_last_price = (float)($yesterday_price[$yesterday_last_time_key] ? $yesterday_price[$yesterday_last_time_key] : 0);
        if($yesterday_p == 0 && $yesterday_last_price == 0)
        {
            return;
        }
        $yesterday_last_percent = (float)($yesterday_p - $yesterday_last_price) / max($yesterday_p, $yesterday_last_price);

        //上周
        $last_week_time_key = $today_time_key - 60 * 60 * 24 * 7;
        //获取lastweek_last_time_key;
        $lastweek_last_time_key = '';
        foreach($last_week_price as $k => $v)
        {
            if($k < $last_week_time_key)
            {
                $lastweek_last_time_key = $k;
            }
            else
            {
                break;
            }
        }

        $last_week_p = (float)($last_week_price[$last_week_time_key] ? $last_week_price[$last_week_time_key] : 0);
        if($last_week_p == 0 && $today_this_time == 0)
        {
            return;
        }
        if($alarm_percent < 0)
            $last_week_percent = (float)($last_week_p - $today_this_time) / max($last_week_p, $today_this_time);
        else
            $last_week_percent = (float)($last_week_p - $today_this_time) / min($last_week_p, $today_this_time);
        $last_week_last_price = (float)($last_week_price[$lastweek_last_time_key] ? $last_week_price[$lastweek_last_time_key] : 0);
        if($last_week_p == 0 && $last_week_last_price == 0)
        {
            return;
        }
        $last_week_last_percent = (float)($last_week_p - $last_week_last_price) / max($last_week_p, $last_week_last_price);

        if($last_week_p == 0)
        {
            $min = (float)$yesterday_p * (1 + $alarm_percent);
        }
        else
        {
            if($yesterday_p == 0)
            {
                $min = (float)$last_week_p * (1 + $alarm_percent);
            }
            else
            {
                $min = ($alarm_percent < 0) ? ((float)min($yesterday_p, $last_week_p) * (1 + $alarm_percent)) : ((float)max($yesterday_p, $last_week_p) * (1 + $alarm_percent));
            }
        }
        $content = "￥" . (int)$today_last_time . "(" . date('H:i', $today_price_keys[count($today_price_keys) - 2]) . "),￥" . (int)$today_this_time . "(" . date('H:i', $today_price_keys[count($today_price_keys) - 1]) . "), " . ($today_percent < 0 ? "降低" : "增长") . ((int)(abs($today_percent) * 100)) . "%" . "参考：该时段上周" . ($last_week_last_percent < 0 ? "降低" : "增长") . ((int)(abs($last_week_last_percent) * 100)) . "%,昨天" . ($yesterday_last_percent < 0 ? "降低" : "增长") . ((int)(abs($yesterday_last_percent) * 100)) . "%";

        $alarm_content = array(
            'level' => 3,
            'content' => '',
            'time' => time(),
        );
        //如果今天降低或者上涨的幅度比上周或者昨天的最小值还小，则报警
        if($alarm_percent < 0 && $today_percent < (min($last_week_last_percent, $yesterday_last_percent) + $alarm_percent))
        {
            $content="￥".(int)$today_last_time."(".date('H:i',$today_price_keys[count($today_price_keys)-2])."),￥".(int)$today_this_time."(".date('H:i',$today_price_keys[count($today_price_keys)-1])."), ".($today_percent<0?"降低":"增长").((int)(abs($today_percent)*100))."%，参考：该时段上周".($last_week_last_percent<0?"降低":"增长").((int)(abs($last_week_last_percent)*100))."%,昨天".($yesterday_last_percent<0?"降低":"增长").((int)(abs($yesterday_last_percent)*100))."%";
			Utility::sendAlert("报警",$name,$content,true);
            $alarm_content['content'] = $name . $content;
			$redis->rpush("alarm-list",json_encode($alarm_content));
            $this->sendToOthers($name, $content);

            return;
        }

        if(($alarm_percent < 0 && $today_this_time < $min) || ($alarm_percent > 0 && $today_this_time > $min))
        {
            $alarm = $alarm_percent < 0 ? '低' : '高';
            $content = "收入比昨天和上周$alarm, ￥" . (int)$today_this_time . "(" . date('H:i', $today_price_keys[count($today_price_keys) - 2]) . "),昨天: ￥" . (int)$yesterday_p . "($alarm" . ((int)(abs($yesterday_percent) * 100)) . "%),上周：￥" . (int)$last_week_p . ("($alarm") . ((int)(abs($last_week_percent) * 100)) . "%)";
			Utility::sendAlert("报警",$name,$content,true);
            $alarm_content['content'] = $name . $content;
			$redis->rpush("alarm-list",json_encode($alarm_content));
            $this->sendToOthers($name, $content);

            switch ($key_patten) {
                //mv monitor
                case 'alarm-price-4-mediav':
                    $this->sendToMV($name, $content);
                    break;
            }
            return;
        }

        echo date('Y-m-d H:i:s') . " $name " . $content . "\n";
    }

    private function sendToMV($name, $content) {
        system("/usr/bin/curl -d 'id=80&key=oTIiByLUGEE&data=[{\"msg\":\"{$name}:$content\",\"phone\":\"18201930004\"}]' http://sms.ops.qihoo.net:8360/sms");
    }
    /**
     * 小流量阶段仅发送给自己人
     *
     * @param string $name
     * @param string $content
     */
    private function sendToOurs($name, $content)
    {
        system("/usr/bin/curl -d 'id=80&key=oTIiByLUGEE&data=[{\"msg\":\"{$name}:$content\",\"phone\":\"18611794976,18701685085,18631225517\"}]' http://sms.ops.qihoo.net:8360/sms");
    }
	private function sendToOthers($name,$content)
	{
		system("/usr/bin/curl -d 'id=80&key=oTIiByLUGEE&data=[{\"msg\":\"{$name}:$content\",\"phone\":\"13810095048,13811827058,18701687405,18501340200,13521233616,15811467315,13811666700,13581937912,18620301915,18618382281,13401145020,13301100658,18610407368,18611582654,15010285875,15801639605,15210013786,13720062901,13810471289,18600582292,18701685085,18631225517,13811004656,15116918296\"}]' http://sms.ops.qihoo.net:8360/sms");
	}
    private function getLastValue($arr,$last_key_index)
    {
        if(count($arr)<$last_key_index)
            return 0;
        $keys=array_keys($arr);
       return $arr[$keys[count($arr)-$last_key_index]];
    }

    public function actionMoveClickDetailLog($days_ago="15")
    {
        set_time_limit(0);
        $date=date('Y-m-d',strtotime("-$days_ago days"));
        $db=Yii::app()->db_click_log;
        while($datas=$db->createCommand("select * from click_detail where create_date='{$date}' limit 500")->queryAll())
        {
                $ids=$this->insertDetail($datas);
                $db->createCommand("delete from click_detail where id in (".implode(",",$ids).")")->execute();
        }
        echo "done:$days_ago\n";

    }
    private function insertDetail($datas,$tableName="click_detail_his")
    {
            $sql="insert ignore into $tableName (".implode(",",array_keys($datas[0])).") values ";
            $values=array();
                $ids=array();
            foreach($datas as $data)
            {
                    foreach ($data as $k=>&$v)
                    {
                            if (is_string($v))
                            {
                                    $v = "'" . mysql_escape_string($v) . "'";
                            }
                            else if(is_array($v))
                            {
                                    $v="'" . mysql_escape_string(@json_encode($v)) . "'";
                            }
                            else if(is_null($v)) {
                                    $v='null';
                            }
                            else if(empty($v) && !($v===0))
                            {
                                    $v="''";
                            }
                    }
                    $values[]="(".implode(',',array_values($data)).")";
                $ids[]=$data['id'];
            }
            $sql .= implode(",",$values);
            $ret=Yii::app()->db_click_log->createCommand($sql)->execute();
                return $ids;
    }

    public function actionSendExceedTimeMsg($time=7200)
    {
        $redis = new ComRedis('esc_monitor_emq', 0);
        $keys=$redis->keys("rmq:*");
        if(empty($keys))
        {
                return;
        }
        foreach($keys as $k)
        {
                //$k = "rmq:".$this->init_param.":".$send_data['exchange']
                $info=explode(":",$k);
                $emq=new ComEMQ($info[1]);
                $emq->exchangeName=$info[2];
                $emq->logid='RESEND'.time();
                $hkeys=$redis->hkeys($k);
                    $c=0;
                foreach($hkeys as $hkey)
                {
                        //hkey="1438847430:fa6a641111a570d2d96a0df1feb98bf2"
                        $t=explode(":",$hkey);
                        $t=(int)$t[0];
                        if($t>0 && time()-$t>$time)
                        {
                                $content=$redis->hget($k,$hkey);
                                $emq->send($content);
                                $redis->hdel($k,$hkey);
                            echo "del:{$k} {$hkey} send:".json_encode($content)."\n";
                            $c++;
                        }
                }
                    if($c>0)
                    Utility::sendAlert(__CLASS__,__FUNCTION__,$k ." resend {$c} msg ",true);
        }

    }
}
