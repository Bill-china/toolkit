<?php

/**
 * 监控中心代码
 */
class ComChocoMonitor
{
    const CONN_TIME_OUT = 10;
    const CONN_CURL_TIME_OUT = 10;
    private static $logid = '';

    private static function getTableName()
    {
        return 'esc_monitor';
    }

    private static $choco_list = array();

    // 脚本执行的时间
    public static $_times = '';

    /**
     * 监控服务的情况
     */
    public static function get_monitor_service_status()
    {
        self::logger(__CLASS__, __FUNCTION__, 'start');
        self::$_times = ((int)(time() / 300)) * 300;
        self::$choco_list = Yii::app()->params['monitor'];
        $monitor_data = self::get_monitor_data();

        $insertValue = array();
        self::logger(__CLASS__, __FUNCTION__, 'monitor_data.data:'. json_encode($monitor_data['data']));
        $not_insert_key = array('sev_name', 'sev_ip', 'sev_port', 'sev_key', 'smsonly', 'mailonly');
        foreach($monitor_data['data'] as $hk => $mdata)
        {
           if($mdata['load_average'])
	   {
               $arr_average = explode(',',$mdata['load_average']);
               $mdata['load_average'] = (string)round(array_sum($arr_average)/3,2);
               
	   }
		
           if($mdata['memory_status'])
	   {
               $mdata['memory_status'] = (string)(int)($mdata['memory_status']);
               
	   }
		
           if($mdata['disk_status'])
	   {
               $mdata['disk_status'] = (string)(int)($mdata['disk_status']);
               
	   }
            $sev_ip   = $mdata['sev_ip'];
            $sev_key  = $mdata['sev_key'];
            $sev_name = $mdata['sev_name'];
            $sev_port = $mdata['sev_port'];
            foreach($mdata as $key => $val)
            {	
                if (!in_array($key, $not_insert_key))
                {
                    $insertValue[] = " ('$sev_name', '$sev_key', '$sev_ip', '$sev_port', '$key', '$val', ".self::$_times.") ";
                }
            }
        }

        self::logger(__CLASS__, __FUNCTION__, 'monitor_data.alarm:'. json_encode($monitor_data['alarm']));
        if ($monitor_data['alarm'])
        {
            foreach($monitor_data['alarm'] as $key => $val)
            {
                $sms = self::$choco_list[$val['sev_name']]['alarm_group']['sms'];
                $mail = self::$choco_list[$val['sev_name']]['alarm_group']['mail'];
                $content = "统计监控获取服务监控数据异常\nsevname:".$val['sev_name'].", sevkey: ".$val['sev_key'].", http://".$val['sev_ip'].":".$val['sev_port']."/s \n";
                self::sendAlert(__CLASS__, __FUNCTION__, "统计监控获取服务监控数据异常", $content, $sms, $mail, true);
            }
        }

        if ($insertValue)
        {
            self::insertMonitorData($insertValue);
        }

        self::logger(__CLASS__, __FUNCTION__, 'done');
    }

    public static function monitor_service()
    {
        self::logger(__CLASS__, __FUNCTION__, 'start');
        $arrAlarm = array();
        self::$choco_list = Yii::app()->params['monitor'];
        try {
            // 取出来最近的一条，再按这个时间来取数据。
            $sql = 'SELECT create_time FROM ' . self::getTableName() . ' ORDER BY id DESC LIMIT 1';
            $row = Yii::app()->db_monitor->createCommand($sql)->queryRow();
            if ($row)
            {
                $create_time = $row['create_time'];
                $sqls = 'SELECT * FROM ' . self::getTableName() . ' WHERE create_time=' . $create_time;
                $list = Yii::app()->db_monitor->createCommand($sqls)->queryAll();
                foreach ($list as $key => $mdata)
                {
                    // 每个服务抓取回来的数据，都需要看一下机器情况。
                    self::monitor_system($mdata, $arrAlarm);
                    self::call($mdata['sev_name'], $mdata, $arrAlarm);
                }
            }

            foreach($arrAlarm as $title => $alarmdata)
            {
                foreach ($alarmdata as $sev_name => $msg)
                {
                    $content = '';
                    $sms = self::$choco_list[$sev_name]['alarm_group']['sms'];
                    $mail = self::$choco_list[$sev_name]['alarm_group']['mail'];
                    foreach($msg as $text)
                    {
                        $content .= $text."\n";
                    }

                    self::sendAlert(__CLASS__, __FUNCTION__, $title, $content, $sms, $mail, true);
                }
            }
        } catch (Exception $e) {
            self::logger(__CLASS__, __FUNCTION__, 'exception：' . $e->getMessage());
        }

        self::logger(__CLASS__, __FUNCTION__, 'done');
    }

    /**
     * 监控收入的情况
     * @param int $drop 降幅默认20%
     * @param int $intval_time 检测间隔时间，默认2分钟
     */
    public static function monitor_income($drop = 20, $intval_time = 2, $limit = 20)
    {
        self::logger(__CLASS__, __FUNCTION__, 'start');
        $alarm_text   = '';
        $pix_table    = 'click_detail';
        self::$_times = strtotime('-1 minute'); //一分钟之前，因为有临界点问题
        $week_time    = 60 * 60 * 24 * 7;       //上周的这个时间段

        $todayTable   = $pix_table . '_' . date('Ymd', self::$_times);

        // 间隔$intval_time时间内的数据
        $btime = strtotime(date('Y-m-d H:i:00', (self::$_times - 60 * ($intval_time - 1))));
        $etime = strtotime(date('Y-m-d H:i:59', self::$_times));
        self::logger(__CLASS__, __FUNCTION__, 'today btime : ' . date('Y-m-d H:i:s', $btime) . ' , etime : ' . date('Y-m-d H:i:s', $etime));
        $tmp   = self::querySou($btime, $etime);
        $todaySou     = $tmp['sou'];
        $todayGuess   = $tmp['guess'];
        $todayMediav  = $tmp['mediav'];
        $todayShouzhu = $tmp['shouzhu'];
        $todayMobile  = self::queryMobile($btime, $etime);
        $todayOnebox  = self::queryOnebox($btime, $etime);
        $todayCheat   = self::queryCheat($btime, $etime);
        $todayIncome  = $todaySou + $todayGuess + $todayMediav + $todayShouzhu + $todayMobile + $todayOnebox;

        self::logger(__CLASS__, __FUNCTION__, "today : $todaySou $todayGuess $todayMediav $todayShouzhu $todayMobile $todayOnebox $todayCheat");

        // 每间隔intval_time时间，检测一次。目前配置以2分钟为维度。
        $btime = strtotime(date('Y-m-d H:i:00', (self::$_times - 60 * ($intval_time * 2 - 1))));
        $etime = strtotime(date('Y-m-d H:i:59', (self::$_times - 60 * ($intval_time))));
        self::logger(__CLASS__, __FUNCTION__, 'five minutes btime : ' . date('Y-m-d H:i:s', $btime) . ' , etime : ' . date('Y-m-d H:i:s', $etime));
        $tmp   = self::querySou($btime, $etime);
        $fiveMinutesSou     = $tmp['sou'];
        $fiveMinutesGuess   = $tmp['guess'];
        $fiveMinutesMediav  = $tmp['mediav'];
        $fiveMinutesShouzhu = $tmp['shouzhu'];
        $fiveMinutesMobile  = self::queryMobile($btime, $etime);
        $fiveMinutesOnebox  = self::queryOnebox($btime, $etime);
        $fiveMinutesCheat   = self::queryCheat($btime, $etime);
        $fiveMinutesIncome  = $fiveMinutesSou + $fiveMinutesGuess + $fiveMinutesMediav + $fiveMinutesShouzhu + $fiveMinutesMobile + $fiveMinutesOnebox + $fiveMinutesCheat;

        self::logger(__CLASS__, __FUNCTION__, "five minutes : $fiveMinutesSou $fiveMinutesGuess $fiveMinutesMediav $fiveMinutesShouzhu $fiveMinutesMobile $fiveMinutesOnebox $fiveMinutesCheat");

        $perSou     = self::get_tip($fiveMinutesSou, $todaySou);
        if ($perSou > $limit) {
            $alarm_text .= '搜索收入，' . self::get_tip_desc($fiveMinutesSou, $todaySou) . ', ' . $perSou . "%\n";
        }
        $perGuess   = self::get_tip($fiveMinutesGuess, $todayGuess);
        if ($perGuess > $limit) {
            $alarm_text .= '导航收入，' . self::get_tip_desc($fiveMinutesGuess, $todayGuess) . ', ' . $perGuess . "%\n";
        }
        $perMediav  = self::get_tip($fiveMinutesMediav, $todayMediav);
        if ($perMediav > $limit) {
            $alarm_text .= 'MediaV收入，' . self::get_tip_desc($fiveMinutesMediav, $todayMediav) . ', ' . $perMediav . "%\n";
        }
        $perShouzhu = self::get_tip($fiveMinutesShouzhu, $todayShouzhu);
        if ($perShouzhu > $limit) {
            $alarm_text .= '手助收入，' . self::get_tip_desc($fiveMinutesShouzhu, $todayShouzhu) . ', ' . $perShouzhu . "%\n";
        }
        $perMobile  = self::get_tip($fiveMinutesMobile, $todayMobile);
        if ($perMobile > $limit) {
            $alarm_text .= '移动收入，' . self::get_tip_desc($fiveMinutesMobile, $todayMobile) . ', ' . $perMobile . "%\n";
        }
        $perOnebox  = self::get_tip($fiveMinutesOnebox, $todayOnebox);
        if ($perOnebox > $limit) {
            $alarm_text .= 'Onebox收入，' . self::get_tip_desc($fiveMinutesOnebox, $todayOnebox) . ', ' . $perOnebox . "%\n";
        }
        $perCheat   = self::get_tip($fiveMinutesCheat, $todayCheat);
        if ($perCheat > $limit) {
            $alarm_text .= '反作弊，' . self::get_tip_desc($fiveMinutesCheat, $todayCheat) . ', ' . $perCheat . "%\n";
        }
        $perIncome   = self::get_tip($fiveMinutesIncome, $todayIncome);
        if ($perIncome > $limit) {
            $alarm_text .= '时段总收入，' . self::get_tip_desc($fiveMinutesIncome, $todayIncome) . ', ' . $perIncome . "%\n";
        }

        // 上周同一时段的时间
        $btime = strtotime(date('Y-m-d H:i:00', (self::$_times - 60 * ($intval_time - 1) - $week_time)));
        $etime = strtotime(date('Y-m-d H:i:59', self::$_times - $week_time));
        self::logger(__CLASS__, __FUNCTION__, 'pre week btime : ' . date('Y-m-d H:i:s', $btime) . ' , etime : ' . date('Y-m-d H:i:s', $etime));
        $tmp   = self::querySou($btime, $etime);
        $preWeekSou     = $tmp['sou'];
        $preWeekGuess   = $tmp['guess'];
        $preWeekMediav  = $tmp['mediav'];
        $preWeekShouzhu = $tmp['shouzhu'];
        $preWeekMobile  = self::queryMobile($btime, $etime);
        $preWeekOnebox  = self::queryOnebox($btime, $etime);
        $preWeekCheat   = self::queryCheat($btime, $etime);
        $preWeekIncome  = $preWeekSou + $preWeekGuess + $preWeekMediav + $preWeekShouzhu + $preWeekMobile + $preWeekOnebox + $preWeekCheat;

        self::logger(__CLASS__, __FUNCTION__, "pre week : $preWeekSou $preWeekGuess $preWeekMediav $preWeekShouzhu $preWeekMobile $preWeekOnebox $preWeekCheat");

        $perSou2     = self::get_tip($preWeekSou, $todaySou);
        if ($perSou2 > $limit) {
            $alarm_text .= '搜索收入，' . self::get_tip_desc($preWeekSou, $todaySou, '上周') . ', ' . $perSou2 . "%\n";
        }
        $perGuess2   = self::get_tip($preWeekGuess, $todayGuess);
        if ($perGuess2 > $limit) {
            $alarm_text .= '导航收入，' . self::get_tip_desc($preWeekGuess, $todayGuess, '上周') . ', ' . $perGuess2 . "%\n";
        }
        $perMediav2  = self::get_tip($preWeekMediav, $todayMediav);
        if ($perMediav2 > $limit) {
            $alarm_text .= 'MediaV收入，' . self::get_tip_desc($preWeekMediav, $todayMediav, '上周') . ', ' . $perMediav2 . "%\n";
        }
        $perShouzhu2 = self::get_tip($preWeekShouzhu, $todayShouzhu);
        if ($perShouzhu2 > $limit) {
            $alarm_text .= '手助收入，' . self::get_tip_desc($preWeekShouzhu, $todayShouzhu, '上周') . ', ' . $perShouzhu2 . "%\n";
        }
        $perMobile2  = self::get_tip($preWeekMobile, $todayMobile);
        if ($perMobile2 > $limit) {
            $alarm_text .= '移动收入，' . self::get_tip_desc($preWeekMobile, $todayMobile, '上周') . ', ' . $perMobile2 . "%\n";
        }
        $perOnebox2  = self::get_tip($preWeekOnebox, $todayOnebox);
        if ($perOnebox2 > $limit) {
            $alarm_text .= 'Onebox收入，' . self::get_tip_desc($preWeekOnebox, $todayOnebox, '上周') . ', ' . $perOnebox2 . "%\n";
        }
        $perCheat2   = self::get_tip($preWeekCheat, $todayCheat);
        if ($perCheat2 > $limit) {
            $alarm_text .= '反作弊，' . self::get_tip_desc($preWeekCheat, $todayCheat, '上周') . ', ' . $perCheat2 . "%\n";
        }
        $perIncome2   = self::get_tip($preWeekIncome, $todayIncome);
        if ($perIncome2 > $limit) {
            $alarm_text .= '时段总收入，' . self::get_tip_desc($preWeekIncome, $todayIncome, '上周') . ', ' . $perIncome2 . "%\n";
        }

        if ($alarm_text) {
            // 如果有收入异常，报警
            self::sendAlert(__CLASS__, __FUNCTION__, '统计监控收入异常', $alarm_text, '', '', true);
        }

        self::logger(__CLASS__, __FUNCTION__, 'done');
    }

    // 搜索
    private static function querySou($btime, $etime)
    {
        $bd = date('d', $btime);
        $ed = date('d', $etime);
        $result = array(
            'guess' => 0,
            'shouzhu' => 0,
            'sou' => 0,
            'mediav' => 0,
        );

        if ($bd == $ed)
        {
            $table = 'click_detail_' . date('Ymd', $btime);
            $sql = "select sum(price-reduce_price) as p,source_system,ver from $table where click_time >=".$btime." and click_time <=".$etime." and pid!=238 group by source_system,ver";
            $tmp = Yii::app()->db_click_log->createCommand($sql)->queryAll();
            foreach ($tmp as $key => $val) {
                $result[$val['ver']] += $val['p'];
            }
        }
        else
        {
            $table = 'click_detail_' . date('Ymd', $btime);
            $sql = "select sum(price-reduce_price) as p,source_system,ver from $table where click_time >=".$btime." and click_time <=".$etime." and pid!=238 group by source_system,ver";
            $tmp = Yii::app()->db_click_log->createCommand($sql)->queryAll();
            foreach ($tmp as $key => $val) {
                $result[$val['ver']] += $val['p'];
            }
            $table = 'click_detail_' . date('Ymd', $etime);
            $sql = "select sum(price-reduce_price) as p,source_system,ver from $table where click_time >=".$btime." and click_time <=".$etime." and pid!=238 group by source_system,ver";
            $tmp = Yii::app()->db_click_log->createCommand($sql)->queryAll();
            foreach ($tmp as $key => $val) {
                $result[$val['ver']] += $val['p'];
            }
        }

        return $result;
    }

    // 移动
    private static function queryMobile($btime, $etime)
    {
        $bd = date('d', $btime);
        $ed = date('d', $etime);
        $mobile = 0;

        if ($bd == $ed)
        {
            $table = 'click_detail_' . date('Ymd', $btime);
            $sql = "select sum(price-reduce_price) as p,source_system,ver,source_type from $table where click_time>=".$btime." and click_time<=".$etime." and source_type=4 group by source_system,ver";
            $tmp = Yii::app()->db_click_log->createCommand($sql)->queryAll();
            foreach ($tmp as $key => $val) {
                $mobile += $val['p'];
            }
        }
        else
        {
            $table = 'click_detail_' . date('Ymd', $btime);
            $sql = "select sum(price-reduce_price) as p,source_system,ver,source_type from $table where click_time>=".$btime." and click_time<=".$etime." and source_type=4 group by source_system,ver";
            $tmp = Yii::app()->db_click_log->createCommand($sql)->queryAll();
            foreach ($tmp as $key => $val) {
                $mobile += $val['p'];
            }
            $table = 'click_detail_' . date('Ymd', $etime);
            $sql = "select sum(price-reduce_price) as p,source_system,ver,source_type from $table where click_time>=".$btime." and click_time<=".$etime." and source_type=4 group by source_system,ver";
            $tmp = Yii::app()->db_click_log->createCommand($sql)->queryAll();
            foreach ($tmp as $key => $val) {
                $mobile += $val['p'];
            }
        }

        return $mobile;
    }

    private static function queryOnebox($btime, $etime)
    {
        $bd = date('d', $btime);
        $ed = date('d', $etime);
        $onebox = 0;

        if ($bd == $ed)
        {
            $table = 'click_detail_' . date('Ymd', $btime);
            $sql = "select sum(price-reduce_price) as p,source_system,ver,source_type from $table where click_time>=".$btime." and click_time<=".$etime." and pid=238 group by source_system,ver";
            $tmp = Yii::app()->db_click_log->createCommand($sql)->queryAll();
            foreach ($tmp as $key => $val) {
                $onebox += $val['p'];
            }
        }
        else
        {
            $table = 'click_detail_' . date('Ymd', $btime);
            $sql = "select sum(price-reduce_price) as p,source_system,ver,source_type from $table where click_time>=".$btime." and click_time<=".$etime." and pid=238 group by source_system,ver";
            $tmp = Yii::app()->db_click_log->createCommand($sql)->queryAll();
            foreach ($tmp as $key => $val) {
                $onebox += $val['p'];
            }
            $table = 'click_detail_' . date('Ymd', $etime);
            $sql = "select sum(price-reduce_price) as p,source_system,ver,source_type from $table where click_time>=".$btime." and click_time<=".$etime." and pid=238 group by source_system,ver";
            $tmp = Yii::app()->db_click_log->createCommand($sql)->queryAll();
            foreach ($tmp as $key => $val) {
                $onebox += $val['p'];
            }
        }

        return $onebox;
    }

    private static function queryCheat($btime, $etime)
    {
        $bd = date('d', $btime);
        $ed = date('d', $etime);
        $cheat = 0;

        if ($bd == $ed)
        {
            $table = 'click_detail_' . date('Ymd', $btime);
            $sql = "select count(*) as p,cheat_type from $table where click_time>=".$btime." and click_time<=".$etime." group by cheat_type";
            $tmp = Yii::app()->db_click_log->createCommand($sql)->queryAll();
            foreach ($tmp as $key => $val) {
                $cheat += $val['p'];
            }
        }
        else
        {
            $table = 'click_detail_' . date('Ymd', $btime);
            $sql = "select count(*) as p,cheat_type from $table where click_time>=".$btime." and click_time<=".$etime." group by cheat_type";
            $tmp = Yii::app()->db_click_log->createCommand($sql)->queryAll();
            foreach ($tmp as $key => $val) {
                $cheat += $val['p'];
            }
            $table = 'click_detail_' . date('Ymd', $etime);
            $sql = "select count(*) as p,cheat_type from $table where click_time>=".$btime." and click_time<=".$etime." group by cheat_type";
            $tmp = Yii::app()->db_click_log->createCommand($sql)->queryAll();
            foreach ($tmp as $key => $val) {
                $cheat += $val['p'];
            }
        }

        return $cheat;
    }

    private static function monitor_system($monitor_data, &$arrAlarm)
    {
        $sev_ip   = $monitor_data['sev_ip'];
        $sev_key  = $monitor_data['sev_key'];
        $sev_name = $monitor_data['sev_name'];
        $sev_port = $monitor_data['sev_port'];
        $key      = $monitor_data['key'];
        $value    = $monitor_data['value'];

        if ($key == 'disk_status' && $value)
        {
            $disk_tmp = explode(',', $value);
            $disk     = isset($disk_tmp[0]) ? str_replace('G', '', $disk_tmp[0]) : 0;
            $disk_use = isset($disk_tmp[1]) ? str_replace('G', '', $disk_tmp[1]) : 0;
            if ($disk > 0 && $disk_use > 0 && ($disk_use / $disk * 100) > 80)
            {
                if (!isset($arrAlarm['统计监控磁盘剩余预警'][$sev_name]))
                {
                    $arrAlarm['统计监控磁盘剩余预警'][$sev_name] = array();
                }
                $arrAlarm['统计监控磁盘剩余预警'][$sev_name][] = "sevname : $sev_name , sevkey : $sev_key , http://$sev_ip:$sev_port/s disk：$disk G, disk used：$disk_use G";
            }
        }

        if ($key == 'load_average' && $value)
        {
            $load_tmp = explode(',', $value);
            if (count($load_tmp) == 3)
            {
                $load1 = $load_tmp[0];
                $load2 = $load_tmp[1];
                $load3 = $load_tmp[2];
            }
        }

        if ($key == 'memory_status' && $value)
        {
            $memory_tmp = explode(',', $value);
            $memory     = isset($memory_tmp[0]) ? $memory_tmp[0] : 0;
            $memory_use = isset($memory_tmp[1]) ? $memory_tmp[1] : 0;
            if ($memory && $memory_use && ($memory_use / $memory * 100) > 80)
            {
                if (!isset($arrAlarm['统计监控内存剩余预警'][$sev_name]))
                {
                    $arrAlarm['统计监控内存剩余预警'][$sev_name] = array();
                }
                $arrAlarm['统计监控内存剩余预警'][$sev_name][] = "sevname : $sev_name , sevkey : $sev_key , http://$sev_ip:$sev_port/s memory：$memory M, memory used：$memory_use M";
            }
        }
    }

    /**
     * 得到监控相应的服务返回的数据报告
     *
     * $return array(
     *  'data'  // 正常返回的服务及数据
     *  'alarm' // 在拉取时返回空的，需要报警的请求
     * )
     */
    public static function get_monitor_data()
    {
        $result = array(
            'data' => array(),
            'alarm' => array()
        );

        foreach(self::$choco_list as $sev_name => $ip_list)
        {
            $mdata = self::connect_mutil_exec($ip_list['service_list']);
            foreach($ip_list['service_list'] as $key => $sev)
            {
                $ipTmp    = explode(':', $sev['sev_url']);
                $sev_ip   = isset($ipTmp[0]) ? $ipTmp[0] : '';
                $sev_port = isset($ipTmp[1]) ? $ipTmp[1] : '';
                $sev_key  = $sev['sev_key'];

                if (isset($mdata[$key]) && $mdata[$key])
                {
                    $jsonData = json_decode($mdata[$key], true);
                    if (!empty($jsonData))
                    {
                        $jsonData['sev_name'] = $sev_name;
                        $jsonData['sev_ip']   = $sev_ip;
                        $jsonData['sev_port'] = $sev_port;
                        $jsonData['sev_key']  = $sev_key;
                    }
                    $result['data'][] = $jsonData;
                }
                else
                {
                    $result['alarm'][] = array(
                        'sev_name' => $sev_name,
                        'sev_ip'   => $sev_ip,
                        'sev_port' => $sev_port,
                        'sev_key'  => $sev_key,
                    );
                }
            }
        }

        return $result;
    }

    /**
     * 添加监控数据
     *
     * @return boolean
     */
    private static function insertMonitorData($values)
    {
        $result = false;
        try {
            $sql = "INSERT INTO " .self::getTableName() . " (`sev_name`, `sev_key`, `sev_ip`, `sev_port`, `key`, `value`, `create_time`) VALUES ";
            $sql .= implode(',', $values);
            $result = Yii::app()->db_monitor->createCommand($sql)->execute();
        } catch (Exception $e) {
            echo $e->getMessage() . "\n";
        }

        return $result;
    }

    protected function connect_mutil_exec($curl_data)
    {
        $mh = curl_multi_init();
        $codes      = array();
        $connects   = array();
        $result     = array();

        foreach($curl_data as $id => $sev) {
            $url = $sev['sev_url'] . '/s';
            $connects[$id] = curl_init($url);
            curl_setopt($connects[$id], CURLOPT_URL, $url);
            curl_setopt($connects[$id], CURLOPT_USERAGENT, 'dianjing.monitor');
            curl_setopt($connects[$id], CURLOPT_HEADER, false);
            curl_setopt($connects[$id], CURLOPT_RETURNTRANSFER, true);
            curl_setopt($connects[$id], CURLOPT_COOKIEFILE, '');
            curl_setopt($connects[$id], CURLOPT_AUTOREFERER, true);
            curl_setopt($connects[$id], CURLOPT_IPRESOLVE, CURL_IPRESOLVE_V4);
            curl_setopt($connects[$id], CURLOPT_REFERER, 'http://www.haosou.com/');
            curl_setopt($connects[$id], CURLOPT_CONNECTTIMEOUT, self::CONN_CURL_TIME_OUT);
            curl_setopt($connects[$id], CURLOPT_TIMEOUT, self::CONN_TIME_OUT);
            curl_setopt($connects[$id], CURLOPT_FOLLOWLOCATION, true);
            curl_setopt($connects[$id], CURLOPT_MAXREDIRS, 3); 
            curl_setopt($connects[$id], CURLOPT_SSL_VERIFYPEER, false);
            curl_multi_add_handle($mh, $connects[$id]);
        }

        do {
            $status = curl_multi_exec($mh, $active);
            $info = curl_multi_info_read($mh, $count);
            if ($info !== false) {
                $codes[(int)$info['handle']] = (int)$info['result'];
            }
        }
        while ($status === CURLM_CALL_MULTI_PERFORM || $active || $count);

        foreach ($connects as $id => $connect) {
            $json = '';
            $errno = curl_error($connect);
            if (empty($errno))
            {
                $json = curl_multi_getcontent($connect);
            }
            $result[$id] = $json;
            curl_close($connect);
        }

        curl_multi_close($mh);

        return $result;
    }

    public static function sendAlert($className = '', $functionName = '', $title = '', $content = '', 
        $smsonly = 'esc_choco_monitor_smsonly', $mailonly = 'esc_choco_monitor_mailonly', $sendSms=false)
    {
// TODO
return;
        if($className . $functionName . $title . $content == '')
        {
            return ;
        }

        $md5=md5($className.$functionName.$content);
        self::logger(__CLASS__,__FUNCTION__,array($className,$functionName,$content,$sendSms));
        $exists=false;
        try{
            $f="/tmp/mail_".$md5;
            if(file_exists($f))
            {
                if((time()-filemtime($f))<1*60)
                {
                    $exists=true;
                }
                else
                {
                    unlink($f);
                }
            }
            else
            {
                touch($f);
            }
        } catch(Exception $e) {
            $exists=false;
        }

        if($exists===true)
        {
            return;
        }

        $ip=`/sbin/ifconfig  | /bin/grep 'inet addr:'| /bin/grep -v '127.0.0.1' | /usr/bin/cut -d: -f2 | /usr/bin/awk 'NR==1 { print $1}'`;
        $table = "<table border=1><tbody><tr><td><b> 统计监控中心报警  from ip:" .($ip) . " logid:".self::getLogId()." pid:".getmypid()."</b></td></tr>\n";
        $table .= "<tr><td>【{$className}::{$functionName}】</td></tr>\n";
        $table .= "<tr><td>" . str_replace("\n", "<br/>", $content) . "</td></tr>\n";
        $table .= "</tbody></table>\n";
        try {
            if (empty($title))
            {
                $title="统计监控中心报警" . '-' . date('Y-m-d H:i:s');
            }
            else
            {
                $title .= '-' . date('Y-m-d H:i:s');
            }
            if($sendSms)
            {
                $ch = curl_init("http://10.108.68.121:888/notice/notice.php?s=&c=".urlencode("$className::$functionName ".substr($content,0,100))."&g=esc_choco_monitor_smsonly");
                curl_exec($ch);
                curl_close($ch);
            }
            $ch = curl_init("http://10.108.68.121:888/notice/notice.php?s=".urlencode($title)."&c=".urlencode($table)."&g=esc_choco_monitor_mailonly");
            curl_exec($ch);
            curl_close($ch);
        } catch (Exception $e) {
            /*发邮件异常*/
        }
    }

    public static function getLogId()
    {
        if (self::$logid == '') {
            self::$logid = 'AUDIT_'.rand(1, 9) . microtime(true)*10000 . rand(100,999);
        }
        return self::$logid;
    }

    public static function logger($class, $func, $content)
    {
        if (is_array($content)) {
            $content = json_encode($content);
        }

        $pid = getmypid();

        echo date('Ymd His') . "\t$class\t$func\tPID_$pid\t$content\n";
    }

    private static function get_tip($v1, $v2)
    {
        if ($v1 == 0 && $v2 == 0) {
            return 0;
        }

        $per=(int)(abs(($v1-$v2)/max($v1,$v2))*100);

        return $per;
    }

    private static function get_tip_desc($v1, $v2, $w = '')
    {
        $tip = '';
        if($v1 < $v2)
        {
            $tip = "同比{$w}上涨";
        }
        else if ($v1 == $v2)
        {
            $tip = '同比{$w}持平';
        }
        else
        {
            $tip = "同比{$w}下降";
        }

        return $tip;
    }

    /**
     * 调用函数
     */
    private static function call($func, $param, &$arrAlarm)
    {
        try {
            $funcationName = $func.'_call';
            $callable = 'ComChocoMonitor::'.$funcationName;
            if (is_callable($callable) == true)
            {
                return call_user_func_array(
                    array('ComChocoMonitor', $funcationName), 
                    array($param, &$arrAlarm)
                );
            }
            else
            {
                throw new Exception(__CLASS__.'.call found not function. call function name is : ' .$funcationName);
            }
        } catch (Exception $e) {
            echo $e->getMessage();
            exit;
        }
    }
    private static function chocodb_call($monitor_data, &$arrAlarm)
    {
        $key      = $monitor_data['key'];
        $value    = $monitor_data['value'];
        $sev_ip   = $monitor_data['sev_ip'];
        $sev_key  = $monitor_data['sev_key'];
        $sev_name = $monitor_data['sev_name'];
        $sev_port = $monitor_data['sev_port'];

        return true;
    }

    private static function chocomonitor_call($monitor_data, &$arrAlarm)
    {
        $key      = $monitor_data['key'];
        $value    = $monitor_data['value'];
        $sev_ip   = $monitor_data['sev_ip'];
        $sev_key  = $monitor_data['sev_key'];
        $sev_name = $monitor_data['sev_name'];
        $sev_port = $monitor_data['sev_port'];
        if ($key == 'handle_file_count')
        {

        }

        if ($key == 'block_file_count' && $value >= 10)
        {
            if (!isset($arrAlarm['统计监控stats处理文件积压'][$sev_name]))
            {
                $arrAlarm['统计监控stats处理文件积压'][$sev_name] = array();
            }
            $arrAlarm['统计监控stats处理文件积压'][$sev_name][] = "sev_name : $sev_name , sevkey : $sev_key , http://$sev_ip:$sev_port/s 积压数：$value";
        }

        return true;
    }

    private static function chocosync_call($monitor_data, &$arrAlarm)
    {
        $key      = $monitor_data['key'];
        $value    = $monitor_data['value'];
        $sev_ip   = $monitor_data['sev_ip'];
        $sev_key  = $monitor_data['sev_key'];
        $sev_name = $monitor_data['sev_name'];
        $sev_port = $monitor_data['sev_port'];

        // 上传文件积压
        if ($sev_key == 'client' && $value >= 10 && $key == 'block_file_count')
        {
            if (!isset($arrAlarm['统计监控上传文件积压'][$sev_name]))
            {
                $arrAlarm['统计监控上传文件积压'][$sev_name] = array();
            }
            $arrAlarm['统计监控上传文件积压'][$sev_name][] = "sevname : $sev_name , sevkey : $sev_key 上传文件积压，数量：$value http://$sev_ip:$sev_port/s";
        }

        return true;
    }
}

