<?php

/**
 * Author: jingguangwen@360.cn
 *
 * Last modified: 2013-07-29 14:38
 *
 * Filename: MonitorCommand.php
 *
 * Description:
 *
 */
class MonitorCommand extends CConsoleCommand
{
    /**
     * 监控昨日消费
     */
    public function actionMonitorDayCost ()
    {
        set_time_limit(0);
        //Yii::import('application.extensions.CSmsclient', true);
        $yestoday = date('Y-m-d', strtotime('-1 day'));
        $ad_user_charge_log_amount = $ad_click_log_amount = $ad_click_log_count =  $click_detail_amount = $click_detail_count = $click_detail_not_mv_count = 0;

        $mv_stats_amount = $mv_stats_1_amount = $mv_stats_2_amount = $mv_stats_3_amount = $mv_stats_4_amount = 0;

        $esc_stats_click_amount = $esc_stats_click_1_amount = $esc_stats_click_2_amount = $esc_stats_click_3_amount = $esc_stats_click_4_amount = 0;
        $esc_stats_click_count = $esc_stats_click_1_count = $esc_stats_click_2_count = $esc_stats_click_3_count = $esc_stats_click_4_count = 0;

        //ad_user_charge_log表结算金额
        $ad_user_charge_log_sql  = "SELECT  sum(cost) as cost_money,create_date from ad_user_charge_log  where  create_date = '".$yestoday."'";
        $ad_user_charge_log_arr = Yii::app()->db_center->createCommand($ad_user_charge_log_sql)->queryRow();
        if (!empty($ad_user_charge_log_arr)) {
            $ad_user_charge_log_amount = $ad_user_charge_log_arr['cost_money'];
        }
        //ad_click_log表结算金额
        // $ad_click_log_sql = "SELECT  sum(price) as cost_money,create_date  from ad_click_log  where create_date = '".$yestoday."' and `status` not in (-1,2)   and deal_status =1 ";
        // $ad_click_log_arr = Yii::app()->db_click_log->createCommand($ad_click_log_sql)->queryRow();
        // if (!empty($ad_click_log_arr)) {
        // 	$ad_click_log_amount = $ad_click_log_arr['cost_money'];
        // }

        //click_detail表结算金额
        $click_detail_sql = "SELECT  sum(price-reduce_price) as cost_money,create_date,count(1) as clicks   from click_detail where create_date = '".$yestoday."' and `status` not in (-1,2,3,4) and deal_status=1  and cheat_type  not  in (2,3) and price != reduce_price";
        $click_detail_arr = ComAdDetail::queryBySql($click_detail_sql, strtotime($yestoday), 'row');
        if (!empty($click_detail_arr)) {
            $click_detail_amount = $click_detail_arr['cost_money'];
            $click_detail_count = $click_detail_arr['clicks'];
        }

        $click_detail_count_sql = "SELECT  sum(price-reduce_price) as cost_money,create_date,count(1) as clicks   from click_detail where create_date = '".$yestoday."' and `status` not in (-1,2,3,4) and deal_status=1  and cheat_type  not  in (2,3) and price != reduce_price and  ver !='mediav' ";
        $click_detail_count_arr = ComAdDetail::queryBySql($click_detail_count_sql, strtotime($yestoday), 'row');
        if (!empty($click_detail_count_arr)) {
            $click_detail_not_mv_amount = $click_detail_count_arr['cost_money'];
            $click_detail_not_mv_count = $click_detail_count_arr['clicks'];
        }
        //mv_stats_表mv金额信息  start

        $mv_stats_table = 'mv_stats_'.date('Ymd', strtotime('-1 day'));
        $mv_stats_sql = "SELECT sum(end_costs) as cost_money,sum(views) as views_all from ".$mv_stats_table;
        //结算库1
        $mv_stats_1_arr = Yii::app()->db_stat_1->createCommand($mv_stats_sql)->queryRow();
        if (!empty($mv_stats_1_arr)) {
            $mv_stats_1_amount = $mv_stats_1_arr['cost_money'];
        }
        //结算库2
        $mv_stats_2_arr = Yii::app()->db_stat_2->createCommand($mv_stats_sql)->queryRow();
        if (!empty($mv_stats_2_arr)) {
        	$mv_stats_2_amount = $mv_stats_2_arr['cost_money'];
        }
        //结算库3
        $mv_stats_3_arr = Yii::app()->db_stat_3->createCommand($mv_stats_sql)->queryRow();
        if (!empty($mv_stats_3_arr)) {
        	$mv_stats_3_amount = $mv_stats_3_arr['cost_money'];
        }
        //结算库4
        $mv_stats_4_arr = Yii::app()->db_stat_4->createCommand($mv_stats_sql)->queryRow();
        if (!empty($mv_stats_4_arr)) {
        	$mv_stats_4_amount = $mv_stats_4_arr['cost_money'];
        }

        $mv_stats_amount = round($mv_stats_1_amount+$mv_stats_2_amount+$mv_stats_3_amount+$mv_stats_4_amount,2);
        //mv_stats_表mv金额信息 end

        //esc_stats_click表结算金额 start
        $esc_stats_click_table = 'esc_stats_click_'.date('Ymd', strtotime('-1 day'));
        $esc_stats_click_sql = "SELECT sum(total_cost) as cost_money,create_date,sum(clicks) as total_clicks from ".$esc_stats_click_table."  where create_date ='".$yestoday."' and  `status`=1";
        //结算库1
        $esc_stats_click_1_arr = Yii::app()->db_stat_1->createCommand($esc_stats_click_sql)->queryRow();
        if (!empty($esc_stats_click_1_arr)) {
        	$esc_stats_click_1_amount = $esc_stats_click_1_arr['cost_money'];
        	$esc_stats_click_1_count = $esc_stats_click_1_arr['total_clicks'];
        }
        //结算库2
        $esc_stats_click_2_arr = Yii::app()->db_stat_2->createCommand($esc_stats_click_sql)->queryRow();
        if (!empty($esc_stats_click_2_arr)) {
        	$esc_stats_click_2_amount = $esc_stats_click_2_arr['cost_money'];
        	$esc_stats_click_2_count = $esc_stats_click_2_arr['total_clicks'];
        }
        //结算库3
        $esc_stats_click_3_arr = Yii::app()->db_stat_3->createCommand($esc_stats_click_sql)->queryRow();
        if (!empty($esc_stats_click_3_arr)) {
        	$esc_stats_click_3_amount = $esc_stats_click_3_arr['cost_money'];
        	$esc_stats_click_3_count = $esc_stats_click_3_arr['total_clicks'];
        }
        //结算库4
        $esc_stats_click_4_arr = Yii::app()->db_stat_4->createCommand($esc_stats_click_sql)->queryRow();
        if (!empty($esc_stats_click_4_arr)) {
        	$esc_stats_click_4_amount = $esc_stats_click_4_arr['cost_money'];
        	$esc_stats_click_4_count = $esc_stats_click_4_arr['total_clicks'];
        }

        $esc_stats_click_amount = round($esc_stats_click_1_amount+$esc_stats_click_2_amount+$esc_stats_click_3_amount+$esc_stats_click_4_amount,2);
        $esc_stats_click_count = round($esc_stats_click_1_count+$esc_stats_click_2_count+$esc_stats_click_3_count+$esc_stats_click_4_count,2);
        //ad_stats_click表结算金额 end

        $monitor_redis_data_arr = array(
            // 金额相关
            'adClicklogAmount' => $ad_click_log_amount,//ad_click_log表结算金额
            'clickDetailAmount' => $click_detail_amount,//click_detail表结算金额
            'adUserChargeLog' => $ad_user_charge_log_amount,//ad_user_charge_log表结算金额
            'clickDetailNotMvAmount' => $click_detail_not_mv_amount,//click_detail表非mv结算金额
            'escStatsClickAmount' => $esc_stats_click_amount,//esc_stats_click结算金额,等于clickDetailNotMvAmount的金额
            'mvStatsAmount' =>$mv_stats_amount,//mv结算金额
            //点击数相关
            'clickDetailCount' => $click_detail_count,//ad_click_log表结算有效点击数
            'clickDetailNotMvCount' => $click_detail_not_mv_count,//click_detail表结算有效点击数
            'escStatsClickCount' => $esc_stats_click_count,//esc_stats_click结算点击数
            'costDate' => $yestoday,//结算日期

        );
        $redis = new ComRedis('esc_monitor_redis', 0);
        $redis->set("monitor-jiesuan-info",json_encode($monitor_redis_data_arr));

        $mobile_arr = array(18610407368,18611794976);

        //message content
        $sms_content = $yestoday."结算：clickDetailAmount:".$click_detail_amount.";adUserChargeLog:".$ad_user_charge_log_amount.";escStatsClickAmount:".$esc_stats_click_amount.";mvStatsAmount:".$mv_stats_amount.";clickDetailNotMvAmount:".$click_detail_not_mv_amount.";clickDetailCount:".$click_detail_count.";clickDetailNotMvCount:".$click_detail_not_mv_count.";escStatsClickCount:".$esc_stats_click_count;
        //短信发送
        foreach ($mobile_arr as $mobile) {
            exec("curl -d 'source=dianjing&sign=5ad0561bf86adeea20109ed2cde8954c&roleName=sms_mc&smsMobile=".$mobile."&smsContent=".$sms_content."' http://mc.ad.360.cn/api.php");
        }


    }
    /**
     * 反作弊监控
     * @author jingguangwen@360.cn
     */
    public function actionMonitorCheatLog ()
    {
        // 作弊数据文件
        $cheatDataDir = Config::item('logDir') . 'cheat/';
        // 作弊数据处理文件标记文件路径
        $finishFlagDir = $cheatDataDir . 'finishFlag/';

        $last_hour = date("YmdH",time()-3600);

        $files = array();
        $d = dir($cheatDataDir);
        $so_has = $shouzhu_has = 0;
        //作弊日志一小时内是否存在
        while (false !== ($entry = $d->read())) {
            if ($entry == '.' || $entry == '..') {
                continue;
            }
            // 201404040500         搜索类点击作弊日志
            // guess.201404040500   猜你喜欢点击作弊日志
            // shouzhu.201404040500 手助点击作弊日志

            //搜索类监控
            if($so_has == 0){
                if (1==preg_match('/^'."$last_hour".'[0-9]{2}$/', $entry) ) {
                    $so_has = 1;
                }
            }

            //手机助手监控
            if($shouzhu_has == 0){
                if (1==preg_match('/^shouzhu\.'."$last_hour".'[0-9]{2}$/', $entry) ) {
                    $shouzhu_has = 1;
                }
            }
            if($so_has == 1 && $shouzhu_has == 1){
                break;
            }
        }
        $d->close();

        $finish_d = dir($finishFlagDir);
        $so_finish_has = $shouzhu_finish_has = 0;
        //作弊日志一小时内是否处理了
        while (false !== ($finish_entry = $finish_d->read())) {
            if ($finish_entry == '.' || $finish_entry == '..') {
                continue;
            }
            //搜索类监控
            if($so_finish_has == 0){
                if (1==preg_match('/^'."$last_hour".'[0-9]{2}\.finish$/', $finish_entry) ) {
                    $so_finish_has = 1;
                }
            }
            //手机助手监控
            if($shouzhu_finish_has == 0){
                if (1==preg_match('/^shouzhu\.'."$last_hour".'[0-9]{2}\.finish$/', $finish_entry) ) {
                    $shouzhu_finish_has = 1;
                }
            }
            if($so_finish_has == 1 && $shouzhu_finish_has == 1){
                break;
            }
        }
        $finish_d->close();

        if($so_has == 0  || $shouzhu_has == 0 || $so_finish_has == 0  || $shouzhu_finish_has == 0){
            $alert_content = '前一小时'.$last_hour.'反作弊异常：';
            $name = '';
            if($so_has == 0){
                $alert_content .= '无搜索类反作弊日志;';
                $name = '搜索类';
            }

            if($shouzhu_has == 0){
                $alert_content .= '无shouzhu类反作弊日志;';
                $name = 'shouzhu类';
            }
            if($so_finish_has == 0){
                $alert_content .= '无搜索类处理日志;';
                $name = '搜索类';
            }

            if($shouzhu_finish_has == 0){
                $alert_content .= '无shouzhu类处理日志;';
                $name = 'shouzhu类';
            }
            Utility::sendAlert("反作弊报警",$name,$alert_content,true);
        }
    }

    /**
     *  监控脚本
     */
    public function actionMonitorData()
    {
        ComChocoMonitor::logger(__CLASS__, __FUNCTION__, 'start');
        ComChocoMonitor::monitor_service();
        ComChocoMonitor::logger(__CLASS__, __FUNCTION__, 'done');
    }

    /**
     *  监控服务状态，定时抓取服务情况
     */
    public function actionMonitorService()
    {
        ComChocoMonitor::logger(__CLASS__, __FUNCTION__, 'start');
        ComChocoMonitor::get_monitor_service_status();
        ComChocoMonitor::logger(__CLASS__, __FUNCTION__, 'done');
    }

    public function actionMonitorIncome($drop = 20, $intval_time = 2, $limit = 20)
    {
        ComChocoMonitor::logger(__CLASS__, __FUNCTION__, 'start');
        ComChocoMonitor::monitor_income($drop, $intval_time, $limit);
        ComChocoMonitor::logger(__CLASS__, __FUNCTION__, 'done');
    }

    /**
     * 监控chocodb 错误日志
     */
    public function actionErrorLog()
    {
        $warningContent = '';
        $directory = "/data/log/";
        $mydir = dir($directory);
        $date = date("Y/m/d", time());
        $time = date("H:i:s", time() - 5 * 60);
        while ($file = $mydir->read()) {
            if ((is_dir("$directory/$file")) || ($file == ".") || ($file == "..") || substr($file, 0, 7) != 'chocodb') {
                continue;
            } else {
                $cmd = "grep {$date} {$directory}{$file}  | grep ERROR | awk '{if ($2>\"{$time}\") print $0}'";
                echo $cmd . "\n";
                exec($cmd, $retval);
                if (count($retval) > 0) {
                    foreach ($retval as $value) {
                        $warningContent .= sprintf("%s:%s\n", $file, $value);
                    }
                }
            }
        }
        if (strlen($warningContent) > 0) {
            $subject = "chocodb ERROR log 报警";
            $body = $warningContent;
            $from = "wangyunlong@alarm.360.cn";
            $toList = array(
                'wangyunlong@360.cn',
                'renyajun@360.cn',
                'jingguangwen@360.cn'
            );
            $res = Utility::sendMail($subject, $body, $from, $toList);
            printf("send email, res[%s]\n", $res);
        }
    }
    /**
     * 每小时执行一次监控，超投数据，并发邮件通知相应人员
     * @author jingguangwen@360.cn
     */
    public function actionMonitorOverChargeUser ()
    {
        set_time_limit(0);

        $beginTime = time();
        $task_name = sprintf("[Monitor_MonitorOverChargeUser %s]", date('Ymd_His', $beginTime));
        printf("%s begin at %s\n", $task_name, date('Y-m-d H:i:s', $beginTime));
        //写文件
        $out_file_file = '/data/log/monitor_over_charge_user_have_send_content.log';
        $fp_read = fopen($out_file_file, 'w+');
        $have_send_arr  = $send_arr = array();
        while(false !== ($line = fgets($fp_read, 10240)) ) {
            $line = trim($line);
            if ($line === '') {
                continue;
            }
            $content_arr = explode("\t", $line);
            if(count($content_arr) != 2){
                continue;
            }
            $content_date = $content_arr[0];
            $content_plan_id = $content_arr[1];
            $have_send_arr[$content_date][$content_plan_id] = 1;
        }
        fclose($fp_read);

        $response = 0;
        $date = date('Ymd',time());
        $table_name = "click_detail_".$date;
        $create_date = date('Y-m-d',time());
        $invalid_sql  =  "SELECT  count(1) as  clicks,sum(reduce_price) as costs,ad_user_id,ad_plan_id  from {$table_name}   where cheat_type  not  in  (2,3) and   status not in (-1,2)  and    extension  in (3) and  ver!='mediav'  group  by ad_plan_id ";
        $valid_sql    =  "SELECT  count(1) as  clicks,sum(price-reduce_price) as  costs,ad_user_id,ad_plan_id from {$table_name}    where cheat_type  not  in  (2,3) and  price!=reduce_price  and status not in (-1,2)  and  ver != 'mediav'  group  by ad_plan_id";

        $invalid_arrs = Yii::app()->db_click_log_slave->createCommand($invalid_sql)->queryAll();

        $valid_arrs = Yii::app()->db_click_log_slave->createCommand($valid_sql)->queryAll();

        $valid_user_cost_info_arrs = array();

        $out_arr = array();
        //$valid_user_cost_info_arrs = $invalid_user_cost_info_arrs = array();
        if(!empty($valid_arrs)){
            foreach ($valid_arrs as $valid_arr) {
                $ad_plan_id = $valid_arr['ad_plan_id'];
                $valid_user_cost_info_arrs[$ad_plan_id] = $valid_arr;
            }
        }

        if(!empty($invalid_arrs)){
            foreach ($invalid_arrs as $invalid_arr) {
                $ad_plan_id = $invalid_arr['ad_plan_id'];
                $ad_user_id = $invalid_arr['ad_user_id'];

                $invalid_clicks = $invalid_arr['clicks'];
                $invalid_costs  = $invalid_arr['costs'];

                $valid_clicks_old = $valid_clicks = 0;
                $valid_costs = 0;
                if(isset($valid_user_cost_info_arrs[$ad_plan_id])){
                    $valid_clicks_old = $valid_clicks = $valid_user_cost_info_arrs[$ad_plan_id]['clicks'];
                    $valid_costs = $valid_user_cost_info_arrs[$ad_plan_id]['costs'];
                }
                $valid_clicks = max(1,$valid_clicks);

                $invalid_rate = $invalid_clicks/$valid_clicks;

                if($invalid_rate>50 && $valid_clicks_old>5){
                    //再加一层条件，发送过的不再发送
                    if(!isset($have_send_arr[$create_date][$ad_plan_id])){

                        $out_arr[] =  array(
                            'ad_user_id'=> $ad_user_id,
                            'ad_plan_id'=> $ad_plan_id,
                            'invalid_clicks'=> $invalid_clicks,
                            'invalid_costs'=> $invalid_costs,
                            'valid_clicks'=> $valid_clicks_old,
                            'valid_costs'=> $valid_costs,
                            'create_date'=> $create_date,
                        );
                        $send_arr[$ad_plan_id] = $create_date;
                    }
                }

            }
        }
        if(!empty($out_arr)){

            $table = '<table border="1">'."\n";
            $table .= "<tr>\n";
            $table .= "<th>日期</th><th>账户</th><th>计划</th><th>正常点击数</th> <th>超投点击数</th> <th>正常消费</th> <th>超投消费</th> \n";
            $table .= "</tr>\n";

            foreach ($out_arr as $arr) {
                $table .= sprintf("<tr>\n");
                $table .= sprintf(
                    "<td>%s</td><td>%d</td><td>%d</td><td>%d</td><td>%d</td><td>%.2f</td><td>%.2f</td>\n",
                    $arr['create_date'], $arr['ad_user_id'],$arr['ad_plan_id'],$arr['valid_clicks'], $arr['invalid_clicks'], $arr['valid_costs'], $arr['invalid_costs']

                );
                $table .= sprintf("</tr>\n");
            }
            $table .= "</table>\n";
            $header = sprintf("超投点击/正常计费点击>50 账户与计划数据统计(不包含mediav的数据)<p>\n");

            $body = $header.$table;
            $toList = array(

                //'guojing-ps@360.cn',
                //'g-pareshen@list.qihoo.net',
                'g-dianjing-faq@list.qihoo.net',
                'sunqian@360.cn',
                'wangguoying@alarm.360.cn',
                'jingguangwen@alarm.360.cn',
                'renyajun@alarm.360.cn',
             );
            $from = 'jingguangwen@alarm.360.cn';
            //$response = Utility::sendMail('超投异常账户与计划统计', $body, $from, $toList);
            $response = Utility::sendEmail($from, $toList, '超投异常账户与计划统计', $body);


            if(!empty($send_arr)){

                $out_file_file = '/data/log/monitor_over_charge_user_have_send_content.log';
                $fp = fopen($out_file_file, 'w');
                $str = '';
                foreach ($send_arr as $ad_plan_id => $create_date) {
                    $str .= $create_date."\t".$ad_plan_id."\n";
                }
                //写发送文件
                fwrite($fp, $str);
                fclose($fp);
            }

        }


        printf("%s send email, res[%s]\n", $task_name, $response);
        $endTime = time();
        printf(
            "%s begin at %s, end at %s\n",
            $task_name, date('Y-m-d H:i:s', $beginTime), date('Y-m-d H:i:s', $endTime)
        );

    }
    /**
     * 每天执行一次监控，计划消费信息，并发邮件通知相应人员
     * @author jingguangwen@360.cn
     */
    public function actionMonitorPlanCost ()
    {
        set_time_limit(0);

        $beginTime = time();
        $task_name = sprintf("[Monitor_PlanCost %s]", date('Ymd_His', $beginTime));
        printf("%s begin at %s\n", $task_name, date('Y-m-d H:i:s', $beginTime));

        $response = 0;
        $out_arr = array();
        $over_charge_arr = array();
        $create_date = date('Y-m-d',strtotime('-1  day'));

        for ($i=0; $i < 10; $i++) {

            $table_name = 'ad_plan_quota_'.$i;
            //总消费
            $total_cost_sql         =  "SELECT  ad_user_id,sum(yesterday_plan_cost) as  costs from {$table_name} where yesterday_plan_cost>yesterday_plan_quota and yesterday_plan_quota != 0  GROUP BY  ad_user_id ";
            $over_charge_cost_sql   =  "SELECT  ad_user_id,sum(yesterday_plan_cost-yesterday_plan_quota) as  over_quota_costs from {$table_name} where yesterday_plan_cost>yesterday_plan_quota and yesterday_plan_quota != 0  GROUP BY  ad_user_id ";

            $total_cost_arrs = Yii::app()->db_quota->createCommand($total_cost_sql)->queryAll();

            $over_charge_cost_arrs = Yii::app()->db_quota->createCommand($over_charge_cost_sql)->queryAll();
            //超投的金额
            if(!empty($over_charge_cost_arrs)){

                foreach ($over_charge_cost_arrs as $over_charge_cost_arr) {

                    $ad_user_id                   = $over_charge_cost_arr['ad_user_id'];
                    $over_quota_costs             = $over_charge_cost_arr['over_quota_costs'];

                    $over_charge_arr[$ad_user_id] = $over_quota_costs;
                }
            }
            //总消费
            if(!empty($total_cost_arrs)){

                foreach ($total_cost_arrs as $total_cost_arr) {

                    $ad_user_id         = $total_cost_arr['ad_user_id'];
                    $user_cost_valid    = $total_cost_arr['costs'];
                    $over_quota_costs   = 0;
                    if(isset($over_charge_arr[$ad_user_id])){

                        $over_quota_costs = $over_charge_arr[$ad_user_id];
                        $user_cost_valid  = round($user_cost_valid-$over_quota_costs,2);
                    }

                    $out_arr[] =  array(
                        'ad_user_id'=> $ad_user_id,
                        'user_cost_valid'=> $user_cost_valid,
                        'over_quota_costs'=> $over_quota_costs,
                        'create_date'=> $create_date,
                    );
                }
            }

        }

        if(!empty($out_arr)){

            $table = '<table border="1">'."\n";
            $table .= "<tr>\n";
            $table .= "<th>日期</th><th>账户id</th><th>全部计费(计划限额内消费)</th><th>Buffer消费(超过计划限额部分的消费)</th> \n";
            $table .= "</tr>\n";

            foreach ($out_arr as $arr) {
                $table .= sprintf("<tr>\n");
                $table .= sprintf(
                    "<td>%s</td><td>%d</td> <td>%.2f</td><td>%.2f</td>\n",
                    $arr['create_date'], $arr['ad_user_id'], $arr['user_cost_valid'], $arr['over_quota_costs']

                );
                $table .= sprintf("</tr>\n");
            }
            $table .= "</table>\n";

            $header = sprintf("计划层级限额相关消费统计<p>\n");

            $body = $header.$table;
            $toList = array(

                'wangguoying@alarm.360.cn',
                'jingguangwen@alarm.360.cn',
                'renyajun@alarm.360.cn',
             );
            $from = 'jingguangwen@alarm.360.cn';
            $response = Utility::sendMail('计划消费统计', $body, $from, $toList);

            //写文件
            $out_file_file = '/data/log/'.date('Ymd',strtotime("-1  days ")).'.html';
            $fp = fopen($out_file_file, 'w');
            fwrite($fp, $table);
            fclose($fp);
        }

        printf("%s send email, res[%s]\n", $task_name, $response);
        $endTime = time();
        printf(
            "%s begin at %s, end at %s\n",
            $task_name, date('Y-m-d H:i:s', $beginTime), date('Y-m-d H:i:s', $endTime)
        );
    }

    public function actionUpdateClickLog ()
    {
        set_time_limit(0);

        $beginTime = time();
        $task_name = sprintf("[UpdateClickLog %s]", date('Ymd_His', $beginTime));
        printf("%s begin at %s\n", $task_name, date('Y-m-d H:i:s', $beginTime));


        $create_date = date('Y-m-d',strtotime('-1  day'));
        $date = date('Ymd',strtotime('-1  day'));

        $update_sql = "update    ad_click_log,click_detail_".$date." set ad_click_log.status=2 where   ad_click_log.click_id=click_detail_".$date.".click_id   and  ad_click_log.create_date  = '".$create_date."'  and  click_detail_".$date.".cheat_type =3;";
        $res  = Yii::app()->db_click_log->createCommand($update_sql)->execute();
        echo "sql\t".$update_sql."\n";
        print_r($res);
        echo "\n";
        $endTime = time();
        printf(
            "%s begin at %s, end at %s\n",
            $task_name, date('Y-m-d H:i:s', $beginTime), date('Y-m-d H:i:s', $endTime)
        );
    }
    /**
     * 每天执行一次，并发邮件通知相应人员
     * @author jingguangwen@360.cn
     */
    public function actionGetLmCost ($date='')
    {
        set_time_limit(0);

        $beginTime = time();
        $task_name = sprintf("[GetLmCost %s]", date('Ymd_His', $beginTime));
        printf("%s begin at %s\n", $task_name, date('Y-m-d H:i:s', $beginTime));

        if(empty($date)){
            $date = date('Y-m-d',strtotime('-1  day'));
        }
        $time = strtotime($date);

        //搜索联盟消费数据
        $sou_lm_sql="select sum(price-reduce_price) as cost,ad_user_id from click_detail where create_date='{$date}'   and   ls like 's%' and deal_status=1  group by ad_user_id";
        $sou_lm_data_arrs=ComAdDetail::queryBySql($sou_lm_sql, $time, 'all');

        //导航联盟数据
        $guess_lm_sql="select sum(price-reduce_price) as cost,ad_user_id,ver from click_detail where create_date='{$date}'   and  ls like 'n%'  and deal_status=1  group by ad_user_id";
        $guess_lm_data_arrs=ComAdDetail::queryBySql($guess_lm_sql, $time, 'all');
        //所有账户消费数据
        $sql="select sum(price-reduce_price) as cost,ad_user_id from click_detail where create_date='{$date}'  and deal_status=1 group by ad_user_id";
        $data_arrs=ComAdDetail::queryBySql($sql, $time, 'all');
        $all_user_cost = array();
        if(!empty($data_arrs)){
            foreach ($data_arrs as $data_arr) {
                $ad_user_id = $data_arr['ad_user_id'];
                $cost = $data_arr['cost'];
                $all_user_cost[$ad_user_id] =$cost;
            }
        }
        $out_put_user_cost = array();
        if(!empty($sou_lm_data_arrs)){
            foreach ($sou_lm_data_arrs as $sou_lm_data_arr) {
                $ad_user_id = $sou_lm_data_arr['ad_user_id'];
                $sou_lm_cost = $sou_lm_data_arr['cost'];

                $cost = $all_user_cost[$ad_user_id];

                if(isset($out_put_user_cost[$ad_user_id])){
                    $out_put_user_cost[$ad_user_id]['sou'] =  $sou_lm_cost;
                } else {
                    $out_put_user_cost[$ad_user_id] =  array(
                        'total_cost' => $cost,
                        'ad_user_id' => $ad_user_id,
                        'create_date' => $date,
                        'sou' => $sou_lm_cost,
                    );
                }



            }
        }

        if(!empty($guess_lm_data_arrs)){
            foreach ($guess_lm_data_arrs as $guess_lm_data_arr) {
                $ad_user_id = $guess_lm_data_arr['ad_user_id'];
                $guess_lm_cost = $guess_lm_data_arr['cost'];

                $cost = $all_user_cost[$ad_user_id];

                if(isset($out_put_user_cost[$ad_user_id])){
                    $out_put_user_cost[$ad_user_id]['guess'] =  $guess_lm_cost;
                } else {
                    $out_put_user_cost[$ad_user_id] =  array(
                        'total_cost' => $cost,
                        'ad_user_id' => $ad_user_id,
                        'create_date' => $date,
                        'guess' => $guess_lm_cost,
                    );
                }



            }
        }

        $title = array(

            'create_date' => '日期',
            'ad_user_id' => '客户id',
            'guess' => '导航联盟消耗',
            'sou' => '搜索联盟消耗',
            'total_cost' => '客户日消耗',
        );
        if (!empty($out_put_user_cost)) {

            $lockDir = '/data/log/';
            $fname = $lockDir. date('Y-m-d',time()).'_lm_cost_'.time() . '.csv';
            $title_new = array();
            foreach ($title as $tk => $tv) {
                $title_new[$tk] = iconv('utf-8', 'gbk//IGNORE', $tv);
            }
            $fp = fopen($fname, 'w');
            fputcsv($fp, $title_new);

            foreach ($out_put_user_cost as $ad_user_id => $cost_arr) {
                $data = array();
                $create_date = $cost_arr['create_date'];
                $total_cost = $cost_arr['total_cost'];

                $sou = isset($cost_arr['sou'])?$cost_arr['sou']:0;
                $guess = isset($cost_arr['guess'])?$cost_arr['guess']:0;

                $data[] = iconv('utf-8', 'gbk//IGNORE', $create_date);
                $data[] = iconv('utf-8', 'gbk//IGNORE', $ad_user_id);

                $data[] = iconv('utf-8', 'gbk//IGNORE', $guess);
                $data[] = iconv('utf-8', 'gbk//IGNORE', $sou);

                $data[] = iconv('utf-8', 'gbk//IGNORE', $total_cost);

                fputcsv($fp, $data);


            }

            fclose($fp);
            //发送邮件
            $from  = 'jingguangwen@alarm.360.cn';
            $to = 'xiangbibo@360.cn,yuhongkun@360.cn,xusheng@360.cn,wanglei-pd@360.cn,renyajun@360.cn,jingguangwen@360.cn,g-union-pm@list.qihoo.net';
            $subject = date('Y-m-d',strtotime('-1 day')).'_cost';
            //定义边界线
            $boundary = uniqid( "" );
            //生成邮件头
            $header = "From: $from\nContent-type: multipart/mixed;boundary=\"$boundary\"\nX-Mailer:PHP\nX-Priority:3";

            //文件的MIME类型-
            $mimeType = "text/csv";

            //获取上传文件的名字-
            $filename = date('Y-m-d',strtotime('-1 day')).'.csv';
            $message = date('Y-m-d',strtotime('-1 day')).'联盟相关消费数据';
            $attach = $fname;
            //对上传文件进行编码和切分
            $fp = fopen($attach, "r");
            $content = fread($fp, filesize($attach));
            $content = chunk_split( base64_encode($content) );

            //生成邮件主体-
            $body ="
--$boundary
Content-type: text/plain; charset=utf-8
Content-transfer-encoding: 8bit

$message

--$boundary
Content-Type: $mimeType; name=$filename; charset=gbk
Content-Disposition: attachment; filename=$filename
Content-Transfer-Encoding: base64

$content

--$boundary--";

            mail( $to, $subject, $body, $header );

        } else {

            $from  = 'jingguangwen@alarm.360.cn';
            $to = 'jingguangwen@360.cn,wanglei-pd@360.cn';
            $subject = date('Y-m-d',strtotime('-1 day')).'_cost';
            //生成邮件头
            $header = "From: $from";
            $body = 'Union no  data!';
            mail($to, $subject, $body,$header);
        }

        $endTime = time();
        printf(
            "%s begin at %s, end at %s\n",
            $task_name, date('Y-m-d H:i:s', $beginTime), date('Y-m-d H:i:s', $endTime)
        );
    }



    /**
     * 监控每日各维度消费与click_detail表是否一致
     */
    public function actionMonitorStatsDayCost ()
    {
        set_time_limit(0);
        //Yii::import('application.extensions.CSmsclient', true);
        $date = date('Y-m-d', strtotime('-1 day'));

        $clickDetailTableName = ComAdDetail::getTableName(strtotime($date));

        /*
         *   esc_stats_click表结算金额w维度对比
         */
        //click_detail表结算金额

        $click_detail_sql = sprintf("select  count(1) as clicks,sum(price-reduce_price) as total_cost from %s where  status not in (-1,2) and deal_status=1 and ver !='mediav' and ver !='shouzhu' and cheat_type not in (2,3) and price != reduce_price",$clickDetailTableName);
        //esc_stats_click表结算金额

        $esc_stats_click_table = 'esc_stats_click_'.date('Ymd', strtotime($date));
        $esc_stats_click_sql = "SELECT sum(total_cost) as cost_money,sum(clicks) as total_clicks from ".$esc_stats_click_table."  where create_date ='".$date."' ";

        self::_compareStatsCost($click_detail_sql,$esc_stats_click_sql,"stats_click",$date);


        /*
         *   esc_stats_interest表结算金额w维度对比
         */
        //click_detail表结算金额
        $click_detail_sql = sprintf("select  count(1) as clicks,sum(price-reduce_price) as total_cost from %s where   status not in (-1,2) and deal_status=1 and ver='guess' and cheat_type not in (2,3) and price != reduce_price ",$clickDetailTableName);

        $esc_stats_click_table = 'esc_stats_interest_click_'.date('Ymd', strtotime($date));
        $esc_stats_click_sql = "SELECT sum(costs) as cost_money,create_date,sum(clicks) as total_clicks from ".$esc_stats_click_table."  where create_date ='".$date."' ";

        self::_compareStatsCost($click_detail_sql,$esc_stats_click_sql,"interest",$date);

        /*
         *   esc_stats_Keyword表结算金额w维度对比
         */
        //click_detail表结算金额
        $click_detail_sql = sprintf("select  count(1) as clicks,sum(price-reduce_price) as total_cost from %s where   status not in (-1,2) and deal_status=1 and ver='sou' and cheat_type not in (2,3) and price != reduce_price  ",$clickDetailTableName);

        $esc_stats_click_table = 'esc_stats_keyword_click_'.date('Ymd', strtotime($date));
        $esc_stats_click_sql = "SELECT sum(costs) as cost_money,sum(clicks) as total_clicks from ".$esc_stats_click_table."  where create_date ='".$date."' ";

        self::_compareStatsCost($click_detail_sql,$esc_stats_click_sql,"keyword",$date);

        /*
         *   esc_stats_area表结算金额w维度对比
         */
        //click_detail表结算金额
        $click_detail_sql = sprintf("select  count(1) as clicks,sum(price-reduce_price) as total_cost from %s where   status not in (-1,2) and deal_status=1 and ver !='mediav' and ver !='shouzhu' and cheat_type not in (2,3) and price != reduce_price  ",$clickDetailTableName);

        $esc_stats_click_table = 'esc_stats_area_click_'.date('Ymd', strtotime($date));
        $esc_stats_click_sql = "SELECT sum(costs) as cost_money,sum(clicks) as total_clicks from ".$esc_stats_click_table."  where create_date ='".$date."' ";

        self::_compareStatsCost($click_detail_sql,$esc_stats_click_sql,"area",$date);


        /*
         *   esc_stats_biyi表结算金额w维度对比
         */
        //click_detail表结算金额
        $click_detail_sql = sprintf("select  count(1) as clicks,sum(price-reduce_price) as total_cost from %s where   status not in (-1,2) and deal_status=1 and ((ver='sou' and sub_ver='biyi') or (ver='guess' and sub_ver='stream')) and cheat_type not in (2,3) and price != reduce_price  ",$clickDetailTableName);

        $esc_stats_click_table = 'esc_stats_biyi_click_'.date('Ymd', strtotime($date));
        $esc_stats_click_sql = "SELECT sum(costs) as cost_money,sum(clicks) as total_clicks from ".$esc_stats_click_table."  where create_date ='".$date."' ";
        self::_compareStatsCost($click_detail_sql,$esc_stats_click_sql,"biyi",$date);

        /*
         *   mv_stats_表结算金额w维度对比
         */
        //click_detail表结算金额
        $click_detail_sql = sprintf("select  count(1) as clicks,sum(price-reduce_price) as total_cost from %s where   status not in (-1,2) and deal_status=1  and ver='mediav' and cheat_type not in (2,3) and price != reduce_price  ",$clickDetailTableName);

        $esc_stats_click_table = 'mv_stats_'.date('Ymd', strtotime($date));
        $esc_stats_click_sql = "SELECT sum(end_costs) as cost_money,sum(clicks) as total_clicks from ".$esc_stats_click_table;
        self::_compareStatsCost($click_detail_sql,$esc_stats_click_sql,"mv",$date);

        /*
         *   ad_app__表结算金额w维度对比
         */
        //click_detail表结算金额
        $click_detail_sql = sprintf("select  count(1) as clicks,sum(price-reduce_price) as total_cost from %s where   status not in (-1,2) and deal_status=1  and ver ='shouzhu'  and cheat_type not in (2,3) and price != reduce_price  ",$clickDetailTableName);


        $esc_stats_click_table = 'ad_app_'.date('Ymd', strtotime($date));
        $esc_stats_click_sql = "SELECT sum(real_costs) as cost_money,sum(clicks) as total_clicks from ".$esc_stats_click_table;
        self::_compareStatsCost($click_detail_sql,$esc_stats_click_sql,"app",$date);
        /*
         *   ad_app_statistic__表结算金额w维度对比
         */
        //click_detail表结算金额
        $click_detail_sql = sprintf("select  count(1) as clicks,sum(price-reduce_price) as total_cost from %s where   status not in (-1,2) and deal_status=1  and ver ='shouzhu'  and cheat_type not in (2,3) and price != reduce_price  ",$clickDetailTableName);

        $esc_stats_click_table = 'ad_app_statistic_'.date('Ymd', strtotime($date));
        $esc_stats_click_sql = "SELECT sum(real_costs) as cost_money,sum(clicks) as total_clicks from ".$esc_stats_click_table." where real_costs>0 ";
        self::_compareStatsCost($click_detail_sql,$esc_stats_click_sql,"app_statistic",$date);



    }
    /**
     * click_detail表数据与各stats比对
     * @param  [string] $click_detail_sql    [description]
     * @param  [string] $esc_stats_click_sql [description]
     * @param  [string] $stats_type          [description]
     * @author jingguangwen@360.cn
     */
    public static  function _compareStatsCost($click_detail_sql,$esc_stats_click_sql,$stats_type,$date){

        $mobile_arr = array(18611794976,18701685085,18631225517);
        $click_detail_arr = Yii::app()->db_click_log_slave->createCommand($click_detail_sql)->queryRow();

        $total_cost = $click_detail_count  = 0;
        if (!empty($click_detail_arr)) {
            $total_cost = $click_detail_arr['total_cost'];
            $click_detail_count = $click_detail_arr['clicks'];
        }

        //esc_stats_click表结算金额
        $esc_stats_click_1_amount=$esc_stats_click_2_amount=$esc_stats_click_3_amount=$esc_stats_click_4_amount = 0;
        $esc_stats_click_1_count=$esc_stats_click_2_count=$esc_stats_click_3_count=$esc_stats_click_4_count=0;

        //结算库1
        $esc_stats_click_1_arr = Yii::app()->db_stat_1->createCommand($esc_stats_click_sql)->queryRow();
        if (!empty($esc_stats_click_1_arr)) {
            $esc_stats_click_1_amount = $esc_stats_click_1_arr['cost_money'];
            $esc_stats_click_1_count = $esc_stats_click_1_arr['total_clicks'];
        }
        //结算库2
        $esc_stats_click_2_arr = Yii::app()->db_stat_2->createCommand($esc_stats_click_sql)->queryRow();
        if (!empty($esc_stats_click_2_arr)) {
            $esc_stats_click_2_amount = $esc_stats_click_2_arr['cost_money'];
            $esc_stats_click_2_count = $esc_stats_click_2_arr['total_clicks'];
        }
        //结算库3
        $esc_stats_click_3_arr = Yii::app()->db_stat_3->createCommand($esc_stats_click_sql)->queryRow();
        if (!empty($esc_stats_click_3_arr)) {
            $esc_stats_click_3_amount = $esc_stats_click_3_arr['cost_money'];
            $esc_stats_click_3_count = $esc_stats_click_3_arr['total_clicks'];
        }
        //结算库4
        $esc_stats_click_4_arr = Yii::app()->db_stat_4->createCommand($esc_stats_click_sql)->queryRow();
        if (!empty($esc_stats_click_4_arr)) {
            $esc_stats_click_4_amount = $esc_stats_click_4_arr['cost_money'];
            $esc_stats_click_4_count = $esc_stats_click_4_arr['total_clicks'];
        }

        $esc_stats_click_amount = round($esc_stats_click_1_amount+$esc_stats_click_2_amount+$esc_stats_click_3_amount+$esc_stats_click_4_amount,2);
        $esc_stats_click_count = $esc_stats_click_1_count+$esc_stats_click_2_count+$esc_stats_click_3_count+$esc_stats_click_4_count;

        echo  date('Y-m-d H:i:s')."\t".$date."\t".$stats_type."\t".$click_detail_count."\t".$total_cost."\t".$esc_stats_click_count."\t".$esc_stats_click_amount."\n";

        if($click_detail_count!=$esc_stats_click_count || round($total_cost-$esc_stats_click_amount,2) != 0){


            //message content
            $sms_content = $date."  click-amount:".$total_cost.";click-count:".$click_detail_count.";{$stats_type}-amount:".$esc_stats_click_amount.";{$stats_type}-count:".$esc_stats_click_count.";不一致，请尽快人工排查";
            //短信发送
            foreach ($mobile_arr as $mobile) {
                echo $sms_content."\n";
                exec("curl -d 'source=dianjing&sign=5ad0561bf86adeea20109ed2cde8954c&roleName=sms_mc&smsMobile=".$mobile."&smsContent=".$sms_content."' http://mc.ad.360.cn/api.php");
            }

        }
    }
}