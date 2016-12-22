<?php
/**
 * ClickLog定制查询 
 * 
 * @author dongdapeng@360.cn
 * @link http://www.360.cn/
 * @date 2015-06-30
 * @filecoding UTF-8 
 * @copyright (c) 2015 360.cn, Inc. All Rights Reserved
 */

class AdepQueryClickLogCommand extends CConsoleCommand {

    //每次查询最大条数
    public static $_maxSelectNum = 1000;

    public function actionGetClickLog($count = 1) {

        $emq = new ComEMQ('esc_emq');
        $emq->exchangeName = 'query_click_log';
        $emq->checkInterval = 20; //多少秒检查一次
        $emq->alarmReceiveTime = 60 * 60 * 2;
        $emq->exitTime = 30 * 60;//超过多少秒发送时间和接受时间差就报警
        $emq->startMultiProcessSubscriber($count);

    }

    public static function _getClickLog($msg) {

        $content = "";
        $currentTime = time();
        $csvFile = '/data/log/data/adep_query_'.$currentTime.'.csv';
        $zipFile = '/data/log/data/adep_query_'.$currentTime.'.zip';
        Utility::writeLog(date('Y-m-d H:i:s') . "\t" .$msg->body . "\n", 'adep_query.log');

        try {
            $content = json_decode($msg->body, true);
            $content = $content['content'];
            $data = json_decode($content['data'], true);
            if (!$data) {
                sleep(2);
                throw new Exception("data json_decode error");
            }

            $mail = $data['mail'];
            $reqId = $content['request_id'];
            $date = date('Ymd' ,strtotime($data['date']));
            $tb = 'click_detail_'.$date;

            $timeStamp = strtotime($date);
            $minTime = strtotime('20150701'); //click_detail 0701
            $maxTime = time();
            if($timeStamp < $minTime || $timeStamp > $maxTime){
                self::sendMail($mail,0);
                ComEMQ::receiveSuccess($msg);
                return;
            }

            self::getDbLog($tb,$data,$reqId,$csvFile);
            if (file_exists($csvFile) && filesize($csvFile) > 0 ){
                self::zip($csvFile,$zipFile);
                self::sendMail($mail,$zipFile);
            }else{
                self::sendMail($mail,0);
            }
            ComEMQ::receiveSuccess($msg);

        } catch (Exception $ex) {

            if (strpos($ex->getMessage(), "2006 MySQL server has gone away") || strpos($ex->getMessage(), "2013 Lost connection to MySQL")) {
                Utility::writeLog(date('Y-m-d H:i:s')."\t".__CLASS__."\t".__FUNCTION__."\t".$ex->getMessage() . "\t". $content['request_id'], 'adep_query.log');
            }
            ComEMQ::receiveFail($msg, true);
            Utility::writeLog(date('Y-m-d H:i:s') . "\t" .$ex->getMessage() . "\t" .$content['request_id'] . "\tfail\n", 'adep_query.log');
            sleep(1);
        }
    }

    private static function getDbLog($tb,$data,$reqId,$csvFile){

        //查询条件
        $rawconds    = array();
        $rawconds['ad_user_id']   = $data['uid'];
        if(!empty($data['planId']))
            $rawconds['ad_plan_id']   = $data['planId'];
        if(!empty($data['advertId']))
            $rawconds['ad_advert_id']   = $data['adverId'];
        if(!empty($data['groupId']))
            $rawconds['ad_group_id']   = $data['groupId'];
        if(!empty($data['keyword']))
            $rawconds['keyword']   = $data['keyword'];


        $obj = new AdepQueryClickLog();
        $obj->setDB(Yii::app()->db_click_log_slave);

        $intTotalCount = $obj->getTableCount($tb, $rawconds);
        Utility::writeLog(date('Y-m-d H:i:s')."[uid:{$data['uid']} clicklog num]:TotalCount is \t" .__CLASS__."\t".__FUNCTION__."\t".$intTotalCount, 'adep_query.log');
        if($intTotalCount > 0){ 
            $intLastId = 0;

            self::putCsvTitle($csvFile);
            //执行查询
            do {
                $intLastSelectCnt = 0;
                $arrClickList = $obj->getList(array($tb),$rawconds,$intLastId, self::$_maxSelectNum);
                $intLastSelectCnt = count($arrClickList);
                if(empty($arrClickList))
                    break;

                $intLastId += $intLastSelectCnt;
                if ( $intLastId <= 0) {
                    break;
                }

                foreach($arrClickList as $key=>$click){
                    self::putCsv($click,$key,$csvFile);
                }
            }while($intLastId<$intTotalCount);
        }
    }

    public static function putCsvTitle($csvFile){
        $title = array('ad_user_id', 'click_id', 'click_time', 'view_id', 'view_time', 'ip', 'mid', 'ad_advert_id', 'ad_group_id', 'ad_plan_id', 'query', 'keyword', 'ls', 'src',
                'area_fid','area_id', 'price', 'bidprice', 'create_date', 'cid', 'pid', 'app_cid', 'ver', 'deal_status', 'reduce_price', 'pos', 'location', 'tag_id',
                'cheat_type', 'source_type', 'source_system', 'extension','update_time','valid_cost');
        $fp = fopen($csvFile , 'a+');
        fputcsv($fp, $title);
        fclose($fp);
    }

    //生成CSV文件
    public static function putCsv($click,$key,$csvFile){

        if(!is_array($click)){
            return false;
        }

        $fp = fopen($csvFile , 'a+');

        $value = array_values($click);
        $value[2] = date('Y-m-d H:i:s',$value[2]);
        $value[4] = date('Y-m-d H:i:s',$value[4]);
        $value[32] = date('Y-m-d H:i:s',$value[32]);

        $value[33] = "其他";
        //写详细值说明
        if(-1 == $value[23]){
            $value[23] = '未计费';
        }elseif(0 == $value[23]){
            $value[23] = '未结算';
        }elseif(1 == $value[23]){
            $value[23] = '已结算';
            if($value[16] != $value[24]){
                $value[33] = "有效计费且计费不为零";
            }
        }


        if(0 == $value[28]){
            $value[28] = '正常点击';
        }elseif(1 == $value[28]){
            $value[28] = '非作弊';
        }elseif(2 == $value[28]){
            $value[28] = '1分钟内作弊';
        }elseif(3 == $value[28]){
            $value[28] = '算法作弊';
        }

        if(1 == $value[29]){
            $value[29] = 'PC展示类广告';
        }elseif(2 == $value[29]){
            $value[29] = '移动展示类广告';
        }elseif(3 == $value[29]){
            $value[29] = 'PC搜索类广告';
        }elseif(4 == $value[29]){
            $value[29] = '移动搜索类广告';
        }

        if(1 == $value[30]){
            $value[30] = '点睛';
        }elseif(2 == $value[30]){
            $value[30] = '如意';
        }elseif(3 == $value[30]){
            $value[30] = '云';
        }


	
        if(-1 == $value[31]){
            $value[31] = '无用户或结算超时';
        }elseif(0 == $value[31]){
            $value[31] = '正常';
        }elseif(1 == $value[31]){
            $value[31] = '余额下线';
        }elseif(2 == $value[31]){
            $value[31] = '账户下线';
        }elseif(3 == $value[31]){
            $value[31] = '计划下线';
        }
	
        foreach($value as $k=>$v){
            if(!empty($v)){
                $info[] = iconv("UTF-8", "GBK//IGNORE", $v);
            }elseif(0 == $v){
                $info[] = 0;
            }else{
		$info[] = '';
	    }
        }   
        fputcsv($fp, $info);

        fclose($fp);
    }

    //压缩文件
    public static function zip($csvFile,$zipFile){
        $zip=new ZipArchive();
        if($zip->open($zipFile,ZIPARCHIVE::CREATE)===TRUE){
            $zip->addFile($csvFile);
            $zip->close();
        }
        self::delFile($csvFile);
    }

    //发邮件
    public function sendMail($mail,$zipFile){

        $emailObj = new ComEmail();
        try {
            if (is_file($zipFile)) {
                $respons= $emailObj->send_attachment_mail('adep_query_click_log', '详情看附件',$mail,array(), $zipFile);
                sleep(6);//发送间隔不能小于5s
                if($respons == 'Empty reply from server'){
                    Utility::writeLog(date('Y-m-d H:i:s')."\t".$respons ."\t".  $zipFile .  " sendmail faild!", 'adep_query.log');
                    return;
                }
                $res = json_decode($respons,true);
                if(isset($res['result']['sent']) && $res['result']['sent'] == 1){
                    Utility::writeLog(date('Y-m-d H:i:s')."\t". $zipFile .  " sendmail success!", 'adep_query.log');
                    self::delFile($zipFile);
                }elseif(isset($res['error']['code']) && $res['error']['code'] == 1015){
                    Utility::writeLog(date('Y-m-d H:i:s')."\t"." Attachment Size Limit: 5 MB" .  $zipFile .  " sendmail faild!", 'adep_query.log');
                }else{
                    $respons= $emailObj->send_attachment_mail('adep_query_click_log', '您使用的查询条件未能查询到记录，可更换查询条件然后继续',$mail);                    
                    self::delFile($zipFile);
                }
            }else{
                $respons= $emailObj->send_attachment_mail('adep_query_click_log', '您使用的查询条件未能查询到记录，可更换查询条件然后继续',$mail);
            }
        } catch (Exception $e) {

            Utility::writeLog(date('Y-m-d H:i:s')."\t".$e->getMessage() . "\t". 'adep_query_click_log 发送邮件失败', 'adep_query.log');
        }
    }

    private static function delFile($file){
        if (file_exists($file)) {
            if (!unlink($file))
            {
                Utility::writeLog(date('Y-m-d H:i:s')."\t"."delete file " .  $file .  " file faild!", 'adep_query.log');
            }
        }       
    }
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
