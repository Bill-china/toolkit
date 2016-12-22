<?php

/**
 * Author: dengchao@360.cn
 * Last modified: 2014-01-07 11:37
 * Filename: checkcustom
 * Description: 检测form库数据的正确性
 */
include __DIR__ . '/CommonCommand.php';
set_time_limit(0);
class CheckCustomReportCommand extends CommonCommand
{ 
    public $sDate;
    public $tableArr;
    public function __construct ()
    {
        
    }    

    public function actionTest()
    {
        echo "sadfa";
    }
    /***
     * 检测每日生成的是否已经被分配任务到task表了
     * 每天8:30执行
     * **/
    public function actionCheckDay()
    {
        $subject = "定制报表报警分配任务报警";
        $content = '';
        ini_set('memory_limit', '2048M');
        $sql = "select id,ad_user_id from " . AdStatsBookReport::model()->tableName() . " where status = " . AdStatsBookReport::STATUS_START ." and book_cycle=2"; 
        $rows = Yii::app()->db_book_report->createCommand($sql)->queryAll();
        if(!$rows)continue;
        foreach ($rows as $val){
        $reportIdArr[] = $val['id'];
       }
       if(!$reportIdArr)continue;
       $reportIdStr =  implode(',', $reportIdArr);
       $sql = "select parent_id from " . AdStatsBookReportTask::model()->tableName() . " where create_date='" . date('Y-m-d') 
            . "' and type=" . AdStatsBookReport::TYPE_MYSQL . " and parent_id in({$reportIdStr})";
        $rows = Yii::app()->db_book_report->createCommand($sql)->queryAll();
        if(!$rows){
            $content = "今日所有每日报告没有生成任务";
            Yii::app()->curl->run("http://10.108.68.121:888/notice/notice.php?s=".$subject."&c=".$content."&g=e_formcheck_new");
            exit;
        }
        foreach($rows as $pid){
            $parentIdArr[] = $pid['parent_id'];
        }
        foreach($reportIdArr as $rpid){
            if(!in_array($rpid,$parentIdArr)){
                $content .= $rpid."-";
            }
        }
        if($content){
            $content.="这些report表的id没有生成task表任务".date('Y-m-d');
            echo $content;
             Yii::app()->curl->run("http://10.108.68.121:888/notice/notice.php?s=".$subject."&c=".$content."&g=e_formcheck_new");
        }
    }
   /***
    * @每分钟检测是否有生成报表失败的
    * 
    * **/
   public function actionCheckFailure()
   {
       $content='';
       $subject="每日生成失败定制报警";
       $sql = "select * from " . AdStatsBookReportTask::model()->tableName() . " where create_date='" . date('Y-m-d') 
            . "' and failure_times>0 and download_key=''";
       $rows = Yii::app()->db_book_report->createCommand($sql)->queryAll();
       if(!$rows){
           echo "no failure";exit;
       }
       foreach($rows as $val){
           $content.='userID'.$val['ad_user_id'].'---'."taskId:".$val['id']."失败<br>";
       }
       if($content){
           echo $content;
           Yii::app()->curl->run("http://10.108.68.121:888/notice/notice.php?s=".$subject."&c=".$content."&g=e_formcheck_new");
       }
   }
   /**
    * @每天中午12点，下午18点检测生成定制超过半个小时的
    * 
    * **/
   public function actionCheckHalfTime(){
       $content = '';
       $subject = '生成定制报告超过半个小时报警';
       $sql = "select a.* from (select id,ad_user_id,parent_id,type,start_date,end_date,UNIX_TIMESTAMP(create_time) as cTime,UNIX_TIMESTAMP(update_time) as uTime from ad_stats_book_report_task where create_date='".date('Y-m-d')."') as a where (a.uTime-a.cTime)>1800";
        $rows = Yii::app()->db_book_report->createCommand($sql)->queryAll();
        if(!$rows)exit;
        foreach($rows as $val){
            $content.="userId:".$val['ad_user_id']."--taskId:".$val['id']."--type:".$val['type'].'--Time:'.($val['uTime']-$val['cTime'])."s"."<br/>";
        }
       if($content){
           Yii::app()->curl->run("http://10.108.68.121:888/notice/notice.php?s=".$subject."&c=".$content."&g=e_formcheck_new");
       }
   }
}