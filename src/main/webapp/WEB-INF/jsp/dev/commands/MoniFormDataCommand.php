<?php

/**
 * Author: dengchao@360.cn
 * Last modified: 2013-09-29 11:37
 * Filename: checkformdbcomman.php
 * Description: 检测form库数据的正确性
 */
include __DIR__ . '/CommonCommand.php';
set_time_limit(0);
class MoniFormDataCommand extends CommonCommand
{ 
    public $sDate;
    public $tableArr;
    public function __construct ()
    {
        
    }    
    public function setFunction ($date=false)
    {
        //$this->eDate = date("Y-m-d", time());
        $this->sDate = ($date)?$date:date('Y-m-d', strtotime('-1 day'));   
       // $this->sDate = '2012-07-11';
        echo "正在校验:".$date."\r\n";
        $this->tableArr = array(
            'ad_stats_report_advert'=>array(
            'count'=>"select count(1) from ad_stats_report_advert",
            'daycount'=>"select count(1) from ad_stats_report_advert where create_date='{$this->sDate}'",
             ),
            'ad_stats_advert_crm'=>array(
            'count'=>"select count(1) from ad_stats_advert_crm",
            'daycount'=>"select count(1) from ad_stats_advert_crm where create_date='{$this->sDate}'",                    
             ),               
            'ad_stats_report_group'=>array(
            'count'=>"select count(1) from ad_stats_report_group where create_date='{$this->sDate}'",
            'daycount'=>"select count(1) from ad_stats_report_group where create_date='{$this->sDate}'",
             ),
            'ad_stats_report_user'=>array(
            'count'=>"select count(1) from ad_stats_report_user",
            'daycount'=>"select count(1) from ad_stats_report_user where create_date='{$this->sDate}'",
             ),
            'ad_stats_report_plan'=>array(
            'count'=>"select count(1) from ad_stats_report_plan",
            'daycount'=>"select count(1) from ad_stats_report_plan where create_date='{$this->sDate}'",                    
             ),
            'ad_stats_area_report_plan'=>array(
            'count'=>"select count(1) from ad_stats_area_report_plan",
            'daycount'=>"select count(1) from ad_stats_area_report_plan where create_date='{$this->sDate}'",                    
             ),
            'ad_stats_area_report_group'=>array(
            'count'=>"select count(1) from ad_stats_area_report_group",
            'daycount'=>"select count(1) from ad_stats_area_report_group where create_date='{$this->sDate}'",               
             ),
            'ad_stats_area_report_province'=>array(
            'count'=>"select count(1) from ad_stats_area_report_province",
            'daycount'=>"select count(1) from ad_stats_area_report_province where create_date='{$this->sDate}'",                    
             ),
            'ad_stats_keyword_report'=>array(
            'count'=>"select count(1) from ad_stats_keyword_report",
            'daycount'=>"select count(1) from ad_stats_keyword_report where create_date='{$this->sDate}'",              
             ),
            'ad_stats_interest_report'=>array(
            'count'=>"select count(1) from ad_stats_interest_report",
            'daycount'=>"select count(1) from ad_stats_interest_report where create_date='{$this->sDate}'",                    
             ),
            'ad_stats_report_channel'=>array(
            'count'=>"select count(1) from ad_stats_report_channel",
            'daycount'=>"select count(1) from ad_stats_report_channel where create_date='{$this->sDate}'",           
             ),                
        );
    }
    public function actionTest()
    {
        echo $this->sDate;
    }
    public function actionCheckData($tableName,$date=false)
    {
        $this->setFunction($date);
        $subject = $date."form库数据表数据量监控";
        $title='';
        if(array_key_exists($tableName,$this->tableArr)){
        //$dbConf = str_replace('ad_', 'db_', $tableName);
        $dbConf = 'db_'.$tableName;
        echo $this->tableArr[$tableName]['count']."\r\n";
        $formCountArr = Yii::app()->$dbConf->createCommand($this->tableArr[$tableName]['count'])->queryColumn();
        $formCount = $formCountArr[0];
        $statsCountArr = Yii::app()->$dbConf->createCommand($this->tableArr[$tableName]['daycount'])->queryColumn();
        $dayCount = $statsCountArr[0];
        $title = $tableName.'总共'.$formCount."条数据"."<br>";
        $title .='昨天数据量'.$dayCount;
        if($title){
            echo $title."\n";
           //ComMessageBox::mail("dengchao@360.cn",$subject,$title);
           // ComMessageBox::sms("13521600967",$title);
            Yii::app()->curl->run("http://10.108.68.121:888/notice/notice.php?s=".$subject."&c=".$title."&g=e_formcheck");
        }
        }else{
            echo 'fail';
            return false;; 
        }
    }    
}