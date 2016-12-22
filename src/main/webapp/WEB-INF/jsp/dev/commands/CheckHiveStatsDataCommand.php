<?php

/**
 * Author: dengchao@360.cn
 * Last modified: 2013-11-27 11:37
 * Filename: checkformdbcomman.php
 * Description: 检测hive中stats数据的正确性
 */
include __DIR__ . '/CommonCommand.php';
set_time_limit(0);
class CheckHiveStatsDataCommand extends CommonCommand
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
        $dateFormat = str_replace("-",'',  $this->sDate);
       // $this->sDate = '2012-07-11';
        echo "正在校验:".$date."\r\n";
        $this->tableArr = array(
            'ad_stats'=>array(
            'statscount'=>"select count(*) from ad_stats_click_{$dateFormat} where create_date='{$this->sDate}' and status in(0,1,2)",
            'statsdata'=>"select sum(clicks) as clicks,sum(views) as views,sum(total_cost) as cost from ad_stats_click_{$dateFormat} where create_date='{$this->sDate}' and status in(0,1,2)",
            'formcount'=>"select count(*) from esc_stats_click where create_date='{$this->sDate}'",
            'formdata'=>"select sum(clicks) as clicks,sum(views) as views,sum(total_cost) as cost from esc_stats_click where create_date='{$this->sDate}'",
             ),             
            'ad_stats_area'=>array(
            'statscount'=>"select count(*) from ad_stats_area_click_{$dateFormat} where create_date='{$this->sDate}'",
            'statsdata'=>"select sum(clicks) as clicks,sum(views) as views,sum(costs) as cost from ad_stats_area_click_{$dateFormat} where create_date='{$this->sDate}'",
            'formcount'=>"select count(*) from esc_stats_area_click where create_date='{$this->sDate}'",
            'formdata'=>"select sum(clicks) as clicks,sum(views) as views,sum(total_cost) as cost from esc_stats_area_click where create_date='{$this->sDate}'",
             ),
            'ad_stats_keyword'=>array(
            'statscount'=>"select count(*) from ad_stats_keyword_click_{$dateFormat} where create_date='{$this->sDate}'",
            'statsdata'=>"select sum(clicks) as clicks,sum(views) as views,sum(costs) as cost from ad_stats_keyword_click_{$dateFormat} where create_date='{$this->sDate}'",
            'formcount'=>"select count(*) from esc_stats_keyword_click where create_date='{$this->sDate}'",
            'formdata'=>"select sum(clicks) as clicks,sum(views) as views,sum(total_cost) as cost from esc_stats_keyword_click where create_date='{$this->sDate}'",
             ),
            'ad_stats_interest'=>array(
            'statscount'=>"select count(*) from ad_stats_interest_click_{$dateFormat} where create_date='{$this->sDate}'",
            'statsdata'=>"select sum(clicks) as clicks,sum(views) as views,sum(costs) as cost from ad_stats_interest_click_{$dateFormat} where create_date='{$this->sDate}'",
            'formcount'=>"select count(*) from esc_stats_interest_click where create_date='{$this->sDate}'",
            'formdata'=>"select sum(clicks) as clicks,sum(views) as views,sum(total_cost) as cost from esc_stats_interest_click where create_date='{$this->sDate}'",
             ),         
        );
    }
    
    public function actionTest()
    {
        echo $this->sDate;
    }
    public function actionCheckData($tableName,$date=false)
    {
        $hiveClient = new ComHive();
        $this->setFunction($date);
        $subject = "导入hive stats统计数据check报警";
        $title='';
        if(array_key_exists($tableName,$this->tableArr)){
        $dbConf = str_replace('ad_', 'db_', $tableName);
        echo $this->tableArr[$tableName]['statscount']."\r\n";
        $formCountArr = Yii::app()->db_stats->createCommand($this->tableArr[$tableName]['statscount'])->queryColumn();
        $formCount = $formCountArr[0];
        $hiveClient->execute($this->tableArr[$tableName]['formcount']);
        $formStatsCount = $hiveClient->fetchOne();
        //先判断导入记录数，不一致直接报警不进入下面步骤
        if($formCount!=$formStatsCount){
                $title = $tableName.'表导入数据总记录数不一致,'."应该导入".$formCount.'条'."实际导入".$formStatsCount.'条';
        }else{
            $statsRow = Yii::app()->db_stats->createCommand($this->tableArr[$tableName]['statsdata'])->queryRow();
            $hiveClient->execute($this->tableArr[$tableName]['formdata']);
            $formRow =  $hiveClient->fetchN(1);
            $formRow = explode("\t", $formRow[0]);
            if($statsRow['clicks']!=$formRow[0]){
                $title .= $tableName.'表导入数据总点击数不一致,'."应该导入".$statsRow['clicks'].'次点击,'."实际导入".$formRow[0]."次";
            }
            if($statsRow['views']!=$formRow[1]){
                $title .= $tableName.'表导入数据总展示数不一致,'."应该导入".$statsRow['views'].'次展示,'."实际导入".$formRow[1]."次";
            }        
             if($statsCo=number_format($statsRow['cost'],2,".","")!=$hCo=number_format($formRow[2],2,".","")){ 
                $title .= $tableName.'表导入数据总消费数不一致,'."应该导入".$statsCo.'元消费额,'."实际导入".$hCo."元"; 
            }         
        }
        if($title){
            $title .=$this->sDate; 
            echo $title."\n";
           //ComMessageBox::mail("dengchao@360.cn",$subject,$title);
           // ComMessageBox::sms("13521600967",$title);
            Yii::app()->curl->run("http://10.108.68.121:888/notice/notice.php?s=".$subject."&c=".$title."&g=e_formcheck");
        }else{
            //echo "正常"."\n";
             //ComMessageBox::mail("dengchao@360.cn",$subject,$tableName.$date."号数据正常");
             //ComMessageBox::sms("13521600967",$tableName.$date."号数据正常");
             //Yii::app()->curl->run("http://10.108.68.121:888/notice/notice.php?s=".$subject."&c=".$tableName.$this->sDate."号数据正常"."&g=e_formcheck");
        }
        }else{
            echo 'fail';
            return false;;
        }
    }
    /**
     * 批量更新所有创意名、组名、计划名之后，检测再有更新不着的报警
     * **/
    public function actionCheckNullInfo($tableName, $field, $date=false) 
    { 
        $comWu = new ComRedisWuliao();
        $dbType = str_replace('ad_', 'db_', $tableName);
        $db = Yii::app()->$dbType;
        $pageSize = 100000;
        $fieldArr = explode(',', $field);
        $countStr = " count(1) ";
        $rowStr = " id, $field ";
        $sqlCount = "select $countStr from $tableName where create_date='$date' and ( 0 ";
        $sqlRow = "select $rowStr from $tableName where create_date='$date' and ( 0 ";
        foreach ($fieldArr as $field) {
            $sqlCount = $sqlCount . " or $field = ''";
            $sqlRow = $sqlRow . " or $field = ''";
        }
        $sqlRow = $sqlRow. ")"; 
        $sqlCount = $sqlCount . ")";
        $content='';
        $count = $db->createCommand($sqlCount)->queryScalar();
        $title = $tableName."表共有".$count."条数据不合格";
        if ($count) {
            for ($i=0; $i<$count; $i=$i+$pageSize) {
                $sql = $sqlRow . " limit $i, $pageSize";
                $rows = $db->createCommand($sql)->queryAll();
                if ($rows) {
                    foreach ($rows as $row) {
                        $updateArr = array();
                        foreach ($row as $field=>$fval) {
                            if ($field == 'id') {
                                continue;
                            } elseif ($field == 'ad_group_name' && $row['ad_group_name']=='') {
                                $content .= "组字段为空id为".$row['ad_group_id'].",用户id为".$row['ad_user_id'];
                            } elseif ($field == 'ad_plan_name' && $row['ad_plan_name']=='' ) {
                                $content .= "计划字段为空id为".$row['ad_plan_id'].",用户id为".$row['ad_user_id'];
                            }elseif($field=='caption' &&  $row['caption']==''){
                                $content .= "创意字段为空id为".$row['ad_advert_id'].",用户id为".$row['ad_user_id'];
                           }
                        } 
                    }   

                }   
            }   
        }
        $content = ($content)?$content:$tableName.$date."没有空物料信息";
        if($content){
             echo $title;
             echo $content;
            // ComMessageBox::mail("dengchao@360.cn",$title,$content);
             Yii::app()->curl->run("http://10.108.68.121:888/notice/notice.php?s=".$title."&c=".$content."&g=e_formcheck");         
        }
        return ;
    }       
}