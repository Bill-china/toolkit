<?php
/**
 * ESC展现全量统计 
 * HiveToMySQL
 * @author dongdapeng@360.cn
 * @link http://www.360.cn/
 * @date 2016-02-24
 * @filecoding UTF-8 
 * @copyright (c) 2016 360.cn, Inc. All Rights Reserved
 */

class HiveToMysqlCommand extends CConsoleCommand
{
    private $fp = null;

    private $dataDir = '/data/log/';

    private $fileName = '';

    private $sqlData = null;   

    private $lineNum = 1;

    private $maxNum = 1000;


    //执行 params: filename = statType
    public function run($params){

        $this->fileName = $this->dataDir.$params[1];

        $this->openFile($this->fileName);

        $this->readNext($params[1],$params[2]);

        if(!empty($this->sqlData)){
            $this->splitLogLine($line,$statType,$date,false);
        }
    }

    //打开文件
    protected function openFile(){
        if(!file_exists($this->fileName) || !is_readable($this->fileName)){
            throw new Exception($this->fileName .' is not existed or readable');
        }
        $this->fp = fopen($this->fileName, 'r');
        if(!$this->fp) throw new Exception($this->fileName . ' open error');
    }

    //关闭文件
    protected function finish(){
        if($this->fp) fclose($this->fp);
        $this->fp = null;
    } 

    //按行读取文件
    protected function readNext($statType,$date){
        if ($this->fp) {
            while (!feof($this->fp)) {
                if(($buffer = fgets($this->fp)) !== false){
                    $line = trim($buffer);
                    if(!$line) continue;
                    $this->splitLogLine($line,$statType,$date);
                }
            }
            $this->finish();
        }
    }

    //分析文件并入库
    protected function splitLogLine(&$line,$statType,$date,$flag=true){

        $hiveToMysql = new HiveToMysql();
        $pieces = explode("\t",$line);
        switch ($statType) {
            case 'area':{
                            $tb = "ad_stats_area_click_" . date('Ymd', strtotime($date));               
                            $fields= '(type, views, clicks, costs, area_id, area_fid, ad_group_id, ad_plan_id, ad_user_id, area_key, create_time, source_type, create_date)';
                            $dbNum = intval($pieces[12]);
                            unset($pieces[12]);
                            $this->HiveToMysql($tb,$fields,$pieces,$dbNum,$flag);
                        }
                        break;
            case 'keyword':{
                               $tb = "ad_stats_keyword_click_" . date('Ymd', strtotime($date));               
                               $fields= '(type, views, clicks, costs, ad_group_id, ad_plan_id, ad_user_id, keyword, create_time, source_type, create_date)';
                               $dbNum = intval($pieces[10]);
                               unset($pieces[10]);
                               $this->HiveToMysql($tb,$fields,$pieces,$dbNum,$flag);
                           }
                           break;
            case 'interest':{
                                $tb = "ad_stats_interest_click_" . date('Ymd', strtotime($date));               
                                $fields= '(type, views, clicks, costs, ad_group_id, ad_plan_id, ad_user_id, inter_id, create_time, create_date)';
                                $dbNum = intval($pieces[9]);
                                unset($pieces[9]);
                                $this->HiveToMysql($tb,$fields,$pieces,$dbNum,$flag);
                            }
                            break;
            case 'creative':{
                                $tb = "ad_stats_click_" . date('Ymd', strtotime($date));               
                                $fields= '(type, views, clicks, total_cost, ad_group_id, ad_plan_id, ad_advert_id, ad_user_id, ad_channel_id, ad_place_id, create_time, source_type, create_date)';
                                $dbNum = intval($pieces[12]);
                                unset($pieces[12]);
                                $this->HiveToMysql($tb,$fields,$pieces,$dbNum,$flag);
                            }
                            break;
            case 'biyi':{
                            $tb = "ad_stats_biyi_click_" . date('Ymd', strtotime($date));               
                            $fields= '(type, views, clicks, costs, ad_group_id, ad_plan_id, sub_id, ad_user_id, create_time, source_type, sub_ad_type, create_date)';
                            $dbNum = intval($pieces[12]);
                            unset($pieces[12]);
                            $this->HiveToMysql($tb,$fields,$pieces,$dbNum,$flag);
                        }
                        break;
            case 'app_statistic':{
                                     $tb = "ad_app_statistic_" . date('Ymd', strtotime($date));               
                                     $fields= '(type, views, clicks, costs, ad_user_id, ad_plan_id, ad_channel_id, ad_place_id, req_src, create_time, create_date)';
                                     $dbNum = intval($pieces[10]);
                                     unset($pieces[10]);
                                     $this->HiveToMysql($tb,$fields,$pieces,$dbNum,$flag);

                                 }
                                 break;
            case 'app':{
                           $tb = "ad_app_" . date('Ymd', strtotime($date));               
                           $fields= '(click_type, views, clicks, costs, ad_user_id, ad_plan_id, ad_group_id, ad_advert_id, app_id, ad_channel_id, area_id, area_fid, area_key, apk_id, place_id, location_id, create_time, create_date)';
                           $dbNum = intval($pieces[17]);
                           unset($pieces[17]);
                           $this->HiveToMysql($tb,$fields,$pieces,$dbNum,$flag);
                       }
                       break;
        }
        $this->lineNum++;   
    }

    private function HiveToMysql($tb,$fields,&$pieces,$dbNum,$flag){ 
        $dbNum+=1;
        $hiveToMysql = new HiveToMysql();
        if($flag==false){
            $this->insert($tb,$fields);
        }else{
            if($this->lineNum%$this->maxNum==0)
                $this->insert($tb,$fields);    
            if(false == $this->checkDbID($dbNum)){
                $l = implode("\t",$pieces);
                error_log("HiveToMysql checkDbID error, line is :{$l}; db_id is:{$dbNum}", 3, "/data/log/HiveToMysql-errors.log");
                throw new Exception('全量导入统计无法获取db_stat_num || 分库ID：'.$dbNum.'传递错误。');
            }

            $statsBranchDB = $this->getDbString($dbNum);
            $hiveToMysql->setDB($statsBranchDB);
            $this->sqlData .= '('.$hiveToMysql->quotes($pieces).'),';
        }
    }


    //检验分库ID
    private function checkDbID($dbNum){ 

        $statsDBSize = Yii::app()->params['db_stat_num'];
        $statsDBSize = intval($statsDBSize);
        if ($statsDBSize <= 0 || $dbNum<=0 || $dbNum>$statsDBSize) {
            return false;
        }
        return true;
    }

    //获取分库链接
    private function getDbString($dbNum){ 
        $statsBranchDB = DbConnectionManager::getStatBranchDB($dbNum);
        if (!$statsBranchDB) {
            throw new Exception("get branch db of stats[{$dbNum}] fail! 此分库未能校验");
        }
        return $statsBranchDB;
    }

    //入库
    private function insert($tb,$fields){  

        $hiveToMysql = new HiveToMysql();
        $this->sqlData = substr($this->sqlData, 0, -1);
        $sql = "INSERT INTO {$tb} {$fields} values {$this->sqlData}" ;
        if(!$hiveToMysql->batchInsert($sql)){                
            error_log("HiveToMysql readLine is :{$this->lineNum}", 3, "/data/log/HiveToMysql-errors.log");
            error_log("HiveToMysql error sql :{$sql}", 3, "/data/log/HiveToMysql-errors.log");
            throw new Exception('HiveToMysql insert error sql!');
        }
        $this->sqlData = null;
    }
}
