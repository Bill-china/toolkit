<?php
/**
 * 达成率 
 * HiveToMySQL
 * @author dongdapeng@360.cn
 * @link http://www.360.cn/
 * @date 2016-06-24
 * @filecoding UTF-8 
 * @copyright (c) 2016 360.cn, Inc. All Rights Reserved
 */

class KeywordRateCommand extends CConsoleCommand
{
    private $fp = null;

    private $fileName = '';

    private $sqlData = null;   

    private $lineNum = 1;

    private $maxNum = 1000;

    // yiic KeywordRate /data/log/rate 20160630
    public function run($params){

        $file = $params[0];
        $date = $params[1];

        $this->fileName = $file;

        $this->openFile($this->fileName);

        $this->readNext($date);

        if(!empty($this->sqlData)){
            $this->splitLogLine($line,$date,false);
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
    protected function readNext($date){
        if ($this->fp) {
            while (!feof($this->fp)) {
                if(($buffer = fgets($this->fp)) !== false){
                    $line = trim($buffer);
                    if(!$line) continue;
                    $this->splitLogLine($line,$date);
                }
            }
            $this->finish();
        }
    }

    protected function splitLogLine(&$line,$date,$flag=true){

        $hiveToMysql = new HiveToMysql();
        $pieces = explode("\t",$line);
        $tb = "ad_keyword_rate_" . date('Ymd', strtotime($date));               
        $fields= '(ad_user_id,ad_plan_id,ad_group_id, keyword, strategy_id, create_date, pc_target_views, pc_target_clicks, pc_target_cost, pc_views, pc_clicks, pc_cost, mobile_target_views,mobile_target_clicks,mobile_target_cost,mobile_views,mobile_clicks,mobile_cost,update_time)';
        $dbNum = $pieces['ad_user_id']%4+1;
        $pieces['update_time'] = time();
        $this->HiveToMysql($tb,$fields,$pieces,$dbNum,$flag);
        $this->lineNum++;   
    }

    private function HiveToMysql($tb,$fields,&$pieces,$dbNum,$flag){ 
        $hiveToMysql = new HiveToMysql();
        if($flag==false){
            $this->insert($tb,$fields);
        }else{
            if($this->lineNum%$this->maxNum==0)
                $this->insert($tb,$fields);    
            if(false == $this->checkDbID($dbNum)){
                $l = implode("\t",$pieces);
                error_log("HiveToMysql checkDbID error, line is :{$l}; db_id is:{$dbNum}", 3, "/data/log/rate-errors.log");
                throw new Exception('全量导入统计无法获取db_rate_num || 分库ID：'.$dbNum.'传递错误。');
            }

            $rateBranchDB = $this->getDbString($dbNum);
            $hiveToMysql->setDB($rateBranchDB);
            $this->sqlData .= '('.$hiveToMysql->quotes($pieces).'),';
        }
    }


    //检验分库ID
    private function checkDbID($dbNum){ 

        $statsDBSize = Yii::app()->params['db_rate_num'];
        $statsDBSize = intval($statsDBSize);
        if ($statsDBSize <= 0 || $dbNum<=0 || $dbNum>$statsDBSize) {
            return false;
        }
        return true;
    }

    //获取分库链接
    private function getDbString($dbNum){ 
        $rateBranchDB = DbConnectionManager::getRateBranchDB($dbNum);
        if (!$rateBranchDB) {
            throw new Exception("get branch db of rate[{$dbNum}] fail! 此分库未能校验");
        }
        return $rateBranchDB;
    }

    //入库
    private function insert($tb,$fields){  

        $hiveToMysql = new HiveToMysql();
        $this->sqlData = substr($this->sqlData, 0, -1);
        $sql = "INSERT INTO {$tb} {$fields} values {$this->sqlData}" ;
        if(!$hiveToMysql->batchInsert($sql)){                
            error_log("HiveToMysql readLine is :{$this->lineNum}", 3, "/data/log/rate-errors.log");
            error_log("HiveToMysql error sql :{$sql}", 3, "/data/log/rate-errors.log");
            throw new Exception('rate HiveToMysql insert error sql!');
        }
        $this->sqlData = null;
    }
}
