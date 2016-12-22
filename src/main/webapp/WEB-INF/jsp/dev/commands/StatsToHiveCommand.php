<?php
/**
 * Author: Kangle.Liu - liukangle@360.cn
 *
 * Last modified: 2013-07-10 15:07
 *
 * Filename: StatsToHiveCommand.php
 *
 * Description: dengchao把数据入到form库，我再把数据导入到hive里。 
 *
 */
include __DIR__ . '/CommonCommand.php';
class StatsToHiveCommand extends CommonCommand
{

    public function beforeAction($action, $params)
    {
        parent::beforeAction($action, $params);
        return true;
    }

    /**
     * add by kangle
     *
     * 2013-07-10
     *
     * 把ad_stats_area_report_group表的数据写到日志里
     */
    public function actionAdStatsAreaReportGroupToLog($date=false)
    {
        if (!$date)
            $date = date('Y-m-d', strtotime('-1 days'));
        try {
            $sql = "select * from ad_stats_area_report_group where create_date=:date";
            $cmd = Yii::app()->db_report->createCommand($sql);
            $cmd->bindParam(':date', $date, PDO::PARAM_STR);
            $rows = $cmd->queryAll();
            echo count($rows);
            return ;
            $file = Yii::app()->params['statsReportLog'] . "AdStatsAreaReportGroup" . date('Ymd', strtotime($date));
            if ($rows) {
                $wh = fopen($file, 'w');
                foreach($rows as $row) {
                    unset($row['id'], $row['create_date']);
                    fwrite($wh, join("\t", $row) . "\n");
                }
            }
            fclose($wh);
        } catch(Exception $e) {

        }
        return ;
    }

    /**
     * add by kangle
     *
     * 2013-07-10
     *
     * 写成一个公用的把表的数据写入到日志里
     */
    public function actionDbToFile($tabName, $date=false)
    {/*{{{*/
        ini_set('memory_limit', '2048M');
        $tabArr = array(
            'ad_stats_area_report_group' => array(
                'field' => '`ad_user_id`, `ad_group_id`, `ad_plan_id`, `ad_group_name`, `ad_plan_name`, `city_id`, `province_id`, `city_name`, `province_name`, `clicks`, `views`, `total_cost`, `click_percent`, `click_cost`, `user_name`, `company_name`, `client_category_name`, `client_category`, `signed_category_name`, `signed_category`, `industry_category_name`, `industry_category`, `create_time`',
            ),
            'ad_stats_area_report_plan' => array(
                'field' => '`ad_user_id`, `ad_plan_id`, `ad_plan_name`, `city_id`, `province_id`, `city_name`, `province_name`, `clicks`, `views`, `total_cost`, `click_percent`, `click_cost`, `user_name`, `company_name`, `client_category_name`, `client_category`, `signed_category_name`, `signed_category`, `industry_category_name`, `industry_category`, `create_time`',
            ),
            'ad_stats_keyword_report' => array(
                'field' => '`ad_user_id`, `ad_group_id`, `ad_plan_id`, `ad_group_name`, `ad_plan_name`, `ad_keyword_id`, `keyword`, `clicks`, `views`, `total_cost`, `click_percent`, `click_cost`, `user_name`, `company_name`, `client_category_name`, `client_category`, `signed_category_name`, `signed_category`, `industry_category_name`, `industry_category`, `create_time`',
            ),
            'ad_stats_report_advert' => array(
                'field' => '`ad_advert_id`, `ad_user_id`, `ad_group_id`, `ad_plan_id`, `ad_group_name`, `ad_plan_name`, `clicks`, `views`, `total_cost`, `click_percent`, `click_cost`, `user_name`, `company_name`, `client_category_name`, `client_category`, `signed_category_name`, `signed_category`, `industry_category_name`, `industry_category`, `caption`, `start_date`, `end_date`, `ad_type`, `create_time`',
            ),
            'ad_stats_report_group' => array(
                'field' => '`ad_user_id`, `ad_group_id`, `ad_plan_id`, `ad_group_name`, `ad_plan_name`, `clicks`, `views`, `total_cost`, `click_percent`, `click_cost`, `user_name`, `company_name`, `client_category_name`, `client_category`, `signed_category_name`, `signed_category`, `industry_category_name`, `industry_category`, `start_date`, `end_date`, `create_time`',
            ),
            'ad_stats_area_report_province' => array(
                'field' => '`ad_user_id`, `province_id`, `city_name`, `province_name`,`clicks`, `views`, `total_cost`, `click_percent`, `click_cost`, `user_name`, `company_name`, `client_category_name`, `client_category`, `signed_category_name`, `signed_category`, `industry_category_name`, `industry_category`,`create_time`',
            ),       
        );
        $offset = 100000;
        if (!$date)
            $date = date('Y-m-d', strtotime('-1 days'));
        $fileName = $tabName . '_' . date('Ymd', strtotime($date));
        try {
            if (!isset($tabArr[$tabName])) {
                throw new Exception("$tabName is not in rule.\n");
            }
            $dbStr = 'db_' . $tabName;
            $sql = "select count(1) from $tabName where create_date=:date";
            $cmd = Yii::app()->$dbStr->createCommand($sql);
            $cmd->bindParam(':date', $date, PDO::PARAM_STR);
            $count = $cmd->queryScalar();
            if (!$count)
                throw new Exception("$tabName get data null.");
            $file = Config::item('statsLog') . 'report_load_log/' . $fileName;
            $wh = fopen($file, 'w');
            if (isset($tabArr[$tabName])) 
                $sql = "select {$tabArr[$tabName]['field']} from $tabName where create_date=:date";
            else
                $sql = "select * from $tabName where create_date=:date";
            for ($i=0; $i<$count; $i=$i+$offset) {
                $executeSql = $sql . " limit $i, $offset";
                $cmd = Yii::app()->$dbStr->createCommand($executeSql);
                $cmd->bindParam(':date', $date, PDO::PARAM_STR);
                $rows = $cmd->queryAll();
                if (!$rows)
                    continue;
                foreach ($rows as $row) {
                    fwrite($wh, join("\t", $row) . "\n");
                }
            }
            fclose($wh);
            rename($file, $file . ".ok");
            echo date('Y-m-d H:i:s') . "---{$fileName} is ok ...\n";
        } catch(Exception $e) {
            echo $e->getMessage() . "\n";
            ComAdLog::write(array(date('Y-m-d H:i:s'), '[error]', 'db_to_file', 'tabName:' . $tabName, 'date:' . $date,  $e->getMessage()), 'stats_to_hive_error_' . date('ymd'));
        }
        return ;
    }/*}}}*/

    /**
     * add by kangle
     *
     * 2013-07-10
     *
     * 把文件同步到hadoop的跳板机上
     *
     */
    public function actionRsyncFile($date=false)
    {/*{{{*/
        if (!$date)
            $date = date('Y-m-d', strtotime('-1 days'));
        $dateStr = date('Ymd', strtotime($date));
        $dir = Config::item('statsLog') . 'report_load_log/';
        $res = `/bin/ls -l $dir | /bin/grep -P '$dateStr.ok$' | awk '{print $9}'`;
        if (!$res) {
            echo "there is not rule file .\n";
            return ;
        }
        $fileArr = explode("\n", trim($res));
        foreach ($fileArr as $fileName) {
            $file = $dir . $fileName;
            $fileName = str_replace('.ok', '', $fileName);

            $res = shell_exec("/usr/bin/rsync -azvP $file " . Yii::app()->params['hiveServerIP'] . "::report_file/{$fileName} --delay-updates --timeout=60");
            if (!$res) {
                ComAdLog::write(array(date('Y-m-d H:i:s'), '[error]', 'rsync_file', 'file:' . $file, 'date:' . $date,  'rsync file failure'), 'stats_to_hive_error_' . date('ymd'));
            } else {
                rename($file, $file . ".rsynced");
            }
        }
        return ;
    }/*}}}*/

    public function getDjBranchDB($uid)
    {
        $centerDB = DbConnectionManager::getDjCenterDB();
        $daoDbRouter    = new DbRouter();
        $daoDbRouter->setDB($centerDB);
        $dbID = $daoDbRouter->getRouter($uid);
        $djBranchDB = DbConnectionManager::getDjBranchDB($dbID);
        return $djBranchDB;
    }
    /**
     * add by kangle
     *
     * 2013-08-15
     *
     * hive查询定制报表
     *
     */
    public function actionCalBookReport()
    {
        ini_set('memory_limit', '20480M'); 
        $idStr='';
        try {
            $sql = "select * from " . AdStatsBookReportTask::model()->tableName() . " where create_date='" . date('Y-m-d') . "' and type=" 
                . AdStatsBookReport::TYPE_HADOOP . " and status=" . AdStatsBookReportTask::STATUS_START . " limit 50";
            $rows = Yii::app()->db_book_report->createCommand($sql)->queryAll();
            if (!$rows)
                throw new Exception("there's no data...\n");
            //更新状态标志位到正在执行任务状态
            foreach($rows as $uKey=>$uVal){
            $idStr.= $uVal['id'].",";
            }
            $idStr = substr($idStr,0,-1);
            $updateStatusSql = "update " . AdStatsBookReportTask::model()->tableName(). " set `status`=3 where id in({$idStr})";
            $updateRows = Yii::app()->db_book_report->createCommand($updateStatusSql)->execute();      
            $prefixFile = date('YmdHis') . "_";
            $filePath = Config::item('statsLog') . 'report_hive_log/';
            $pid = pcntl_fork();
            if ($pid == -1) {
                echo "fork process failure...";
            } elseif ($pid) {
                $pidBak = $pid;
                Yii::app()->db_book_report->setActive(false);
                Yii::app()->db_book_report->setActive(true);
                pcntl_waitpid($status, $pid);
                $cassandra = new ComCassandra();
                foreach ($rows as $row) {
                    try {
                        $file = $filePath . $prefixFile . $pidBak . "_" . $row['id'];
                        if (!file_exists($file)) {
                            throw new Exception(date('Y-m-d H:i:s') . " $file is not exists...");
                        }
                        $hash = md5("{$row['id']}-{$row['ad_user_id']}-" . $prefixFile . "-" . $pidBak).'yasuo'; 
                        $cassandra->save($hash, gzcompress(file_get_contents($file)));
                        @rename($file, $file . ".del");
                        $arr = array(
                            'download_key' => $hash,
                            'update_time' => date('Y-m-d H:i:s'),
                            'status' => AdStatsBookReportTask::STATUS_FINISH,
                        );
                        Yii::app()->db_book_report->createCommand()->update(AdStatsBookReportTask::model()->tableName(), $arr, 'id=' . (int)$row['id']);
                        echo date('Y-m-d H:i:s') . "--" . $file . "is ok\n";
                    } catch (Exception $e) {
                        $arr = array(
                            'failure_times' => $row['failure_times'] + 1,
                            'update_time' => date('Y-m-d H:i:s'),
                            'status'=>AdStatsBookReportTask::STATUS_START,
                        );
                        if ($arr['failure_times'] >= 3)
                            $arr['status'] = AdStatsBookReportTask::STATUS_FAILURE;
                        Yii::app()->db_book_report->createCommand()->update(AdStatsBookReportTask::model()->tableName(), $arr, 'id=' . (int)$row['id']);
                        print_r($e->getMessage() . "\n");
                    }
                }
            } else {
                $pid = getmypid();
                $hiveClient = new ComHive();
                foreach ($rows as $key => $row) {
                    $data = json_decode($row['data'], true);
                    $sql = "select {$data['select']} from {$data['table_name']} where create_date>='{$row['start_date']}' and create_date<='{$row['end_date']}' and ad_user_id='{$row['ad_user_id']}'";
                    if (isset($data['where']) && $data['where'] != '')
                        $sql .= "and {$data['where']}";
                    if (isset($data['grouphive']) && $data['grouphive'] != '')
                        $sql = $sql  . " group by {$data['grouphive']} ";
                    $resArr = array();
                    //$sql = $data['hivesql'];
                    $hiveClient->execute("set mapred.job.priority=HIGH");
                    $hiveClient->execute("set mapred.job.name=dengchao_report");
                    $hiveClient->execute($sql);
                    $resArr=array();
                    while ($res = $hiveClient->fetchN(1000)) {;
                        $resArr = array_merge($resArr, $res);
                    }
                    $wholeArr = array();
                    $fieldStr = trim(preg_replace('/round\((.+?),\d\)/i', "$1", $data['select']));                    
                    $fieldArr = explode(',', $fieldStr);
                    //$fieldArr = explode(',', $data['hivefiled']);
                    foreach ($fieldArr as $key => $field) {
                        $fieldArr[$key] = trim(preg_replace('/^(?:.+\s+as\s+)?\s*(\w*)\b\s*$/i', "$1", $field));
                    }
                    foreach ($resArr as $k => $value) {
                        $arr = explode("\t", $value);
                        foreach ($fieldArr as $key => $field) {
                            $wholeArr[$k][$field] = $arr[$key];
                            if($field=='ad_group_name' && (!($wholeArr[$k]['ad_group_name']) || $wholeArr[$k]['ad_group_name']=='NULL')){
                                $djBranchDB = $this->getDjBranchDB($row['ad_user_id']);
                                $groupTitle = $djBranchDB->createCommand("select title from ad_group where id='{$wholeArr[$k]['ad_group_id']}'")->queryScalar();
                                $wholeArr[$k][$field] = $groupTitle;
                            }
                            if($field=='ad_plan_name' && (!($wholeArr[$k]['ad_plan_name']) || $wholeArr[$k]['ad_plan_name']=='NULL')){
                                $djBranchDB = $this->getDjBranchDB($row['ad_user_id']);
                                $planTitle = $djBranchDB->createCommand("select title from ad_plan where id='{$wholeArr[$k]['ad_plan_id']}'")->queryScalar();
                                $wholeArr[$k][$field] = $planTitle;
                            }
                            if($field=='caption' && (!($wholeArr[$k]['caption']) || $wholeArr[$k]['caption']=='NULL')){
                                $djBranchDB = $this->getDjBranchDB($row['ad_user_id']);
                                $advertTitle = $djBranchDB->createCommand("select caption from ad_advert where id='{$wholeArr[$k]['ad_advert_id']}'")->queryScalar();
                                $wholeArr[$k][$field] = $advertTitle;
                            }
                            if($field=='zilian' && (!($wholeArr[$k]['zilian']) || $wholeArr[$k]['zilian']=='NULL')){
                                $djBranchDB = $this->getDjBranchDB($row['ad_user_id']);
                                $advertTitle = $djBranchDB->createCommand("select ad_type,caption from ad_advert where id='{$wholeArr[$k]['sub_id']}'")->queryRow();
                                $wholeArr[$k][$field] = $advertTitle['caption'];
                                if($advertTitle['ad_type']==11 || $advertTitle['ad_type']==16)
                                {
                                   $wholeArr[$k][$field] = '图片';
                                }
                            }                        
                            if($field=='description1' && (!($wholeArr[$k]['description1']) || $wholeArr[$k]['description1']=='NULL' || $wholeArr[$k]['description1']=='\\\\N')){
                                $djBranchDB = $this->getDjBranchDB($row['ad_user_id']);
                                $advertDesc = $djBranchDB->createCommand("select description from ad_advert where id='{$wholeArr[$k]['ad_advert_id']}'")->queryScalar();
                                $advertDescArr = explode("^",$advertDesc);
                                $wholeArr[$k][$field] = $advertDescArr[0];
                                $wholeArr[$k]["description2"] = $advertDescArr[1];
                            }
                        if($data['table_name']=='ad_stats_area_group_report' || $data['table_name']=='ad_stats_area_plan_report' || $data['table_name']=='ad_stats_area_report_province' ){ //地域时单取省份名
                            $areaInfo = Yii::app()->db_center->createCommand("select area_name from ad_areas where area_id='{$wholeArr[$k]['province_id']}'")->queryRow();
                            $wholeArr[$k]["province_name"] = $areaInfo['area_name'];
                        }
                            
                        }
                        foreach ($data['ext_filed'] as $field => $ruleArr) {
                           // if($field=='click_cost') continue; //直接再语句里面写了
                            if ($ruleArr[1] == '/') {
                                $wholeArr[$k][$field] = ($wholeArr[$k][$ruleArr[2]] == 0) ? 0 : round($wholeArr[$k][$ruleArr[0]] / $wholeArr[$k][$ruleArr[2]], 4);
                                if (isset($ruleArr[3]) && $ruleArr[3])
                                    $wholeArr[$k][$field] = ($wholeArr[$k][$field] * 100) . "%";
                            }
                        }
                    }
                    $file = $filePath . $prefixFile . $pid . "_" . $row['id'];
                    $wh = fopen($file, 'w');
                    fwrite($wh, join("\t", $data['title']) . "\n");
                    foreach ($wholeArr as $value) {
                        $titleArr = array();
                        $arr = array();
                        foreach ($data['title'] as $k => $v) {
                            $arr[] = $value[$k];
                        }
                        $content .= join("\t", $arr) . "\n";
                        fwrite($wh, join("\t", $arr) . "\n");
                    }
                    fclose($wh);
                }
                $hiveClient->closeConnect();
                exit;
            }
        } catch (Exception $e) {
            print_r($e->getMessage());
        }
        return ;
    }/*}}}*/
}
