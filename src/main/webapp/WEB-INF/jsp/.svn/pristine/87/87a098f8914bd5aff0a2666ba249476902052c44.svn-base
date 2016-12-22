<?php
/**
 * Filename: ExportCommand.php
 *
 * Description: 导出数据
 *
 */
class ExportCommand extends CConsoleCommand
{
    /**
     * 导出 ad_stats_click 表数据
     */
    public function actionAdstats ($outPutFile,$date=null) {
        $taskName = sprintf("[adstats %s]", date('Ymd_His'));
        $beginTime = date('Y-m-d H:i:s');
        if (is_null($date)) {
            $date = date('Ymd',strtotime('-1 day'));
        } else {
            $date = date('Ymd', strtotime($date));
        }
        $outPutFile = $outPutFile.'_'.$date;
        $outPutFileFinish = $outPutFile.'.ok';
        printf("%s begin at %s, date[%s], outputfile[%s]\n", $taskName, $beginTime, $date, $outPutFile);

        // $daoStats = new EdcStats();
        $fields = array(
            'id',
            'clicks',
            'views',
            'total_cost',
            'trans',
            'status',
            'create_date',
            'ad_group_id',
            'ad_plan_id',
            'ad_advert_id',
            'ad_user_id',
            'ad_channel_id',
            'ad_place_id',
            'admin_user_id',
            'last_update_time',
            'data_source',
            'create_time',
            'update_time',
            'type',
        );
        $sql = sprintf('select %s from %s where id>:id limit %d',
            implode(',', $fields), 'ad_stats_click_'.$date, 1000
        );

        $statsDBSize = Yii::app()->params['db_stat_num'];
        $statsDataDir = Config::item('statsDataDir');
        for ($i=1; $i<=$statsDBSize; ++$i) {
            $statsBranchDB = DbConnectionManager::getStatBranchDB($i);
            if (!$statsBranchDB) {
                printf("%s get branch db of stats[%d] fail!\n", $taskName, $i);
                continue;
            }
            $cmd = $statsBranchDB->createCommand($sql);

            $lastID = 0;
            while (true) {
                $cmd->bindParam(':id', $lastID, PDO::PARAM_INT);
                $ret = $cmd->queryAll();
                if (empty($ret)) {
                    break;
                }
                $strOut = '';
                foreach ($ret as $oneItem) {
                    $lastID = $oneItem['id'];
                    $strOut .= implode("\x01", $oneItem)."\n";
                }
                file_put_contents($statsDataDir.$outPutFile, $strOut, FILE_APPEND);
            }
        }
        file_put_contents($statsDataDir.$outPutFileFinish, '');
        $endTime = date('Y-m-d H:i:s');
        printf("%s begin at %s, end at %s\n", $taskName, $beginTime, $endTime);
    }
    /**
     * 导出 ad_click_log 表数据
     */
    public function actionAdclicklog ($outPutFile,$date=null) {
        $taskName = sprintf("[adclicklog %s]", date('Ymd_His'));
        $beginTime = date('Y-m-d H:i:s');
        if (is_null($date)) {
            $date = date('Ymd',strtotime('-1 day'));
            $create_date = date('Y-m-d',strtotime('-1 day'));
        } else {
            $date = date('Ymd', strtotime($date));
            $create_date = date('Y-m-d', strtotime($date));
        }
        $outPutFile = $outPutFile.'_'.$date;
        $outPutFileFinish = $outPutFile.'.ok';
        printf("%s begin at %s, date[%s], outputfile[%s]\n", $taskName, $beginTime, $date, $outPutFile);
    
        // $daoStats = new EdcStats();
        $fields = array(
            'id',
            'click_id',
            'ad_user_id',
            'ad_advert_id',
            'ad_group_id',
            'ad_plan_id',
            'keyword',
            'area_key',
            'create_time',
            'price',
            'create_date',
            'status',
            'inter_id',
            'ad_channel_id',
            'ad_place_id',
            'place',
            'ver',
            'try_num',
            'update_time',
            'save_time',
            'sub_ver',
            'sub_data',
        );
        $sql = sprintf('select %s from %s where create_date=:create_date and id>:id and id<=:id+%d',
                implode(',', $fields), 'ad_click_log', 10000
        );
        
        $statsDataDir = Config::item('statsDataDir');
        $dbNum = 1;
        $statsBranchDB = DbConnectionManager::getStatBranchDB($dbNum);
        if (!$statsBranchDB) {
            printf("%s get branch db of stats[%d] fail!\n", $taskName, $dbNum);
            continue;
        }
        $cmd = $statsBranchDB->createCommand($sql);
        $cmd->bindParam(':create_date', $create_date, PDO::PARAM_STR);
        $lastID = 0;
        //查询符合条件的最小id
        $sql_min = sprintf('select min(id) as min_id,max(id) as max_id  from %s where create_date=:create_date',
         'ad_click_log'
        );
        $cmd_min = $statsBranchDB->createCommand($sql_min);
        $cmd_min->bindParam(':create_date', $create_date, PDO::PARAM_STR);
        $ret_min = $cmd_min->queryRow();
        if (!empty($ret_min)) {
            $lastID = $ret_min['min_id']-1;
        }
        while (true) {
            $cmd->bindParam(':id', $lastID, PDO::PARAM_INT);
            $ret = $cmd->queryAll();
            if (empty($ret)) {
                break;
            }
            $strOut = '';
            foreach ($ret as $oneItem) {
                $lastID = $oneItem['id'];
                $strOut .= implode("\x01", $oneItem)."\n";
            }
            file_put_contents($statsDataDir.$outPutFile, $strOut, FILE_APPEND);
        }
        
        file_put_contents($statsDataDir.$outPutFileFinish, '');
        $endTime = date('Y-m-d H:i:s');
        printf("%s begin at %s, end at %s\n", $taskName, $beginTime, $endTime);
    }
    /**
     * 导出 ad_stats_click 表数据
     */
    public function actionAdstatskeyword ($outPutFile,$date=null) {
        $taskName = sprintf("[adstatskeyword %s]", date('Ymd_His'));
        $beginTime = date('Y-m-d H:i:s');
        if (is_null($date)) {
            $date = date('Ymd',strtotime('-1 day'));
        } else {
            $date = date('Ymd', strtotime($date));
        }
        $outPutFile = $outPutFile.'_'.$date;
        $outPutFileFinish = $outPutFile.'.ok';
        printf("%s begin at %s, date[%s], outputfile[%s]\n", $taskName, $beginTime, $date, $outPutFile);
    
        // $daoStats = new EdcStats();
        $fields = array(
            'id',
            'clicks',
            'views',
            'costs',
            'trans',
            'ad_group_id',
            'ad_plan_id',
            'ad_keyword_id',
            'ad_user_id',
            'create_date',
            'keyword',
            'create_time',
            'update_time',
            'type',
        );
        $sql = sprintf('select %s from %s where id>:id limit %d',
            implode(',', $fields), 'ad_stats_keyword_click_'.$date, 1000
        );
    
        $statsDBSize = Yii::app()->params['db_stat_num'];
        $statsDataDir = Config::item('statsDataDir');
        for ($i=1; $i<=$statsDBSize; ++$i) {
            $statsBranchDB = DbConnectionManager::getStatBranchDB($i);
            if (!$statsBranchDB) {
                printf("%s get branch db of stats[%d] fail!\n", $taskName, $i);
                continue;
            }
            $cmd = $statsBranchDB->createCommand($sql);
    
            $lastID = 0;
            while (true) {
                $cmd->bindParam(':id', $lastID, PDO::PARAM_INT);
                $ret = $cmd->queryAll();
                if (empty($ret)) {
                    break;
                }
                $strOut = '';
                foreach ($ret as $oneItem) {
                    $lastID = $oneItem['id'];
                    $strOut .= implode("\x01", $oneItem)."\n";
                }
                file_put_contents($statsDataDir.$outPutFile, $strOut, FILE_APPEND);
            }
        }
        file_put_contents($statsDataDir.$outPutFileFinish, '');
        $endTime = date('Y-m-d H:i:s');
        printf("%s begin at %s, end at %s\n", $taskName, $beginTime, $endTime);
    }
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
