<?php
/**
 * Author: Kangle.Liu - liukangle@360.cn
 *
 * Last modified: 2012-12-26 10:41
 *
 * Filename: CrontabCommand.php
 *
 * Description:广告展现、点击统计 
 *
 */
ini_set('memory_limit', '2048M');
ini_set('max_execution_time', '9000');


class CrontabCommand extends CConsoleCommand
{

	public function actionTest()
	{
		print_r(Config::item('statsServerRoom'));
		return ;
		print_r(Yii::app()->db);
		return ;
	}

	function my_sort($a, $b)
	{
        if($a == $b) return 0;
        return ($a>$b) ? 1 : -1;
    }

    public function actionSetFlag($serverRoom=false)
	{
        if (!in_array($serverRoom, Yii::app()->params['statsServerRoom']))
            $serverRoom = Yii::app()->params['statsServerRoom'][0];
        $dbStr = 'stats_db_' . $serverRoom;
        $filePath = Yii::app()->params['statsLog'];
        $content = shell_exec("/bin/ls -l {$filePath}|/bin/grep -P '\.gz$'|/bin/awk '{print $9}'");
        if($content === null)
            return ;
        $files = explode("\n", trim($content));
        $collectQueue = new ComCollectQueue();
        if (is_array($files)) {
            foreach ($files as $file) {
                $fileName = $filePath . $file;
                $flagFile = $filePath . $file . '.ok.' . $serverRoom;
                if(file_exists($fileName) && !file_exists($flagFile)){
                    $flagKey = str_replace('.gz', '', $file);
                    try{ 
                        $flagArr = explode("-", $flagKey);
                        $arr = array(
                            'hour_str' => $flagArr[0],
                            'time_str' => sprintf("%d%02d", $flagArr[0], $flagArr[1]),
                            'm_inter' => $flagArr[1],
                            'ip' => $flagArr[2],
                            'sid' => $flagArr[3],
                            'hash_value' => md5_file($fileName),
                            'status' => StatsFileLog::STATUS_STATISTICED,
                            'update_time' => date('Y-m-d H:i:s'),
                        );   
                        $flag = Yii::app()->$dbStr->createCommand()->insert(StatsFileLog::model()->tableName(), $arr);
                        if ($flag) {
                            file_put_contents($flagFile, ''); 
                        }    
                    }catch(Exception $e){ 
                        print_r($e->getMessage());
                        echo "\n";
                    }    
                }    
            }    
        }    
        return ;
    } 

	public function actionSplitDb()
	{
        $sql = "select ad_user_id, count(1) count from ad_group where 1 group by ad_user_id order by count desc";
        $cmd = Yii::app()->db->createCommand($sql);
        $rows = $cmd->queryAll();
        $res = array();
        $resCount = array();
        for($i=0; $i<50; $i++){
            $res[$i] = array();
        }
        foreach($rows as $value) {
            usort($res, function($a, $b){
                if (array_sum($a) == array_sum($b)) return 0;
                return (array_sum($a) > array_sum($b)) ? 1 : -1;
            });
            $res[0][$value['ad_user_id']] = $value['count'];
        }
        foreach($res as $k => $value) {
            echo $k . "=>" . array_sum($value) . "\n";
        }
        exit;
        print_r($res);
        exit;
        Yii::app()->end();
    }

    /*
     * 从dianjing_load库里把数据汇总到dianjing_stats库里
     * 修改dianjing_load库里stats表的逻辑为,先把所有<maxid的状态设置为1，再修改failArr里的为0;
     */
	public function actionSumStats()
	{
        $maxId = Stats::model()->getMaxId();
        $minId = Stats::model()->getMinId();
        $cacheArr = array(
            'max_id' => $maxId,
            'min_id' => $minId,
            'start_time' => date('Y-m-d H:i:s'),
            'type' => 1,
        );
        $sql = "select advert_id, stats_date, sum(view_times) as sum_view_times, sum(click_times) as sum_click_times, sum(trans_times) as sum_trans_times, sum(total_cost) as sum_total_cost from stats "
           . " where status = 0 group by stats_date, advert_id";
        $rows = Yii::app()->db->createCommand($sql)->queryAll();
        $sql = " select advert_id,plan_id, group_id, user_id from " . Stats::model()->tableName() . " where id >= $minId and id <= $maxId group by advert_id, plan_id, group_id, user_id";
        $adInfo = Yii::app()->db->createCommand($sql)->queryAll();
        $adArr = array();
        if($adInfo){
            foreach($adInfo as $value){
                $adArr[$value['advert_id']] = array(
                    'plan_id' => $value['plan_id'],
                    'group_id' => $value['group_id'],
                    'user_id' => $value['user_id'],
                );
            }
            unset($adInfo);
        }
        $i = 1;
        $failArr = array();
        foreach($rows as $key => $value){
            $advertId = $value['advert_id'];
            $sql = "select count(1) as count from ad_stats where create_date = '" . $value['stats_date'] . "' and ad_advert_id = " . $advertId ;
            if(Yii::app()->db_stats->createCommand($sql)->queryScalar()){
            $sql = "update ad_stats set clicks = clicks + " . $value['sum_click_times'] . ", views = views + " . $value['sum_view_times'] . ", total_cost = total_cost + " . $value['sum_total_cost']
                . ", trans = trans + " . $value['sum_trans_times'] . ", last_update_time = " . time() . " where create_date = '" . $value['stats_date'] . "' and ad_advert_id = " . $advertId . " and status = 0 limit 1";
                if(Yii::app()->db_stats->createCommand($sql)->execute()){
                    Yii::log($cacheArr['start_time'] . "\tupdate\t$advertId\t" . $value['sum_view_times'] . "\t" . $value['sum_click_times'] . "\t" . $value['sum_total_cost'] . "\t" . $value['sum_trans_times'], CLogger::LEVEL_INFO, 'dj_stats_log');
                }else{
                    $failArr[$value['stats_date']][] = $advertId;
                }
            }else{
                $statsArr = array(
                    'ad_advert_id' => $advertId,
                    'clicks' => $value['sum_click_times'],
                    'views' => $value['sum_view_times'],
                    'trans' => $value['sum_trans_times'],
                    'total_cost' => $value['sum_total_cost'],
                    'create_date' => $value['stats_date'],
                    'last_update_time' => time(),
                );
                if(isset($adArr[$advertId])){
                    $statsArr['ad_group_id'] = $adArr[$advertId]['group_id'];
                    $statsArr['ad_plan_id'] = $adArr[$advertId]['plan_id'];
                    $statsArr['ad_user_id'] = $adArr[$advertId]['user_id'];
                }
                $flag = Yii::app()->db_stats->createCommand()->insert('ad_stats', $statsArr);
                if($flag){
                    Yii::log($cacheArr['start_time'] . "\tinsert\t$advertId\t" . $value['sum_view_times'] . "\t" . $value['sum_click_times'] . "\t" . $value['sum_total_cost'] . "\t" . $value['sum_trans_times'], CLogger::LEVEL_INFO, 'dj_stats_log');
                }else{
                    $failArr[$value['stats_date']][] = $advertId;
                }
            }

        }
        $sql = "update " . Stats::model()->tableName() . " set status = 1 where id <= $maxId";
        Yii::app()->db->createCommand($sql)->execute();
        if(count($failArr) > 0){
            foreach($failArr as $key => $value){
                if(count($value) > 0){
                    $ids = join(',', $value);
                    Yii::log($cacheArr['start_time'] . "\tsave failure\t$key\t$ids", CLogger::LEVEL_INFO, 'dj_stats_failure_adverts');
                    $sql = "update " . Stats::model()->tableName() . " set status = -1 where id <= $maxId and stats_date='$key' and advert_id in ($ids)";
                    $flag = Yii::app()->db->createCommand($sql)->execute();
                }
            }

        }
        $cacheArr['end_time'] = date('Y-m-d H:i:s');
        Yii::app()->db_stats->createCommand()->insert('record_cache', $cacheArr);
        Yii::app()->end();
    }

    public function actionDelData()
    {
        $arr = array('stats', 'area', 'interest', 'keyword');
        $daysAgo = date('Y-m-d', strtotime('-3 days'));
        foreach($arr as $value){
            $sql = "delete from $value where stats where status = 1 and stats_date <= '$dayAgo'"; 
            $rowCount = Yii::app()->db->createCommand($sql)->execute();
            Yii::log(date('Y-m-d H:i:s') . "\tdelete\t$value\t" . $rowCount , CLogger::LEVEL_INFO, 'dj_' . $value . '_delete');
        }
        Yii::app()->end();
    }

    /*
     * 从dianjing_load库里把数据汇总到dianjing_stats库里
     * 修改dianjing_load库里stats表的逻辑为,先把所有<maxid的状态设置为1，再修改failArr里的为0;
     */
	public function actionSumArea()
	{
        $maxId = Area::model()->getMaxId();
        $minId = Area::model()->getMinId();
        $cacheArr = array(
            'max_id' => $maxId,
            'min_id' => $minId,
            'start_time' => date('Y-m-d H:i:s'),
            'type' => 2,
        );
        $time1 = time();
        $sql = "select stats_date, group_id, area_fid, area_id, sum(view_times) as sum_view_times, sum(click_times) as sum_click_times, sum(trans_times) as sum_trans_times, sum(total_cost) as sum_total_cost from " . Area::model()->tableName()
           . " where status = 0 group by stats_date, group_id, area_fid, area_id";
        $rows = Yii::app()->db->createCommand($sql)->queryAll();
        $failArr = array();
        foreach($rows as $key => $value){
            $groupId = $value['group_id'];
            $sql = "select count(1) as count from ad_stats_area where create_date = '" . $value['stats_date'] . "' and ad_group_id = " . $groupId;
            if(Yii::app()->db_stats->createCommand($sql)->queryScalar()){
                $sql = "update ad_stats_area set clicks = clicks + " . $value['sum_click_times'] . ", views = views + " . $value['sum_view_times'] . ", costs = costs + " . $value['sum_total_cost']
                    . ", trans = trans + " . $value['sum_trans_times'] . ", update_time ='" . date('Y-m-d H:i:s') . "' where create_date = '" . $value['stats_date'] . "' and ad_group_id = " . $groupId . " limit 1";
                if(Yii::app()->db_stats->createCommand($sql)->execute()){
                    Yii::log($cacheArr['start_time'] . "\tupdate\t$groupId\t" . $value['sum_view_times'] . "\t" . $value['sum_click_times'] . "\t" . $value['sum_total_cost'] . "\t" . $value['sum_trans_times'], CLogger::LEVEL_INFO, 'dj_area_log');
                }else{
                    $failArr[$value['stats_date']][] = $groupId;
                }
            }else{
                $statsArr = array(
                    'ad_group_id' => $groupId,
                    'clicks' => $value['sum_click_times'],
                    'views' => $value['sum_view_times'],
                    'trans' => $value['sum_trans_times'],
                    'costs' => $value['sum_total_cost'],
                    'create_date' => $value['stats_date'],
                    'update_time' => date('Y-m-d H:i:s'),
                    'area_fid' => $value['area_fid'],
                    'area_id' => $value['area_id'],
                );
                $groupInfo = AdStats::model()->getGroupInfo($groupId);
                if($groupInfo){
                    $statsArr['ad_plan_id'] = $groupInfo['ad_plan_id'];
                    $statsArr['ad_user_id'] = $groupInfo['ad_user_id'];
                }
                $flag = Yii::app()->db_stats->createCommand()->insert('ad_stats_area', $statsArr);
                if($flag){
                    Yii::log($cacheArr['start_time'] . "\tinsert\t$groupId\t" . $value['sum_view_times'] . "\t" . $value['sum_click_times'] . "\t" . $value['sum_total_cost'] . "\t" . $value['sum_trans_times'], CLogger::LEVEL_INFO, 'dj_area_log');
                }else{
                    $failArr[$value['stats_date']][] = $groupId;
                }
            }

        }
        $sql = "update " . Area::model()->tableName() . " set status = 1 where id <= $maxId and id >= $minId";
        Yii::app()->db->createCommand($sql)->execute();
        if(count($failArr) > 0){
            foreach($failArr as $key => $value){
                if(count($value) > 0){
                    $ids = join(',', $value);
                    Yii::log($cacheArr['start_time'] . "\tsave failure\t$key\t$ids", CLogger::LEVEL_INFO, 'dj_area_failure_groups');
                    $sql = "update " . Area::model()->tableName() . " set status = -1 where id <= $maxId and stats_date='$key' and group_id in ($ids)";
                    $flag = Yii::app()->db->createCommand($sql)->execute();
                }
            }

        }
        $cacheArr['end_time'] = date('Y-m-d H:i:s');
        Yii::app()->db_stats->createCommand()->insert('record_cache', $cacheArr);
        Yii::app()->end();
    }

	public function actionSumInterest()
	{
        $maxId = Interest::model()->getMaxId();
        $minId = Interest::model()->getMinId();
        $cacheArr = array(
            'max_id' => $maxId,
            'min_id' => $minId,
            'start_time' => date('Y-m-d H:i:s'),
            'type' => 3,
        );
        $time1 = time();
        $sql = "select stats_date, group_id, inter_id, sum(view_times) as sum_view_times, sum(click_times) as sum_click_times, sum(trans_times) as sum_trans_times, sum(total_cost) as sum_total_cost from " . Interest::model()->tableName()
           . " where status = 0 group by stats_date, group_id, inter_id";
        $rows = Yii::app()->db->createCommand($sql)->queryAll();
        $failArr = array();
        foreach($rows as $key => $value){
            $groupId = $value['group_id'];
            $sql = "select count(1) as count from ad_stats_interest where create_date = '" . $value['stats_date'] . "' and ad_group_id = " . $groupId;
            if(Yii::app()->db_stats->createCommand($sql)->queryScalar()){
                $sql = "update ad_stats_interest set clicks = clicks + " . $value['sum_click_times'] . ", views = views + " . $value['sum_view_times'] . ", costs = costs + " . $value['sum_total_cost']
                    . ", trans = trans + " . $value['sum_trans_times'] . ", update_time ='" . date('Y-m-d H:i:s') . "' where create_date = '" . $value['stats_date'] . "' and ad_group_id = " . $groupId . " limit 1";
                if(Yii::app()->db_stats->createCommand($sql)->execute()){
                    Yii::log($cacheArr['start_time'] . "\tupdate\t$groupId\t" . $value['sum_view_times'] . "\t" . $value['sum_click_times'] . "\t" . $value['sum_total_cost'] . "\t" . $value['sum_trans_times'], CLogger::LEVEL_INFO, 'dj_interest_log');
                }else{
                    $failArr[$value['stats_date']][] = $groupId;
                }
            }else{
                $statsArr = array(
                    'ad_group_id' => $groupId,
                    'clicks' => $value['sum_click_times'],
                    'views' => $value['sum_view_times'],
                    'trans' => $value['sum_trans_times'],
                    'costs' => $value['sum_total_cost'],
                    'create_date' => $value['stats_date'],
                    'update_time' => date('Y-m-d H:i:s'),
                    'inter_id' => $value['inter_id'],
                );
                $groupInfo = AdStats::model()->getGroupInfo($groupId);
                if($groupInfo){
                    $statsArr['ad_plan_id'] = $groupInfo['ad_plan_id'];
                    $statsArr['ad_user_id'] = $groupInfo['ad_user_id'];
                }
                $flag = Yii::app()->db_stats->createCommand()->insert('ad_stats_interest', $statsArr);
                if($flag){
                    Yii::log($cacheArr['start_time'] . "\tinsert\t$groupId\t" . $value['sum_view_times'] . "\t" . $value['sum_click_times'] . "\t" . $value['sum_total_cost'] . "\t" . $value['sum_trans_times'], CLogger::LEVEL_INFO, 'dj_interest_log');
                }else{
                    $failArr[$value['stats_date']][] = $groupId;
                }
            }

        }
        $sql = "update " . Interest::model()->tableName() . " set status = 1 where id <= $maxId";
        Yii::app()->db->createCommand($sql)->execute();
        if(count($failArr) > 0){
            foreach($failArr as $key => $value){
                if(count($value) > 0){
                    $ids = join(',', $value);
                    Yii::log($cacheArr['start_time'] . "\tsave failure\t$key\t$ids", CLogger::LEVEL_INFO, 'dj_interest_failure_groups');
                    $sql = "update " . Interest::model()->tableName() . " set status = -1 where id <= $maxId and stats_date='$key' and group_id in ($ids)";
                    $flag = Yii::app()->db->createCommand($sql)->execute();
                }
            }

        }
        $cacheArr['end_time'] = date('Y-m-d H:i:s');
        Yii::app()->db_stats->createCommand()->insert('record_cache', $cacheArr);
        Yii::app()->end();
    }

	public function actionSumKeyword()
	{
        $tabAdKeyword = AdKeyword::model()->tableName();
        $maxId = Keyword::model()->getMaxId();
        $minId = Keyword::model()->getMinId();
        $cacheArr = array(
            'max_id' => $maxId,
            'min_id' => $minId,
            'start_time' => date('Y-m-d H:i:s'),
            'type' => 4,
        );
        $time1 = time();
        $sql = "select stats_date, group_id, keyword, sum(view_times) as sum_view_times, sum(click_times) as sum_click_times, sum(trans_times) as sum_trans_times, sum(total_cost) as sum_total_cost from " . Keyword::model()->tableName()
           . " where status = 0 group by stats_date, group_id, keyword";
        $rows = Yii::app()->db->createCommand($sql)->queryAll();
        $failArr = array();
        foreach($rows as $key => $value){
            $groupId = $value['group_id'];
            $sql = "select count(1) as count from $tabAdKeyword where create_date = '" . $value['stats_date'] . "' and ad_group_id = " . $groupId;
            if(Yii::app()->db_stats->createCommand($sql)->queryScalar()){
                $sql = "update $tabAdKeyword set clicks = clicks + " . $value['sum_click_times'] . ", views = views + " . $value['sum_view_times'] . ", costs = costs + " . $value['sum_total_cost']
                    . ", trans = trans + " . $value['sum_trans_times'] . ", update_time ='" . date('Y-m-d H:i:s') . "' where create_date = '" . $value['stats_date'] . "' and ad_group_id = " . $groupId . " limit 1";
                if(Yii::app()->db_stats->createCommand($sql)->execute()){
                    Yii::log($cacheArr['start_time'] . "\tupdate\t$groupId\t" . $value['sum_view_times'] . "\t" . $value['sum_click_times'] . "\t" . $value['sum_total_cost'] . "\t" . $value['sum_trans_times'], CLogger::LEVEL_INFO, 'dj_keyword_log');
                }else{
                    $failArr[$value['stats_date']][] = $groupId;
                }
            }else{
                $statsArr = array(
                    'ad_group_id' => $groupId,
                    'clicks' => $value['sum_click_times'],
                    'views' => $value['sum_view_times'],
                    'trans' => $value['sum_trans_times'],
                    'costs' => $value['sum_total_cost'],
                    'create_date' => $value['stats_date'],
                    'update_time' => date('Y-m-d H:i:s'),
                    'keyword' => $value['keyword'],
                );
                $groupInfo = AdStats::model()->getGroupInfo($groupId);
                if($groupInfo){
                    $statsArr['ad_plan_id'] = $groupInfo['ad_plan_id'];
                    $statsArr['ad_user_id'] = $groupInfo['ad_user_id'];
                }
                $flag = Yii::app()->db_stats->createCommand()->insert($tabAdKeyword, $statsArr);
                if($flag){
                    Yii::log($cacheArr['start_time'] . "\tinsert\t$groupId\t" . $value['sum_view_times'] . "\t" . $value['sum_click_times'] . "\t" . $value['sum_total_cost'] . "\t" . $value['sum_trans_times'], CLogger::LEVEL_INFO, 'dj_keyword_log');
                }else{
                    $failArr[$value['stats_date']][] = $groupId;
                }
            }

        }
        $sql = "update " . Keyword::model()->tableName() . " set status = 1 where id <= $maxId";
        Yii::app()->db->createCommand($sql)->execute();
        if(count($failArr) > 0){
            foreach($failArr as $key => $value){
                if(count($value) > 0){
                    $ids = join(',', $value);
                    Yii::log($cacheArr['start_time'] . "\tsave failure\t$key\t$ids", CLogger::LEVEL_INFO, 'dj_keyword_failure_groups');
                    $sql = "update " . Interest::model()->tableName() . " set status = -1 where id <= $maxId and stats_date='$key' and group_id in ($ids)";
                    $flag = Yii::app()->db->createCommand($sql)->execute();
                }
            }

        }
        $cacheArr['end_time'] = date('Y-m-d H:i:s');
        Yii::app()->db_stats->createCommand()->insert('record_cache', $cacheArr);
        Yii::app()->end();
    }

    public function actionCompareStatsArea($date=null)
    {
        if(!$date)
            $date = date('Y-m-d', strtotime('-1 days'));
        $statsRows = AdStats::model()->getGroupData($date);
        $areaRows = AdArea::model()->getGroupData($date);
        foreach($statsRows as $groupId => $stats){
            if(isset($areaRows[$groupId])){
                $area = $areaRows[$groupId];
                $diffArr = array();
                if(round($stats['sum_total_cost'] - $areaRows[$groupId]['sum_total_cost'], 2) != 0.00)
                    $diffArr['costs'] = round($stats['sum_total_cost'] - $areaRows[$groupId]['sum_total_cost'], 2);
                if($stats['sum_clicks'] != $area['sum_clicks'])
                    $diffArr['clicks'] = $stats['sum_clicks'] - $area['sum_clicks'];
                if($stats['sum_views'] != $area['sum_views'])
                    $diffArr['views'] = $stats['sum_views'] - $area['sum_views'];
                if($stats['sum_trans'] != $area['sum_trans'])
                    $diffArr['trans'] = $stats['sum_trans'] - $area['sum_trans'];
                if(empty($diffArr))
                    continue;
                //print_r($stats);
                //print_r($area);
                //print_r($diffArr);
                //exit;
                $otherArea = AdArea::model()->getOtherAreaByGroupId($groupId, $date);
                $otherFlag = 1;
                if(!$otherArea){
                    $otherFlag = 0;
                    $otherArea = array(
                        'clicks' => 0,
                        'views' => 0,
                        'costs' => 0,
                        'trans' => 0,
                    );
                }
                foreach($diffArr as $key => $value){
                    if($key == 'costs')
                        $otherArea[$key] = round($otherArea[$key] + $value, 2);
                    else
                        $otherArea[$key] += $value;
                    if($otherArea[$key] < 0){
                        AdArea::model()->getCommonArea($date, $groupId, $key, $otherArea[$key]); 
                        $otherArea[$key] = 0;
                    }
                }
                if($otherFlag){
                    $id = $otherArea['id'];
                    Yii::app()->db_stats->createCommand()->update(AdArea::model()->tableName(), $otherArea, 'id=:id', array(':id' => $id));
                }else{
                    $otherArea['ad_group_id'] = $groupId;
                    $otherArea['create_date'] = $date;
                    $otherArea['area_fid'] = 0;
                    $otherArea['area_id'] = 0;
                    $otherArea['update_time'] = date('Y-m-d H:i:s');
                    $groupInfo = AdStats::model()->getGroupInfo($groupId);
                    if($groupInfo){
                        $groupInfo['ad_plan_id'] = $groupInfo['ad_plan_id'];
                        $groupInfo['ad_user_id'] = $groupInfo['ad_user_id'];
                    }
                    Yii::app()->db_stats->createCommand()->insert(AdArea::model()->tableName(), $otherArea);

                }
                Yii::log($date('Y-m-d H:i:s') . "\tupdate area 10001,10001\t$groupId\t" . join(',', $area), CLogger::LEVEL_INFO, 'dj_compare_area_notequal');
            }else{
                $arr = array(
                    'area_id' => 0,
                    'area_fid' => 0,
                    'ad_group_id' => $groupId,
                    'clicks' => $stats['sum_clicks'],
                    'views' => $stats['sum_views'],
                    'costs' => $stats['sum_total_cost'],
                    'trans' => $stats['sum_trans'],
                    'create_date' => $date,
                    'update_time' => date('Y-m-d H:i:s'),
                );
                $groupInfo = AdStats::model()->getGroupInfo($groupId);
                if($groupInfo){
                    $arr['ad_plan_id'] = $groupInfo['ad_plan_id'];
                    $arr['ad_user_id'] = $groupInfo['ad_user_id'];
                }
                if(Yii::app()->db_stats->createCommand()->insert(AdArea::model()->tableName(), $arr)){
                    Yii::log($arr['update_time'] . "\tadd area 10001,10001\t$groupId\t" . join(',', $arr), CLogger::LEVEL_INFO, 'dj_compare_area_add');
                    unset($statsRows[$groupId]);
                }
            }
        }
        Yii::app()->end();
    }
}

?>
