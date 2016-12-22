<?php
/**
 * 结算
 */
class SettleCommand extends CConsoleCommand {

    /**
     * 结算完毕之后倒入ad_stats_click统计表
     * @param date $date 结算日期
     * @author jingguangwen@360.cn  2014-09-05
     */
    public function actionImportStatsData ($date=null) {
        ini_set('memory_limit', '20480M');
        $task_name = sprintf("[ImportStatsData %s]", date('Y-m-d H:i:s'));
        $beginTime = date('Y-m-d H:i:s');
        printf("%s begin at %s\n", $task_name, $beginTime);
        $today = date('Y-m-d');
        if (is_null($date)) {
            $date = date('Y-m-d', strtotime('-1 day'));
        } else {
            $date = date('Y-m-d', strtotime($date));
        }
        if ($date==$today) {
            printf("%s today can not run\n", $task_name);
            return ;
        }
        // 获取ad_click_log库连接
        try {
            $clickLogDB = DbConnectionManager::getClickLogDB();
            if (false === $clickLogDB) {
                printf("%s get click_log  db fail\n", $task_name);
            }
        } catch (Exception $e) {
            printf("%s get dj click_log db fail, error [%s]!\n", $task_name, $e->getMessage());
            $clickLogDB = false;
        }
        if (!$clickLogDB) {
            $content = $task_name.'新统计无法获取点click_log库连接,请尽快手工结算。'.$date;
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            return ;
        }
        $statsDBSize = Yii::app()->params['db_stat_num'];
        $statsDBSize = intval($statsDBSize);

        if ($statsDBSize <= 0) {
            $content = $task_name.'新统计无法获取db_stat_num,请尽快手工结算。'.$date;
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            return ;
        }
        $statsDB = 'statsBranchDB';
        $statsTableName = "esc_stats_click_" . date('Ymd', strtotime($date));
        for ($i=1; $i <= $statsDBSize ; $i++) {
            // 获取分库连接
            ${$statsDB.'_'.$i} = DbConnectionManager::getStatBranchDB($i);
            if (!${$statsDB.'_'.$i}) {
                $content = sprintf("%s get branch db of stats[%d] fail! 此分库连接失败，请尽快手动处理",
                    $task_name, $i
                );
                printf("%s\n", $content);
                Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
                continue;
            }
        }
        $tableName = ComAdDetail::getTableName(strtotime($date));
        $sql = sprintf("select  count(1) as clicks,sum(price-reduce_price) as total_cost,ad_group_id,ad_plan_id,ad_advert_id,ad_user_id,cid as ad_channel_id,pid as ad_place_id,create_date,source_type from $tableName where  create_date='%s' and status not in (-1,2) and deal_status=1 and ver !='mediav' and ver !='shouzhu' and cheat_type not in (2,3) and price != reduce_price  group  by ad_advert_id,cid,pid,source_type ",$date);
        $clickLogRows = $clickLogDB->createCommand($sql)->queryAll();
        $daoStats   = new EdcStats();
        $have_error = 0;
        $error_num = 0;
        $error_mess = '';
        if (!empty($clickLogRows)) {
            foreach ($clickLogRows as $clickLogArr) {
                $ad_user_id = $clickLogArr['ad_user_id'];
                $clicks = $clickLogArr['clicks'];
                $total_cost = $clickLogArr['total_cost'];
                $ad_group_id = $clickLogArr['ad_group_id'];
                $ad_plan_id = $clickLogArr['ad_plan_id'];
                $ad_advert_id = intval($clickLogArr['ad_advert_id']);
                $ad_channel_id = intval($clickLogArr['ad_channel_id']);
                $ad_place_id = intval($clickLogArr['ad_place_id']);
                $create_date = $clickLogArr['create_date'];
                $plat_type = 1;
                if($ad_channel_id == 29 && $ad_place_id == 238) {
                    $plat_type = 3;
                }
                $source_type = intval($clickLogArr['source_type']);
                //所需入库的子库
                //更新 子库统计表 按照用户id取模
                $db_id = $ad_user_id%$statsDBSize+1;
                // 获取分库连接
                $statsBranchDB = ${$statsDB.'_'.$db_id};
                if (!$statsBranchDB) {
                    continue;
                }
                $daoStats->setDB($statsBranchDB);
                //入库
                $insert_arr = array(
                    'clicks' => $clicks,
                    'views' => 0,
                    'total_cost' => $total_cost,
                    'trans' => 0,
                    'status' => 1,
                    'create_date' => $create_date,
                    'plat_type'=>$plat_type,
                    'ad_group_id' => $ad_group_id,
                    'ad_plan_id' => $ad_plan_id,
                    'ad_advert_id' => $ad_advert_id,
                    'ad_user_id' => $ad_user_id,
                    'ad_channel_id' => $ad_channel_id,
                    'ad_place_id' => $ad_place_id,
                    'last_update_time' => time(),
                    'data_source' => 0,
                    'create_time' => date('Y-m-d H:i:s', time()),
                    'update_time' => date('Y-m-d H:i:s', time()),
                    'type' => 1,
                    'source_type' => $source_type
                );
                try {
                    $return_status = $daoStats->insertRow($statsTableName, $insert_arr);
                } catch(Exception $e) {
                    $have_error = 1;
                    $return_status = 0;
                    $error_mess = $e->getMessage();
                }

                //存入失败
                if (!$return_status) {
                    $have_error = 1;
                    $error_num++;
                    printf("%s branch db of stats[%d] insert  fail! ad_advert_id[%d] ad_channel_id[%d] ad_place_id[%d] error_mess[%s]\n", $task_name,$db_id,$ad_advert_id,$ad_channel_id,$ad_place_id,$error_mess);
                }
            }
        }
        if ($have_error) {
            $content = sprintf("%s insert db have  error,数量:[%d],原因:[%s] ! 请尽快手动排查",
                $task_name,$error_num,$error_mess
            );
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
        }
        $statsDBSize = Yii::app()->params['db_stat_num'];
        for ($i = 1; $i <= $statsDBSize; $i++) {
            $importStatsAreaDataShell = dirname(__FILE__)."/../yiic settle importStatsAreaData  --dbNum=".$i." >> /data/log/dj_importStatsAreaData_".$i.".log &";

            exec($importStatsAreaDataShell);
        }


        $endTime = date('Y-m-d H:i:s');
        printf("%s begin at %s, end at %s\n",
            $task_name, $beginTime, $endTime
        );
    }

    /**
     * 结算完毕之后倒入ad_stats_interest_click统计表
     * @param date $date 结算日期
     * @author jingguangwen@360.cn  2014-09-05
     */
    public function actionImportStatsInterestData ($date=null) {
        ini_set('memory_limit', '20480M');
        $task_name = sprintf("[ImportStatsInterestData %s]", date('Y-m-d H:i:s'));
        $beginTime = date('Y-m-d H:i:s');
        printf("%s begin at %s\n", $task_name, $beginTime);
        $today = date('Y-m-d');
        if (is_null($date)) {
            $date = date('Y-m-d', strtotime('-1 day'));
        } else {
            $date = date('Y-m-d', strtotime($date));
        }
        if ($date==$today) {
            printf("%s today can not run\n", $task_name);
            return ;
        }
        // 获取ad_click_log库连接
        try {
            $clickLogDB = DbConnectionManager::getClickLogDB();
            if (false === $clickLogDB) {
                printf("%s get click_log  db fail\n", $task_name);
            }
        } catch (Exception $e) {
            printf("%s get dj click_log db fail, error [%s]!\n", $task_name, $e->getMessage());
            $clickLogDB = false;
        }
        if (!$clickLogDB) {
            $content = $task_name.'系统新统计无法获取点click_log库连接,请尽快手工结算。'.$date;
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            return ;
        }
        $statsDBSize = Yii::app()->params['db_stat_num'];
        $statsDBSize = intval($statsDBSize);

        if ($statsDBSize <= 0) {
            $content = $task_name.'新统计无法获取db_stat_num,请尽快手工结算。'.$date;
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            return ;
        }
        $statsDB = 'statsBranchDB';
        $statsTableName = "esc_stats_interest_click_" . date('Ymd', strtotime($date));
        for ($i=1; $i <= $statsDBSize ; $i++) {
            // 获取分库连接
            ${$statsDB.'_'.$i} = DbConnectionManager::getStatBranchDB($i);
            if (!${$statsDB.'_'.$i}) {
                $content = sprintf("%s get branch db of stats[%d] fail! 此分库连接失败，请尽快手动处理",
                    $task_name, $i
                );
                printf("%s\n", $content);
                Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);

                continue;
            }
        }

        $tableName = ComAdDetail::getTableName(strtotime($date));
        $sql = sprintf("select  count(1) as clicks,sum(price-reduce_price) as costs,ad_group_id,ad_plan_id,ad_user_id,tag_id as inter_id,create_date from $tableName where  create_date='%s' and status not in (-1,2) and deal_status=1 and ver='guess' and cheat_type not in (2,3) and price != reduce_price  group  by ad_group_id,tag_id ",$date);
        $clickLogRows = $clickLogDB->createCommand($sql)->queryAll();
        $daoStats   = new EdcStatsInterest();
        $have_error = 0;
        $error_num = 0;
        $error_mess = '';
        if (!empty($clickLogRows)) {
            foreach ($clickLogRows as $clickLogArr) {
                $ad_user_id = $clickLogArr['ad_user_id'];
                $clicks = $clickLogArr['clicks'];
                $costs = $clickLogArr['costs'];
                $ad_group_id = $clickLogArr['ad_group_id'];
                $ad_plan_id = $clickLogArr['ad_plan_id'];
                $inter_id = $clickLogArr['inter_id'];
                $create_date = $clickLogArr['create_date'];
                //更新 子库统计表 按照用户id取模
                $db_id = $ad_user_id%$statsDBSize+1;
                // 获取分库连接
                $statsBranchDB = ${$statsDB.'_'.$db_id};
                if (!$statsBranchDB) {
                    continue;
                }
                $daoStats->setDB($statsBranchDB);
                //入库
                $insert_arr = array(
                    'ad_group_id' => intval($ad_group_id),
                    'ad_plan_id' => $ad_plan_id,
                    'ad_user_id' => $ad_user_id,
                    'inter_id' => intval($inter_id),
                    'clicks' => $clicks,
                    'views' => 0,
                    'costs' => $costs,
                    'trans' => 0,
                    'create_date' => $create_date,
                    'create_time' => date('Y-m-d H:i:s', time()),
                    'update_time' => date('Y-m-d H:i:s', time()),
                    'type' => 1
                );
                try {
                    $return_status = $daoStats->insertRow($statsTableName, $insert_arr);
                } catch(Exception $e) {
                    $have_error = 1;
                    $return_status = 0;
                    $error_mess = $e->getMessage();
                }
                //存入失败
                if (!$return_status) {
                    $have_error = 1;
                    $error_num++;
                    printf("%s  branch db of stats[%d] insert  fail! ad_group_id[%d] inter_id[%d] error_mess[%s]\n", $task_name,$db_id,$ad_group_id,$inter_id,$error_mess);
                }
            }
        }
        if ($have_error) {
            $content = sprintf("%s  insert db have  error,数量:[%d],原因:[%s]  ! 请尽快手动排查",
                $task_name,$error_num,$error_mess
            );
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
        }
        $endTime = date('Y-m-d H:i:s');
        printf("%s begin at %s, end at %s\n",
            $task_name, $beginTime, $endTime
        );
    }
    /**
     * 结算完毕之后倒入ad_stats_keyword_click统计表
     * @param date $date 结算日期
     * @author jingguangwen@360.cn  2014-09-05
     */
    public function actionImportStatsKeywordData ($date=null) {
        ini_set('memory_limit', '20480M');

        $task_name = sprintf("[ImportStatsKeywordData %s]", date('Y-m-d H:i:s'));
        $beginTime = date('Y-m-d H:i:s');
        printf("%s begin at %s\n", $task_name, $beginTime);
        $today = date('Y-m-d');
        if (is_null($date)) {
            $date = date('Y-m-d', strtotime('-1 day'));
        } else {
            $date = date('Y-m-d', strtotime($date));
        }
        if ($date==$today) {
            printf("%s today can not run\n", $task_name);
            return ;
        }
        // 获取ad_click_log库连接
        try {
            $clickLogDB = DbConnectionManager::getClickLogDB();
            if (false === $clickLogDB) {
                printf("%s get click_log  db fail\n", $task_name);
            }
        } catch (Exception $e) {
            printf("%s get dj click_log db fail, error [%s]!\n", $task_name, $e->getMessage());
            $clickLogDB = false;
        }
        if (!$clickLogDB) {
            $content = $task_name.'系统新统计无法获取点click_log库连接,请尽快手工结算。'.$date;
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            return ;
        }
        $statsDBSize = Yii::app()->params['db_stat_num'];
        $statsDBSize = intval($statsDBSize);

        if ($statsDBSize <= 0) {
            $content = $task_name.'新统计无法获取db_stat_num,请尽快手工结算。'.$date;
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            return ;
        }
        $statsDB = 'statsBranchDB';
        $statsTableName = "esc_stats_keyword_click_" . date('Ymd', strtotime($date));
        for ($i=1; $i <= $statsDBSize ; $i++) {
            // 获取分库连接
            ${$statsDB.'_'.$i} = DbConnectionManager::getStatBranchDB($i);
            if (!${$statsDB.'_'.$i}) {
                $content = sprintf("%s get branch db of stats[%d] fail! 此分库连接失败，请尽快手动处理",
                    $task_name, $i
                );
                printf("%s\n", $content);
                Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
                continue;
            }
        }

        $tableName = ComAdDetail::getTableName(strtotime($date));
        $sql = sprintf("select  count(1) as clicks,sum(price-reduce_price) as costs,ad_group_id,ad_plan_id,ad_user_id,keyword,style_id,create_date,source_type,cid,pid from $tableName where  create_date='%s' and status not in (-1,2) and deal_status=1 and ver='sou' and cheat_type not in (2,3) and price != reduce_price   group  by ad_group_id,keyword,source_type,style_id,cid,pid ",$date);
        $clickLogRows = $clickLogDB->createCommand($sql)->queryAll();
        $daoStats   = new EdcStatsKeyword();
        $have_error = 0;
        $error_num = 0;
        $error_mess = '';
        if (!empty($clickLogRows)) {
            $res = array();
            foreach($clickLogRows as $row) {
                $plat_type = 1;
                if($row['cid'] == 29 && $row['pid'] == 238) {
                    $plat_type = 3;
                }
                $key = $row['ad_group_id'].'_'.$row['keyword'].'_'.$row['source_type'].'_'.$row['style_id'].'_'.$plat_type;
                if(!isset($res[$key])) {
                    $res[$key] = array(
                        'clicks' => 0,
                        'views' => 0,
                        'costs' => 0,
                        'trans' => 0,
                        'ad_group_id' => $row['ad_group_id'],
                        'ad_plan_id' => $row['ad_plan_id'],
                        'ad_user_id' => $row['ad_user_id'],
                        'keyword' => !empty($row['keyword'])?$row['keyword']:'',
                        'style_id' => $row['style_id'],
                        'plat_type'=>$plat_type,
                        'create_date' => $row['create_date'],
                        'create_time' => date('Y-m-d H:i:s', time()),
                        'update_time' => date('Y-m-d H:i:s', time()),
                        'type' => 1,
                        'source_type' => intval($row['source_type']),
                    );
                }
                $res[$key]['clicks'] +=$row['clicks'];
                $res[$key]['costs'] +=$row['costs'];
            }
            foreach ($res as $clickLogArr) {
                $ad_user_id = $clickLogArr['ad_user_id'];
                $ad_group_id = $clickLogArr['ad_group_id'];
                $keyword = $clickLogArr['keyword'];
                //更新 子库统计表 按照用户id取模
                $db_id = $ad_user_id%$statsDBSize+1;
                // 获取分库连接
                $statsBranchDB = ${$statsDB.'_'.$db_id};
                if (!$statsBranchDB) {
                    continue;
                }
                $daoStats->setDB($statsBranchDB);
                //入库
//                $insert_arr = array(
//                    'clicks' => $clicks,
//                    'views' => 0,
//                    'costs' => $costs,
//                    'trans' => 0,
//                    'ad_group_id' => $ad_group_id,
//                    'ad_plan_id' => $ad_plan_id,
//                    'ad_user_id' => $ad_user_id,
//                    'keyword' => $keyword,
//                    'style_id' => $clickLogArr['style_id'],
//                    'plat_type'=>$plat_type,
//                    'create_date' => $create_date,
//                    'create_time' => date('Y-m-d H:i:s', time()),
//                    'update_time' => date('Y-m-d H:i:s', time()),
//                    'type' => 1,
//                    'source_type' => $source_type
//                );
                try {
                    $return_status = $daoStats->insertRow($statsTableName, $clickLogArr);
                } catch(Exception $e) {
                    $have_error = 1;
                    $return_status = 0;
                    $error_mess = $e->getMessage();
                }
                //存入失败
                if (!$return_status) {
                    $have_error = 1;
                    $error_num++;
                    printf("%s  branch db of stats[%d] insert  fail! ad_group_id[%d] keyword[%s] error_mess[%s]\n", $task_name,$db_id,$ad_group_id,$keyword,$error_mess);
                }
            }
        }
        if ($have_error) {
            $content = sprintf("%s  insert db have  error,数量:[%d],原因:[%s] ! 请尽快手动排查",
                $task_name,$error_num,$error_mess
            );
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
        }
        $endTime = date('Y-m-d H:i:s');
        printf("%s begin at %s, end at %s\n",
            $task_name, $beginTime, $endTime
        );
    }
    /**
     * 结算完毕之后倒入ad_stats_area_click与mv_stats_area_click统计表
     * @param int $dbNum 哪一个子库
     * @param date $date 结算日期
     * @author jingguangwen@360.cn  2014-09-05
     */
    public function actionImportStatsAreaData ($dbNum,$date=null) {
        ini_set('memory_limit', '20480M');

        $task_name = sprintf("[ImportStatsAreaData %s]", date('Y-m-d H:i:s'));
        $beginTime = date('Y-m-d H:i:s');
        printf("%s begin at %s\n", $task_name, $beginTime);
        $today = date('Y-m-d');
        if (is_null($date)) {
            $date = date('Y-m-d', strtotime('-1 day'));
        } else {
            $date = date('Y-m-d', strtotime($date));
        }
        if ($date==$today) {
            printf("%s today can not run\n", $task_name);
            return ;
        }
        // 获取ad_click_log库连接
        try {
            $clickLogDB = DbConnectionManager::getClickLogDB();
            if (false === $clickLogDB) {
                printf("%s get click_log  db fail\n", $task_name);
            }
        } catch (Exception $e) {
            printf("%s get dj click_log db fail, error [%s]!\n", $task_name, $e->getMessage());
            $clickLogDB = false;
        }
        if (!$clickLogDB) {
            $content = $task_name.'系统新统计无法获取点click_log库连接,请尽快手工结算。'.$date;
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            return ;
        }
        $daoStatsArea   = new EdcStatsArea();
        $statsTableName         = "esc_stats_click_" . date('Ymd', strtotime($date));
        $statsAreaTableName     = "esc_stats_area_click_" . date('Ymd', strtotime($date));
        $statsDBSize = Yii::app()->params['db_stat_num'];
        $statsDBSize = intval($statsDBSize);

        if ($statsDBSize <= 0) {
            $content = $task_name.'新统计无法获取db_stat_num,请尽快手工结算。'.$date;
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            return ;
        }
        $have_error = 0;
        $error_num = 0;
        $error_mess = '';
        $dbNum=intval($dbNum);
        if ($dbNum<=0 || $dbNum>$statsDBSize) {
            $content = $task_name.'子库'.$dbNum.'传递错误,请尽快手工结算。'.$date;
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            return ;
        }
        // 获取分库连接
        $statsBranchDB = DbConnectionManager::getStatBranchDB($dbNum);
        if (!$statsBranchDB) {
            $content = sprintf("%s get branch db of stats[%d] fail! 此分库未能校验，请尽快手动处理",
                $task_name, $dbNum
            );
            printf("%s\n", $content);
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            continue;
        }
        //查询用户id
        $stats_sql = sprintf("select  ad_user_id from %s where  create_date='%s'  group  by ad_user_id ",$statsTableName,$date);
        $user_ids_arr = $statsBranchDB->createCommand($stats_sql)->queryColumn();
        $daoStatsArea->setDB($statsBranchDB);
        $result_arr = $app_result_arr = $app_statistic_arr = array();
        if (!empty($user_ids_arr)) {
            //按照1000分组
            $user_ids_arr_num = count($user_ids_arr);
            //如果用户id太多，则拆分，按照每组1000个用户id分多次查询
            if ($user_ids_arr_num>1000) {
                $slice_num = ceil($user_ids_arr_num/1000);
                for ($n =1; $n <= $slice_num; $n++) {
                    $offset = ($n-1)*1000;
                    $length = 1000;
                    $user_ids_arr_new = array_slice($user_ids_arr, $offset,$length);
                    $user_ids_str = 0;
                    if (!empty($user_ids_arr_new)) {
                        $user_ids_arr_new = array_map('intval', $user_ids_arr_new);
                        $user_ids_str = implode(',', $user_ids_arr_new);
                    }
                    $tableName = ComAdDetail::getTableName(strtotime($date));
                    $sql = sprintf("select  * from $tableName where  create_date='%s' and status not in (-1,2) and deal_status=1 and ad_user_id in (%s)  and ver !='mediav' and ver !='shouzhu'  and cheat_type not in (2,3) and price != reduce_price  ",$date,$user_ids_str);
                    $clickLogRows = $clickLogDB->createCommand($sql)->queryAll();
                    $daoStatsArea->getClikResult($clickLogRows, $result_arr, $app_result_arr,$app_statistic_arr);
                }

            } else {
                $user_ids_str = 0;
                if (!empty($user_ids_arr)) {
                    $user_ids_arr = array_map('intval', $user_ids_arr);
                    $user_ids_str = implode(',', $user_ids_arr);
                    $tableName = ComAdDetail::getTableName(strtotime($date));
                    $sql = sprintf("select  * from $tableName where  create_date='%s' and status not in (-1,2) and deal_status=1 and  ad_user_id in (%s)  and ver !='mediav' and ver !='shouzhu' and cheat_type not in (2,3) and price != reduce_price  ",$date,$user_ids_str);
                    $clickLogRows = $clickLogDB->createCommand($sql)->queryAll();
                    $daoStatsArea->getClikResult($clickLogRows, $result_arr, $app_result_arr,$app_statistic_arr);
                }

            }
            //入库
            if (!empty($result_arr)) {
                foreach ($result_arr as $arr) {


                    $area_id = $arr['area_id'];
                    $area_fid = $arr['area_fid'];
                    $ad_group_id = $arr['ad_group_id'];
                    $source_type = $arr['source_type'];

                    //入库
                    $insert_arr = array(
                        'area_id' => intval($area_id),
                        'area_fid' => intval($area_fid),
                        'ad_group_id' => $ad_group_id,
                        'area_type' => $arr['area_type'],
                        'ad_plan_id' => $arr['ad_plan_id'],
                        'ad_user_id' => $arr['ad_user_id'],
                        'clicks' => $arr['clicks'],
                        'plat_type'=>$arr['plat_type'],
                        'views' => 0,
                        'costs' => $arr['costs'],
                        'trans' => 0,
                        'create_date' => $date,
                        'area_key' => $arr['area_key'],
                        'create_time' => date('Y-m-d H:i:s', time()),
                        'update_time' => date('Y-m-d H:i:s', time()),
                        'type' => 1,
                        'source_type' => intval($source_type)
                    );
                    try {
                        $return_status = $daoStatsArea->insertRow($statsAreaTableName, $insert_arr);
                    } catch(Exception $e) {
                        $have_error = 1;
                        $return_status = 0;
                        $error_mess = $e->getMessage();
                    }
                    //存入失败
                    if (!$return_status) {
                        $have_error = 1;
                        $error_num++;
                        printf("%s  branch db of stats_area[%d] insert  fail! ad_group_id[%d] area_id[%d] area_fid[%d] error_mess[%s]\n", $task_name,$dbNum,$ad_group_id,$area_id,$area_fid,$error_mess);
                    }
                }
            }

        }



        if ($have_error) {
            $content = sprintf("%s  insert db have  error,数量:[%d],原因:[%s] ! 请尽快手动排查",
                $task_name,$error_num,$error_mess
            );
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
        }
        $endTime = date('Y-m-d H:i:s');
        printf("%s begin at %s, end at %s\n",
            $task_name, $beginTime, $endTime
        );
    }
    /**
     * 结算完毕之后倒入ad_stats_neighbor_click统计表
     * @param date $date 结算日期
     * @author jingguangwen@360.cn  2014-09-05
     */
    public function actionImportStatsNeighborData ($date=null) {
        ini_set('memory_limit', '20480M');

        $task_name = sprintf("[ImportStatsNeighborData %s]", date('Y-m-d H:i:s'));
        $beginTime = date('Y-m-d H:i:s');
        printf("%s begin at %s\n", $task_name, $beginTime);
        $today = date('Y-m-d');
        if (is_null($date)) {
            $date = date('Y-m-d', strtotime('-1 day'));
        } else {
            $date = date('Y-m-d', strtotime($date));
        }
        if ($date==$today) {
            printf("%s today can not run\n", $task_name);
            return ;
        }
        // 获取ad_click_log库连接
        try {
            $clickLogDB = DbConnectionManager::getClickLogDB();
            if (false === $clickLogDB) {
                printf("%s get click_log  db fail\n", $task_name);
            }
        } catch (Exception $e) {
            printf("%s get dj click_log db fail, error [%s]!\n", $task_name, $e->getMessage());
            $clickLogDB = false;
        }
        if (!$clickLogDB) {
            $content = $task_name.'系统新统计无法获取点click_log库连接,请尽快手工结算。'.$date;
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            return ;
        }
        $statsDBSize = Yii::app()->params['db_stat_num'];
        $statsDBSize = intval($statsDBSize);

        if ($statsDBSize <= 0) {
            $content = $task_name.'新统计无法获取db_stat_num,请尽快手工结算。'.$date;
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            return ;
        }
        $statsDB = 'statsBranchDB';
        $statsTableName = "esc_stats_neighbor_click_" . date('Ymd', strtotime($date));
        for ($i=1; $i <= $statsDBSize ; $i++) {
            // 获取分库连接
            ${$statsDB.'_'.$i} = DbConnectionManager::getStatBranchDB($i);
            if (!${$statsDB.'_'.$i}) {
                $content = sprintf("%s get branch db of stats[%d] fail! 此分库连接失败，请尽快手动处理",
                    $task_name, $i
                );
                printf("%s\n", $content);
                Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
                continue;
            }
        }

        $tableName = ComAdDetail::getTableName(strtotime($date));
        $sql = sprintf("select  count(1) as clicks,sum(price-reduce_price) as costs,ad_group_id,ad_plan_id,ad_user_id,keyword,create_date from $tableName where  create_date='%s' and status not in (-1,2) and deal_status=1 and ver='sou' and cid=32 and pid=162  and cheat_type not in (2,3) and price != reduce_price  group  by ad_group_id,keyword ",$date);
        $clickLogRows = $clickLogDB->createCommand($sql)->queryAll();
        $daoStats   = new EdcStatsNeighbor();
        $have_error = 0;
        $error_num = 0;
        $error_mess = '';
        if (!empty($clickLogRows)) {
            foreach ($clickLogRows as $clickLogArr) {
                $ad_user_id = $clickLogArr['ad_user_id'];
                $clicks = $clickLogArr['clicks'];
                $costs = $clickLogArr['costs'];
                $ad_group_id = $clickLogArr['ad_group_id'];
                $ad_plan_id = $clickLogArr['ad_plan_id'];
                $keyword = $clickLogArr['keyword'];
                $create_date = $clickLogArr['create_date'];
                //更新 子库统计表 按照用户id取模
                $db_id = $ad_user_id%$statsDBSize+1;
                // 获取分库连接
                $statsBranchDB = ${$statsDB.'_'.$db_id};
                if (!$statsBranchDB) {
                    continue;
                }
                $daoStats->setDB($statsBranchDB);
                //入库
                $insert_arr = array(
                    'clicks' => $clicks,
                    'views' => 0,
                    'costs' => $costs,
                    'trans' => 0,
                    'ad_group_id' => $ad_group_id,
                    'ad_plan_id' => $ad_plan_id,
                    'ad_user_id' => $ad_user_id,
                    'keyword' => $keyword,
                    'create_date' => $create_date,
                    'create_time' => date('Y-m-d H:i:s', time()),
                    'update_time' => date('Y-m-d H:i:s', time()),
                    'type' => 1
                );
                $return_status = $daoStats->insertRow($statsTableName, $insert_arr);
                //存入失败
                if (!$return_status) {
                    $have_error = 1;
                    printf("%s  branch db of stats[%d] insert  fail! ad_group_id[%d] keyword[%d]\n", $task_name,$db_id,$ad_group_id,$keyword);
                }
            }
        }
        if ($have_error) {
            $content = sprintf("%s  insert db have  error ! 请尽快手动排查",
                $task_name
            );
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
        }
        $endTime = date('Y-m-d H:i:s');
        printf("%s begin at %s, end at %s\n",
            $task_name, $beginTime, $endTime
        );
    }
    /**
     * 结算完毕之后倒入ad_stats_biyi_click统计表
     * @param date $date 结算日期
     * @author jingguangwen@360.cn  2014-09-05
     */
    public function actionImportStatsBiyiData ($date=null) {
        ini_set('memory_limit', '20480M');

        $task_name = sprintf("[ImportStatsBiyiData %s]", date('Y-m-d H:i:s'));
        $beginTime = date('Y-m-d H:i:s');
        printf("%s begin at %s\n", $task_name, $beginTime);
        $today = date('Y-m-d');
        if (is_null($date)) {
            $date = date('Y-m-d', strtotime('-1 day'));
        } else {
            $date = date('Y-m-d', strtotime($date));
        }
        if ($date==$today) {
            printf("%s today can not run\n", $task_name);
            return ;
        }
        // 获取ad_click_log库连接
        try {
            $clickLogDB = DbConnectionManager::getClickLogDB();
            if (false === $clickLogDB) {
                printf("%s get click_log  db fail\n", $task_name);
            }
        } catch (Exception $e) {
            printf("%s get dj click_log db fail, error [%s]!\n", $task_name, $e->getMessage());
            $clickLogDB = false;
        }
        if (!$clickLogDB) {
            $content = $task_name.'系统新统计无法获取点click_log库连接,请尽快手工结算。'.$date;
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            return ;
        }
        $statsDBSize = Yii::app()->params['db_stat_num'];
        $statsDBSize = intval($statsDBSize);

        if ($statsDBSize <= 0) {
            $content = $task_name.'新统计无法获取db_stat_num,请尽快手工结算。'.$date;
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            return ;
        }
        $statsDB = 'statsBranchDB';
        $statsTableName = "esc_stats_biyi_click_" . date('Ymd', strtotime($date));
        for ($i=1; $i <= $statsDBSize ; $i++) {
            // 获取分库连接
            ${$statsDB.'_'.$i} = DbConnectionManager::getStatBranchDB($i);
            if (!${$statsDB.'_'.$i}) {
                $content = sprintf("%s get branch db of stats[%d] fail! 此分库连接失败，请尽快手动处理",
                    $task_name, $i
                );
                printf("%s\n", $content);
                Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
                continue;
            }
        }

        $tableName = ComAdDetail::getTableName(strtotime($date));
        $sql = sprintf("select  count(1) as clicks,sum(price-reduce_price) as costs,ad_group_id,ad_plan_id,ad_user_id,sub_ad_info,create_date,source_type from $tableName where  create_date='%s' and status not in (-1,2) and deal_status=1 and ((ver='sou' and sub_ver='biyi') or (ver='guess' and sub_ver='stream')) and cheat_type not in (2,3) and price != reduce_price  group  by ad_group_id,sub_ad_info,source_type ",$date);
        $clickLogRows = $clickLogDB->createCommand($sql)->queryAll();
        $daoStats   = new EdcStatsBiYi();
        $have_error = 0;
        $error_num = 0;
        $error_mess = '';
        if (!empty($clickLogRows)) {
            foreach ($clickLogRows as $clickLogArr) {
                $ad_user_id = $clickLogArr['ad_user_id'];
                $clicks = $clickLogArr['clicks'];
                $costs = $clickLogArr['costs'];
                $ad_group_id = $clickLogArr['ad_group_id'];
                $ad_plan_id = $clickLogArr['ad_plan_id'];
                //$sub_id = $clickLogArr['sub_data'];
                $create_date = $clickLogArr['create_date'];
                $source_type = intval($clickLogArr['source_type']);
                //$sub_ad_type = $clickLogArr['sub_ad_type'];
                //解析sub_ad_info
                $sub_ad_info =$clickLogArr['sub_ad_info'];
                $sub_id = 0;
                $sub_ad_type = 0;
                if(!empty($sub_ad_info)){
                    $sub_ad_info_arr = json_decode($sub_ad_info,true);

                    if (is_array($sub_ad_info_arr) && !empty($sub_ad_info_arr)) {
                        foreach ($sub_ad_info_arr as $arr) {
                            $sub_id = $arr['id'];
                            $sub_ad_type = $arr['type'];
                        }
                    }
                }
                //更新 子库统计表 按照用户id取模
                $db_id = $ad_user_id%$statsDBSize+1;
                // 获取分库连接
                $statsBranchDB = ${$statsDB.'_'.$db_id};
                if (!$statsBranchDB) {
                    continue;
                }

                if(empty($sub_id)){
                    $sub_id = 0;
                }
                if(empty($sub_ad_type)){
                    $sub_ad_type = 0;
                }


                $daoStats->setDB($statsBranchDB);
                //入库
                $insert_arr = array(
                    'clicks' => $clicks,
                    'views' => 0,
                    'costs' => $costs,
                    'trans' => 0,
                    'ad_group_id' => $ad_group_id,
                    'ad_plan_id' => $ad_plan_id,
                    'ad_user_id' => $ad_user_id,
                    'sub_id' => $sub_id,
                    'create_date' => $create_date,
                    'create_time' => date('Y-m-d H:i:s', time()),
                    'update_time' => date('Y-m-d H:i:s', time()),
                    'type' => 1,
                    'source_type' => $source_type,
                    'sub_ad_type' => $sub_ad_type
                );
                try {
                    $return_status = $daoStats->insertRow($statsTableName, $insert_arr);
                 } catch (Exception $e) {
                    $have_error = 1;
                    $error_mess = $e->getMessage();
                    $return_status = 0;
                    printf("%s  branch db of stats[%d] insert  fail exception! ad_group_id[%d] sub_id[%d] error_mess[%s]\n", $task_name,$db_id,$ad_group_id,$sub_id,$error_mess);
                }
                //存入失败
                if (!$return_status) {
                    $have_error = 1;
                    $error_num++;
                    printf("%s  branch db of stats[%d] insert  fail! ad_group_id[%d] sub_id[%d] error_mess[%s]\n", $task_name,$db_id,$ad_group_id,$sub_id,$error_mess);
                }
            }
        }
        if ($have_error) {
            $content = sprintf("%s  insert db have  error,数量:[%d],原因:[%s] ! 请尽快手动排查",
                $task_name,$error_num,$error_mess
            );
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
        }
        $endTime = date('Y-m-d H:i:s');
        printf("%s begin at %s, end at %s\n",
            $task_name, $beginTime, $endTime
        );
    }
    /**
     * 结算完毕之后倒入mv_stats_xxxx统计表
     * @param date $date 结算日期
     * @author jingguangwen@360.cn  2014-09-05
     */
    public function actionUpdateStatsMvData ($date=null) {
        ini_set('memory_limit', '20480M');

        $task_name = sprintf("[ImportStatsMvData %s]", date('Y-m-d H:i:s'));
        $beginTime = date('Y-m-d H:i:s');
        printf("%s begin at %s\n", $task_name, $beginTime);
        $today = date('Y-m-d');
        if (is_null($date)) {
            $date = date('Y-m-d', strtotime('-1 day'));
        } else {
            $date = date('Y-m-d', strtotime($date));
        }
        if ($date==$today) {
            printf("%s today can not run\n", $task_name);
            return ;
        }
        // 获取ad_click_log库连接
        try {
            $clickLogDB = DbConnectionManager::getClickLogDB();
            if (false === $clickLogDB) {
                printf("%s get click_log  db fail\n", $task_name);
            }
        } catch (Exception $e) {
            printf("%s get dj click_log db fail, error [%s]!\n", $task_name, $e->getMessage());
            $clickLogDB = false;
        }
        if (!$clickLogDB) {
            $content = $task_name.'系统新统计无法获取点click_log库连接,请尽快手工结算。'.$date;
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            return ;
        }
        $statsDBSize = Yii::app()->params['db_stat_num'];
        $statsDBSize = intval($statsDBSize);

        if ($statsDBSize <= 0) {
            $content = $task_name.'新统计无法获取db_stat_num,请尽快手工结算。'.$date;
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            return ;
        }
        $statsDB = 'statsBranchDB';
        $statsTableName = "mv_stats_" . date('Ymd', strtotime($date));
        for ($i=1; $i <= $statsDBSize ; $i++) {
            // 获取分库连接
            ${$statsDB.'_'.$i} = DbConnectionManager::getStatBranchDB($i);
            if (!${$statsDB.'_'.$i}) {
                $content = sprintf("%s get branch db of stats[%d] fail! 此分库连接失败，请尽快手动处理",
                    $task_name, $i
                );
                printf("%s\n", $content);
                Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
                continue;
            }
        }

        $tableName = ComAdDetail::getTableName(strtotime($date));
        $sql = sprintf("select  count(1) as clicks,sum(price-reduce_price) as costs,ad_user_id,source_type,pid,create_date from $tableName where  create_date='%s' and status not in (-1,2) and deal_status=1 and ver='mediav' and cheat_type not in (2,3) and price != reduce_price  group  by ad_user_id,source_type,pid ",$date);
        $clickLogRows = $clickLogDB->createCommand($sql)->queryAll();
        $have_error = 0;
        $error_num = 0;
        $error_mess = '';
        if (!empty($clickLogRows)) {


            foreach ($clickLogRows as $clickLogArr) {
                $ad_user_id = $clickLogArr['ad_user_id'];
                $device_type = $clickLogArr['source_type'];
                $pid = $clickLogArr['pid'];
                $clicks = $clickLogArr['clicks'];
                $costs = $clickLogArr['costs'];
                $create_date = $clickLogArr['create_date'];

                //更新 子库统计表 按照用户id取模
                $db_id = $ad_user_id%$statsDBSize+1;
                // 获取分库连接
                $statsBranchDB = ${$statsDB.'_'.$db_id};
                if (!$statsBranchDB) {
                    continue;
                }
                $mv_stats_update_sql = sprintf("update %s set end_costs=%s,clicks=%d,update_time='%s' where ad_user_id =%s AND device_type=%s AND pid=%s ",$statsTableName,$costs,$clicks,date('Y-m-d H:i:s', time()),$ad_user_id, $device_type, $pid);

                try {
                    $mv_stats_update_return = $statsBranchDB->createCommand($mv_stats_update_sql)->execute();
                    //无数据更新 则插入
                    if (!$mv_stats_update_return) {

                        $insert_arr = array(
                            'clicks' => $clicks,
                            'views' => 0,
                            'costs' => $costs,
                            'end_costs' => $costs,
                            'ad_user_id' => $ad_user_id,
                            'device_type' => $device_type,
                            'pid' => $pid,
                            'create_time' => date('Y-m-d H:i:s', time()),
                            'update_time' => date('Y-m-d H:i:s', time()),
                        );

                        $mv_stats_update_return = $statsBranchDB->createCommand()->insert($statsTableName, $insert_arr);
                        printf("%s  branch db of stats[%d] reinsert! ad_user_id[%d]  reinsert\n", $task_name,$db_id,$ad_user_id);

                    }
                 } catch (Exception $e) {
                    $have_error = 1;
                    $mv_stats_update_return = 0;
                    $error_mess = $e->getMessage();
                    printf("%s  branch db of stats[%d] insert  fail exception! ad_user_id[%d]  error_mess[%s]\n", $task_name,$db_id,$ad_user_id,$error_mess);
                }

                //更新失败
                if (!$mv_stats_update_return) {
                    $have_error = 1;
                    $error_num++;
                    printf("%s  branch db of stats[%d] update  fail! ad_user_id[%d] error_mess[%s] \n", $task_name,$db_id,$ad_user_id,$error_mess);
                }
            }
        }
        if ($have_error) {
            $content = sprintf("%s  update db have  error,数量:[%d] ! 请尽快手动排查",
                $task_name,$error_num
            );
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
        }
        $endTime = date('Y-m-d H:i:s');
        printf("%s begin at %s, end at %s\n",
        $task_name, $beginTime, $endTime
        );
    }

    public function actionImportClickLogByMv ($date=null) {
        ini_set('memory_limit', '20480M');

        $task_name = sprintf("[ImportClickLogByMv %s]", date('Y-m-d H:i:s'));
        $beginTime = date('Y-m-d H:i:s');
        printf("%s begin at %s\n", $task_name, $beginTime);
        $today = date('Y-m-d');
        if (is_null($date)) {
            $date = date('Y-m-d', strtotime('-1 day'));
        } else {
            $date = date('Y-m-d', strtotime($date));
        }
        if ($date==$today) {
            printf("%s today can not run\n", $task_name);
            return ;
        }
        // 获取ad_click_log库连接
        try {
            $clickLogDB = DbConnectionManager::getClickLogDB();
            if (false === $clickLogDB) {
                printf("%s get click_log  db fail\n", $task_name);
            }
        } catch (Exception $e) {
            printf("%s get dj click_log db fail, error [%s]!\n", $task_name, $e->getMessage());
            $clickLogDB = false;
        }
        if (!$clickLogDB) {
            $content = $task_name.'系统统计mv入库无法获取点click_log库连接,请尽快手工结算。'.$date;
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            return ;
        }
        $mvClickLogTableName = "mv_click_log_" . date('Ymd', strtotime($date));
        $adClickLogTableName = "ad_click_log";
        $daoAdClickLog        = new AdClickLog();
        $daoMvClickLog        = new MvClickLog();
        $daoAdClickLog->setDB($clickLogDB);
        $daoMvClickLog->setDB($clickLogDB);
        $have_error = 0;
        $sql = sprintf("select  max(id) as max_id,ad_user_id,sum(real_price) as costs,create_time from %s where   deal_status=0  group  by ad_user_id ",$mvClickLogTableName);
        $mvClickLogRows = $clickLogDB->createCommand($sql)->queryAll();
        if (!empty($mvClickLogRows)) {
            try {
                foreach ($mvClickLogRows as $clickLogArr) {
                    $mv_id = $clickLogArr['max_id'];
                    $ad_user_id = $clickLogArr['ad_user_id'];
                    $costs = $clickLogArr['costs'];
                    $createTime = $clickLogArr['create_time'];
                    //入库
                    $insert_arr = array(
                        'click_id'      => 'mediav-'.$mv_id,
                        'ad_user_id'    => $ad_user_id,
                        'ad_advert_id'  => 0,
                        'ad_group_id'   => 0,
                        'ad_plan_id'    => 0,
                        'area_key'      => '',
                        'create_time'   => date('Y-m-d H:i:s', strtotime($createTime)),
                        'price'         => $costs,
                        'create_date'   => date('Y-m-d', strtotime($createTime)),
                        'ad_channel_id' => 49,
                        'ad_place_id'   => 199,
                        'ver'           => 'mediav',
                        'save_time'     => time(),
                    );

                    $insert_return_status = $daoAdClickLog->insertRow($adClickLogTableName, $insert_arr);
                    $update_return_status = $daoMvClickLog->setSettled($ad_user_id, $date, $mv_id);
                    //存入失败
                    if (!$insert_return_status && !$update_return_status) {
                        $have_error = 1;
                        printf("%s  insert  or update   fail! ad_user_id[%d]\n", $task_name,$ad_user_id);
                    }
                    //更新数据

                }
            } catch (Exception $e) {
                printf ("%s update fail, msg[%s]", $task_name, $e->getMessage());
                $fail++;
                continue;
            }
        }
        if ($have_error) {
            $content = sprintf("%s  insert db have  error ! 请尽快手动排查",
                $task_name
            );
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
        }
        $endTime = date('Y-m-d H:i:s');
        printf("%s begin at %s, end at %s\n",
            $task_name, $beginTime, $endTime
        );


    }
    public function actionUpdateUserCache ($date=null) {

        $task_name = sprintf('[UpdateUserCach %s]', date('Y-m-d H:i:s'));
        $beginTime = date('Y-m-d H:i:s');
        printf("%s begin\n", $task_name);

        if (is_null($date)) {
            $yesterday = date('Y-m-d', strtotime('-1 day'));
        } else {
            $yesterday = $date;
        }
        // 获取中心库
        try {
            $centerDB = DbConnectionManager::getDjCenterDB();
            if (false === $centerDB) {
                printf("%s get dj center db fail\n", $task_name);
            }
        } catch (Exception $e) {
            printf("%s get dj center db fail, error [%s]!\n", $task_name, $e->getMessage());
            $centerDB = false;
        }
        if (!$centerDB) {
            $content = 'esc更新用户缓存无法获取点睛中心库连接。请尽快手工结算。'.$yesterday;
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            return ;
        }
        $daoUserCL          = new UserChargeLog();
        $daoUserCL->setDB($centerDB);
        $user_cost_arrs = $daoUserCL->getUserCharge($yesterday);
        if(!empty($user_cost_arrs)){
            $user_num = 0;
            foreach ($user_cost_arrs as $user_cost_arr) {
                $ad_user_id = $user_cost_arr['ad_user_id'];
                //更新cache
                $res = Utility::apiPost('tool/updateRedisKey', array('key' => 'eapi_user_detail_' . $ad_user_id));
                $user_num++;
            }
        }
        $endTime = date('Y-m-d H:i:s');
        printf("%s begin at %s end at %s, process %d user\n",
            $task_name, $beginTime, $endTime, $user_num
        );
    }
    /**
     * 定时更新账户余额到引擎
     * @author jingguangwen@360.cn
     */
    public function actionReflushUserBalance()
    {
        $user_sql = "SELECT id,balance  from  ad_user where  balance>0  and  status  in  (1,2) ";
        $ad_user_arrs = Yii::app()->db_center->createCommand($user_sql)->queryAll();
        if(!empty($ad_user_arrs)){
            $cnn = Yii::app()->db_quota;
            foreach ($ad_user_arrs as $user_info) {
                $ad_user_id = $userID = $user_info['id'];

                //查询账户余额
                $table_name = 'ad_user_quota_'.$ad_user_id%10;
                $sql = sprintf("select  *  from  %s where ad_user_id=%d ", $table_name,$ad_user_id);
                $user_quota_arr = $cnn->createCommand($sql)->queryRow();

                if(!empty($user_quota_arr)){
                    $userCost = $user_quota_arr['dj_cost'] + $user_quota_arr['mv_cost']  + $user_quota_arr['yesterday_dj_cost']  + $user_quota_arr['yesterday_mv_cost'];
                    $budgetUserBalance = $user_quota_arr['balance']-$userCost;
                if ($budgetUserBalance<0) {
                    $budgetUserBalance = 0;
                }
                ComBudgetData::sendUserBalance($userID, $budgetUserBalance);
                echo $userID."\t".$budgetUserBalance."\n";
            }

            }
        }
    }

    /**
     * ssp统计表结算
     * @param date $date    结算日期
     */
    public function actionImportShouZhuSspData($date = null)
    {
        ini_set('memory_limit', '20480M');
        $beginTime = date('Y-m-d H:i:s');
        $task_name = sprintf("[%s %s]", __FUNCTION__, $beginTime);
        printf("%s begin at %s\n", $task_name, $beginTime);

        //生成统计日期
        if (is_null($date)) {
            $date = date('Y-m-d', strtotime('-1 day'));
        } else {
            $date = date('Y-m-d', strtotime($date));
        }
        $today = date('Y-m-d');
        if ($date==$today) {
            printf("%s today can not run\n", $task_name);
            return ;
        }

        // 获取ad_click_log库连接
        try {
            $clickLogDB = DbConnectionManager::getClickLogDB();
            if (false === $clickLogDB) {
                printf("%s get click_log  db fail\n", $task_name);
            }
        } catch (Exception $e) {
            printf("%s get dj click_log db fail, error [%s]!\n", $task_name, $e->getMessage());
            $clickLogDB = false;
        }
        if (!$clickLogDB) {
            $content = $task_name.'系统统计无法获取点click_log库连接,请尽快手工结算。'.$date;
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            return ;
        }

        $have_error = 0;
        $error_num = 0;
        $error_mess = '';

        $statsDBSize = Yii::app()->params['db_stat_num'];
        $statsDBSize = intval($statsDBSize);
        if ($statsDBSize <= 0) {
            $content = $task_name.'统计无法获取db_stat_num,请尽快手工结算。'.$date;
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            return ;
        }
        $statsDB = 'statsBranchDB';
        for ($i=1; $i <= $statsDBSize ; $i++) {
            // 获取分库连接
            ${$statsDB.'_'.$i} = DbConnectionManager::getStatBranchDB($i);
            if (!${$statsDB.'_'.$i}) {
                $content = sprintf("%s get branch db of stats[%d] fail! 此分库连接失败，请尽快手动处理",
                    $task_name, $i
                );
                printf("%s\n", $content);
                Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
                continue;
            }
        }
        //查询用户id
        $tableName = ComAdDetail::getTableName(strtotime($date));
        $sql = sprintf("select distinct ad_user_id from $tableName where  create_date='%s' and status not in (-1,2) and deal_status=1 and ver ='shouzhu'  and cheat_type not in (2,3) and price != reduce_price and source_system = 5 ",$date);
        $user_ids_arr = $clickLogDB->createCommand($sql)->queryColumn();

        if (!empty($user_ids_arr))
        {
            /*
             * 取log
             */
            $clickLog = array();
            //按照1000分组
            $user_ids_arr_num = count($user_ids_arr);
            //如果用户id太多，则拆分，按照每组1000个用户id分多次查询
            $sliceSize = 1000;
            if($user_ids_arr_num > $sliceSize)
            {
                $slice_num = ceil($user_ids_arr_num / $sliceSize);
                for($n = 1; $n <= $slice_num; $n++)
                {
                    $offset = ($n - 1) * $sliceSize;
                    $length = $sliceSize;
                    $user_ids_arr_new = array_slice($user_ids_arr, $offset, $length);
                    $user_ids_str = 0;
                    if(!empty($user_ids_arr_new))
                    {
                        $user_ids_arr_new = array_map('intval', $user_ids_arr_new);
                        $user_ids_str = implode(',', $user_ids_arr_new);
                    }
                    $sql = sprintf("select  location,sum(price) price,sum(reduce_price) reduce_price,count(*) num from $tableName where  create_date='%s' and status not in (-1,2) and deal_status=1 and ad_user_id in (%s)  and ver ='shouzhu'  and cheat_type not in (2,3) and price != reduce_price and source_system = 5  group by location", $date, $user_ids_str);
                    $clickLogRows = $clickLogDB->createCommand($sql)
                                               ->queryAll();
                    $clickLog = array_merge($clickLogRows, $clickLog);
                }
            }
            else
            {
                $user_ids_arr = array_map('intval', $user_ids_arr);
                $user_ids_str = implode(',', $user_ids_arr);
                $tableName = ComAdDetail::getTableName(strtotime($date));
                $sql = sprintf("select  location,sum(price) price,sum(reduce_price) reduce_price,count(*) num from $tableName where  create_date='%s' and status not in (-1,2) and deal_status=1 and  ad_user_id in (%s)  and ver ='shouzhu' and cheat_type not in (2,3) and price != reduce_price and source_system = 5  group by location", $date, $user_ids_str);
                $clickLog = $clickLogDB->createCommand($sql)
                                               ->queryAll();
            }

            /*
             * 合并
             */
            $statisticLog = array();
            foreach($clickLog as $row) {
                if(!array_key_exists($row['location'], $statisticLog)) {
                    $statisticLog[$row['location']] = array(
                        'costs'=>0,
                        'clicks'=>0,
                    );
                }
                $statisticLog[$row['location']]['costs'] += round($row['price']-$row['reduce_price'],2);
                $statisticLog[$row['location']]['clicks'] += $row['num'];
            }

            /*
             * 入库
             */
            $sspTableName     = "ad_ssp_" . date('Ymd', strtotime($date));
            foreach($statisticLog as $k => $v) {
                //更新 子库统计表 按照用户id取模
                $db_id = hexdec(substr(md5($k),0,8))%$statsDBSize+1;
                // 获取分库连接
                $statsBranchDB = ${$statsDB.'_'.$db_id};
                if (!$statsBranchDB) {
                    continue;
                }
                ////更新db数据
                $sql = "update  $sspTableName set real_costs= {$v['costs']}, clicks= {$v['clicks']}, update_time='{$beginTime}' where ad_place_key='{$k}'";
                $cmd = $statsBranchDB->createCommand($sql);
//                $cmd->bindParam(':costs', $v['costs'], PDO::PARAM_INT);

                try {
                    $return_status = $cmd->execute();
                } catch(Exception $e) {
                    $have_error = 1;
                    $return_status = 0;
                    $error_mess = $e->getMessage();
                }

                //更新失败
                if (!$return_status) {
                    $have_error = 1;
                    $error_num++;
                    printf("%s  branch db of ad_ssp[%d] update  fail! ad_place_key[%s] costs[%d] clicks[%d] error_mess[%s]\n", $task_name,$db_id,$k,$v['costs'],$v['clicks'],$error_mess);
                }
            }
        }
        if ($have_error) {
            $content = sprintf("%s  insert db have  error,数量:[%d],原因:[%s] ! 请尽快手动排查",
                $task_name,$error_num,$error_mess
            );
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
        }
        $endTime = date('Y-m-d H:i:s');
        printf("%s begin at %s, end at %s\n",
            $task_name, $beginTime, $endTime
        );
        return;
    }

    /**
     * 结算完毕之后倒入ad_stats_area_click与mv_stats_area_click统计表
     * @param date $date 结算日期
     * @author jingguangwen@360.cn  2015-12-03
     */
    public function actionImportShouZhuData ($date=null) {
        ini_set('memory_limit', '20480M');

        $task_name = sprintf("[ImportShouZhuData %s]", date('Y-m-d H:i:s'));
        $beginTime = date('Y-m-d H:i:s');
        printf("%s begin at %s\n", $task_name, $beginTime);
        $today = date('Y-m-d');
        if (is_null($date)) {
            $date = date('Y-m-d', strtotime('-1 day'));
        } else {
            $date = date('Y-m-d', strtotime($date));
        }
        if ($date==$today) {
            printf("%s today can not run\n", $task_name);
            return ;
        }
        // 获取ad_click_log库连接
        try {
            $clickLogDB = DbConnectionManager::getClickLogDB();
            if (false === $clickLogDB) {
                printf("%s get click_log  db fail\n", $task_name);
            }
        } catch (Exception $e) {
            printf("%s get dj click_log db fail, error [%s]!\n", $task_name, $e->getMessage());
            $clickLogDB = false;
        }
        if (!$clickLogDB) {
            $content = $task_name.'系统新统计无法获取点click_log库连接,请尽快手工结算。'.$date;
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            return ;
        }
        $appTableName     = "ad_app_" . date('Ymd', strtotime($date));
        $appStatiisticTableName     = "ad_app_statistic_" . date('Ymd', strtotime($date));

        $have_error = 0;
        $error_num = 0;
        $error_mess = '';

        $statsDBSize = Yii::app()->params['db_stat_num'];
        $statsDBSize = intval($statsDBSize);

        if ($statsDBSize <= 0) {
            $content = $task_name.'新统计无法获取db_stat_num,请尽快手工结算。'.$date;
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            return ;
        }
        $statsDB = 'statsBranchDB';
        for ($i=1; $i <= $statsDBSize ; $i++) {
            // 获取分库连接
            ${$statsDB.'_'.$i} = DbConnectionManager::getStatBranchDB($i);
            if (!${$statsDB.'_'.$i}) {
                $content = sprintf("%s get branch db of stats[%d] fail! 此分库连接失败，请尽快手动处理",
                    $task_name, $i
                );
                printf("%s\n", $content);
                Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
                continue;
            }
        }
        //查询用户id
        $tableName = ComAdDetail::getTableName(strtotime($date));
        $sql = sprintf("select  ad_user_id from $tableName where  create_date='%s' and status not in (-1,2) and deal_status=1 and ver ='shouzhu'  and cheat_type not in (2,3) and price != reduce_price  group  by ad_user_id ",$date);
        $user_ids_arr = $clickLogDB->createCommand($sql)->queryColumn();
        ///


        $result_arr = $app_result_arr = $app_statistic_arr = array();
        if (!empty($user_ids_arr)) {
            //按照1000分组
            $user_ids_arr_num = count($user_ids_arr);
            //如果用户id太多，则拆分，按照每组1000个用户id分多次查询
            if ($user_ids_arr_num>1000) {
                $slice_num = ceil($user_ids_arr_num/1000);
                for ($n =1; $n <= $slice_num; $n++) {
                    $offset = ($n-1)*1000;
                    $length = 1000;
                    $user_ids_arr_new = array_slice($user_ids_arr, $offset,$length);
                    $user_ids_str = 0;
                    if (!empty($user_ids_arr_new)) {
                        $user_ids_arr_new = array_map('intval', $user_ids_arr_new);
                        $user_ids_str = implode(',', $user_ids_arr_new);
                    }
                    $sql = sprintf("select area_fid,area_id,ad_group_id,ad_plan_id,ad_user_id,price,reduce_price,ver,source_type,cid,pid,src,app_cid,ad_advert_id,type,source_system from $tableName where  create_date='%s' and status not in (-1,2) and deal_status=1 and ad_user_id in (%s)  and ver ='shouzhu'  and cheat_type not in (2,3) and price != reduce_price  ",$date,$user_ids_str);
                    $clickLogRows = $clickLogDB->createCommand($sql)->queryAll();

                    EdcStatsArea::getClikResult($clickLogRows, $result_arr, $app_result_arr,$app_statistic_arr,"shouzhu");
                }

            } else {
                $user_ids_str = 0;
                if (!empty($user_ids_arr)) {
                    $user_ids_arr = array_map('intval', $user_ids_arr);
                    $user_ids_str = implode(',', $user_ids_arr);
                    $tableName = ComAdDetail::getTableName(strtotime($date));
                    $sql = sprintf("select area_fid,area_id,ad_group_id,ad_plan_id,ad_user_id,price,reduce_price,ver,source_type,cid,pid,src,app_cid,ad_advert_id,type,source_system from $tableName where  create_date='%s' and status not in (-1,2) and deal_status=1 and  ad_user_id in (%s)  and ver ='shouzhu' and cheat_type not in (2,3) and price != reduce_price  ",$date,$user_ids_str);
                    $clickLogRows = $clickLogDB->createCommand($sql)->queryAll();

                    EdcStatsArea::getClikResult($clickLogRows, $result_arr, $app_result_arr,$app_statistic_arr,"shouzhu");
                }

            }
            //入库
            //app_result
            if (!empty($app_result_arr)) {
                foreach ($app_result_arr as $arr) {

                    $real_costs = $arr['costs'];
                    $clicks     = $arr['clicks'];
                    $ad_user_id = $arr['ad_user_id'];
                    $ad_plan_id = $arr['ad_plan_id'];
                    $ad_group_id = $arr['ad_group_id'];

                    $ad_advert_id = $arr['ad_advert_id'];
                    $ad_channel_id = $arr['ad_channel_id'];
                    $area_fid = $arr['area_fid'];
                    $area_id = $arr['area_id'];
                    $location_id = $arr['location_id'];
                    $place_id = $arr['place_id'];
                    $click_type = $arr['click_type'];

                    $source_system = $arr['source_system'];

                    //更新 子库统计表 按照用户id取模
                    $db_id = $ad_user_id%$statsDBSize+1;
                    // 获取分库连接
                    $statsBranchDB = ${$statsDB.'_'.$db_id};
                    if (!$statsBranchDB) {
                        continue;
                    }
                    ////更新db数据
                    $sql = "update " . $appTableName . " set real_costs=" . $real_costs. ", clicks=".$clicks.", update_time='" . date('Y-m-d H:i:s') . "'";
                    $sql .= " where ad_plan_id=:ad_plan_id and ad_group_id=:groupId and ad_advert_id=:ad_advert_id  and ad_channel_id=:ad_channel_id and area_fid=:area_fid and area_id=:area_id and location_id=:location_id and place_id=:place_id and click_type=:click_type limit 1";
                    $cmd = $statsBranchDB->createCommand($sql);

                    $cmd->bindParam(':ad_plan_id', $ad_plan_id, PDO::PARAM_INT);
                    $cmd->bindParam(':groupId', $ad_group_id, PDO::PARAM_INT);
                    $cmd->bindParam(':ad_advert_id', $ad_advert_id, PDO::PARAM_INT);

                    $cmd->bindParam(':ad_channel_id', $ad_channel_id, PDO::PARAM_INT);
                    $cmd->bindParam(':area_fid', $area_fid, PDO::PARAM_INT);
                    $cmd->bindParam(':area_id', $area_id, PDO::PARAM_INT);
                    $cmd->bindParam(':location_id', $location_id, PDO::PARAM_INT);
                    $cmd->bindParam(':place_id', $place_id, PDO::PARAM_INT);
                    $cmd->bindParam(':click_type', $click_type, PDO::PARAM_INT);

                    try {
                        $return_status = $cmd->execute();

                        //无数据更新则插入
                        if (!$return_status) {

                            //入库
                            $insert_arr = array(

                                'ad_user_id' => $ad_user_id,
                                'ad_plan_id' => $ad_plan_id,
                                'ad_advert_id' => $ad_advert_id,

                                'click_type' => $click_type,
                                'app_id' => $arr['app_id'],
                                'ad_group_id' => $ad_group_id,
                                'ad_channel_id' => $ad_channel_id,
                                'area_fid' => $area_fid,
                                'area_id' => $area_id,
                                'area_key' => $arr['area_key'],
                                'views' => $clicks,
                                'clicks' => $clicks,
                                'costs' => $real_costs,
                                'real_costs' => $real_costs,

                                //'apk_id' => 0,
                                'place_id' => $place_id,
                                'location_id' => $location_id,
                                'source_system'=> $source_system,

                                'create_time' => date('Y-m-d H:i:s', time()),
                                'update_time' => date('Y-m-d H:i:s', time()),

                            );
                            $return_status = $statsBranchDB->createCommand()->insert($appTableName, $insert_arr);
                            printf("%s  branch db of ad_app[%d] reinsert! ad_group_id[%d] ad_channel_id[%d] area_id[%d] area_fid[%d] location_id[%d] place_id[%d] ad_advert_id[%d] click_type[%d] area_key[%s]\n", $task_name,$db_id,$ad_group_id,$ad_channel_id,$area_id,$area_fid,$location_id,$place_id,$ad_advert_id,$click_type,$arr['area_key']);
                        }



                    } catch(Exception $e) {
                        $have_error = 1;
                        $return_status = 0;
                        $error_mess = $e->getMessage();
                    }

                    //更新失败
                    if (!$return_status) {
                        $have_error = 1;
                        $error_num++;
                        printf("%s  branch db of ad_app[%d] update  fail! ad_group_id[%d] ad_channel_id[%d] area_id[%d] area_fid[%d] location_id[%d] place_id[%d] ad_advert_id[%d] click_type[%d] area_key[%s] error_mess[%s]\n", $task_name,$db_id,$ad_group_id,$ad_channel_id,$area_id,$area_fid,$location_id,$place_id,$ad_advert_id,$click_type,$arr['area_key'],$error_mess);
                    }
                }
            }
            if (!empty($app_statistic_arr)) {
                foreach ($app_statistic_arr as $arr) {

                    $real_costs = $arr['costs'];
                    $clicks = $arr['clicks'];
                    $ad_user_id = $arr['ad_user_id'];

                    $ad_channel_id = $arr['ad_channel_id'];
                    $ad_plan_id = $arr['ad_plan_id'];
                    $ad_place_id = $arr['ad_place_id'];
                    $req_src = $arr['req_src'];

                    //更新 子库统计表 按照用户id取模
                    $db_id = $ad_user_id%$statsDBSize+1;
                    // 获取分库连接
                    $statsBranchDB = ${$statsDB.'_'.$db_id};
                    if (!$statsBranchDB) {
                        continue;
                    }
                    ////更新db数据
                    $sql = "update " . $appStatiisticTableName . " set real_costs=" . $real_costs
                    . ",clicks=".$clicks.",  update_time='" . date('Y-m-d H:i:s') . "'";
                    $sql .= " where ad_plan_id=:ad_plan_id and ad_channel_id=:ad_channel_id and ad_place_id=:ad_place_id and req_src=:req_src  limit 1";
                    $cmd = $statsBranchDB->createCommand($sql);

                    $cmd->bindParam(':ad_plan_id', $ad_plan_id, PDO::PARAM_INT);
                    $cmd->bindParam(':ad_channel_id', $ad_channel_id, PDO::PARAM_INT);
                    $cmd->bindParam(':ad_place_id', $ad_place_id, PDO::PARAM_INT);
                    $cmd->bindParam(':req_src', $req_src, PDO::PARAM_STR);
                    try {
                        $return_status = $cmd->execute();

                        //无数据更新则插入
                        if (!$return_status) {

                            //入库
                            $insert_arr = array(

                                'ad_user_id' => $ad_user_id,
                                'ad_plan_id' => $ad_plan_id,
                                'ad_channel_id' => $ad_channel_id,
                                'ad_place_id' => $ad_place_id,
                                'req_src' => $req_src,

                                'views' => $clicks,
                                'clicks' => $clicks,
                                'costs' => $real_costs,
                                'real_costs' => $real_costs,

                                'create_time' => date('Y-m-d H:i:s', time()),
                                'update_time' => date('Y-m-d H:i:s', time()),

                            );
                            $return_status = $statsBranchDB->createCommand()->insert($appStatiisticTableName, $insert_arr);
                            printf("%s  branch db of ad_app_statistic[%d] reinsert! ad_plan_id[%d] ad_channel_id[%d] ad_place_id[%d] req_src[%s]\n", $task_name,$db_id,$ad_plan_id,$ad_channel_id,$ad_place_id,$req_src);

                        }

                    } catch(Exception $e) {
                        $have_error = 1;
                        $return_status = 0;
                        $error_mess = $e->getMessage();
                    }

                    //更新失败
                    if (!$return_status) {
                        $have_error = 1;
                        $error_num++;
                        printf("%s  branch db of ad_app_statistic[%d] update  fail! ad_plan_id[%d] ad_channel_id[%d] ad_place_id[%d] req_src[%s] error_mess[%s]\n", $task_name,$db_id,$ad_plan_id,$ad_channel_id,$ad_place_id,$req_src,$error_mess);
                    }

                }
            }

        }

        if ($have_error) {
            $content = sprintf("%s  insert db have  error,数量:[%d],原因:[%s] ! 请尽快手动排查",
                $task_name,$error_num,$error_mess
            );
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
        }
        $endTime = date('Y-m-d H:i:s');
        printf("%s begin at %s, end at %s\n",
            $task_name, $beginTime, $endTime
        );
    }

    public function actionUpdateProductUserUserChargeLog ($date=null) {

        ini_set('memory_limit', '20480M');
        $task_name = sprintf("[UpdateUserChargeLog %s]", date('Y-m-d H:i:s'));
        $beginTime = date('Y-m-d H:i:s');
        printf("%s begin at %s\n", $task_name, $beginTime);
        $today = date('Y-m-d');
        if (is_null($date)) {
            $date = date('Y-m-d', strtotime('-1 day'));
        } else {
            $date = date('Y-m-d', strtotime($date));
        }
        if ($date==$today) {
            printf("%s today can not run\n", $task_name);
            return ;
        }
        // 获取ad_click_log库连接
        try {
            $clickLogDB = DbConnectionManager::getClickLogDB();
            if (false === $clickLogDB) {
                printf("%s get click_log  db fail\n", $task_name);
            }
        } catch (Exception $e) {
            printf("%s get dj click_log db fail, error [%s]!\n", $task_name, $e->getMessage());
            $clickLogDB = false;
        }
        if (!$clickLogDB) {
            $content = $task_name.'新统计无法获取点click_log库连接,请尽快手工处理。'.$date;
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            return ;
        }

        $tableName = ComAdDetail::getTableName(strtotime($date));
        $daoUserCL          = new UserChargeLog();
        //shouhu与mediav
        $sql = sprintf("select  sum(price-reduce_price) as total_cost,ad_user_id,ver,create_date from $tableName where  create_date='%s' and status not in (-1,2) and deal_status=1 and ver in ('mediav','shouzhu') and cheat_type not in (2,3) and price != reduce_price  group  by ad_user_id,ver ",$date);
        $clickLogRows = $clickLogDB->createCommand($sql)->queryAll();
        $have_error = 0;
        $error_num = 0;
        if (!empty($clickLogRows)) {
            foreach ($clickLogRows as $clickLogArr) {
                $ad_user_id = $clickLogArr['ad_user_id'];
                $total_cost = $clickLogArr['total_cost'];
                $ver = $clickLogArr['ver'];
                $create_date = $clickLogArr['create_date'];
                $type = 2;
                if($ver=='mediav'){
                    $type = 4;
                }

                $return_status = $daoUserCL->updateProductCost($ad_user_id, $total_cost, $create_date,$type);

                //存入失败
                if (!$return_status) {
                    $have_error = 1;
                    $error_num++;
                    printf("%s update  fail! ad_user_id[%d] total_cost[%.2f] create_date[%s] type[%d]\n", $task_name,$ad_user_id,$total_cost,$create_date,$type);
                }
            }
        }


        //如意的处理
        $sql = sprintf("select  sum(price-reduce_price) as total_cost,ad_user_id,create_date from $tableName where  create_date='%s' and status not in (-1,2) and deal_status=1 and ver in ('sou')  and  cid = 29  and pid = 238 and cheat_type not in (2,3) and price != reduce_price  group  by ad_user_id ",$date);
        $clickLogRows = $clickLogDB->createCommand($sql)->queryAll();
        if (!empty($clickLogRows)) {
            foreach ($clickLogRows as $clickLogArr) {
                $ad_user_id = $clickLogArr['ad_user_id'];
                $total_cost = $clickLogArr['total_cost'];
                $create_date = $clickLogArr['create_date'];
                $type = 3;

                $return_status = $daoUserCL->updateProductCost($ad_user_id, $total_cost, $create_date,$type);

                //存入失败
                if (!$return_status) {
                    $have_error = 1;
                    $error_num++;
                    printf("%s update  fail! ad_user_id[%d] total_cost[%.2f] create_date[%s] type[%d]\n", $task_name,$ad_user_id,$total_cost,$create_date,$type);
                }
            }
        }

        //搜索的处理
        $sql = sprintf("select  sum(price-reduce_price) as total_cost,ad_user_id,create_date from $tableName where  create_date='%s' and status not in (-1,2) and deal_status=1 and ver in ('sou')  and  (cid != 29  or pid != 238) and cheat_type not in (2,3) and price != reduce_price  group  by ad_user_id ",$date);
        $clickLogRows = $clickLogDB->createCommand($sql)->queryAll();
        if (!empty($clickLogRows)) {
            foreach ($clickLogRows as $clickLogArr) {
                $ad_user_id = $clickLogArr['ad_user_id'];
                $total_cost = $clickLogArr['total_cost'];
                $create_date = $clickLogArr['create_date'];
                $type = 1;

                $return_status = $daoUserCL->updateProductCost($ad_user_id, $total_cost, $create_date,$type);

                //存入失败
                if (!$return_status) {
                    $have_error = 1;
                    $error_num++;
                    printf("%s update  fail! ad_user_id[%d] total_cost[%.2f] create_date[%s] type[%d]\n", $task_name,$ad_user_id,$total_cost,$create_date,$type);
                }
            }
        }


        if ($have_error) {
            $content = sprintf("%s update  have  error,数量:[%d]! 请尽快手动排查",
                $task_name,$error_num
            );
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
        }



        $endTime = date('Y-m-d H:i:s');
        printf("%s begin at %s, end at %s\n",
            $task_name, $beginTime, $endTime
        );
    }

    public function actionImportStatsAreaKeywordData ($date=null) {
        ini_set('memory_limit', '20480M');
        $task_name = __CLASS__ . '_' . __FUNCTION__;
        $beginTime = date('Y-m-d H:i:s');
        printf("%s begin at %s\n", $task_name, $beginTime);
        $today = date('Y-m-d');
        if (is_null($date)) {
            $date = date('Y-m-d', strtotime('-1 day'));
        } else {
            $date = date('Y-m-d', strtotime($date));
        }
        if ($date==$today) {
            printf("%s today can not run\n", $task_name);
            return ;
        }
        // 获取ad_click_log库连接
        try {
            $db = DbConnectionManager::getDB('click_log_slave');
            if (false === $db) {
                printf("%s get click_log  db fail\n", $task_name);
            }
        } catch (Exception $e) {
            printf("%s get dj click_log db fail, error [%s]!\n", $task_name, $e->getMessage());
            $db = false;
        }
        if (!$db) {
            $content = $task_name.'系统新统计无法获取点click_log库连接,请尽快手工结算。'.$date;
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            return ;
        }
        $statsDBSize = Yii::app()->params['db_esc_stat_num'];
        $statsDBSize = intval($statsDBSize);

        if ($statsDBSize <= 0) {
            $content = $task_name.'新统计无法获取db_stat_num,请尽快手工结算。'.$date;
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            return ;
        }
        $statsDB = 'statsBranchDB';
        for ($i=1; $i <= $statsDBSize ; $i++) {
            // 获取分库连接
            ${$statsDB.'_'.$i} = DbConnectionManager::getDB('db_esc_stat_' . $i);
            if (!${$statsDB.'_'.$i}) {
                $content = sprintf("%s get branch db of stats[%d] fail! 此分库连接失败，请尽快手动处理",
                    $task_name, $i
                );
                printf("%s\n", $content);
                Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);

                continue;
            }
        }

        $db = DbConnectionManager::getDB('click_log_slave');
        $tableName = ComAdDetail::getTableName(strtotime($date));
        $sql = sprintf("select  count(1) as clicks,sum(price-reduce_price) as costs,ad_group_id,ad_plan_id,ad_user_id,keyword,area_id,area_fid from $tableName where  create_date='%s' and status not in (-1,2) and deal_status=1 and ver='sou' and cheat_type not in (2,3) and price != reduce_price  group  by ad_group_id,keyword, area_id ",$date);
        $clickLogRows = $db->createCommand($sql)->queryAll();
        $daoStats   = new EdcStatsAreaKeyword();
        $have_error = 0;
        $error_num = 0;
        $error_mess = '';
        if (!empty($clickLogRows)) {
            $statsData = array();
            foreach ($clickLogRows as $clickLogArr) {
                $ad_user_id = $clickLogArr['ad_user_id'];
                //更新 子库统计表 按照用户id取模
                $db_id = $ad_user_id%$statsDBSize+1;

                //入库
                $statsData[$db_id][] = array(
                    'ad_group_id' => intval($clickLogArr['ad_group_id']),
                    'ad_plan_id' => intval($clickLogArr['ad_plan_id']),
                    'ad_user_id' => $ad_user_id,
                    'keyword' => $clickLogArr['keyword'],
                    'area_fid' => $clickLogArr['area_fid'],
                    'area_id' => $clickLogArr['area_id'],
                    'clicks' => $clickLogArr['clicks'],
                    'costs' => $clickLogArr['costs'],
                    'create_date' => $date,
                    'create_time' => time(),
                );
                if(count($statsData[$db_id]) >= 100) {
                    // 获取分库连接
                    $statsBranchDB = ${$statsDB.'_'.$db_id};
                    if (!$statsBranchDB) {
                        continue;
                    }
                    $daoStats->setDB($statsBranchDB);
                    try {
                        $return_status = $daoStats->insertRows($statsData[$db_id], $date);
                    } catch(Exception $e) {
                        $have_error = 1;
                        $return_status = 0;
                        $error_mess = $e->getMessage();
                    }
                    //存入失败
                    if (!$return_status) {
                        $have_error = 1;
                        $error_num++;
                        printf("%s  branch db of stats[%d] insert  fail! data[%s] error_mess[%s]\n", $task_name,$db_id,json_encode($statsData[$db_id]),$error_mess);
                    }
                    unset($statsData[$db_id]);
                }
            }
        }

        foreach($statsData as $db_id=>$data) {
            if(empty($data)) {
                continue;
            }
            // 获取分库连接
            $statsBranchDB = ${$statsDB.'_'.$db_id};
            if (!$statsBranchDB) {
                continue;
            }
            $daoStats->setDB($statsBranchDB);
            try {
                $return_status = $daoStats->insertRows($statsData[$db_id], $date);
            } catch(Exception $e) {
                $have_error = 1;
                $return_status = 0;
                $error_mess = $e->getMessage();
            }
            //存入失败
            if (!$return_status) {
                $have_error = 1;
                $error_num++;
                printf("%s  branch db of stats[%d] insert  fail! data[%s] error_mess[%s]\n", $task_name,$db_id,json_encode($statsData[$db_id]),$error_mess);
            }
        }
        if ($have_error) {
            $content = sprintf("%s  insert db have  error,数量:[%d],原因:[%s]  ! 请尽快手动排查",
                $task_name,$error_num,$error_mess
            );
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
        }
        $endTime = date('Y-m-d H:i:s');
        printf("%s begin at %s, end at %s\n",
            $task_name, $beginTime, $endTime
        );
    }

    /**
     * 定时更新无效点击
     * @param null $date    数据日期
     * @param int $isSettle 是否结算
     */
    public function actionUpdateInvalidClick($date = null, $isSettle = 0) {
        ini_set('memory_limit', '20480M');
        $taskName = __CLASS__ . '_' . __FUNCTION__;
        $beginTime = date('Y-m-d H:i:s');
        printf("%s begin at %s\n", $taskName, $beginTime);

        if (!is_null($date)) {
            $date = date('Ymd', strtotime($date));
        }
        //计算最大时间戳
        if ($isSettle !== 0) {
            $lastTime = strtotime(date('Ymd').' -1 second');
            if (is_null($date)) {
                $date = date('Ymd', strtotime('-1 day'));
            }
            $clickSuffix = ' deal_status=1 and status not in (-1,2) ';
        } else {
            $lastTime = strtotime(date('Ymd').' +1 days -1 second');
            if (is_null($date)) {
                $date = date('Ymd');
            }
            $clickSuffix = ' deal_status!=-1 ';
        }

        $dateTimeStamp = strtotime($date);
        if ($dateTimeStamp>$lastTime) {
            printf("%s can not run\n", $taskName);
            return ;
        }

        // 获取镜像ad_click_log库连接
        try {
            $db = DbConnectionManager::getDB('click_log_slave');
            if (false === $db) {
                printf("%s get click_log  db fail\n", $taskName);
            }
        } catch (Exception $e) {
            printf("%s get dj click_log db fail, error [%s]!\n", $taskName, $e->getMessage());
            $db = false;
        }
        if (!$db) {
            $content = $taskName.'系统新统计无法获取点click_log库连接,请尽快手工结算。'.$date;
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            return ;
        }

        //统计库
        $statsDBSize = Yii::app()->params['db_stat_num'];
        $statsDBSize = intval($statsDBSize);

        if ($statsDBSize <= 0) {
            $content = $taskName.'新统计无法获取db_stat_num,请尽快手工结算。'.$date;
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            return ;
        }

        $statsDB = 'statsBranchDB';
        for ($i=1; $i <= $statsDBSize ; $i++) {
            // 获取分库连接
            ${$statsDB.'_'.$i} = DbConnectionManager::getStatBranchDB($i);
            if (!${$statsDB.'_'.$i}) {
                $content = sprintf("%s get branch db of stats[%d] fail! 此分库连接失败，请尽快手动处理", $taskName, $i);
                printf("%s\n", $content);
                Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
                continue;
            }
        }

        $currentDate = date('Y-m-d', $dateTimeStamp);
        $currentTime = date('Y-m-d H:i:s', time());
        //结算清空原数据
        if ($isSettle !== 0) {
            $sql = "UPDATE ad_invalid_click_report_$date SET total_clicks=0,invalid_clicks=0,invalid_cost=0,update_time='$currentTime';";
            for($i=1;$i<=$statsDBSize ; $i++) {
                $statsBranchDB = ${$statsDB.'_'.$i};
                try {
                    $statsBranchDB->createCommand($sql)->queryAll();
                } catch(Exception $e) {
                    $have_error = 1;
                    $return_status = 0;
                    $error_mess = $e->getMessage();
                }
            }
            if ($have_error) {
                $content = sprintf("%s  clear db have  error,原因:[%s]  ! 请尽快手动排查", $taskName,$error_mess);
                Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
            }
        }

        //统计点击分布
        $sql = "select ad_user_id,ad_group_id,ad_plan_id,
(case when source_type=3 then 1 else 2 end) device_type,
sum(case when cheat_type=0 and price=reduce_price then 0 else 1 end) total_clicks,
sum(case when cheat_type in (2,3) then 1 else 0 end) invalid_clicks,
sum(case when cheat_type in (2,3) then price else 0 end) invalid_cost
 from click_detail_$date where create_date='$currentDate' and ver='sou' and pid!=238 and source_type in (3,4) and $clickSuffix group by ad_user_id,ad_group_id,ad_plan_id,device_type";
        $clickLogRows = $db->createCommand($sql)->queryAll();

        $statTable = 'ad_invalid_click_report_' . $date;
        $error_num = 0;
        foreach ($clickLogRows as $row) {
            //更新 子库统计表 按照用户id取模
            $db_id = $row['ad_user_id']%$statsDBSize+1;
            // 获取分库连接
            $statsBranchDB = ${$statsDB.'_'.$db_id};
            $sql = 'INSERT INTO ' . $statTable . '(ad_user_id,ad_plan_id,ad_group_id,device_type,create_date,update_time,total_clicks,invalid_clicks,invalid_cost) VALUES (' .
                $row['ad_user_id'] . ',' .
                $row['ad_plan_id'] . ',' .
                $row['ad_group_id'] . ',' .
                $row['device_type'] . ",'" .
                $currentDate . "','" .
                $currentTime . "'," .
                $row['total_clicks'] . ',' .
                $row['invalid_clicks'] . ',' .
                $row['invalid_cost'] . ')' .
                ' ON DUPLICATE KEY UPDATE ' .
                ' total_clicks=total_clicks+' . $row['total_clicks'] .
                ', invalid_clicks=invalid_clicks+' . $row['invalid_clicks'] .
                ', invalid_cost=invalid_cost+' . $row['invalid_cost'] ;
            try {
                $return_status = $statsBranchDB->createCommand($sql)->execute();
            } catch(Exception $e) {
                $have_error = 1;
                $return_status = 0;
                $error_mess = $e->getMessage();
            }
            //存入失败
            if (!$return_status) {
                $have_error = 1;
                $error_num++;
                printf("%s  branch db of stats[%d] insert  fail! data[%s] error_mess[%s]\n", $taskName,$db_id,$sql,$error_mess);
            }
        }

        if ($have_error) {
            $content = sprintf("%s  insert db have  error,数量:[%d],原因:[%s]  ! 请尽快手动排查", $taskName,$error_num,$error_mess);
            Utility::sendAlert(__CLASS__,__FUNCTION__,$content,true);
        }
        $endTime = date('Y-m-d H:i:s');
        printf("%s begin at %s, end at %s\n", $taskName, $beginTime, $endTime);
    }
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */