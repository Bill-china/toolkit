<?php
/**
 * 新的点击作弊处理流程
 */
Yii::import('application.extensions.CEmqPublisher');
class CheatnewCommand extends CConsoleCommand {

    /**
     * 导入作弊点击日志
     */
    public function actionImportCheatLog() {/*{{{*/
        ini_set('memory_limit', '2048M');

        $taskName = 'importcheatlog_'.date('YmdHis');
        $taskStart = date('Y-m-d H:i:s');

        // 作弊数据文件
        $cheatDataDir = Config::item('logDir') . 'cheat/';
        if (!is_dir($cheatDataDir)) {
            echo $taskName. " dir[".$cheatDataDir."] not exists\n";
            return;
        }

        // 作弊数据处理文件标记文件路径
        $finishFlagDir = $cheatDataDir . 'finishFlag/';
        if (!is_dir($finishFlagDir)) {
            echo $taskName. " dir[".$finishFlagDir."] not exists\n";
            return;
        }

        $files = array();
        $d = dir($cheatDataDir);
        while (false !== ($entry = $d->read())) {
            if ($entry == '.' || $entry == '..') {
                continue;
            }
            // 201404040500         搜索类点击作弊日志
            // guess.201404040500   猜你喜欢点击作弊日志
            // fix.*                部分反作弊数据后期补充
            if (1!=preg_match('/^[0-9]{12}$/', $entry) 
                && 1!=preg_match('/^guess\.[0-9]{12}$/', $entry) 
                && 1!=preg_match('/^shouzhu\.[0-9]{12}$/', $entry) 
                && 1!=preg_match('/^fix\.[0-9]{12}$/', $entry) 
                && 1!=preg_match('/^fix\.guess\.[0-9]{12}$/', $entry) 
                && 1!=preg_match('/^fix\.shouzhu\.[0-9]{12}$/', $entry)) {
                continue;
            }

            $file = $cheatDataDir . $entry; // 数据文件
            $finishFile = $cheatDataDir. $entry . '.finish'; // 数据完整性的标记文件
            $isFinishedFile = $finishFlagDir . $entry . '.finish'; // 是否处理完成的标记文件
            if (is_file($file) && is_file($finishFile) && !is_file($isFinishedFile)) {
                $files[] = $entry;
            }
        }
        $d->close();
        // var_dump($files);

        // 获取点击数据库
        try {
            $clickLogDB = Yii::app()->db_click_log;
        } catch (Exception $e) {
            echo $taskName." get db fail, error [".$e->getMessage()."]!\n";
            return ;
        }

        $sql = "update ad_click_log set status=1 where click_id=:click_id and (ver='sou' or ver='guess' or ver='shouzhu') and status=0";
        $cmd = $clickLogDB->createCommand($sql);

        $time = time();
        $count = 0;
        foreach($files as $file) {
            $fh = fopen($cheatDataDir . $file, 'r');
            while(false !== ($line = fgets($fh, 10240)) ) {
                $id = trim($line);
                if ($id === '') {
                    continue;
                }
 /*               $cmd->bindParam(':click_id', $id, PDO::PARAM_STR);
                if ($cmd->execute()) {
                    $count++;
                }

                $detail_sql = "update click_detail set status=1,cheat_type=3,reduce_price=price,update_time=".time()." where click_id='".$id."' and status=0 and cheat_type=0 and deal_status =0 ";
                ComAdDetail::queryBySql($detail_sql, $time, 'exec');

                if(date('G') < 1){
                    $detail_sql = "update click_detail set status=1,cheat_type=3,reduce_price=price,update_time=".time()." where click_id='".$id."' and status=0 and cheat_type=0 and deal_status =0 ";
                    ComAdDetail::queryBySql($detail_sql, strtotime('-1 day'), 'exec');
                }
                ComAdDetail::insertOperateLog($id,2,'3',1);
*/
                static   $emq;
                $emq=new ComEMQ('emq_esc');
                $emq->exchangeName='cheat_click_id';
                $emq->logid=Utility::$logid;
                $emq->send($id);
            }
            fclose($fh);
            file_put_contents($finishFlagDir . $file . '.finish', '');
            echo $taskName." process file [".$file."] at time[".date('Y-m-d H:i:s')."]\n";
        }
        $taskEnd = date('Y-m-d H:i:s');
        printf("%s begin at %s, end at %s, process count %d\n", $taskName, $taskStart, $taskEnd, $count);
    }/*}}}*/

    /**
     * 点击作弊统计处理，注意只能运行一个进程
     * // todo date
     */
    public function actionCheatCheck($ad_channel_id=0) {/*{{{*/
        ini_set('memory_limit', '2048M');
        $taskName = sprintf("[cheatcheck_%s]", date('YmdHis'));
        $beginTime = date('Y-m-d H:i:s');
        printf("task %s begin at %s\n", $taskName, $beginTime);

        $lockDir = Config::item('logDir');
        $lock_file = $lockDir.'cheat/cheat_check_new_'.$ad_channel_id.'.lock';
        $lock_fp = fopen($lock_file,'w');
        if(!flock($lock_fp, LOCK_NB | LOCK_EX)) {
            printf("task %s lock fail, quit\n", $taskName);
            return ;
        }
        //校验ad_channel_id
        $ad_channel_id = intval($ad_channel_id);

        if(!in_array($ad_channel_id, array(0,52,40,16))){
            printf("task %s ad_channel_id[%s] error, quit\n", $taskName,$ad_channel_id);
            return ;
        }
        if($ad_channel_id == 0){
            $where_channel = ' and cid not  in (52,40,16) ';
        } else {
            $where_channel = ' and cid = '.$ad_channel_id.' ';
        }
        // 获取 ad_click_log 连接
        try {
            $clickLogDB = Yii::app()->db_click_log;
        } catch (Exception $e) {
            printf("task %s get ad_click_db fail, mas[%s], quit\n", $taskName, $e->getMessage());
            flock($lock_fp, LOCK_UN);
            fclose($lock_fp);
            return ;
        }

        $adStats            = new EdcStats();
        $adStatsArea        = new EdcStatsArea();
        $adStatsKeyword     = new EdcStatsKeyword();
        $adStatsInterest    = new EdcStatsInterest();
        $adApp              = new AdApp();
        $adStatsBiyi        = new EdcStatsBiYi();
        //$adStatsBilin       = new EdcStatsNeighbor();
        // 在地域关键词项目上线前，不要合并此部分代码至 online; start
        //$daoStatsAreakw     = new EdcStatsAreakw();
        // 在地域关键词项目上线前，不要合并此部分代码至 online end

        $adClickLog = new AdClickLog();
        $adClickLog->setDB($clickLogDB);

        $dbSize = Yii::app()->params['db_stat_num'];


        // 在地域关键词项目上线前，不要合并此部分代码至 online; start
        //$areakwDbSize = Yii::app()->params['db_areakw_num'];
        // 在地域关键词项目上线前，不要合并此部分代码至 online end

        $count = $success = 0;
        if (date('G') >= 1) {
            $dateList = array(
                date('Y-m-d'),
            );
        } else {
            $dateList = array(
                date('Y-m-d', strtotime('-1 day')),
                date('Y-m-d'),
            );
        }
        foreach ($dateList as $oneDay) {
            $tableName = ComAdDetail::getTableName(strtotime($oneDay));
            $sql = sprintf("select * from $tableName where create_date='%s' and status=1 and cheat_type=3 and deal_status =0  %s limit 5000", $oneDay, $where_channel);

            $queryCmd = $clickLogDB->createCommand($sql);
            while (true) {
                // 取出作弊数据
                $arrData = $queryCmd->queryAll();
                if (empty($arrData)) {
                    break;
                }
                // var_dump($arrData);

                foreach ($arrData as $oneCheatData) {
                    Utility::log(__CLASS__,"getcheatclick",$oneCheatData);
                    $count++;
                    ////jingguangwen 20150701 addd
                    $amount = round($oneCheatData['price']-$oneCheatData['reduce_price'], 2);
                    //先更新price=reduce_price
                    $detail_sql = "update click_detail set reduce_price=price,update_time=".time()." where id=".$oneCheatData['id'];
                    $need_continue =0;
                    if(round($amount-0,2) <=0){
                        $detail_sql = "update click_detail set reduce_price=price,status=2,update_time=".time()." where id=".$oneCheatData['id'];
                        $need_continue =1;
                    }
                    $res_update = ComAdDetail::queryBySql($detail_sql, strtotime($oneDay), 'exec');
                    if(!$res_update){//报警
                        Utility::sendAlert(__CLASS__,__FUNCTION,"click_detail update failed,id:".$oneCheatData['id']);
                    }
                    if($need_continue == 1) {
                        continue;
                    }
                    ///jingguangwen  add  end
                    $dbIndex = $oneCheatData['ad_user_id'] % $dbSize + 1;
                    $dbConfName = 'db_stat_'.$dbIndex;
                    $dbStat = Yii::app()->$dbConfName;

                    // 在地域关键词项目上线前，不要合并此部分代码至 online; start
                    //$areakwIndex = $oneCheatData['ad_user_id'] % $areakwDbSize + 1;
                    //$areakwDbConfName = 'db_areakw_'.$areakwIndex;
                    //$areakwDb = Yii::app()->$areakwDbConfName;
                    // 在地域关键词项目上线前，不要合并此部分代码至 online; end


                    $adStats->setDB($dbStat);
                    $adStatsArea->setDB($dbStat);
                    $adStatsKeyword->setDB($dbStat);
                    $adStatsInterest->setDB($dbStat);
                    $adApp->setDB($dbStat);
                    $adStatsBiyi->setDB($dbStat);
                    //$adStatsBilin->setDB($dbStat);
                    // 在地域关键词项目上线前，不要合并此部分代码至 online; start
                    //$daoStatsAreakw->setDB($areakwDb);
                    // 在地域关键词项目上线前，不要合并此部分代码至 online; end
                    //兼容老程序格式，组装数据  jingguangwen@360.cn  add

                    $oneCheatData['ad_channel_id'] = $oneCheatData['cid'];
                    $oneCheatData['ad_place_id'] = $oneCheatData['pid'];
                    $oneCheatData['area_key'] = $oneCheatData['area_fid'].','.$oneCheatData['area_id'];
                    $oneCheatData['inter_id'] = $oneCheatData['tag_id'];

                    $ver = AdClickLog::VER_SOU;
                    if (!empty($oneCheatData['ver'])) {
                        $ver = trim($oneCheatData['ver']);
                    }
                    $try_num_add = 0;//是否需要以后再次重试逻辑

                    do {
                        //第二天1点以后，不退反作弊数据了
                        if (time() - strtotime($oneCheatData['create_date']) > 86400+3600) {
                            printf("task %s, in the second day, time out, click id[%s]\n", $taskName, $oneCheatData['click_id']);

                            ComAdDetail::updateStatusByClickId($oneCheatData['click_id'], ComAdDetail::STATUS_TIMEOUT, 1, strtotime($oneDay));
                            //防止与通过ad_click_log统计状态变更并发冲突，此处添加状态条件jingguangwen@360.cn 2014-09-18
                            $adClickLog->updateStatusByClickId($oneCheatData['click_id'], AdClickLog::STATUS_TIMEOUT,1);
                            Utility::log(__CLASS__,"updateStatusByClickId1",$oneCheatData['click_id']);
                            break;
                        }

                        // 从 ad_stats 表里取数据
                        $ad_stats_arr = $adStats->getByAdvertIdAndDateAndChannelAndPlace($oneCheatData['ad_advert_id'],$oneCheatData['create_date'],$oneCheatData['ad_channel_id'],$oneCheatData['ad_place_id']);
                        if (empty($ad_stats_arr)) {
                            printf("task %s, can not get data from ad_stats, aid[%s], create_date[%s], channel_id[%s], place_id[%s]\n",
                                $taskName,
                                $oneCheatData['ad_advert_id'],
                                $oneCheatData['create_date'],
                                $oneCheatData['ad_channel_id'],
                                $oneCheatData['ad_place_id']
                            );
                            $try_num_add = 1;
                            break;
                        }

                        // 检验是否已经结算了
                        // if ($ad_stats_arr['status'] == EdcStats::STATUS_STATED) {
                        //     //如果统计状态已结算
                        //     $adClickLog->updateStatusByClickId($oneCheatData['click_id'], AdClickLog::STATUS_TIMEOUT);
                        //     Utility::log(__CLASS__,"updateStatusByClickId2",$oneCheatData['click_id']);
                        //     break;
                        // }

                        if (($ad_stats_arr['total_cost'] - $amount) < 0 || ($ad_stats_arr['clicks']-1) < 0 ) {
                            printf("task %s, check amount fail, total_cost[%s], clicks[%s], click_id[%s]\n",
                                $taskName,
                                $ad_stats_arr['total_cost'],
                                $ad_stats_arr['clicks'],
                                $oneCheatData['click_id']
                            );
                            $try_num_add = 1;
                            break;
                        }

                        //查询ad_stats_area表对应的数据
                        $ad_stats_area_arr = $adStatsArea->getByDateAndAreaKey(
                            $oneCheatData['ad_group_id'],
                            $oneCheatData['area_key'],
                            $oneCheatData['create_date']
                        );
                        if (empty($ad_stats_area_arr)) {
                            printf("task %s, can not get data from ad_stats_area, gid[%s], area_key[%s], create_date[%s]\n",
                                $taskName,
                                $oneCheatData['ad_group_id'],
                                $oneCheatData['area_key'],
                                $oneCheatData['create_date']
                            );
                            $try_num_add = 1;
                            break;
                        }
                        //查询ad_stats_keyword表、ad_stats_interest表、ad_app表 对应的数据
                        $ad_stats_keyword_arr = $ad_stats_interest_arr = $ad_app_arr = array();
                        //区分sou与guess
                        if ($ver == AdClickLog::VER_SOU) {
                            //查询ad_stats_keyword统计表
                            $ad_stats_keyword_arr = $adStatsKeyword->getByDateAndKeyword(
                                $oneCheatData['ad_group_id'],
                                $oneCheatData['keyword'],
                                $oneCheatData['create_date']
                            );

                            if (empty($ad_stats_keyword_arr)) {
                                printf("task %s, can not get data from ad_stats_keyword, gid[%s], keyword[%s], create_date[%s]\n",
                                    $taskName,
                                    $oneCheatData['ad_group_id'],
                                    $oneCheatData['keyword'],
                                    $oneCheatData['create_date']
                                );
                                $try_num_add = 1;
                                break;
                            }

                        }elseif ($ver == AdClickLog::VER_GUESS) {
                            //查询ad_stats_interest表
                            $ad_stats_interest_arr = $adStatsInterest->getByDateAndInterId(
                                $oneCheatData['ad_group_id'],
                                $oneCheatData['inter_id'],
                                $oneCheatData['create_date']
                            );
                            if (empty($ad_stats_interest_arr)) {
                                printf("task %s, can not get data from ad_stats_interest, gid[%s], inter_id[%s], create_date[%s]\n",
                                    $taskName,
                                    $oneCheatData['ad_group_id'],
                                    $oneCheatData['inter_id'],
                                    $oneCheatData['create_date']
                                );
                                $try_num_add = 1;
                                break;
                            }
                        }elseif ($ver == 'shouzhu') {
                            //查询 ad_app 表
                            $ad_app_arr = $adApp->getByDateAndAreaKey(
                                $oneCheatData['ad_group_id'],
                                $oneCheatData['area_key'],
                                $oneCheatData['create_date']
                            );
                            if (empty($ad_app_arr)) {
                                printf("task %s, can not get data from ad_app, gid[%s], areakey[%s], create_date[%s]\n",
                                    $taskName,
                                    $oneCheatData['ad_group_id'],
                                    $oneCheatData['area_key'],
                                    $oneCheatData['create_date']
                                );
                                $try_num_add = 1;
                                break;
                            }

                        }
                        // 比翼
                        $ad_stats_biyi_arr = array();
                        if ($oneCheatData['sub_ver']=='biyi') {
                             //解析sub_data 2015-04-20 jingguangwen@360.cn  add
                             $subdata = '';
                            //解析新加属性 sub_ad_info
                            if (isset($oneCheatData['sub_ad_info']) && !empty($oneCheatData['sub_ad_info'])) {
                                $sub_ad_info = json_decode($oneCheatData['sub_ad_info'],true);
                                if(is_array($sub_ad_info) &&!empty($sub_ad_info)){
                                    foreach ($sub_ad_info as $sub_ad_info_arr) {
                                        $subdata = $sub_ad_info_arr['id'];
                                    }
                                }
                            } else {
                                if (isset($oneCheatData['subdata']) && !empty($oneCheatData['subdata'])) {
                                    $subdata_arr = json_decode($oneCheatData['subdata'],true);
                                    if(is_array($subdata_arr) &&!empty($subdata_arr)){
                                        $subdata = implode(',', $c['subdata']);
                                    }
                                }
                            }
                            $oneCheatData['sub_data'] = $subdata;
                            //add  end
                            $ad_stats_biyi_arr = $adStatsBiyi->getByGroupIDSubID(
                                $oneCheatData['ad_group_id'],
                                $oneCheatData['sub_data'],
                                $oneCheatData['create_date']
                            );
                            if (empty($ad_stats_biyi_arr)) {
                                printf("task %s, can not get data from ad_stats_biyi, gid[%s], sub_id[%s], create_date[%s]\n",
                                    $taskName,
                                    $oneCheatData['ad_group_id'],
                                    $oneCheatData['sub_data'],
                                    $oneCheatData['create_date']
                                );
                                $try_num_add = 1;
                                break;
                            }
                        }
                        // 比邻
                        // $ad_stats_bilin_arr = array();
                        // if ($oneCheatData['ad_channel_id']==32 && $oneCheatData['ad_place_id']=162) {
                        //     $ad_stats_bilin_arr = $adStatsBilin->getByGroupIDAndKeyword(
                        //         $oneCheatData['ad_group_id'],
                        //         $oneCheatData['keyword'],
                        //         $oneCheatData['create_date']
                        //     );
                        //     if (empty($ad_stats_bilin_arr)) {
                        //         printf("task %s, can not get data from ad_stats_neighbor, gid[%s], keyword[%s], create_date[%s]\n",
                        //             $taskName,
                        //             $oneCheatData['ad_group_id'],
                        //             $oneCheatData['keyword'],
                        //             $oneCheatData['create_date']
                        //         );
                        //         $try_num_add = 1;
                        //         break;
                        //     }
                        // }

                        // 在地域关键词项目上线前，不要合并此部分代码至 online start
                        // 获取地域关键词信息
                        // $ad_areakw_arr = array();
                        // if ($oneCheatData['ver'] == 'sou' && !empty($oneCheatData['keyword'])) {
                        //     list ($fid, $id) = explode(',', $oneCheatData['area_key']);
                        //     if ($fid==10001) {
                        //         $fid = $id = 0;
                        //     } else if ($id == 10001) {
                        //         $id = 0;
                        //     }
                        //     $fid = intval($fid); $id = intval($id);
                        //     $ad_areakw_arr = $daoStatsAreakw->getByGidAreadAndKeyword(
                        //         $oneCheatData['ad_group_id'],
                        //         $fid,
                        //         $id,
                        //         $oneCheatData['keyword'],
                        //         $oneCheatData['create_date']
                        //     );
                        //     if (empty($ad_areakw_arr)) {
                        //         printf("task %s, can not get data from area_keyword, gid[%s], area_fid[%], area_id[%d], keyword[%s], create_date[%s]\n",
                        //             $taskName,
                        //             $oneCheatData['ad_group_id'],
                        //             $fid,
                        //             $id,
                        //             $oneCheatData['keyword'],
                        //             $oneCheatData['create_date']
                        //         );
                        //         $try_num_add = 1;
                        //         break;
                        //     }
                        // }
                        // 在地域关键词项目上线前，不要合并此部分代码至 online end

                        //使用事物逻辑处理
                        $trans = $dbStat->beginTransaction();
                        try {
                            $adStats->cheatClickRefund($ad_stats_arr['id'], $amount, $oneCheatData['create_date']);
                            $adStatsArea->cheatClickRefund($ad_stats_area_arr['id'],$amount, $oneCheatData['create_date']);
                            //sou  ad_stats_keyword
                            if ($ad_stats_keyword_arr) {
                                $adStatsKeyword->cheatClickRefund($ad_stats_keyword_arr['id'], $amount, $oneCheatData['create_date']);
                            }
                            //guess ad_stats_interest
                            if ($ad_stats_interest_arr) {
                                $adStatsInterest->cheatClickRefund($ad_stats_interest_arr['id'],$amount, $oneCheatData['create_date']);
                            }
                            // shouzhu
                            if ($ad_app_arr) {
                                $adApp->cheatClickRefund($ad_app_arr['id'], $amount, $oneCheatData['create_date']);
                            }

                            // biyi
                            if ($ad_stats_biyi_arr) {
                                $adStatsBiyi->cheatClickRefund($ad_stats_biyi_arr['id'], $amount, $oneCheatData['create_date']);
                            }

                            // bilin
                            // if ($ad_stats_bilin_arr) {
                            //     $adStatsBilin->cheatClickRefund($ad_stats_bilin_arr['id'], $amount, $oneCheatData['create_date']);
                            // }

                            // 在地域关键词项目上线前，不要合并此部分代码至 online start
                            // areakw
                            // if ($ad_areakw_arr) {
                            //     $daoStatsAreakw->cheatClickRefund($ad_areakw_arr['id'], $amount, $oneCheatData['create_date']);
                            // }
                            // 在地域关键词项目上线前，不要合并此部分代码至 online end

                            //更新ad_click_log表status更新为2状态，表示已处理完毕
                            $adClickLog->updateStatusByClickId($oneCheatData['click_id'], AdClickLog::STATUS_DONE);
                            ComAdDetail::updateStatusByClickId($oneCheatData['click_id'], ComAdDetail::STATUS_DONE, 1, strtotime($oneDay));
                            Utility::log(__CLASS__,"updateStatusByClickId3",$oneCheatData['click_id']);
                            // 发送退款消息
                            $msg = array(
                                'msg_type'  => 'update',
                                'msg_src'   => 'esc_cheat_new',
                                'msg_id'    => '',
                                'time'      => time(),
                                'content'   => array(
                                    'ad_user_id'    => $ad_stats_arr['ad_user_id'],
                                    'data'          => array(
                                        'ad_user_id'      => $ad_stats_arr['ad_user_id'],
                                        'ad_plan_id'      => $ad_stats_arr['ad_plan_id'],
                                        'amount'          => $amount,
                                        'ver'             => $oneCheatData['ver'],
                                        'create_time'     => $oneCheatData['click_time'],
                                        'click_id'        => $oneCheatData['click_id'],
                                        'cheat_exec_time' => date('Y-m-d H:i:s'),
                                    ),
                                ),
                            );

                            // 直接发送反作弊信息至rmq
                            // rmq 中格式
                            // {"mid":"ab93ff6459da9ded8bf10bf25657207d","msg_src":"esc_cheatnew","time":1414482269,"logid":"ESC_141448226969079754","exchange":"ex_CheatRefund","content":{"ad_user_id":"251166974","data":{"ad_user_id":"251166974","ad_plan_id":"1087926","amount":0.31,"create_time":1413734401,"click_id":"f5bf25d864fcc41a","cheat_exec_time":"2014-10-28 15:44:29"}}}
                            $curTime = gettimeofday();
                            $logID = sprintf("ESC_%d%06d%02d", $curTime['sec'], $curTime['usec'], mt_rand(0, 99));
                            $mqData = $msg['content'];
                            CEmqPublisher::send(
                                Yii::app()->params['exchange']['cheatRefund'],
                                'esc_cheatnew',
                                json_encode($mqData),
                                $logID,
                                Yii::app()->params['emq']
                            );
                            $logIDCheatCheck = sprintf("ESC_CHEAT_CHECK%d%06d%02d", $curTime['sec'], $curTime['usec'], mt_rand(0, 99));
                            //由写DB改为写mq，不能保证原子性 2014.12.31
                            CEmqPublisher::send(
                                'cron_cheat_check',
                                'esc_cheat_new',
                                json_encode($mqData),
                                $logIDCheatCheck,
                                Yii::app()->params['emq']
                            );

                            $trans->commit();

                            //同时更新redis用户消费金额信息
                            ComAdQuotaV2::refundPush($ad_stats_arr['ad_user_id'], $ad_stats_arr['ad_plan_id'], $amount, $oneCheatData['click_time']);

                        } catch (Exception $e) {
                            $trans->rollback();
                            printf("task %s change data fail, errmsg[%s]\n", $taskName, $e->getMessage());
                            break;
                        }
                    } while (false);
                    //如果需要重试的，重试次数加一，如果达到最大重试次数5次的，则更新对应的数据状态为4
                    if($try_num_add == 1){

                        $ad_stats_interst_update_arr =  array(
                            'update_time' => time(),
                            'status' => 4,
                        );

                        $adClickLog->updateStatusByClickId($oneCheatData['click_id'], AdClickLog::STATUS_FAIL);


                        ComAdDetail::updateById($oneCheatData['id'], $ad_stats_interst_update_arr, strtotime($oneDay));
                        //发送消息
                        // 发送退款消息
                        $msg = array(
                            'msg_type'  => 'update',
                            'msg_src'   => 'esc_cheat_new',
                            'msg_id'    => '',
                            'time'      => time(),
                            'content'   => array(
                                'ad_user_id'    => $oneCheatData['ad_user_id'],
                                'data'          => array(
                                    'ad_user_id'      => $oneCheatData['ad_user_id'],
                                    'ad_plan_id'      => $oneCheatData['ad_plan_id'],
                                    'amount'          => $amount,
                                    'ver'             => $oneCheatData['ver'],
                                    'create_time'     => $oneCheatData['click_time'],
                                    'click_id'        => $oneCheatData['click_id'],
                                    'cheat_exec_time' => date('Y-m-d H:i:s'),
                                ),
                            ),
                        );

                        // 直接发送反作弊信息至rmq
                        // rmq 中格式
                        // {"mid":"ab93ff6459da9ded8bf10bf25657207d","msg_src":"esc_cheatnew","time":1414482269,"logid":"ESC_141448226969079754","exchange":"ex_CheatRefund","content":{"ad_user_id":"251166974","data":{"ad_user_id":"251166974","ad_plan_id":"1087926","amount":0.31,"create_time":1413734401,"click_id":"f5bf25d864fcc41a","cheat_exec_time":"2014-10-28 15:44:29"}}}
                        $curTime = gettimeofday();
                        $logID = sprintf("ESC_%d%06d%02d", $curTime['sec'], $curTime['usec'], mt_rand(0, 99));
                        $mqData = $msg['content'];
                        CEmqPublisher::send(
                            Yii::app()->params['exchange']['cheatRefund'],
                            'esc_cheatnew',
                            json_encode($mqData),
                            $logID,
                            Yii::app()->params['emq']
                        );
                        $logIDCheatCheck = sprintf("ESC_CHEAT_CHECK%d%06d%02d", $curTime['sec'], $curTime['usec'], mt_rand(0, 99));
                        //由写DB改为写mq，不能保证原子性 2014.12.31
                        CEmqPublisher::send(
                            'cron_cheat_check',
                            'esc_cheat_new',
                            json_encode($mqData),
                            $logIDCheatCheck,
                            Yii::app()->params['emq']
                        );
                        //同时更新redis用户消费金额信息
                        ComAdQuotaV2::refundPush($oneCheatData['ad_user_id'], $oneCheatData['ad_plan_id'], $amount, $oneCheatData['click_time']);

                    } else {
                        $success++;
                    }
                }
            } // end while
        } // end foreach ($dateList as $oneDay)


        flock($lock_fp, LOCK_UN);
        fclose($lock_fp);
        printf("task %s end at %s, process %d success %d fail %d\n", $taskName, date('Y-m-d H:i:s'), $count, $success, $count-$success);
    }/*}}}*/

}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
