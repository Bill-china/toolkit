<?php
/**
 * 统计表里的关键词id补齐
 * 更新 ad_stats_keyword_click_{date}  表里的关键词id
 */
include 'KeywordRankCommand.php';
ini_set('memory_limit', '20480M');
class KeywordCommand extends CConsoleCommand {

    protected $_task_name = '';
    protected $_arrUserRouter = array();
    protected $_arrGroupTypes = array();
    protected $_arrGroupKws   = array();
    protected $_resMutexFile = null; //需要保证单进程执行时flock的文件句柄
//    protected $_sqlData = null;
    protected $_redis = null;


    /**
     * 更新 ad_stats_keyword_click_{date} 表里的关键词id
     */
    public function actionUpdateKwID($dbid, $date=null) {

        $this->_task_name = "upkwid_".$dbid."-".date("Ymd-His");
        if ($date == null) {
           $date = date('Y-m-d', strtotime('-3 hour'));
        }
        printf("task [%s] dbid[%d] date [%s] begin at %s\n", $this->_task_name, $dbid, $date, date('Y-m-d H:i:s'));

        $total      = 0;
        $success    = 0;

        $this->_redis = new ComRedis('report_keyword', 0);

        DbConnectionManager::init();

        // 用户点睛库路由缓存
        $this->_clearUserRouter();
        // 获取统计子库
        $dbStat = DbConnectionManager::getStatBranchDB($dbid);

        if ($dbStat===false) {
            printf("task [%s] get db fail, dbid[%d]\n", $this->_task_name, $dbid);
            return ;
        }
        // 查询 ad_stats_keyword_click_ 表
        $lastID = 0;
        $limit = 1000;
        $tableName = 'ad_stats_keyword_click_'.date('Ymd', strtotime($date));
        $sql = sprintf('select id, ad_user_id as uid, ad_group_id as gid, keyword as kw from %s where id>:id and ad_keyword_id=0 limit %d',
            $tableName, $limit
        );
        $cmd = $dbStat->createCommand($sql);
        while (true) {
            $cmd->bindParam(':id', $lastID);
            // echo "lastID : ".$lastID."\n";
            printf("task [%s] last id [%s]\n",
                        $this->_task_name, $lastID
            );
            $arrKeywordData = $cmd->queryAll();
            // var_dump($arrKeywordData);
            if (empty($arrKeywordData)) {
                break;
            }
            foreach ($arrKeywordData as $oneKwInfo) {
                try {
                    $total++;
                    $lastID = $oneKwInfo['id'];

                    $userID     = $oneKwInfo['uid'];
                    $groupID    = $oneKwInfo['gid'];
                    $kw         = $oneKwInfo['kw'];
                    // 获取用户路由
                    $djDBID = $this->_getUserRouter($userID);
                    if ($djDBID===false) {
                        printf("task [%s] can not get dbrouter user, id[%s] uid[%s] gid[%s] kw[%s]\n",
                            $this->_task_name, $oneKwInfo['id'], $userID, $groupID, $kw
                        );
                        continue;
                    }
                    $djDB = DbConnectionManager::getDjBranchDB($djDBID);
                    if ($djDB===false) {
                        printf("task [%s] can not get dianjing db, dbid[%s] id[%s] uid[%s] gid[%s] kw[%s]\n",
                            $this->_task_name, $djDBID, $oneKwInfo['id'], $userID, $groupID, $kw
                        );
                        continue;
                    }

                    // 获取组类型
                    $groupType = $this->_getGroupType($djDB, $groupID);
                    if ($groupType == false) {
                        printf("task [%s] can not get group type, dbid[%s] id[%s] uid[%s] gid[%s] kw[%s]\n",
                            $this->_task_name, $djDBID, $oneKwInfo['id'], $userID, $groupID, $kw
                        );
                        continue;
                    }

                    // 获取kwid
                    //$kwID = $this->_getKwID ($djDB, $groupID, $groupType, $kw);//效率低
                    $kwID = $this->_getKwIDSingle($djDB, $groupID, $groupType, $kw);
                    if (empty($kwID)) {
                        printf("task [%s] fail! id[%s] uid[%s] gid[%s] kw[%s]\n",
                            $this->_task_name, $oneKwInfo['id'], $userID, $groupID, $kw
                        );
                        continue;
                    }

                    //改为批量更新
                    /*
                    if(is_numeric($oneKwInfo['id']) && is_numeric($kwID)){
                        $where[] = $oneKwInfo['id']; 
                        $this->_sqlData .= " when {$oneKwInfo['id']} then {$kwID}";
                    }
                    */
                    
                    $this->_updateKwID($dbStat, $tableName, $oneKwInfo['id'], $kwID);
                    printf("task [%s] success! id[%s] uid[%s] gid[%s] kw[%s] kwid[%s]\n",
                        $this->_task_name, $oneKwInfo['id'], $userID, $groupID, $kw, $kwID
                    );
                    
                    $success++;
                } catch (Exception $e) {
                    if(strpos($e->getMessage(),'2006 MySQL server has gone away') || strpos($e->getMessage(),'2013 Lost connection')) {
                        $djDB->setActive(false);
                        $djDB->setActive(true);
                        $dbStat->setActive(false);
                        $dbStat->setActive(true);
                    }
                }
            }

            //批量更新 20160511
            /*
            if(!empty($where) && $this->_sqlData){
                $res = $this->_batchUpdateKwID($dbStat, $tableName, $where);
                if(!$res){
                    printf("task [%s] batchUpdate fail! KwInfoId&kwID[%s] \n",
                            $this->_task_name, $this->_sqlData 
                          );
                }
                $this->_sqlData = null;
            }
            */
            unset($arrKeywordData);
        }

        printf("task [%s] dbid[%d] date [%s] end at %s, process total %d success %d fail %d\n",
            $this->_task_name, $dbid, $date, date('Y-m-d H:i:s'), $total, $success, $total-$success
        );
    }

    /**
     * 更新 ad_stats_neighbor_click_{date} 表里的关键词id
     */
    public function actionUpdateNeighberKwID($dbid, $date=null) {
        $this->_task_name = "upkwid_".$dbid."-".date("Ymd-His");
        if ($date == null) {
           $date = date('Y-m-d', strtotime('-3 hour'));
        }
        printf("task [%s] dbid[%d] date [%s] begin at %s\n", $this->_task_name, $dbid, $date, date('Y-m-d H:i:s'));

        $total      = 0;
        $success    = 0;

        DbConnectionManager::init();

        // 用户点睛库路由缓存
        $this->_clearUserRouter();
        // 获取统计子库
        $dbStat = DbConnectionManager::getStatBranchDB($dbid);

        if ($dbStat===false) {
            printf("task [%s] get db fail, dbid[%d]\n", $this->_task_name, $dbid);
            return ;
        }
        // 查询 ad_stats_neighbor_click_{date} 表
        $lastID = 0;
        $limit = 10;
        $tableName = 'ad_stats_neighbor_click_'.date('Ymd', strtotime($date));
        $sql = sprintf('select id, ad_user_id as uid, ad_group_id as gid, keyword as kw from %s where id>:id and ad_keyword_id=0 limit %d',
            $tableName, $limit
        );
        $cmd = $dbStat->createCommand($sql);
        while (true) {
            $cmd->bindParam(':id', $lastID);
            // echo "lastID : ".$lastID."\n";
            $arrKeywordData = $cmd->queryAll();
            // var_dump($arrKeywordData);
            if (empty($arrKeywordData)) {
                break;
            }
            foreach ($arrKeywordData as $oneKwInfo) {
                $total++;
                $lastID = $oneKwInfo['id'];

                $userID     = $oneKwInfo['uid'];
                $groupID    = $oneKwInfo['gid'];
                $kw         = $oneKwInfo['kw'];
                // 获取用户路由
                $djDBID = $this->_getUserRouter($userID);
                if ($djDBID===false) {
                    printf("task [%s] can not get dbrouter user, id[%s] uid[%s] gid[%s] kw[%s]\n",
                        $this->_task_name, $oneKwInfo['id'], $userID, $groupID, $kw
                    );
                    continue;
                }
                $djDB = DbConnectionManager::getDjBranchDB($djDBID);
                if ($djDB===false) {
                    printf("task [%s] can not get dianjing db, dbid[%s] id[%s] uid[%s] gid[%s] kw[%s]\n",
                        $this->_task_name, $djDBID, $oneKwInfo['id'], $userID, $groupID, $kw
                    );
                    continue;
                }

                // 获取组类型
                $groupType = $this->_getGroupType($djDB, $groupID);
                if ($groupType == false) {
                    printf("task [%s] can not get group type, dbid[%s] id[%s] uid[%s] gid[%s] kw[%s]\n",
                        $this->_task_name, $djDBID, $oneKwInfo['id'], $userID, $groupID, $kw
                    );
                    continue;
                }

                // 获取kwid
                //$kwID = $this->_getKwID ($djDB, $groupID, $groupType, $kw); 
                $kwID = $this->_getKwIDSingle($djDB, $groupID, $groupType, $kw);
                if (empty($kwID)) {
                    printf("task [%s] fail! id[%s] uid[%s] gid[%s] kw[%s] kwid[%s]\n",
                        $this->_task_name, $oneKwInfo['id'], $userID, $groupID, $kw, $kwID
                    );
                    continue;
                }
                $this->_updateKwID($dbStat, $tableName, $oneKwInfo['id'], $kwID);
                printf("task [%s] success! id[%s] uid[%s] gid[%s] kw[%s] kwid[%s]\n",
                    $this->_task_name, $oneKwInfo['id'], $userID, $groupID, $kw, $kwID
                );
                $success++;
            }
        }
        printf("task [%s] dbid[%d] date [%s] end at %s, process total %d success %d fail %d\n",
            $this->_task_name, $dbid, $date, date('Y-m-d H:i:s'), $total, $success, $total-$success
        );
    }

    /**
     * 更新 ad_click_area_keyword_{date} 表里的关键词id
     */
    public function actionUpdateAreaKwID ($dbid, $date=null) {
        $this->_task_name = "updateareakwid_".$dbid."-".date("Ymd-His");

        // 避免内存浪费，每次只运行一个任务
        $strLockFileName = __METHOD__ . "::updateareakwid_{$dbid}";
        if (! $this->_getMutex($strLockFileName)) {
            printf("task [%s] get mutex fail, already has run, quit\n", $this->_task_name);
            return;
        }

        if ($date == null) {
           $date = date('Y-m-d', strtotime('-3 hour'));
        }
        printf("task [%s] dbid[%d] date [%s] begin at %s\n", $this->_task_name, $dbid, $date, date('Y-m-d H:i:s'));

        $total      = 0;
        $success    = 0;

        DbConnectionManager::init();

        // 用户点睛库路由缓存
        $this->_clearUserRouter();
        // 获取地域关键词子库
        $dbStat = DbConnectionManager::getAreakwDB($dbid);

        if ($dbStat===false) {
            printf("task [%s] get db fail, dbid[%d]\n", $this->_task_name, $dbid);
            return ;
        }
        // 查询 ad_stats_keyword_click_ 表
        $lastID = 0;
        $limit = 1000;
        $tableName = 'ad_click_area_keyword_'.date('Ymd', strtotime($date));
        $sql = sprintf('select id, ad_user_id as uid, ad_group_id as gid, keyword as kw from %s where id>:id and ad_keyword_id=0 limit %d',
            $tableName, $limit
        );
        $cmd = $dbStat->createCommand($sql);
        while (true) {
            $cmd->bindParam(':id', $lastID);
            // echo "lastID : ".$lastID."\n";
            printf("task [%s] last id [%s]\n",
                        $this->_task_name, $lastID
            );
            $arrKeywordData = $cmd->queryAll();
            // var_dump($arrKeywordData);
            if (empty($arrKeywordData)) {
                break;
            }
            foreach ($arrKeywordData as $oneKwInfo) {
                $total++;
                $lastID = $oneKwInfo['id'];

                $userID     = $oneKwInfo['uid'];
                $groupID    = $oneKwInfo['gid'];
                $kw         = $oneKwInfo['kw'];
                // 获取用户路由
                $djDBID = $this->_getUserRouter($userID);
                if ($djDBID===false) {
                    printf("task [%s] can not get dbrouter user, id[%s] uid[%s] gid[%s] kw[%s]\n",
                        $this->_task_name, $oneKwInfo['id'], $userID, $groupID, $kw
                    );
                    continue;
                }
                $djDB = DbConnectionManager::getDjBranchDB($djDBID);
                if ($djDB===false) {
                    printf("task [%s] can not get dianjing db, dbid[%s] id[%s] uid[%s] gid[%s] kw[%s]\n",
                        $this->_task_name, $djDBID, $oneKwInfo['id'], $userID, $groupID, $kw
                    );
                    continue;
                }

                // 获取组类型
                $groupType = $this->_getGroupType($djDB, $groupID);
                if ($groupType == false) {
                    printf("task [%s] can not get group type, dbid[%s] id[%s] uid[%s] gid[%s] kw[%s]\n",
                        $this->_task_name, $djDBID, $oneKwInfo['id'], $userID, $groupID, $kw
                    );
                    continue;
                }

                // 获取kwid
                //$kwID = $this->_getKwID ($djDB, $groupID, $groupType, $kw);
                $kwID = $this->_getKwIDSingle($djDB, $groupID, $groupType, $kw);
                if (empty($kwID)) {
                    printf("task [%s] fail! id[%s] uid[%s] gid[%s] kw[%s]\n",
                        $this->_task_name, $oneKwInfo['id'], $userID, $groupID, $kw
                    );
                    continue;
                }
                $this->_updateKwID($dbStat, $tableName, $oneKwInfo['id'], $kwID);
                printf("task [%s] success! id[%s] uid[%s] gid[%s] kw[%s] kwid[%s]\n",
                    $this->_task_name, $oneKwInfo['id'], $userID, $groupID, $kw, $kwID
                );
                $success++;
            }
        }
        printf("task [%s] dbid[%d] date [%s] end at %s, process total %d success %d fail %d\n",
            $this->_task_name, $dbid, $date, date('Y-m-d H:i:s'), $total, $success, $total-$success
        );
    }

    /**
     * 更新 esc_stats_keyword_click_{date} 表里的关键词id
     */
    public function actionUpdateEscKwID($dbid, $date=null) {
        $this->_task_name = "upesckwid_".$dbid."-".date("Ymd-His");
        $strLockFileName = __METHOD__ . "::upesckwid_{$dbid}";
        if (! $this->_getMutex($strLockFileName)) {
            echo date('Y-m-d H:i:s') . "\tok\t{$strLockFileName} already running\n";
            return;
        }
        if ($date == null) {
            $date = date('Y-m-d', strtotime('-1 day'));
        }
        printf("task [%s] dbid[%d] date [%s] begin at %s\n", $this->_task_name, $dbid, $date, date('Y-m-d H:i:s'));

        $total      = 0;
        $success    = 0;

        DbConnectionManager::init();

        // 用户点睛库路由缓存
        $this->_clearUserRouter();
        // 获取统计子库
        $dbStat = DbConnectionManager::getStatBranchDB($dbid);

        if ($dbStat===false) {
            printf("task [%s] get db fail, dbid[%d]\n", $this->_task_name, $dbid);
            return ;
        }
        // 查询 ad_stats_keyword_click_ 表
        $lastID = 0;
        $limit = 1000;
        $tableName = 'esc_stats_keyword_click_'.date('Ymd', strtotime($date));
        $sql = sprintf('select id, ad_user_id as uid, ad_group_id as gid, keyword as kw from %s where id>:id and ad_keyword_id=0 limit %d',
                $tableName, $limit
        );
        $cmd = $dbStat->createCommand($sql);
        while (true) {
            $cmd->bindParam(':id', $lastID);
            // echo "lastID : ".$lastID."\n";
            printf("task [%s] last id [%s]\n",
            $this->_task_name, $lastID
            );
            $arrKeywordData = $cmd->queryAll();
            // var_dump($arrKeywordData);
            if (empty($arrKeywordData)) {
                break;
            }
            foreach ($arrKeywordData as $oneKwInfo) {
                $total++;
                $lastID = $oneKwInfo['id'];

                $userID     = $oneKwInfo['uid'];
                $groupID    = $oneKwInfo['gid'];
                $kw         = $oneKwInfo['kw'];
                // 获取用户路由
                $djDBID = $this->_getUserRouter($userID);
                if ($djDBID===false) {
                    printf("task [%s] can not get dbrouter user, id[%s] uid[%s] gid[%s] kw[%s]\n",
                    $this->_task_name, $oneKwInfo['id'], $userID, $groupID, $kw
                    );
                    continue;
                }
                $djDB = DbConnectionManager::getDjBranchDB($djDBID);
                if ($djDB===false) {
                    printf("task [%s] can not get dianjing db, dbid[%s] id[%s] uid[%s] gid[%s] kw[%s]\n",
                    $this->_task_name, $djDBID, $oneKwInfo['id'], $userID, $groupID, $kw
                    );
                    continue;
                }

                // 获取组类型
                $groupType = $this->_getGroupType($djDB, $groupID);
                if ($groupType == false) {
                    printf("task [%s] can not get group type, dbid[%s] id[%s] uid[%s] gid[%s] kw[%s]\n",
                    $this->_task_name, $djDBID, $oneKwInfo['id'], $userID, $groupID, $kw
                    );
                    continue;
                }

                // 获取kwid
                //$kwID = $this->_getKwID ($djDB, $groupID, $groupType, $kw);
                $kwID = $this->_getKwIDSingle($djDB, $groupID, $groupType, $kw);
                if(empty($kwID)) {
                    printf("task [%s] fail! id[%s] uid[%s] gid[%s] kw[%s]\n",
                    $this->_task_name, $oneKwInfo['id'], $userID, $groupID, $kw
                    );
                    continue;
                }
                $this->_updateKwID($dbStat, $tableName, $oneKwInfo['id'], $kwID);
                printf("task [%s] success! id[%s] uid[%s] gid[%s] kw[%s] kwid[%s]\n",
                $this->_task_name, $oneKwInfo['id'], $userID, $groupID, $kw, $kwID
                );
                $success++;
            }
        }
        printf("task [%s] dbid[%d] date [%s] end at %s, process total %d success %d fail %d\n",
        $this->_task_name, $dbid, $date, date('Y-m-d H:i:s'), $total, $success, $total-$success
        );
    }

    /**
     * 更新 esc_stats_neighbor_click_{date} 表里的关键词id
     */
    public function actionUpdateEscNeighberKwID($dbid, $date=null) {
        $this->_task_name = "upesckwid_".$dbid."-".date("Ymd-His");
        $strLockFileName = __METHOD__ . "::upesckwid_{$dbid}";
        if (! $this->_getMutex($strLockFileName)) {
            echo date('Y-m-d H:i:s') . "\tok\t{$strLockFileName} already running\n";
            return;
        }
        if ($date == null) {
            $date = date('Y-m-d', strtotime('-1 day'));
        }
        printf("task [%s] dbid[%d] date [%s] begin at %s\n", $this->_task_name, $dbid, $date, date('Y-m-d H:i:s'));

        $total      = 0;
        $success    = 0;

        DbConnectionManager::init();

        // 用户点睛库路由缓存
        $this->_clearUserRouter();
        // 获取统计子库
        $dbStat = DbConnectionManager::getStatBranchDB($dbid);

        if ($dbStat===false) {
            printf("task [%s] get db fail, dbid[%d]\n", $this->_task_name, $dbid);
            return ;
        }
        // 查询 ad_stats_neighbor_click_{date} 表
        $lastID = 0;
        $limit = 10;
        $tableName = 'esc_stats_neighbor_click_'.date('Ymd', strtotime($date));
        $sql = sprintf('select id, ad_user_id as uid, ad_group_id as gid, keyword as kw from %s where id>:id and ad_keyword_id=0 limit %d',
                $tableName, $limit
        );
        $cmd = $dbStat->createCommand($sql);
        while (true) {
            $cmd->bindParam(':id', $lastID);
            // echo "lastID : ".$lastID."\n";
            $arrKeywordData = $cmd->queryAll();
            // var_dump($arrKeywordData);
            if (empty($arrKeywordData)) {
                break;
            }
            foreach ($arrKeywordData as $oneKwInfo) {
                $total++;
                $lastID = $oneKwInfo['id'];

                $userID     = $oneKwInfo['uid'];
                $groupID    = $oneKwInfo['gid'];
                $kw         = $oneKwInfo['kw'];
                // 获取用户路由
                $djDBID = $this->_getUserRouter($userID);
                if ($djDBID===false) {
                    printf("task [%s] can not get dbrouter user, id[%s] uid[%s] gid[%s] kw[%s]\n",
                    $this->_task_name, $oneKwInfo['id'], $userID, $groupID, $kw
                    );
                    continue;
                }
                $djDB = DbConnectionManager::getDjBranchDB($djDBID);
                if ($djDB===false) {
                    printf("task [%s] can not get dianjing db, dbid[%s] id[%s] uid[%s] gid[%s] kw[%s]\n",
                    $this->_task_name, $djDBID, $oneKwInfo['id'], $userID, $groupID, $kw
                    );
                    continue;
                }

                // 获取组类型
                $groupType = $this->_getGroupType($djDB, $groupID);
                if ($groupType == false) {
                    printf("task [%s] can not get group type, dbid[%s] id[%s] uid[%s] gid[%s] kw[%s]\n",
                    $this->_task_name, $djDBID, $oneKwInfo['id'], $userID, $groupID, $kw
                    );
                    continue;
                }

                // 获取kwid
                //$kwID = $this->_getKwID ($djDB, $groupID, $groupType, $kw);
                $kwID = $this->_getKwIDSingle($djDB, $groupID, $groupType, $kw);
                if (empty($kwID)) {
                    printf("task [%s] fail! id[%s] uid[%s] gid[%s] kw[%s] kwid[%s]\n",
                    $this->_task_name, $oneKwInfo['id'], $userID, $groupID, $kw, $kwID
                    );
                    continue;
                }
                $this->_updateKwID($dbStat, $tableName, $oneKwInfo['id'], $kwID);
                printf("task [%s] success! id[%s] uid[%s] gid[%s] kw[%s] kwid[%s]\n",
                $this->_task_name, $oneKwInfo['id'], $userID, $groupID, $kw, $kwID
                );
                $success++;
            }
        }
        printf("task [%s] dbid[%d] date [%s] end at %s, process total %d success %d fail %d\n",
        $this->_task_name, $dbid, $date, date('Y-m-d H:i:s'), $total, $success, $total-$success
        );
    }

    /**************
     *  私有方法  *
     **************/
    // 清理用户路由
    protected function _clearUserRouter () {
        $this->_arrUserRouter = array();
    }
    // 获取用户路由
    protected function _getUserRouter ($userID) {
        if (!isset($this->_arrUserRouter[$userID])) {
            $dbCenter = DbConnectionManager::getDjCenterDB();
            if ($dbCenter===false) {
                printf("task [%s] get dianjing center db fail\n");
                return false;
            }
            $sql = 'select db_id from edc_db_router where user_id=:user_id';
            $cmd = $dbCenter->createCommand($sql);
            $cmd->bindParam(':user_id', $userID);
            $ret = $cmd->queryRow();
            // var_dump($ret);
            /*
            array(1) {
              ["db_id"]=>
              string(1) "3"
            }
            */
            $this->_arrUserRouter[$userID] = isset($ret['db_id']) ? $ret['db_id'] : false;
        }
        return $this->_arrUserRouter[$userID];
    }
    // 清理组类型缓存
    protected function _clearGroupType () {
        $this->_arrGroupTypes = array();
    }
    // 获取组类型
    protected function _getGroupType ($db, $groupID) {
        if (!isset($this->_arrGroupTypes[$groupID])) {
            try {
                $sql = 'select type from ad_group where id=:id';
                $ret = $db->createCommand($sql)->bindParam(':id', $groupID)->queryRow();
            } catch (Exception $e) {
                if(strpos($e->getMessage(),'2006 MySQL server has gone away') || strpos($e->getMessage(),'2013 Lost connection'))
                {
                    $db->setActive(false);
                    $db->setActive(true);
                    $sql = 'select type from ad_group where id=:id';
                    $ret = $db->createCommand($sql)->bindParam(':id', $groupID)->queryRow();
                }
            }
            $this->_arrGroupTypes[$groupID] = isset($ret['type']) ? $ret['type'] : false;
        }
        return $this->_arrGroupTypes[$groupID];
    }
    // 清理组关键词缓存
    protected function _clearGroupKw () {
        $this->_arrGroupKws = array();
    }

    // 获取关键词id
    protected function _getKwID ($db, $groupID, $groupType, $kw) {

        $redis = new ComRedis('report_keyword', 0);
        $keyword_key = md5($groupID.$kw);
        $keyword_val = $redis->get($keyword_key);
        if(false === $keyword_val){
             if ($groupType == 6 || $groupType == 7 || $groupType == 10 || $groupType==12) { // 商品搜索api或者手机助手或者游戏如意
                // 12 表示是如意
                $sql = 'select keyword as kw, id from ad_search_keywords_commodity where ad_group_id=:gid';
            } else {
                $sql = 'select keyword as kw, id from ad_search_keywords where ad_group_id=:gid';
            }
            $cmd = $db->createCommand($sql);
            $cmd->bindParam(':gid', $groupID);
            $ret = $cmd->queryAll();
            // var_dump($ret);
            /*
                array(2) {
                  [0]=>
                  array(2) {
                    ["kw"]=>
                    string(27) "那拉提机场天气实况"
                    ["id"]=>
                    string(8) "98765295"
                  }
                  [1]=>
                  array(2) {
                    ["kw"]=>
                    string(30) "伊犁州那拉提天气预报"
                    ["id"]=>
                    string(8) "98765296"
                  }
                }
            */
             if (!empty($ret)) {
                foreach ($ret as $oneItem) {
                    $key = md5($groupID.$oneItem['kw']);
                    $redis->setex($key,172800,$oneItem['id']);
                }
            }          
        }

        return !empty($keyword_val) ? $keyword_val : $this->_getKwIDSingle($db, $groupID, $groupType, $kw);
    }

    // 避免大小写问题
    protected function _getKwIDSingle($db, $groupID, $groupType, $kw) {

        if(!empty($this->_redis)){
            $keyword_key = md5($groupID.$kw);
            $keyword_val = $this->_redis->get($keyword_key);
            if(!empty($keyword_val)){
                $this->_cache++;
                $this->_redis->setex($keyword_key,172800,$keyword_val);
                return $keyword_val;
            }else{
                $this->_notCache++;
                return $this->_getDbKwIDSingle($db, $groupID, $groupType, $kw);
            }
        }else{ 
            $this->_notCache++;
            return $this->_getDbKwIDSingle($db, $groupID, $groupType, $kw);
        }
    }

    protected function _getDbKwIDSingle($db, $groupID, $groupType, $kw) {

        $keyword_key = md5($groupID.$kw);
        if ($groupType == 6 || $groupType == 7 || $groupType == 10 || $groupType == 12)  {
            // 商品搜索api或者手机助手或者游戏如意
            $sql = 'select id from ad_search_keywords_commodity where ad_group_id=:gid and keyword=:keyword';
        } else {
            $sql = 'select id from ad_search_keywords where ad_group_id=:gid and keyword=:keyword';
        }
        $cmd = $db->createCommand($sql);
        $cmd->bindParam(':gid', $groupID);
        $cmd->bindParam(':keyword', $kw, PDO::PARAM_STR);
        $ret = $cmd->queryRow();
        if($ret['id']){
            if($this->_redis){
                $this->_redis->setex($keyword_key,172800,$ret['id']);
            }
            return $ret['id'];
        }else{
            return false;
        }
    }

    // 更新关键词id
    protected function _updateKwID ($statDb, $tableName, $id, $kwID) {
        $sql = 'update '.$tableName.' set ad_keyword_id=:kwid where id=:id';
        $cmd = $statDb->createCommand($sql);
        $cmd->bindParam(':kwid', $kwID);
        $cmd->bindParam(':id', $id);
        return $cmd->execute();
    }

    // 批量更新关键词id
    protected function _batchUpdateKwID ($statDb, $tableName,$where) {
        $strWhere = implode(',',$where);
        $sql = 'update '.$tableName.' set ad_keyword_id = case id ';
        $sql .= $this->_sqlData . ' end where id in (' . $strWhere .')';
        $cmd = $statDb->createCommand($sql);
        return $cmd->execute();
    }

    /**
     * 使用锁机制保证单进程执行任务
     *
     * @param string $strFilename 进行flock操作的文件
     * @return boolean 加锁成功返回true，否则false
     */
    protected function _getMutex($strFilename) {
    	$bolRet = false;
    	if ($strFilename == '') {
    		return $bolRet;
    	}
    	if (isset($GLOBALS['dbID'])) {
    		$strFilename .= "_{$GLOBALS['dbID']}";
    	}
    	$strMutexFile = Yii::app()->runtimePath . "/crontab_lock_{$strFilename}.txt";
    	$resFile = fopen($strMutexFile, 'a');
    	if ($resFile) {
    		$bolRet = flock($resFile, LOCK_EX | LOCK_NB);
    		if ($bolRet) {
    			$this->_resMutexFile = $resFile;
    		} else {
    			fclose($resFile);
    		}
    	}

    	return $bolRet;
    }

    protected function afterAction($action,$params)
    {
    	if ($this->_resMutexFile) {
    		flock($this->_resMutexFile, LOCK_UN);
    		fclose($this->_resMutexFile);
    		$this->_resMutexFile = null;
    	}
    	parent::afterAction($action, $params);
    }

    public function actionRankKeywordUpdateBatch() {
        printf("task [%s] begin at %s\n", __FUNCTION__, date('Y-m-d H:i:s'));
        $redisKeyword =new ComRedis('rank');
        $base = 5000;
        for($i=1; $i<10; $i++) {
            $djBranchDB = DbConnectionManager::getDjBranchDB($i);
            if(!$djBranchDB)
                continue;
            $limit = 0;
            $loop = true;
            do {
                $sql = sprintf("select gid,keyword  from ad_ranking where status=1 limit %s,%s", $limit, $base);
                $keywords=$djBranchDB->createCommand($sql)->queryAll();
                printf("task [%s] get db[%s] limit[%s,%s] at %s\n", __FUNCTION__, $i, $limit, count($keywords), date('Y-m-d H:i:s'));
                if(count($keywords)<$base) {
                    $loop = false;
                }
                foreach($keywords as $keyword) {
                    $key = KeywordRankCommand::REDIS_RANK_CONF . '_' . md5($keyword['gid'] . '_' . $keyword['keyword']);
                    $redisKeyword->set($key, '');
                }

                $limit += $base;
            }while($loop);
        }
        printf("task [%s] end at %s\n", __FUNCTION__, date('Y-m-d H:i:s'));
    }
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

