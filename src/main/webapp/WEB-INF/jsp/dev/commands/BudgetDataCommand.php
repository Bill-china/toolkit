<?php
// fuck goverment
class BudgetDataCommand extends CConsoleCommand {

    protected $_task_name       = '';

    // 实时消费的redis
    protected $_consume_redis        = null;

    protected $_consume_redis_conf   = array();

    protected $_consume_is_connected = false;


    // 保存用户信息的redis
    protected $_user_redis        = array();

    protected $_user_redis_conf   = array();

    protected $_user_is_connected = array();

    protected $_user_redis_num    = 0;

    private $_resMutexFile = null;

    /**
     * 处理单个文件
     */
    public function actionGetDataFromOneFile ($time) {
        ini_set('memory_limit', '20480M');

        $beginTime = date('Ymd_His');
        $task_name = sprintf('[GetDataFromOneFile_%s]', $beginTime);
        $this->_task_name = $task_name;
        printf("%s begin\n", $task_name);

        if (is_null($time)) {
            $time = time();
        } else{
            $time = strtotime($time);
        }
        $fileName = 'statsLog.'.date('YmdHi', $time);

        $this->_processOneFile($fileName);

        $endTime = date('Y-m-d H:i:s');
        printf("%s begin at %s end at %s\n", $task_name, $beginTime, $endTime);
    }

    /**
     * 处理mv单个文件
     */
    public function actionProcessOneMvFile ($fileName) {
        ini_set('memory_limit', '20480M');

        $beginTime = time();
        $this->_task_name = $task_name = sprintf('[ProcessOneMvFile_%s]', date('Y-m-d H:i:s', $beginTime));
        printf("%s begin\n", $task_name);


        $this->_processOneMvFile($fileName);

        $endTime = time();
        printf("%s begin at %s end at %s\n",
            $task_name,
            date('Y-m-d H:i:s', $beginTime),
            date('Y-m-d H:i:s', $endTime)
        );
    }

    /**
     * 处理整个 mv 目录
     * 源文件      'mediav_click_res'      => '/data/log/mediav/click_res/'
     * 标记文件    'budgetLogPath'         => '/data/log/statsLog/budget/',
     * 锁目录      'budgetLockPath'        => '/data/log/statsLog/budget/lock/',
     */
    public function actionProcessMvDir () {
        ini_set('memory_limit', '20480M');
        $beginTime = time();
        $this->_task_name = $task_name = sprintf('[ProcessMvDir %s]', date('Y-m-d H:i:s', $beginTime));
        printf("%s begin\n", $task_name);

        // 获取文件列表
        $dirPath = Config::item('mediav_click_res');
        $dh = opendir($dirPath);
        if (!$dh) {
            printf("%s open dir[%s] fail\n", $task_name, $dirPath);
            return ;
        }

        $finishPath = Config::item('budgetLogPath');
        $arrFileList = array();
        while (($_oneFile = readdir($dh)) !== false) {
            if ($_oneFile=='.' || $_oneFile=='..') {
                continue;
            }
            if (is_dir($dirPath.$_oneFile)) {
                continue;
            }
            if (!file_exists($dirPath.$_oneFile.'.ok')) {
                continue;
            }
            if (file_exists($finishPath.$_oneFile.'.finish')) {
                continue;
            }
            $arrFileList[] = $_oneFile;
        }
        closedir($dh);

        $sucessFile = $failFile = 0;
        if (!empty($arrFileList)) {
            sort($arrFileList);
            foreach ($arrFileList as $_oneFile) {
                if ($this->_processOneMvFile($_oneFile) ) {
                    $sucessFile++;
                } else {
                    $failFile++;
                }
            }
        }

        $endTime = time();
        printf("%s begin at %s end at %s, file sucess %d, fail %d\n",
            $task_name,
            date('Y-m-d H:i:s', $beginTime),
            date('Y-m-d H:i:s', $endTime),
            $sucessFile,
            $failFile
        );
    }

    /**
     * 处理整个目录
     * 源文件      'statlogend'            => '/data/log/e/stats/',
     * 标记文件    'budgetLogPath'         => '/data/log/statsLog/budget/',
     * 锁目录      'budgetLockPath'        => '/data/log/statsLog/budget/lock/',
     */
    public function actionGetDataFromDir () {
        ini_set('memory_limit', '20480M');
        $beginTime = time();
        $task_name = sprintf('[GetDataFromDir-%s]', date('Ymd_His'));
        $this->_task_name = $task_name;
        printf("%s begin\n", $task_name);

        // 获取文件列表
        $dirPath = Config::item('statlogend');
        $dh = opendir($dirPath);
        if (!$dh) {
            printf("%s open dir[%s] fail\n", $task_name, $dirPath );
            return ;
        }

        $finishPath = Config::item('budgetLogPath');
        $arrFileList = array();
        while (($_oneFile = readdir($dh)) !== false) {
            if ($_oneFile=='.' || $_oneFile=='..') {
                continue;
            }
            if (is_dir($dirPath.$_oneFile)) {
                continue;
            }
            if (file_exists($finishPath.$_oneFile.'.finish')) {
                continue;
            }
            $arrFileList[] = $_oneFile;
        }
        closedir($dh);

        if (!empty($arrFileList)) {
            sort($arrFileList);
            foreach ($arrFileList as $_oneFile) {
                $this->_processOneFile($_oneFile);
            }
        }

        $endTime = time();
        date('Y-m-d H:i:s');
        printf("%s begin at %s end at %s\n", $task_name, date('Y-m-d H:i:s', $beginTime), date('Y-m-d H:i:s', $endTime));
    }

    /**
     * 从redis里读取cheat的信息
     */
    public function actionGetDataFromRedis() {
        ini_set('memory_limit', '20480M');

        $beginTime = time();
        $task_name = sprintf('[GetDataFromRedis_%s]', date('Y-m-d H:i', $beginTime));
        $this->_task_name = $task_name;
        printf("%s begin at %s\n", $task_name, date('Y-m-d H:i:s', $beginTime) );

        $userFail = $userSuccess = 0;
        $planFail = $planSuccess = 0;
        $cnn = Yii::app()->db_quota;
        while (true) {
            $arrDatas = ComConsume::pop(500);
            if ($arrDatas==false || empty($arrDatas)) {
                break;
            }
            $arrMsg = array();
            foreach ($arrDatas as $oneData) {
                $_tmp = json_decode($oneData, true);
                if (is_null($_tmp)) {
                    printf("%s invalid json[%s]\n", $task_name, $oneData);
                    continue;
                }
                $arrMsg[] = $_tmp;
            }
            if (empty($arrMsg)) {
                continue;
            }

            foreach ($arrMsg as $oneData) {
                $ad_user_id = $userID = $oneData['u'];
                // 获取用户quota
                //$userInfo = ComAdQuotaV2::getUserDJQuotaInfo($userID);
                $table_name = 'ad_user_quota_'.$ad_user_id%10;
                $sql = sprintf("select  *  from  %s where ad_user_id=%d ", $table_name,$ad_user_id);
                $user_quota_arr = $cnn->createCommand($sql)->queryRow();
                // var_dump($userInfo);
                if (empty($user_quota_arr)) {
                    printf("%s get user[%s] quota info fail!\n", $task_name, $userID);
                    $userFail++;
                    continue;
                }
                $djCost =$user_quota_arr['dj_cost'];
                $mvCost =$user_quota_arr['mv_cost'];

                $yesterday_dj_cost =$user_quota_arr['yesterday_dj_cost'];
                $yesterday_mv_cost =$user_quota_arr['yesterday_mv_cost'];

                $userbudget = $user_quota_arr['dj_quota'];
                //计划
                $table_name_plan = 'ad_plan_quota_'.$ad_user_id%10;
                $sql_plan = sprintf("select  *  from  %s where ad_user_id=%d ", $table_name_plan,$ad_user_id);
                $plan_quota_arrs = $cnn->createCommand($sql_plan)->queryAll();
                $plan_use_arr = array();
                if(!empty($plan_quota_arrs)){
                    foreach ($plan_quota_arrs as $plan_quota_arr) {
                        $ad_plan_id = $plan_quota_arr['ad_plan_id'];
                        $plan_quota = $plan_quota_arr['plan_quota'];
                        $plan_cost = $plan_quota_arr['plan_cost'];

                        $plan_use_arr[$ad_plan_id] = array(
                            'plan_quota' => $plan_quota,
                            'plan_cost' =>$plan_cost
                        );
                    }

                }
                //////
                // 获取用户cost
                //$costInfo   = ComAdQuotaV2::getUserDJCostInfo(date('j'), $userID);
                //$mvCost     = ComAdQuotaV2::getUserMediavCost(date('j'), $userID);
                // var_dump($costInfo);
                $cheat_deal_time = isset($oneData['t'])?$oneData['t']:0;
                $cid = isset($oneData['cid'])?$oneData['cid']:0;
                $clickprice = isset($oneData['clickprice'])?$oneData['clickprice']:0;
                if (!empty($oneData['p'])) {

                    foreach ($oneData['p'] as $planID) {
                        if (!isset($plan_use_arr[$planID])) {
                            printf("%s get plan quota info fail, uid[%d] pid[%d]\n", $task_name, $userID, $planID);
                            $planFail++;
                            continue;
                        }
                        $data = array(
                            'planbudget'    => $plan_use_arr[$planID]['plan_quota'],
                            'plancost'      => $plan_use_arr[$planID]['plan_cost']>0 ? $plan_use_arr[$planID]['plan_cost'] : 0,
                            'cheatdealtime' => $cheat_deal_time,
                            'cid'           => $cid,
                            'clickprice'    => $clickprice,
                        );
                        ComBudgetData::sendOnePlanConsumeData($userID, $planID, $data, ComBudgetData::MSG_TYPE_CHEAT);
                        $planSuccess++;
                    }
                }

                //$djCost   = isset($costInfo['cost']) ? $costInfo['cost'] : 0;
                $userCost = $djCost + $mvCost + $yesterday_dj_cost + $yesterday_mv_cost;
                $userTodayCost = round($djCost,2);
                $balance =  round($user_quota_arr['balance'] - $userCost,2);
                if ($balance<0) {
                    $balance = 0;
                }
                $data = array(
                    'balance'       => $balance,
                    'userbudget'    => $userbudget,
                    'usercost'      => $userTodayCost,
                    'cheatdealtime' => $cheat_deal_time,
                    'cid'           => $cid,
                    //'clickprice'    => $clickprice,
                );
                ComBudgetData::sendOneUserConsumeData($userID, $data, ComBudgetData::MSG_TYPE_CHEAT);
                $userSuccess++;
            }
        }

        $endTime = time();
        printf("%s process, begin at %s, end at %s, use %d sedcods, user success %d fail %d, plan success %d fail %d\n",
            $task_name,
            date('Y-m-d H:i:s', $beginTime),
            date('Y-m-d H:i:s', $endTime),
            $endTime - $beginTime,
            $userSuccess, $userFail,
            $planSuccess, $planFail
        );
    }

    /**
     * 清理finish文件，三小时运行一次
     */
    public function actionClearFinishFiles () {
        $this->_task_name = $task_name = sprintf('[ClearFinishFiles_%s]', date('Ymd_His'));
        $beginTime = time();

        printf("%s begin\n", $task_name);

        $finishPath = Config::item('budgetLogPath');
        $dh = opendir($finishPath);
        if (!$dh) {
            printf("%s can not open dir[%s]\n", $this->_task_name, $finishPath);
            return ;
        }

        $dataDirPath    = Config::item('statlogend');
        $mvDataDirPath  = Config::item('mediav_click_res');
        $arrFileList = array();
        while (($_oneFile = readdir($dh)) !== false) {
            if ($_oneFile=='.' || $_oneFile=='..') {
                continue;
            }
            if (is_dir($finishPath.$_oneFile)) {
                continue;
            }
            if (substr($_oneFile, -7)!=='.finish') {
                continue;
            }
            $fileName = substr($_oneFile, 0, strlen($_oneFile) - 7);
            $dataFile = $dataDirPath.substr($_oneFile, 0, strlen($_oneFile) - 7);
            if (file_exists($dataDirPath.$fileName) || file_exists($mvDataDirPath.$fileName)) {
                continue;
            }
            $arrFileList[] = $_oneFile;
        }
        closedir($dh);
        if (!empty($arrFileList)) {
            foreach ($arrFileList as $_oneFinishFile) {
                printf("%s del finish file[%s]\n", $this->_task_name, $_oneFinishFile);
                @unlink($finishPath.$_oneFinishFile);
            }
        }
        // 日志
        $endTime = time();
        printf("%s begin at %s end at %s, process %d files\n",
            $this->_task_name,
            date('Y-m-d H:i:s', $beginTime), date('Y-m-d H:i:s', $endTime),
            count($arrFileList)
        );
    }

    // 加锁
    private function _mutex($fileName) {
        $bolRet = false;
        if ($fileName == '') {
            return $bolRet;
        }

        $strMutexFile = Config::item('budgetLockPath').$fileName.'.lock';
        $resFile = @fopen($strMutexFile, 'a');
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

    // 解锁
    private function _unmutex($fileName) {
        fclose($this->_resMutexFile);
        $strMutexFile = Config::item('budgetLockPath').$fileName.'.lock';
        @unlink($strMutexFile);
    }

    // 处理单个文件
    private function _processOneFile ($fileName) {
        // 检查是否完成了
        $finishFile = Config::item('budgetLogPath').$fileName.'.finish';
        if (file_exists($finishFile)) {
            printf("%s file[%s] already done\n", $this->_task_name, $fileName);
            return ;
        }

        $dataFilePath = Config::item('statlogend').$fileName;
        $fh = @fopen($dataFilePath, 'r'); // If the open fails, an error of level E_WARNING is generated.
        if (!$fh) {
            printf("%s can not open file[%s]\n", $this->_task_name, $dataFilePath);
            return ;
        }
        // 加锁
        if ($this->_mutex($fileName)==false) {
            printf("%s get mutex fail, filename[%s]\n", $this->_task_name, $fileName);
            fclose($fh);
            return;
        }
        $beginTime  = time();

        $clickFail = $clickSuccess = 0;
        $userPlanList = array();
        while (false !== ($strLine = fgets($fh, 102400))) {
            $strLine = trim($strLine);
            if ($strLine == '') {
                continue;
            }
            list ($saveTime, $type, $content) = explode("\t", $strLine);
            if ($type!='esc_click') { // 只做点击类
                // echo $strLine."\n";
                continue;
            }
            $data = json_decode($content, true);
            if (is_null($data)) {
                printf("%s json_decode fail, file[%s] line[%s]\n",
                    $this->_task_name, $dataFilePath, $strLine
                );
                $clickFail++;
                continue;
            }
            if ($data['ver']!='sou') { // 只做搜索类
                continue;
            }
            $userID = intval($data['uid']); $planID = intval($data['pid']);
            // 发送每一条消费数据
            $arrInfo = array(
                'query'         => $data['query'],          // 用户搜索串
                'src'           => $data['src'],            // 来源
                'keyword'       => $data['keyword'],
                'clickprice'    => $data['clickPrice'],     // 点击价格(扣费)
                'matchtype'     => $data['matchtype'],
                'channel_id'    => $data['channel_id'],
                'place_id'      => $data['place_id'],
                'clicktime'     => $data['now'],            // 点击时间
                'settletime'    => isset($data['settleTime'])?$data['settleTime']:0,     // 计价时间
                'gspprice'      => $data['price'],     //引擎出价
            );
            ComBudgetData::sendOneConsumeData($userID, $planID, $arrInfo);

            if (!isset($userPlanList[$userID])) {
                $userPlanList[$userID] = array();
            }
            $userPlanList[$userID][$planID] = 1;

            $clickSuccess++;
        }

        // 发送每一个计划及用户的消费
        $planSuccess = $planFail = $userSuccess = $userFail = 0;
        $cnn = Yii::app()->db_quota;
        if (!empty($userPlanList)) {
            foreach ($userPlanList as $userID => $planList) {
                // 获取用户quota
                //$userInfo = ComAdQuotaV2::getUserDJQuotaInfo($userID);

                $ad_user_id = $userID;
                $table_name = 'ad_user_quota_'.$ad_user_id%10;
                $sql = sprintf("select  *  from  %s where ad_user_id=%d ", $table_name,$ad_user_id);
                $user_quota_arr = $cnn->createCommand($sql)->queryRow();
                // var_dump($userInfo);
                if (empty($user_quota_arr)) {
                    printf("%s get user[%s] quota info fail!\n", $task_name, $userID);
                    $userFail++;
                    continue;
                }
                $djCost =$user_quota_arr['dj_cost'];
                $mvCost =$user_quota_arr['mv_cost'];

                $yesterday_dj_cost =$user_quota_arr['yesterday_dj_cost'];
                $yesterday_mv_cost =$user_quota_arr['yesterday_mv_cost'];

                $userbudget = $user_quota_arr['dj_quota'];
                //计划
                $table_name_plan = 'ad_plan_quota_'.$ad_user_id%10;
                $sql_plan = sprintf("select  *  from  %s where ad_user_id=%d ", $table_name_plan,$ad_user_id);
                $plan_quota_arrs = $cnn->createCommand($sql_plan)->queryAll();
                $plan_use_arr = array();
                if(!empty($plan_quota_arrs)){
                    foreach ($plan_quota_arrs as $plan_quota_arr) {
                        $ad_plan_id = $plan_quota_arr['ad_plan_id'];
                        $plan_quota = $plan_quota_arr['plan_quota'];
                        $plan_cost = $plan_quota_arr['plan_cost'];

                        $plan_use_arr[$ad_plan_id] = array(
                            'plan_quota' => $plan_quota,
                            'plan_cost' =>$plan_cost
                        );
                    }

                }

                // 获取用户cost
                // $costInfo   = ComAdQuotaV2::getUserDJCostInfo(date('j'), $userID);
                // $mvCost     = ComAdQuotaV2::getUserMediavCost(date('j'), $userID);
                // 发送每个计划的消费信息
                foreach ($planList as $planID => $tmp) {
                    if (!isset($plan_use_arr[$planID])) {
                        printf("%s get plan quota info fail, uid[%d] pid[%d]\n", $this->_task_name, $userID, $planID);
                        $planFail++;
                        continue;
                    }
                    $data = array(
                        'planbudget'    => $plan_use_arr[$planID]['plan_quota'],
                        'plancost'      => $plan_use_arr[$planID]['plan_cost']>0 ? $plan_use_arr[$planID]['plan_cost'] : 0,
                    );
                    ComBudgetData::sendOnePlanConsumeData($userID, $planID, $data, ComBudgetData::MSG_TYPE_USER_PLAN);
                    $planSuccess++;
                }

                $userCost = $djCost + $mvCost + $yesterday_dj_cost + $yesterday_mv_cost;
                $userTodayCost = round($djCost ,2);
                $balance =  round($user_quota_arr['balance'] - $userCost,2);
                if ($balance<0) {
                    $balance = 0;
                }
                $data = array(
                    'balance'       => $balance,
                    'userbudget'    => $user_quota_arr['dj_quota'],
                    'usercost'      => $userTodayCost,
                );
                ComBudgetData::sendOneUserConsumeData($userID, $data, ComBudgetData::MSG_TYPE_USER_PLAN);
                $userSuccess++;
            }
        }

        // 清理 & 日志
        $this->_unmutex($fileName);
        touch($finishFile);

        $endTime = time();
        printf("%s process file [%s], begin at %s, end at %s, use %d sedcods, click success %d, fail %d, user success %d, fail %d, plan success %d, plan fail %d\n",
            $this->_task_name,
            $dataFilePath,
            date('Y-m-d H:i:s', $beginTime),
            date('Y-m-d H:i:s', $endTime),
            $endTime - $beginTime,
            $clickSuccess, $clickFail,
            $userSuccess, $userFail,
            $planSuccess, $planFail
        );

    }

    /**
     * 处理mv单个文件 成功返回 true 失败返回 false
     */
    protected function _processOneMvFile ($fileName) {
        $task_name = $this->_task_name;

        $dataFileDir = Config::item('mediav_click_res');
        $dataFile = $dataFileDir.$fileName;

        $okFile = $dataFile.'.ok';
        if (!file_exists($okFile)) {
            printf("%s file[%s] not ready\n", $task_name, $dataFile);
            return false;
        }

        $finishPath = Config::item('budgetLogPath');
        $finishFile = $finishPath.$fileName.'.finish';
        if (file_exists($finishFile)) {
            printf("%s file[%s] already processed\n", $task_name, $dataFile);
            return false;
        }

        $infh = @fopen($dataFile, 'r');
        if (!$infh) {
            printf("%s can not open file[%s]\n", $task_name, $dataFile);
            return false;
        }

        if ($this->_mutex($fileName)==false) {
            printf("%s get mutex fail, filename[%s]\n", $task_name, $fileName);
            fclose($infh);
            return;
        }

        $beginTime = time();
        $userSuccess = $userFail = 0;
        $cnn = Yii::app()->db_quota;
        while (false !== ($strLine = fgets($infh, 10240))) {
            $strLine = trim($strLine);
            if ($strLine=='')  {
                continue;
            }
            $arrData = json_decode($strLine, true);
            if (false === $arrData) {
                printf("%s invalid data, file[%s] line[%s]\n", $task_name, $dataFile, $strLine);
                $userFail++;
                continue;
            }
            if ($arrData['result'] != 0) {
                continue;
            }

            $userID = $arrData['ad_user_id'];
            //$usreBalance = $this->_getUserBalance($userID);
            $table_name = 'ad_user_quota_'.$userID%10;
            $sql = sprintf("select  *  from  %s where ad_user_id=%d ", $table_name,$userID);
            $user_quota_arr = $cnn->createCommand($sql)->queryRow();
            if (empty($user_quota_arr)) {
                $userFail++;
                continue;
            }
            $djCost =$user_quota_arr['dj_cost'];
            $mvCost =$user_quota_arr['mv_cost'];
            $yesterday_dj_cost =$user_quota_arr['yesterday_dj_cost'];
            $yesterday_mv_cost =$user_quota_arr['yesterday_mv_cost'];
            $userCost = $djCost + $mvCost + $yesterday_dj_cost + $yesterday_mv_cost;

            $balance =  $user_quota_arr['balance'] - $userCost;
            if ($balance < 0) {
                $balance = 0;
            }
            ComBudgetData::sendOneUserMVConsumeData($userID, $balance);
            $userSuccess++;
        }

        // 清理 & 日志
        $this->_unmutex($fileName);

        touch($finishFile);
        fclose($infh);

        $endTime = time();
        printf("%s process file [%s], begin at %s, end at %s, use %d seconds, user success %d, fail %d\n",
            $task_name,
            $dataFile,
            date('Y-m-d H:i:s', $beginTime),
            date('Y-m-d H:i:s', $endTime),
            $endTime - $beginTime,
            $userSuccess,
            $userFail
        );
        return true;
    }

    // 成功返回用户的实时余额
    protected function _getUserBalance ($userID) {
        $task_name = $this->_task_name;
        $djUserInfo = ComAdQuotaV2::getUserDJQuotaInfo($userID);
        if (empty($djUserInfo)) {
            printf("%s can not get user[%d] budget info\n", $task_name, $userID);
            return false;
        }

        $djCostInfo = ComAdQuotaV2::getUserDJCostInfo(date('j'), $userID);
        $djCost = isset($djCostInfo['cost']) ? $djCostInfo['cost'] : 0;
        $mvCost = ComAdQuotaV2::getUserMediavCost(date('j'), $userID);

        $balance = $djUserInfo['balance'] - $djCost - $mvCost;
        if ($balance < 0) {
            $balance = 0;
        }
        $balance = number_format($balance, 2, '.', '');
        return $balance;
    }
    public function actionClickInfoSubScriber($count=1)
    {
        $emq = new ComEMQ('emq_esc');

        $emq->exchangeName = 'click_info';
        $emq->checkInterval = 20;           //多少秒检查一次
        $emq->alarmReceiveTime = 60*60*2;   //超过多少秒发送时间和接受时间差就报警
        $emq->exitTime = 30*60;             //30分钟
        $emq->startMultiProcessSubscriber($count);
    }

    //推送给引擎数据
    public static function _clickInfoSubScriber($msg)
    {
        $data = json_decode($msg->body,true);
        $content = $data['content'];
        Utility::log(__CLASS__,"CLICKINFO_START",$content);
        if(!in_array($content['ver'], array('sou','mediav','shouzhu')) || !isset($content['cost_info']['user_quota_info'])){
            ComEMQ::receiveSuccess($msg);
            return;
        }

        $userID = intval($content['uid']);
        $settleTime = isset($content['settleTime'])?intval($content['settleTime']):time();
        $realBalance = max(round($content['cost_info']['user_quota_info']['balance']-$content['cost_info']['user_quota_info']['dj_cost']-$content['cost_info']['user_quota_info']['mv_cost']-$content['cost_info']['user_quota_info']['yesterday_dj_cost']-$content['cost_info']['user_quota_info']['yesterday_mv_cost'],2),0);

        $content['realBalance']= $realBalance;
        if($content['ver'] == 'sou'){//sou

            $planID = intval($content['pid']);
            $planQuota = $content['cost_info']['plan_quota_info']['plan_quota'];
            $planCost  = $content['cost_info']['plan_quota_info']['plan_cost'];

            $userQuota = $content['cost_info']['user_quota_info']['dj_quota'];
            $userCost  = $content['cost_info']['user_quota_info']['dj_cost'];

            $productQuota = $content['cost_info']['user_quota_info']['product_quota'];
            $productCost  = $content['cost_info']['user_quota_info']['product_cost'];

            // 发送每一条消费数据
            $arrInfo = array(
                'query'         => $content['query'],          // 用户搜索串
                'src'           => $content['src'],            // 来源
                'keyword'       => $content['keyword'],
                'clickprice'    => $content['clickPrice'],     // 点击价格(扣费)
                'matchtype'     => $content['matchtype'],
                'channel_id'    => intval($content['channel_id']),
                'place_id'      => intval($content['place_id']),
                'clicktime'     => $content['now'],            // 点击时间
                'settletime'    => $settleTime,     // 计价时间
                'gspprice'      => $content['price'],     //引擎出价
                'bucket_id'      => intval($content['buckettest']),     //bucket_id
                'bidprice'      => $content['bidprice'],     //bidprice
                'lsid'          => isset($content['lsid'])?intval($content['lsid']):0,     //lsid
                'product_line'  => isset($content['product_line'])?intval($content['product_line']):0,     //product_line
            );
            ComBudgetData::sendOneConsumeData($userID, $planID, $arrInfo);

            $planData = array(
                'planbudget'    => round($planQuota,2),
                'plancost'      => round($planCost,2),
                'product_line'  => isset($content['product_line'])?intval($content['product_line']):0,     //product_line
            );
            ComBudgetData::sendOnePlanConsumeData($userID, $planID, $planData, ComBudgetData::MSG_TYPE_USER_PLAN);

            $userData = array(
                'balance'       => $realBalance,
                //'userbudget'    => round($userQuota,2),
                'userbudget'    => 0,//写死
                //'usercost'      => round($userCost,2),
                'usercost'      => round($productCost,2),//对应的产品线消费
                'product_budget'  => $productQuota,     //product_line
                'product_line'  => isset($content['product_line'])?intval($content['product_line']):0,     //product_line
            );

           ComBudgetData::sendOneUserConsumeData($userID, $userData, ComBudgetData::MSG_TYPE_USER_PLAN);
	       //printf("%s %s:\t%d\t%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\n",date('Y-m-d H:i:s'),'sou',$arrInfo['clicktime'],$userID,$planID,$userQuota,$userCost,$realBalance,$planQuota,$planCost,$arrInfo['clickprice']);
        } else {//mediav
            ComBudgetData::sendOneUserMVConsumeData($userID, $realBalance);
	        //printf("%s %s:\t%d\t%d\t%.2f\n",date('Y-m-d H:i:s'),'mediav',$content['now'],$userID, $realBalance);
        }
        Utility::log(__CLASS__,"CLICKINFO_END",$realBalance);
        ComEMQ::receiveSuccess($msg);
    }
    public function actionCheatInfoSubscriber($count = 1)
    {
        $emq = new ComEMQ('emq_esc');
        $emq->exchangeName = 'cheat_dimension_reduce';
        $emq->checkInterval = 20; //多少秒检查一次
        $emq->alarmReceiveTime = 60 * 60 * 2; //超过多少秒发送时间和接受时间差就报警
        $emq->startMultiProcessSubscriber($count);
    }

    public static function _cheatInfoSubscriber($msg)
    {
        $data = json_decode($msg->body,true);
        $content = $data['content'];
        Utility::log(__CLASS__,"CHEATINFO_START",$content);

        $userID = intval($content['ad_user_id']);
        $planID = intval($content['ad_plan_id']);
        $cheatDealTime = isset($content['cheat_info']['cheat_deal_time'])?intval($content['cheat_info']['cheat_deal_time']):time();

        //产品线信息
        $product_line_info = isset($content['product_line_info'])?$content['product_line_info']:array();
        if(empty($product_line_info) ){
            Utility::sendAlert(__CLASS__,__FUNCTION__,"product_line_info empty {$userID} : {$planID} ");
            ComEMQ::receiveSuccess($msg);
            return;
        }

        if(isset($content['cheat_info']['need_search_quota']) && isset($content['cheat_info']['need_search_quota']) == 0){

            $planbudget  = round($content['cost_info']['plan_quota_info']['plan_quota'],2);
            $plancost    = round($content['cost_info']['plan_quota_info']['plan_cost'],2);

            // $userbudget = round($content['cost_info']['user_quota_info']['dj_quota'],2);
            // $usercost   = round($content['cost_info']['user_quota_info']['dj_cost'],2);

            $productQuota = $content['cost_info']['user_quota_info']['product_quota'];
            $productCost  = $content['cost_info']['user_quota_info']['product_cost'];

            $realBalance = max(round($content['cost_info']['user_quota_info']['balance']-$content['cost_info']['user_quota_info']['dj_cost']-$content['cost_info']['user_quota_info']['mv_cost']-$content['cost_info']['user_quota_info']['yesterday_dj_cost']-$content['cost_info']['user_quota_info']['yesterday_mv_cost'],2),0);

        } else {//重新查询

            $cnn = Yii::app()->db_quota;
            //user_quota
            $user_quota_sql = sprintf("select  *  from  %s where ad_user_id=%d ", 'ad_user_quota_'.$userID%10,$userID);
            $user_quota_arr = $cnn->createCommand($user_quota_sql)->queryRow();

            //plan_qaota
            $plan_quota_sql = sprintf("select  *  from  %s where ad_user_id=%d and ad_plan_id=%d", 'ad_plan_quota_'.$userID%10,$userID,$planID);
            $plan_quota_arr = $cnn->createCommand($plan_quota_sql)->queryRow();

            if(empty($user_quota_arr) || empty($plan_quota_arr)){
                Utility::sendAlert(__CLASS__,__FUNCTION__,"quota empty {$userID} : {$planID} ");
                ComEMQ::receiveSuccess($msg);
                return;
            }

            $planbudget  = round($plan_quota_arr['plan_quota'],2);
            $plancost    = round($plan_quota_arr['plan_cost'],2);

            // $userbudget = round($user_quota_arr['dj_quota'],2);
            // $usercost   = round($user_quota_arr['dj_cost'],2);

            $productQuota = round($user_quota_arr[$product_line_info['product_quota']],2);
            $productCost  = round($user_quota_arr[$product_line_info['product_cost']],2);

            $realBalance = max(round($user_quota_arr['balance']-$user_quota_arr['dj_cost']-$user_quota_arr['mv_cost']-$user_quota_arr['yesterday_dj_cost']-$user_quota_arr['yesterday_mv_cost'],2),0);

        }

        if($content['ver'] == 'sou'){//sou

            $content['realBalance']= $realBalance;
            $planData = array(
                'planbudget'    => $planbudget,
                'plancost'      => $plancost,
                'cheatdealtime' => $cheatDealTime,
                'cid'           => $content['cid'],
                'clickprice'    => round($content['cheat_info']['cheat_price'],2),
                'lsid'          => isset($content['lsid'])?intval($content['lsid']):0,     //lsid
                'product_line'  =>  (int) $product_line_info['product_line'],
            );
            ComBudgetData::sendOnePlanConsumeData($userID, $planID, $planData, ComBudgetData::MSG_TYPE_CHEAT);
            $userData = array(
                'balance'       => round($content['realBalance'],2),
                'userbudget'    => 0,
                'usercost'      => $productCost,
                'cheatdealtime' => $cheatDealTime,
                'cid'           => $content['cid'],
                'lsid'          => isset($content['lsid'])?intval($content['lsid']):0,     //lsid
                'product_budget'=>  $productQuota,
                'product_line'  =>  (int) $product_line_info['product_line'],
            );
            ComBudgetData::sendOneUserConsumeData($userID, $userData, ComBudgetData::MSG_TYPE_CHEAT);
        } else {
            ComBudgetData::sendOneUserMVConsumeData($userID, $realBalance);
        }
        Utility::log(__CLASS__,"CHEATINFO_END",$realBalance);

        ComEMQ::receiveSuccess($msg);
    }
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
