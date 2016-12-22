<?php
/**
 * mediav相关的操作
 *
 */
Yii::import('application.extensions.CEmqPublisher');
class MediavCommand extends CConsoleCommand {

    // 计费失败类型
    const COST_SUCCESS              = 0; // 计费成功
    const COST_TIMEOUT              = 1; // 第二天两点后不再计费以前的消费
    const COST_USER_QUOTA           = 2; // 无帐户预算
    const COST_PLAN_QUOTA           = 3; // 没有计划预算
    const COST_USER_QUOTA_FULL      = 4; // 帐户预算已经满
    const COST_PLAN_QUOTA_FULL      = 5; // 计划预算已经满

    // 跟 midiav 回传的数据类型
    const DIFF_TYPE_SAME        = 0; // 完全相同
    const DIFF_TYPE_TIME_OUT    = 1; // 超时不计费
    const DIFF_TYPE_INVALID     = 2; // 物料有效性检测失败(用户不存在、计划预算不存在)
    const DIFF_TYPE_USER_QUOTA  = 4; // 表示超用户当天预算
    const DIFF_TYPE_PLAN_QUOTA  = 8; // 超推广计划当天预算

    const MEDIAV_CHANNEL_ID = 49;
    const MEDIAV_PLACE_ID   = 199;

    protected $_task_name = '';

    protected $_resMutexFile = null;

    protected $_arrSendDataFile = array();

    const DSP_COST_TIMES = 100000;//dsp要求计费在元的基础上乘上一个系数
    const DSP_REAL_COST_RATIO = 0.5;//dsp真实消费=esc计费*系数
    const DSP_ID = 122;//dsp指定id
    const DSP_HEADER_X_MEDIAV_AUTH  = "X-mediav-auth:bool@mvad.com + g4y~Ine`6K";
    const DSP_HEADER_X_MEDIAV_ENTITY = "X-mediav-entity:galileo + g4y~Ine`6K";
    const DSP_HEADER_BODY_SIGNATURE_KEY = 'body-signature:';
    const DSP_MAX_ROWS = 500;
    const DSP_RESPONSE_CODE_SUC = 200;
    const DSP_TIMEOUT = 30;
    /**
     * 同步dsp消费到mediav
     * @param null $date    2016-03-16
     */
    public function actionDspCostUpdate($date=null) {
        printf("%s begin, date [%s]\n", __FUNCTION__, date('Y-m-d H:i:s'));

        $limitTimeStamp = strtotime(date("Y-m-d")." -1 second");
        if(is_null($date)) {
            $timeStamp = $limitTimeStamp;
        } else {
            $timeStamp = strtotime($date);
        }

        if($timeStamp > $limitTimeStamp) {
            printf("%s error[invalid date], date [%s]\n", __FUNCTION__, date('Y-m-d H:i:s'));
            return;
        }

        $url = Config::item('mediav_dsp_url');
        if(empty($url)) {
            printf("%s error[invalid conf], url [%s]\n", __FUNCTION__, $url);
            return;
        }

        $requestDate = date('Y-m-d', $timeStamp);

        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_POST, true);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($ch, CURLOPT_TIMEOUT, self::DSP_TIMEOUT);

        $dspData = $this->_getDsp($timeStamp);

        $isError = false;
        //数据发送
        foreach($dspData as $requestOnce) { //分页获取数据
            $requestDataArr = array(
                'ctime'=>time(),
                'dsp_id'=>self::DSP_ID,
                'date'=>$requestDate,
                'data'=>$requestOnce,
            );
            $requestDataStr = json_encode($requestDataArr);
            $md5Sign = md5($requestDataStr);
            $header = array(
                self::DSP_HEADER_X_MEDIAV_AUTH,
                self::DSP_HEADER_X_MEDIAV_ENTITY,
                self::DSP_HEADER_BODY_SIGNATURE_KEY . $md5Sign,
            );
            curl_setopt($ch, CURLOPT_HTTPHEADER, $header);
            curl_setopt($ch, CURLOPT_POSTFIELDS, $requestDataStr);
            $response = curl_exec($ch);
            $responseArr = json_decode($response, true);
            if(is_array($responseArr) &&
                array_key_exists('code', $responseArr) &&
                $responseArr['code'] == self::DSP_RESPONSE_CODE_SUC) {
                printf("%s request suc, md5 【%s】, response 【%s】\n", __FUNCTION__, $md5Sign, $response);
            } else {
                $isError = true;
                printf("%s request error, data 【%s】, response 【%s】\n", __FUNCTION__, $requestDataStr, $response);
            }
        }
        curl_close($ch);

        if($isError) {
            Utility::sendAlert(__CLASS__,__FUNCTION__,'DSP数据同步出现异常',true);
        }
        printf("%s end, date [%s]\n", __FUNCTION__, date('Y-m-d H:i:s'));
        return;
    }

    /**
     * 统计click_detail dsp数据
     * @param $timeStamp int   时间戳
     *
     * @return array        dsp分组统计数据
     */
    private function _getDsp($timeStamp) {
        ini_set('memory_limit', '10240M');
        $res = array();
        $sql = "select location adspace,count(*) click,sum(price-reduce_price) ad_cost from click_detail where status not in (-1,2) and deal_status=1 and ver ='shouzhu'  and cheat_type not in (2,3) and price != reduce_price and source_system=6 group by location";
        $queryInfo = ComAdDetail::queryBySql($sql, $timeStamp);

        $count = 0;
        $tmp = array();
        $request = array(
            'request' => 0,
            'show' => 0,
            'extended'=> '',
        );
        foreach($queryInfo as $row) {
            $request['adspace'] = $row['adspace'];
            $request['click'] = intval($row['click']);
            $request['ad_cost'] = round($row['ad_cost'],2) * self::DSP_COST_TIMES;
            $request['real_cost'] =  round($row['ad_cost'],2) * self::DSP_COST_TIMES * self::DSP_REAL_COST_RATIO;
            $tmp[] = $request;
            $count ++;
            if($count % self::DSP_MAX_ROWS == 0) {
                $res[] = $tmp;
                $tmp = array();
            }
        }
        if(!empty($tmp)) {
            $res[] = $tmp;
        }
        //结束标记
        $res[] = array(
            array(
                'adspace'=>'0',
                'request'=>0,
                'show'=>0,
                'click'=>0,
                'ad_cost'=>0,
                'real_cost'=>0,
                'extended'=>'done',
            ),
        );
        return $res;
    }
    /**
     * 每天两点
     */
    public function actionCaculateUserBudget($date=null) {
        $taskName = sprintf("[CaculateUserBudget_%s]", date('Ymd-His'));
        $beginTime = time();
        if (is_null($date)) {
            $date = date("Ymd");
        }
        printf("%s begin, date [%s]\n", $taskName, $date);

        $rate = Config::item('mediav_rate');

        // 连接数据库
        $centerDB = DbConnectionManager::getDjCenterDB();
        if (false === $centerDB) {
            printf("%s get dianjing center db fail\n", $taskName);
            return ;
        }
        $daoUserExt         = new UserExt();        $daoUserExt->setDB($centerDB);
        $daoUser            = new User();           $daoUser->setDB($centerDB);
        $daoUserCL          = new UserChargeLog();  $daoUserCL->setDB($centerDB);
        $daoUserQuotaThird  = new UserQuotaThird(); $daoUserQuotaThird->setDB($centerDB);

        // 获取用户扩展信息
        $userExtMediaV = $daoUserExt->getUserThirdPartyAll();

        // 获取每个用户
        $lastID = 0;
        while (true) {
            $userList = $daoUser->getValidUser($lastID, 500);
            if (empty($userList)) {
                break;
            }
            foreach ($userList as $oneUser) {
                $userID = $lastID = $oneUser['id'];
                // client_category 0 表示是搜索中小客户，只有这部分客户会给mediav分配预算。
                if (!isset($userExtMediaV[$userID]) || false == $userExtMediaV[$userID]['is_media_v'] || $oneUser['client_category']!=0) {
                    // 不是mediav用户
                    continue;
                }
                // printf("mediav user id %d\n", $userID);
                if ($oneUser['day_quota']!=0) {
                    // 按预算
                    $quotaTotal     = $oneUser['day_quota'];
                    $method         = UserQuotaThird::METHOD_PERSENT;
                } else { // 按消费
                    $endDate    = date('Y-m-d',  strtotime($date) - 86400);
                    $startDate  = date('Y-m-d', strtotime($date) - 7*86400);
                    // printf("startDate %s endDate %s \n", $startDate, $endDate);
                    $quotaTotal     = $daoUserCL->getUserOldChargeAverage($userID, $startDate, $endDate, 7);
                    $method         = UserQuotaThird::METHOD_CONSUME;
                }

                $totalQuota = $oneUser['day_quota'];

                $djTodayCostInfo = ComAdQuotanew::getUserDJCostInfo(date('j'), $userID);
                $mediavTodayCostInfo = ComAdQuotanew::getUserMediavCostInfo(date('j'), $userID);
                // 未结算时获取昨日消费信息
                if (!ComAdQuotanew::isSettledSuccess()) {
                    $djYesterdayCostInfo = ComAdQuotanew::getUserDJCostInfo(date('j', strtotime('-1 day')), $userID);
                    $mediavYesterdayCostInfo = ComAdQuotanew::getUserMediavCostInfo(date('j', strtotime('-1 day')), $userID);
                } else {
                    $djYesterdayCostInfo        = array();
                    $mediavYesterdayCostInfo    = array();
                }
                $djTodayCost         = isset($djTodayCostInfo['cost']) ? $djTodayCostInfo['cost'] : 0;                   // 点睛今天消费
                $mediavTodayCost     = isset($mediavTodayCostInfo['cost']) ? $mediavTodayCostInfo['cost'] : 0;           // mediav今天消费
                $djYesterdayCost     = isset($djYesterdayCostInfo['cost']) ? $djYesterdayCostInfo['cost'] : 0;           // 点睛昨日消费
                $mediavYesterdayCost = isset($mediavYesterdayCostInfo['cost']) ? $$mediavYesterdayCostInfo['cost'] : 0;  // mediav今天消费
                $curBalance = $oneUser['balance'] - $djTodayCost - $mediavTodayCost - $djYesterdayCost - $mediavYesterdayCost;

                if ($curBalance<0) {
                    $curBalance = 0;
                }
                if ($curBalance<$quotaTotal) {
                    $quotaTotal = $curBalance;
                    $method     = UserQuotaThird::METHOD_BALANCE;
                }
                $mediavQuota = floor($quotaTotal * $rate / 100);

                if ($totalQuota!=0) {
                    $djQuota = $totalQuota - $mediavQuota;
                } else {
                    $djQuota = 0;
                }
                if ($mediavQuota<0) {
                    $mediavQuota = 0;
                }

                // 更新 redis
                ComAdQuotanew::setUserQuota($userID, $totalQuota, $djQuota, $mediavQuota);

                // 更新表
                $data = array(
                    'ad_user_quota' => $mediavQuota,
                    'rate'          => $rate,
                    'method'        => $method,
                );
                $daoUserQuotaThird->insertUpdateMediav($userID, date('Y-m-d'), $data);

                // 发送消息
                $logID = sprintf("esc_%d_%5d", time(), mt_rand(0, 99999));
                $mqData = array(
                    'user_id'   => $userID,
                    'quota'     => (float)$mediavQuota,
                );
                $ret = CEmqPublisher::send(
                    Yii::app()->params['exchange']['mediavUserQuota'],
                    'emqapi',
                    json_encode($mqData),
                    $logID,
                    Yii::app()->params['emq']
                );

                printf("%s dj_c[%.2f] m_c[%.2f] dj_y_c[%.2f] m_y_c[%.2f] user[%d] quota[%.2f] method[%d]\n",
                    $taskName, $djTodayCost, $mediavTodayCost, $djYesterdayCost, $mediavYesterdayCost, $userID, $mediavQuota, $method
                );
            }
        }

        $endTime = time();
        printf("%s begin at %s, end at %s\n",
            $taskName, date('Y-m-d H:i:s', $beginTime), date('Y-m-d H:i:s', $endTime)
        );
    }

    /**
     * 每五分钟下载一次数据文件
     * time 精确到分 201407211200
     * meta文件的重试，文件不存在时的处理
     */
    public function actionGetDataFile ($time=null) {
        $this->_task_name = $taskName = sprintf('[getdatafile_%s]', date('Ymd-His'));

        if (is_null($time)) {
            $_tmp = date('YmdH');
            $minute = floor(date('j') / 5) * 5 ;
            $time = $_tmp.$minute;
        }

        $beginTime = time();
        printf("%s begin, param time[%s]\n", $taskName, $time);

        // 使用curl下载
        $fileName = $time.'.meta';
        $url        = Config::item('mediav_url');
        $filePath   = Config::item('mediav_log_path');

        $metaFileTryTimes = 0;
        while ($metaFileTryTimes<10) {
            $ret = $this->_downloadFile($url.$fileName, $filePath.$fileName);
            if ($ret === true) {
                break;
            }
            $metaFileTryTimes++;
            printf("%s get meta file[%s] fail, try time %d!\n", $taskName, $fileName, $metaFileTryTimes);
            sleep(30);
        }
        if ($metaFileTryTimes == 10) {
            printf("%s get meta file[%s] fail!\n", $taskName, $fileName);
            return ;
        } else {
            printf("%s get meta file[%s] success!\n", $taskName, $fileName);
        }

        $metaContents = file($filePath.$fileName);
        if (empty($metaContents)) {
            printf("%s mete file[%s] is empty!\n", $taskName, $fileName);
            return ;
        }
        foreach ($metaContents as $strLine) {
            $strLine = trim($strLine);
            if ($strLine=='') {
                continue;
            }
            list ($md5sum, $fileName) = explode("  ", $strLine);
            $urlSource = $url.$fileName;
            $localPath = $filePath.$fileName;
            $tryNum = 0;
            while ($tryNum<=4) {
                $ret = $this->_downloadFile($urlSource, $localPath);
                if ($ret === false) {
                    $tryNum++;
                    printf("%s get file[%s] fail, tryNum %d\n", $taskName, $urlSource, $tryNum);
                    continue;
                }
                $md5Local = md5_file($localPath);
                if ($md5Local!=$md5sum) {
                    $tryNum++;
                    printf("%s file[%s] md5sum diff, local[%s] remote[%s], tryNum %d\n", $taskName, $urlSource, $md5Local, $md5sum, $tryNum);
                    continue;
                } else {
                    break;
                }
            }
            if ($tryNum>4) {
                printf("%s get data file[%s] fail\n", $taskName, $fileName);
                continue;
            } else {
                printf("%s get data file[%s] success\n", $taskName, $fileName);
            }

            // 先备份，再解压
            $cmd = sprintf("cp %s %s.bk; /usr/bin/xz -f -d %s", $localPath, $localPath, $localPath);
            system($cmd);

            $fileName   = substr($fileName, 0, -3);
            $localPath  = $filePath.$fileName;
            if (preg_match('/\\.c\\./', $fileName)) {
                $toPath = Config::item('mediav_click_path').$fileName;
            } else {
                $toPath = Config::item('mediav_view_path').$fileName;
            }
            copy($localPath, $toPath);
            touch($toPath.'.ok');
        }

        $endTime = time();
        printf("%s begin at %s, end at %s\n", $taskName, date('Y-m-d H:i:s', $beginTime), date('Y-m-d H:i:s', $endTime));
    }

    /**
     * 五分钟计费
     */
    public function actionProcessClick($time=null) {
        if (is_null($time)) {
            $_tmp = date('YmdH');
            $minute = floor(date('i') / 5) * 5 ;
            $time = $_tmp.sprintf("%02d", $minute);
        }

        $this->_task_name = $taskName = sprintf("[ProcessClick_%s]", date('Ymd-His'));
        $beginTime = time();
        printf("%s begin, param time[%s]\n", $taskName, $time);

        $mediavClickPath = Config::item('mediav_click_path');
        $dh = opendir($mediavClickPath);

        $arrFile = array();
        while (($file=readdir($dh)) !== false) {
            if ($file=='.' || $file=='..') {
                continue;
            }
            if (is_dir($mediavClickPath.$file)) {
                continue;
            }
            if (substr($file, -3, 3)=='.ok') {
                continue;
            }
            $arrFile[] = $file;
        }
        // var_dump($arrFile);

        $processFileCnt = 0;
        if (!empty($arrFile)) {
            $finishFilePath = Config::item('mediav_click');
            foreach ($arrFile as $oneFile) {
                if (!file_exists($mediavClickPath.$oneFile.'.ok')) {
                    continue;
                }
                if (file_exists($finishFilePath.$oneFile.'.ok')) {
                    continue;
                }
                $this->_processOneClickFile($oneFile);
                $processFileCnt++;
            }
        }

        // 写meta文件
        $strOut = '';
        if (!empty($this->_arrSendDataFile)) {
            foreach ($this->_arrSendDataFile as $_fn => $_md5) {
                $strOut .= sprintf("%s  %s\n", $_md5, $_fn);
            }
        }
        $metaFile = Config::item('mediav_send_data_ptah').$time.'.meta';
        file_put_contents($metaFile, $strOut);

        $endTime = time();
        printf("%s begin at %s, end at %s, process %d files\n",
            $taskName, date('Y-m-d H:i:s', $beginTime), date('Y-m-d H:i:s', $endTime), $processFileCnt
        );
    }

    /**
     * 点击 area
     */
    public function actionProcessClickArea() {
        $this->_task_name = $taskName = sprintf("[ProcessClickArea_%s]", date('Ymd-His'));
        $beginTime = time();
        printf("%s begin\n", $taskName);

        $clickLogPath = Config::item('mediav_click');
        $dh = opendir($clickLogPath);

        $arrFile = array();
        while (($file=readdir($dh)) !== false) {
            if ($file=='.' || $file=='..') {
                continue;
            }
            if (is_dir($clickLogPath.$file)) {
                continue;
            }
            if (substr($file, -3, 3)=='.ok') {
                continue;
            }
            $arrFile[] = $file;
        }
        closedir($dh);
        // var_dump($arrFile);

        if (!empty($arrFile)) {
            $finishFilePath = Config::item('mediav_area');
            foreach ($arrFile as $oneFile) {
                if (!file_exists($clickLogPath.$oneFile.'.ok')) {
                    continue;
                }
                if (file_exists($finishFilePath.$oneFile.'.ok')) {
                    continue;
                }
                $this->_processOneAreaClickFile($oneFile);
            }
        }

        $endTime = time();
        printf("%s begin at %s, end at %s\n",
            $taskName, date('Y-m-d H:i:s', $beginTime), date('Y-m-d H:i:s', $endTime)
        );
    }
    /**
     * 展现area
     */
    public function actionProcessViewArea() {
        $this->_task_name = $taskName = sprintf("[ProcessView_%s]", date('Ymd-His'));
        $beginTime = time();
        printf("%s begin\n", $taskName);
        $mediavViewPath = Config::item('mediav_view_path');
        $dh = opendir($mediavViewPath);

        $arrFile = array();
        while (($file=readdir($dh)) !== false) {
            if ($file=='.' || $file=='..') {
                continue;
            }
            if (is_dir($mediavViewPath.$file)) {
                continue;
            }
            if (substr($file, -3, 3)=='.ok') {
                continue;
            }
            $arrFile[] = $file;
        }
        // var_dump($arrFile);

        if (!empty($arrFile)) {
            $finishFilePath = Config::item('mediav_area');
            foreach ($arrFile as $oneFile) {
                if (!file_exists($mediavViewPath.$oneFile.'.ok')) {
                    continue;
                }
                if (file_exists($finishFilePath.$oneFile.'.ok')) {
                    continue;
                }
                $this->_processOneAreaViewFile($oneFile);
            }
        }
        $endTime = time();
        printf("%s begin at %s, end at %s\n",
            $taskName, date('Y-m-d H:i:s', $beginTime), date('Y-m-d H:i:s', $endTime)
        );
    }

    /**
     * 点击 area
     */
    public function actionProcessClickCreative() {
        $this->_task_name = $taskName = sprintf("[ProcessClickCreative_%s]", date('Ymd-His'));
        $beginTime = time();
        printf("%s begin\n", $taskName);

        $clickLogPath = Config::item('mediav_click');
        $dh = opendir($clickLogPath);

        $arrFile = array();
        while (($file=readdir($dh)) !== false) {
            if ($file=='.' || $file=='..') {
                continue;
            }
            if (is_dir($clickLogPath.$file)) {
                continue;
            }
            if (substr($file, -3, 3)=='.ok') {
                continue;
            }
            $arrFile[] = $file;
        }
        closedir($dh);
        // var_dump($arrFile);

        if (!empty($arrFile)) {
            $finishFilePath = Config::item('mediav_stats');
            foreach ($arrFile as $oneFile) {
                if (!file_exists($clickLogPath.$oneFile.'.ok')) {
                    continue;
                }
                if (file_exists($finishFilePath.$oneFile.'.ok')) {
                    continue;
                }
                $this->_processOneCreativeClickFile($oneFile);
            }
        }

        $endTime = time();
        printf("%s begin at %s, end at %s\n",
            $taskName, date('Y-m-d H:i:s', $beginTime), date('Y-m-d H:i:s', $endTime)
        );
    }
    /**
     * 展现 stats
     */
    public function actionProcessViewCreative() {
        $this->_task_name = $taskName = sprintf("[ProcessViewCreative_%s]", date('Ymd-His'));
        $beginTime = time();
        printf("%s begin\n", $taskName);
        $mediavViewPath = Config::item('mediav_view_path');
        $dh = opendir($mediavViewPath);

        $arrFile = array();
        while (($file=readdir($dh)) !== false) {
            if ($file=='.' || $file=='..') {
                continue;
            }
            if (is_dir($mediavViewPath.$file)) {
                continue;
            }
            if (substr($file, -3, 3)=='.ok') {
                continue;
            }
            $arrFile[] = $file;
        }
        // var_dump($arrFile);

        if (!empty($arrFile)) {
            $finishFilePath = Config::item('mediav_stats');
            foreach ($arrFile as $oneFile) {
                if (!file_exists($mediavViewPath.$oneFile.'.ok')) {
                    continue;
                }
                if (file_exists($finishFilePath.$oneFile.'.ok')) {
                    continue;
                }
                $this->_processOneCreativeViewFile($oneFile);
            }
        }
        $endTime = time();
        printf("%s begin at %s, end at %s\n",
            $taskName, date('Y-m-d H:i:s', $beginTime), date('Y-m-d H:i:s', $endTime)
        );
    }

    /**
     * 结算完成后生成点击日志汇总文件
     * date 2014-07-21
     */
    public function actionGenerateCostFile($date=null) {
        $this->_task_name = $taskName = sprintf("[GenerateCostFile_%s]", date('Ymd-His'));
        $beginTime = time();

        if (is_null($date)) {
            $date = date('Y-m-d', strtotime('-1 day'));
        } else {
            $date = date('Y-m-d', strtotime($date));
        }
        printf("%s begin, file date[%s]\n", $taskName, $date);

        $clickDB = DbConnectionManager::getDB('click_log');
        if (false === $clickDB) {
            printf("%s get click_log fail!\n", $taskName);
            return ;
        }

        // 生成消费汇总文件
        // 20140721_total.log
        $dataFilePath = Config::item('mediav_cost_file_path');
        $totalFileName = sprintf('%s.total', str_replace('-', '', $date));
        $totalFilePath = $dataFilePath.$totalFileName;
        file_put_contents($totalFilePath, '');

        $sql = 'select ad_user_id, sum(price) as total from ad_click_log where create_date=:create_date group by ad_user_id';
        $cmd = $clickDB->createCommand($sql);
        $cmd->bindParam(':create_date', $date, PDO::PARAM_STR);
        $ret = $cmd->queryAll();
        // var_dump($ret);
        if (!empty($ret)) {
            $i = 0;
            $strOut = '';
            foreach ($ret as $oneItem) {
                $strOut .= sprintf("%d %.2f\n", $oneItem['ad_user_id'], $oneItem['total']);
                $i++;
                if ($i==100) {
                    file_put_contents($totalFilePath, $strOut, FILE_APPEND);
                    $strOut = ''; $i=0;
                }
            }
            if (!empty($strOut)) {
                file_put_contents($totalFilePath, $strOut, FILE_APPEND);
                $strOut = ''; $i=0;
            }
        }
        $totalMd5FileName = $totalFileName.'.md5';
        system("/usr/bin/xz -z -f ".$totalFilePath);
        $totalFileMd5 = md5_file($totalFilePath.'.xz');

        // 生成详细点击日志文件
        $detailFileName = sprintf('%s.detail', str_replace('-', '', $date));
        $detailFilePath = $dataFilePath.$detailFileName;
        file_put_contents($detailFilePath, '');
        $fields = array(
            'id',
            'ad_user_id',
            'ad_plan_id',
            'ad_group_id',
            'ad_advert_id',
            'create_time',
            'area_key',
            'price',
        );
        $sql = sprintf('select %s from ad_click_log where create_date=:create_date and id>:last_id limit 1000', implode(',', $fields));
        $cmd = $clickDB->createCommand($sql);
        $cmd->bindParam(':create_date', $date);

        $lastID = 0;
        while (true) {
            $cmd->bindParam(':last_id', $lastID, PDO::PARAM_INT);
            $ret = $cmd->queryAll();
            if (empty($ret)) {
                break;
            }
            $strOut = '';
            foreach ($ret as $oneItem) {
                $lastID = $oneItem['id'];
                $_tmp = array(
                    $oneItem['ad_user_id'],
                    $oneItem['ad_plan_id'],
                    $oneItem['ad_group_id'],
                    $oneItem['ad_advert_id'],
                    $oneItem['create_time'],
                    $oneItem['area_key'],
                    $oneItem['price'],
                );
                $strOut .= implode(chr(1), $_tmp)."\n";
            }
            file_put_contents($detailFilePath, $strOut, FILE_APPEND);
        }
        system("/usr/bin/xz -z -f ".$detailFilePath);
        $detailFileMd5 = md5_file($detailFilePath.'.xz');

        // 生成meta文件
        $metaFileName = str_replace('-', '', $date).'.meta';
        $metaFilePath = $dataFilePath.$metaFileName;
        $strOut = sprintf("%s  %s\n", $totalFileMd5, $totalFileName.'.xz');
        $strOut .= sprintf("%s  %s\n",  $detailFileMd5, $detailFileName.'.xz');
        file_put_contents($metaFilePath, $strOut);

        $endTime = time();
        printf("%s, date[%s], begin at %s, end at %s\n",
            $taskName, $date, date('Y-m-d H:i:s', $beginTime), date('Y-m-d H:i:s', $endTime)
        );
    }

    /**
     * 同步给mediav的文件并转移
     * todo lock
     */
    public function actionSyncMediavData () {
        $this->_task_name = $taskName = sprintf("syncmediavdata-%s", date('Ymd_his'));
        $beginTime = time();
        printf("%s begin at %s\n", $taskName, date("Y-m-d H:i:s", $beginTime));

        $bakDir     = Config::item('mediav_send_data_his');
        $sourceDir  = Config::item('mediav_send_data_ptah');
        $dh = opendir($sourceDir);
        if (!$dh) {
            printf("%s open dir [%s] fail\n", $taskName, $sourceDir);
            return ;
        }

        $rsyncIP = Yii::app()->params['sync_machine'];

        $arrMetaFile = array();
        while (($_oneFile = readdir($dh)) !== false) {
            if ($_oneFile=='.' || $_oneFile=='..') {
                continue;
            }
            if (is_dir($sourceDir.$_oneFile)) {
                continue;
            }
            $_tmp = explode('.', $_oneFile);
            if ($_tmp[count($_tmp) - 1] == 'meta') {
                $arrMetaFile[] = $_oneFile;
            }
        }
        closedir($dh);

        if (!empty($arrMetaFile)) {
            foreach ($arrMetaFile as $_oneMetaFile) {
                $fh = fopen($sourceDir.$_oneMetaFile, 'r');
                while (($strLine=fgets($fh, 102400)) !== false) {
                    $strLine = trim($strLine);
                    if ($strLine==='') {
                        continue;
                    }
                    // cdf1e5b7ba65baa62dc00276c14bdea5  201408081830-lp10dg-9-qihu.c.data.xz
                    list($_md5, $_tmp, $_file_name) = explode(' ', $strLine);
                    if (!file_exists($sourceDir.$_file_name)) {
                        printf("%s file[%s] not exists of metafile[%s]\n", $taskName, $_file_name, $_oneMetaFile);
                        continue;
                    }
                    $_local_md5 = md5_file($sourceDir.$_file_name);
                    if ($_local_md5!==$_md5) {
                        printf("%s file[%s] md5 not equl of metafile[%s], md5 [%s] locale md5[%s]\n", $taskName, $_file_name, $_oneMetaFile, $_md5, $_local_md5);
                    }
                    // sync
                    $cmd = sprintf('/usr/bin/rsync %s %s::mediav_stats', $sourceDir.$_file_name, $rsyncIP);
                    system($cmd);
                    // mvoe
                    rename($sourceDir.$_file_name, $bakDir.$_file_name);
                }
                fclose($fh);
                // sync
                $cmd = sprintf('/usr/bin/rsync %s %s::mediav_stats', $sourceDir.$_oneMetaFile, $rsyncIP);
                system($cmd);
                // move
                rename($sourceDir.$_oneMetaFile, $bakDir.$_oneMetaFile);
            }
        }

        $endTime = time();
        printf("%s begin at %s, end at %s, use %d second\n",
            $taskName, date('Y-m-d H:i:s', $beginTime), date('Y-m-d H:i:s', $endTime), $endTime-$beginTime
        );

    }

    // 私有函数 todo 检验
    protected function _downloadFile($url, $output) {
        $fhOutput = fopen($output, 'w');
        $ch = curl_init($url);
        curl_setopt($ch, CURLOPT_FILE, $fhOutput);
        $ret = curl_exec($ch);
        if ($ret==false) {
            printf("%s curl return false, url[%s] time[%s]\n", $this->_task_name, $url, date('Y-m-d H:i:s'));
            curl_close($ch);
            return false;
        }

        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        if ($httpCode!=200) {
            printf("%s curl return http_code %d, url[%s] time[%s]\n", $this->_task_name, $httpCode, $url, date('Y-m-d H:i:s'));
            curl_close($ch);
            return false;
        }
        curl_close($ch);
        return true;
    }

    // 处理 展现area
    protected function _processOneAreaViewFile ($fileName) {
        $dataFile   = Config::item('mediav_view_path').$fileName;
        $okFile     = $dataFile.'.ok';
        $finishFile = Config::item('mediav_area').$fileName.'.ok';
        if (!file_exists($dataFile)) {
            printf("%s datafile[%s] not exists\n", $this->_task_name, $dataFile);
            return ;
        }
        if (!file_exists($okFile)) {
            printf("%s datafile[%s] not ready\n", $this->_task_name, $dataFile);
            return ;
        }
        if (file_exists($finishFile)) {
            printf("%s datafile[%s] already processed\n", $this->_task_name, $dataFile);
            return ;
        }
        $fh = fopen($dataFile, 'r');
        if (!$fh) {
            printf("%s datafile[%s] can not open\n", $this->_task_name, $dataFile);
            return ;
        }

        $outputFile = Config::item('mediav_area').$fileName;
        touch($outputFile);
        while (false !== ($strLine = fgets($fh, 102400)) ){
            $strLine = trim($strLine);
            if ($strLine == '') {
                continue;
            }
            // mediav^A48^A0^A9908^A253880^A1123253^A41451658^A232322^A18^A1499190325472094033^A8,48^A1405917677^A1
            // ^A 是 php 中的 chr(1)
            /**
             *  0  src             固定值
             *  1  view            展现数(可能为0)
             *  2  click           点击数(可能为0)
             *  3  pid             广告计划id
             *  4  gid             广告组id
             *  5  aid             广告创意id
             *  6  uid             广告主用户id
             *  7  recordid        mediav 的记录id
             *  8  cost            消费(整数单位为分)
             *  9  keyword         hash，不需要
             * 10  city            地域id
             * 11  now             打点时时间戳
             * 12  valid           是否作弊
             */
            $arrData = explode(chr(1), $strLine);
            if ($arrData[1]==0 && $arrData[2]==0) { // 如果展现和点击都是0退出
                continue;
            }
            if ($arrData[12]==0) { // 被过滤的
                continue;
            }
            list ($areaFid, $areaID) = explode(",", $arrData[10]);
            if ($areaFid==10001 && $areaID==10001) {
                $areaFid = 0;
                $areaID = 0;
            } else if ($areaFid!=10001 && $areaID==10001) {
                $areaID = 0;
            }

            $data = array(
                'area_id'       => (int)$areaID,
                'area_fid'      => (int)$areaFid,
                'area_key'      => $arrData[10],
                'ad_user_id'    => (int)$arrData[6],
                'ad_plan_id'    => (int)$arrData[3],
                'ad_group_id'   => (int)$arrData[4],
                'clicks'        => 0,
                'views'         => (int)$arrData[1],
                'costs'         => 0,
                'create_date'   => date('Y-m-d', $arrData[11]),
                'create_time'   => date('Y-m-d H:i:s', $arrData[11]),
                'update_time'   => date('Y-m-d H:i:s', $arrData[11]),
                //
                '_db_id_'       => (int)$arrData[6],
                '_table_'       => date('Ymd', $arrData[11]),
            );
            file_put_contents($outputFile, json_encode($data)."\n", FILE_APPEND);
        }

        fclose($fh);
        touch($finishFile);
    }

    // 处理点击 area
    protected function _processOneAreaClickFile ($fileName) {
        $dataFile   = Config::item('mediav_click').$fileName;
        $okFile     = $dataFile.'.ok';
        $finishFile = Config::item('mediav_area').$fileName.'.ok';
        if (!file_exists($dataFile)) {
            printf("%s datafile[%s] not exists\n", $this->_task_name, $dataFile);
            return ;
        }
        if (!file_exists($okFile)) {
            printf("%s datafile[%s] not ready\n", $this->_task_name, $dataFile);
            return ;
        }
        if (file_exists($finishFile)) {
            printf("%s datafile[%s] already processed\n", $this->_task_name, $dataFile);
            return ;
        }
        $fh = fopen($dataFile, 'r');
        if (!$fh) {
            printf("%s datafile[%s] can not open\n", $this->_task_name, $dataFile);
            return ;
        }

        $strOut = '';
        while (false !== ($strLine = fgets($fh, 102400)) ){
            $strLine = trim($strLine);
            if ($strLine == '') {
                continue;
            }

            $arrData = json_decode($strLine, true);
            list ($areaFid, $areaID) = explode(",", $arrData['area_key']);
            if ($areaFid==10001 && $areaID==10001) {
                $areaFid = 0;
                $areaID = 0;
            } else if ($areaFid!=10001 && $areaID==10001) {
                $areaID = 0;
            }

            $data = array(
                'area_id'       => (int)$areaID,
                'area_fid'      => (int)$areaFid,
                'area_key'      => $arrData['area_key'],
                'ad_user_id'    => (int)$arrData['ad_user_id'],
                'ad_plan_id'    => (int)$arrData['ad_plan_id'],
                'ad_group_id'   => (int)$arrData['ad_group_id'],
                'clicks'        => 1,
                'views'         => 0,
                'costs'         => $arrData['price'],
                'create_date'   => $arrData['create_date'],
                'create_time'   => $arrData['create_time'],
                'update_time'   => $arrData['create_time'],
                //
                '_db_id_'       => (int)$arrData['_db_id_'],
                '_table_'       => str_replace('-', '', $arrData['create_date']),
            );
            $strOut .= json_encode($data)."\n";
        }

        fclose($fh);
        $outputFile = Config::item('mediav_area').$fileName;
        file_put_contents($outputFile, $strOut);
        touch($finishFile);
    }

    // 处理 展现creative
    protected function _processOneCreativeViewFile ($fileName) {
        $dataFile   = Config::item('mediav_view_path').$fileName;
        $okFile     = $dataFile.'.ok';
        $finishFile = Config::item('mediav_stats').$fileName.'.ok';
        if (!file_exists($dataFile)) {
            printf("%s datafile[%s] not exists\n", $this->_task_name, $dataFile);
            return ;
        }
        if (!file_exists($okFile)) {
            printf("%s datafile[%s] not ready\n", $this->_task_name, $dataFile);
            return ;
        }
        if (file_exists($finishFile)) {
            printf("%s datafile[%s] already processed\n", $this->_task_name, $dataFile);
            return ;
        }
        $fh = fopen($dataFile, 'r');
        if (!$fh) {
            printf("%s datafile[%s] can not open\n", $this->_task_name, $dataFile);
            return ;
        }

        $outputFile = Config::item('mediav_stats').$fileName;
        file_put_contents($outputFile, '');
        while (false !== ($strLine = fgets($fh, 102400)) ){
            $strLine = trim($strLine);
            if ($strLine == '') {
                continue;
            }
            // 格式与_processOneAreaViewFile中描述相同
            $arrData = explode(chr(1), $strLine);
            if ($arrData[1]==0 && $arrData[2]==0) { // 如果展现和点击都是0
                continue;
            }
            if ($arrData[12]==0) { // 被过滤的
                continue;
            }

            $data = array(
                'ad_user_id'    => (int)$arrData[6],
                'ad_plan_id'    => (int)$arrData[3],
                'ad_group_id'   => (int)$arrData[4],
                'ad_advert_id'  => (int)$arrData[5],
                'clicks'        => (int)$arrData[2],
                'views'         => (int)$arrData[1],
                'total_cost'    => 0,
                'create_date'   => date('Y-m-d', $arrData[11]),
                'create_time'   => date('Y-m-d H:i:s', $arrData[11]),
                'update_time'   => date('Y-m-d H:i:s', $arrData[11]),
                'ad_channel_id' => (int)self::MEDIAV_CHANNEL_ID,
                'ad_place_id'   => (int)self::MEDIAV_PLACE_ID,
                'type'          => 2, // 1 点击 2 展示
                //
                '_db_id_'       => (int)$arrData[6],
                '_table_'       => date('Ymd', $arrData[11]),
            );
            file_put_contents($outputFile, json_encode($data)."\n", FILE_APPEND);
        }

        fclose($fh);

        touch($finishFile);
    }

    // 处理 点击 creative
    protected function _processOneCreativeClickFile ($fileName) {
        $dataFile   = Config::item('mediav_click').$fileName;
        $okFile     = $dataFile.'.ok';
        $finishFile = Config::item('mediav_stats').$fileName.'.ok';
        if (!file_exists($dataFile)) {
            printf("%s datafile[%s] not exists\n", $this->_task_name, $dataFile);
            return ;
        }
        if (!file_exists($okFile)) {
            printf("%s datafile[%s] not ready\n", $this->_task_name, $dataFile);
            return ;
        }
        if (file_exists($finishFile)) {
            printf("%s datafile[%s] already processed\n", $this->_task_name, $dataFile);
            return ;
        }
        $fh = fopen($dataFile, 'r');
        if (!$fh) {
            printf("%s datafile[%s] can not open\n", $this->_task_name, $dataFile);
            return ;
        }

        $strOut = '';
        while (false !== ($strLine = fgets($fh, 102400)) ){
            $strLine = trim($strLine);
            if ($strLine == '') {
                continue;
            }
            $arrData = json_decode($strLine, true);

            $data = array(
                'ad_user_id'    => (int)$arrData['ad_user_id'],
                'ad_plan_id'    => (int)$arrData['ad_plan_id'],
                'ad_group_id'   => (int)$arrData['ad_group_id'],
                'ad_advert_id'  => (int)$arrData['ad_advert_id'],
                'clicks'        => 1,
                'views'         => 0,
                'total_cost'    => (float)$arrData['price'],
                'create_date'   => $arrData['create_date'],
                'create_time'   => $arrData['create_time'],
                'update_time'   => $arrData['create_time'],
                'ad_channel_id' => (int)$arrData['ad_channel_id'],
                'ad_place_id'   => (int)$arrData['ad_place_id'],
                'type'          => 1, // 1 点击 2 展示
                //
                '_db_id_'       => (int)$arrData['_db_id_'],
                '_table_'       => str_replace('-', '', $arrData['create_date']),
            );
            $strOut .= json_encode($data)."\n";
        }

        fclose($fh);
        $outputFile = Config::item('mediav_stats').$fileName;
        file_put_contents($outputFile, $strOut);
        touch($finishFile);
    }

    // 处理点击日志
    protected function _processOneClickFile ($fileName) {
        $dataFile   = Config::item('mediav_click_path').$fileName;
        $okFile     = $dataFile.'.ok';
        $finishFile = Config::item('mediav_click').$fileName.'.ok';
        if (!file_exists($dataFile)) {
            printf("%s datafile[%s] not exists\n", $this->_task_name, $dataFile);
            return ;
        }
        if (!file_exists($okFile)) {
            printf("%s datafile[%s] not ready\n", $this->_task_name, $dataFile);
            return ;
        }
        if (file_exists($finishFile)) {
            printf("%s datafile[%s] already processed\n", $this->_task_name, $dataFile);
            return ;
        }
        // lock
        if (false == $this->_lock('click', $fileName)) {
            printf("%s get lock faild, prefix[%s] fname[%s]\n", $this->_task_name, 'click', $fileName);
            return ;
        }
        $fh = fopen($dataFile, 'r');
        if (!$fh) {
            printf("%s datafile[%s] can not open\n", $this->_task_name, $dataFile);
            $this->_unlock('click', $fileName);
            return ;
        }

        $outputFile = Config::item('mediav_click').$fileName;
        // 创建新文件
        file_put_contents($outputFile, '');
        while (false !== ($strLine = fgets($fh, 102400)) ){
            $strLine = trim($strLine);
            if ($strLine == '') {
                continue;
            }
            // 格式与_processOneAreaViewFile中描述相同
            $arrData = explode(chr(1), $strLine);
            if ($arrData[2]==0) { // 如果点击是0
                continue;
            }

            if ($arrData[12]==0) { // mediav的过滤
                continue;
            }

            $data = array(
                'click_id'      => 'media_v',   // 固定
                'ad_user_id'    => (int)$arrData[6],
                'ad_advert_id'  => (int)$arrData[5],
                'ad_group_id'   => (int)$arrData[4],
                'ad_plan_id'    => (int)$arrData[3],
                'area_key'      => $arrData[10],
                'bidPrice'      => number_format($arrData[8]/100, 2, '.', ''),
                'price'         => 0, // 会在计费流程中更新
                'create_time'   => date('Y-m-d H:i:s', $arrData[11]),
                'create_date'   => date('Y-m-d', $arrData[11]),
                'ver'           => 'mediav',
                'save_time'     => time(),
                'ad_channel_id' => (int)self::MEDIAV_CHANNEL_ID,
                'ad_place_id'   => (int)self::MEDIAV_PLACE_ID,
                // 辅助变量
                '_db_id_'       => (int)$arrData[6],
                'result'        => (int)self::COST_SUCCESS, // 计费是否成功及失败类型
                'diff_type'     => (int)self::DIFF_TYPE_SAME,  // 计费是否和360的一样
            );
            // 计费流程
            $valid = $this->_caculatePrice($data, $arrData[11]);

            // 写回传日志
            if ($data['diff_type']!==self::DIFF_TYPE_SAME) {
                $this->_writeMediavData($fileName, $data, $arrData);
            }
            if ($valid == false) {
                continue;
            }

            file_put_contents($outputFile, json_encode($data)."\n", FILE_APPEND);
        }

        fclose($fh);
        touch($finishFile);

        // 处理回传日志文件事后处理
        $this->_processMediavDataFile ($fileName);

        $this->_unlock('click', $fileName);

        printf("%s process click file [%s] success\n", $this->_task_name, $fileName);
    }

    protected function _lock($prefix, $fname) {
        $lockPath = Config::item('mediav_lock_path');
        $strMutexFile = sprintf('%s%s_%s.lock', $lockPath, $prefix, $fname);
        $resFile = fopen($strMutexFile, 'a');
        $bolRet = false;
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

    protected function _unlock($prefix, $fname) {
        fclose($this->_resMutexFile);
        $lockPath = Config::item('mediav_lock_path');
        $strMutexFile = sprintf('%s%s_%s.lock', $lockPath, $prefix, $fname);
        @unlink($strMutexFile);
    }

    // 计费 true 成功 false 失败
    protected function _caculatePrice (&$data, $createTime) {
        $logFile = Config::item('mediav_cost_file_path').'mediav_cost';
        // 如果超过两点不再计费，避免影响结算
        $curDate = date('Y-m-d');
        if ($curDate!=$data['create_date']) {
            $curTime = time();
            if ($curTime<$createTime || $curTime - strtotime($data['create_date']) > 86400+3600*2) {
                $data['result'] = self::COST_TIMEOUT;
                $data['diff_type'] |= self::DIFF_TYPE_TIME_OUT;
                $comBineLog = date('YmdHis') . "\t" . "esc_mediav_filter" . "\t" . json_encode($data);
                ComAdLog::write($comBineLog, $logFile);
                return false;
            }
        }
        $userID = $data['ad_user_id'];
        $planID = $data['ad_plan_id'];
        // 获取用户限额
        $userQuotaInfo = ComAdQuotanew::getUserMediavQuotaInfo($userID);
        if (empty($userQuotaInfo)) { // 没有帐户预算
            $data['result'] = self::COST_USER_QUOTA;
            $data['diff_type'] |= self::DIFF_TYPE_INVALID;
            $comBineLog = date('YmdHis') . "\t" . "esc_mediav_filter" . "\t" . json_encode($data);
            ComAdLog::write($comBineLog, $logFile);
            return false;
        }
        if (!isset($userQuotaInfo[$planID])) { // 没有计划预算
            $data['result'] = self::COST_PLAN_QUOTA;
            $data['diff_type'] |= self::DIFF_TYPE_INVALID;
            $comBineLog = date('YmdHis') . "\t" . "esc_mediav_filter" . "\t" . json_encode($data);
            ComAdLog::write($comBineLog, $logFile);
            return false;
        }
        // 获取用户消费情况
        $userCostInfo = ComAdQuotanew::getUserMediavCostInfo(date('j', $createTime), $userID);
        if (empty($userCostInfo)) {
            $userCostInfo = array(
                'cost'  => 0,
            );
        }
        if (!isset($userCostInfo[$planID])) {
            $userCostInfo[$planID] = 0;
        }

        // 检查预算
        if ($userQuotaInfo['quota'] - $userCostInfo['cost']<=0) {
            $data['result'] = self::COST_USER_QUOTA_FULL;
            $data['diff_type'] |= self::DIFF_TYPE_USER_QUOTA;
            $comBineLog = date('YmdHis') . "\t" . "esc_mediav_filter" . "\t" . json_encode($data);
            ComAdLog::write($comBineLog, $logFile);
            return false;
        }
        if ($userQuotaInfo[$planID]!=0 && $userQuotaInfo[$planID] - $userCostInfo[$planID]<=0) {
            // 计划预算为0表示不限制
            $data['result'] = self::COST_PLAN_QUOTA_FULL;
            $data['diff_type'] |= self::DIFF_TYPE_PLAN_QUOTA;
            $comBineLog = date('YmdHis') . "\t" . "esc_mediav_filter" . "\t" . json_encode($data);
            ComAdLog::write($comBineLog, $logFile);
            return false;
        }
        // 计费应扣费用
        $clickPrice = round($data['bidPrice'], 2);
        if ($clickPrice > $userQuotaInfo['quota'] - $userCostInfo['cost']) {
            $clickPrice = round($userQuotaInfo['quota'] - $userCostInfo['cost'], 2);
            $data['diff_type'] |= self::DIFF_TYPE_USER_QUOTA;
        }
        if ($userQuotaInfo[$planID]!=0 && $clickPrice>$userQuotaInfo[$planID] - $userCostInfo[$planID]) {
            $clickPrice = round($userQuotaInfo[$planID] - $userCostInfo[$planID], 2);
            $data['diff_type'] |= self::DIFF_TYPE_PLAN_QUOTA;
        }

        $userCostInfo['cost'] += $clickPrice;
        $userCostInfo[$planID] += $clickPrice;
        ComAdQuotanew::setUserMediavCostInfo(date('j', $createTime), $userID, $userCostInfo);

        $data['price'] = $clickPrice;
        $comBineLog = date('YmdHis') . "\t" . "esc_mediav_click" . "\t" . json_encode($data);
        ComAdLog::write($comBineLog, $logFile);
        return true;
    }

    // 创建回传日志文件
    protected function _createMediavDataFile ($fileName) {
        // todo conf
        $path = Config::item('mediav_send_data_ptah');
        file_put_contents($path.$fileName, '');
    }

    // 写回传日志
    protected function _writeMediavData ($fileName, $logdata, $oriData) {
        $path = Config::item('mediav_send_data_ptah');
        $arrOut = array(
            'mediav',                           // 固定值
            '0',                                // 展现数 0
            '1',                                // 点击数 1
            $logdata['ad_plan_id'],             // 计划id
            $logdata['ad_group_id'],            // 组id
            $logdata['ad_advert_id'],           // 创意id
            $logdata['ad_user_id'],             // 用户id
            $oriData[7],                        // record id for mediav
            $oriData[8],                        // mediav charge cost
            round(100*$logdata['price'], 0),    // cost
            $logdata['diff_type'],              // drop reason
            $oriData[9],                        // keyword hash
            $oriData[10],                       // city
            $oriData[11],                       // 原来日志里的时间戳
            1,                                  // valid 固定为1
        );
        file_put_contents($path.$fileName, implode(chr(1), $arrOut)."\n", FILE_APPEND);
    }

    // 压缩 & md5
    protected function _processMediavDataFile ($fileName) {
        $path = Config::item('mediav_send_data_ptah');
        $filePath = $path.$fileName;
        if (!file_exists($filePath)) {
            return ;
        }
        $cmd = sprintf("/usr/bin/xz -f -z %s", $filePath);
        system($cmd);

        $xzFilePath = $filePath.'.xz';
        $md5sum = md5_file($xzFilePath);
        $this->_arrSendDataFile[$fileName.'.xz'] = $md5sum;

    }
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
