<?php
/**
 * mediav相关的操作
 *
 */
Yii::import('application.extensions.CEmqPublisher');
class MediavV2Command extends CConsoleCommand {

    // 计费失败类型
    const COST_SUCCESS              = 0; // 计费成功
    const COST_TIMEOUT              = 1; // 第二天两点后不再计费以前的消费
    const COST_USER_QUOTA           = 2; // 无帐户预算
    const COST_PLAN_QUOTA           = 3; // 没有计划预算
    const COST_USER_QUOTA_FULL      = 4; // 帐户预算已经满
    const COST_PLAN_QUOTA_FULL      = 5; // 计划预算已经满
    const COST_USER_BALANCE         = 6; // 用户已无余额
    const COST_SYSTEM_ERROR         = 7; // 系统问题，计费失败，比如redis连接失败

    // 跟 midiav 回传的数据类型
    const DIFF_TYPE_SAME        = 0; // 完全相同
    const DIFF_TYPE_TIME_OUT    = 1; // 超时不计费
    const DIFF_TYPE_INVALID     = 2; // 物料有效性检测失败(用户不存在、计划预算不存在)
    const DIFF_TYPE_USER_QUOTA  = 4; // 表示超用户当天预算
    const DIFF_TYPE_PLAN_QUOTA  = 8; // 超推广计划当天预算
    const DIFF_TYPE_BALANCE     = 16;// 无余额

    const MEDIAV_CHANNEL_ID = 49;
    const MEDIAV_PLACE_ID   = 199;

    protected $_task_name = '';

    protected $_resMutexFile = array();

    protected $_arrSendDataFile = array();

    // 下线消息时使用
    protected $_balance = 0;
    protected $_quota   = 0;

    /**
     * 每五分钟下载一次数据文件
     * time 精确到分 201407211200
     * done
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

        $daoMvMetaFile = new MvMetaFile(); $daoMvMetaFile->setDB(Yii::app()->db_click_log);
        $daoMvDataFile = new MvDataFile(); $daoMvDataFile->setDB(Yii::app()->db_click_log);


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

        $metaID = $daoMvMetaFile->addNewFile($fileName);
        if ($metaID === false) {
            printf("%s insert into meta db fail, file[%s]!\n", $taskName, $fileName);
            return ;
        }

        $metaContents = file($filePath.$fileName);
        if (!empty($metaContents)) {

            foreach ($metaContents as $strLine) {
                $strLine = trim($strLine);
                if ($strLine=='') {
                    continue;
                }
                list ($md5sum, $dataFileName) = explode("  ", $strLine);
                $urlSource = $url.$dataFileName;
                $localPath = $filePath.$dataFileName;
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
                    printf("%s get data file[%s] fail\n", $taskName, $dataFileName);
                    continue;
                } else {
                    printf("%s get data file[%s] success\n", $taskName, $dataFileName);
                }

                // 先备份，再解压
                $cmd = sprintf("cp %s %s.bk; /usr/bin/xz -f -d %s", $localPath, $localPath, $localPath);
                system($cmd);

                $dataFileName   = substr($dataFileName, 0, -3);
                $localPath      = $filePath.$dataFileName;
                if (preg_match('/\\.c\\./', $dataFileName)) {
                    $fileType = MvDataFile::FILE_TYPE_CLICK;
                    $toPath = Config::item('mediav_click_path').$dataFileName;
                } else {
                    $fileType = MvDataFile::FILE_TYPE_VIEW;
                    $toPath = Config::item('mediav_view_path').$dataFileName;
                }
                copy($localPath, $toPath);
                touch($toPath.'.ok');

                $daoMvDataFile->addNewFile($dataFileName, $md5sum, $metaID, $fileType);
            }
        } else {
            printf("%s mete file[%s] is empty!\n", $taskName, $fileName);
        }

        // 标记已经下载完成
        $daoMvMetaFile->updateStatusByName($fileName, 1);

        $endTime = time();
        printf("%s begin at %s, end at %s\n", $taskName, date('Y-m-d H:i:s', $beginTime), date('Y-m-d H:i:s', $endTime));
    }

    /**
     * 五分钟计费
     * 每五分钟启动一次，通过文件锁与 StatsCommand 保持互斥
     */
    public function actionProcessClick() {
        $this->_task_name = $taskName = sprintf("[ProcessClick_%s]", date('Ymd-His'));
        $beginTime = time();
        printf("%s begin\n", $taskName);

        // 获取锁
        if (false == $this->_lock('task', 'click')) {
            printf("%s get lock fail, quit\n", $taskName);
            return ;
        }

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
            if (!file_exists($mediavClickPath.$file.'.ok')) {
                continue;
            }
            $arrFile[] = $file;
        }
        // var_dump($arrFile);


        $processFileCnt = 0;
        if (!empty($arrFile)) {
            $daoMvDataFile = new MvDataFile(); $daoMvDataFile->setDB(Yii::app()->db_click_log);
            $finishFilePath = Config::item('mediav_click_res');
            foreach ($arrFile as $oneFile) {
                if (!file_exists($mediavClickPath.$oneFile.'.ok')) {
                    continue;
                }
                if (file_exists($finishFilePath.$oneFile.'.ok')) {
                    continue;
                }
                $ret = $this->_processOneClickFile($oneFile);
                if ($ret === true) {
                    $daoMvDataFile->updateStatusByName($oneFile, 1);
                }
                $processFileCnt++;
            }
        }
        // sleep(100);
        $this->_unlock('task', 'click');
        $endTime = time();
        printf("%s begin at %s, end at %s, process %d files\n",
            $taskName, date('Y-m-d H:i:s', $beginTime), date('Y-m-d H:i:s', $endTime), $processFileCnt
        );
    }

    /**
     * 处理展现日志，每五分钟处理一次
     * done
     */
    public function actionProcessView($time=null) {
        if (is_null($time)) {
            $_tmp = date('YmdH');
            $minute = floor(date('i') / 5) * 5 ;
            $time = $_tmp.sprintf("%02d", $minute);
        }

        $this->_task_name = $taskName = sprintf("[ProcessView_%s]", date('Ymd-His'));
        $beginTime = time();
        printf("%s begin, param time[%s]\n", $taskName, $time);

        if (false == $this->_lock(__CLASS__, __FUNCTION__)) {
            printf("%s get lock fail, quit\n", $taskName);
            return ;
        }

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
            if (!file_exists($mediavViewPath.$file.".ok")) {
                continue;
            }
            $arrFile[] = $file;
        }
        // var_dump($arrFile);

        $processFileCnt = 0;
        if (!empty($arrFile)) {
            $daoMvDataFile = new MvDataFile(); $daoMvDataFile->setDB(Yii::app()->db_click_log);
            $finishFilePath = Config::item('mediav_view_second');
            foreach ($arrFile as $oneFile) {
                if (!file_exists($mediavViewPath.$oneFile.'.ok')) {
                    continue;
                }
                if (file_exists($finishFilePath.$oneFile.'.ok')) {
                    continue;
                }
                $ret = $this->_processOneViewFile($oneFile);
                if ($ret===true) {
                    $daoMvDataFile->updateStatusByName($oneFile, 1);
                }
                $processFileCnt++;
            }
        }

        $this->_unlock(__CLASS__, __FUNCTION__);
        $endTime = time();
        printf("%s begin at %s, end at %s, process %d files\n",
            $taskName, date('Y-m-d H:i:s', $beginTime), date('Y-m-d H:i:s', $endTime), $processFileCnt
        );
    }

    /**
     * 生成超投文件
     * done
     */
    public function actionProcessOverCharge() {
        $beginTime = time();
        $this->_task_name = $taskName = sprintf("[ProcessOvercharge %s]", date('Ymd-His', $beginTime));
        printf("%s begin\n", $taskName);

        $daoMvMetaFile = new MvMetaFile(); $daoMvMetaFile->setDB(Yii::app()->db_click_log);
        $daoMvDataFile = new MvDataFile(); $daoMvDataFile->setDB(Yii::app()->db_click_log);

        $totalMeta = 0;
        do {
            // 取出所有已经下载完的待处理 meta 文件
            $arrMeta = $daoMvMetaFile->getByStatus(1);
            // var_dump($arrMeta);
            if (empty($arrMeta)) {
                break;
            }
            foreach ($arrMeta as $oneMeta) {
                $metaID = $oneMeta['id'];
                // 取出所有属于这个meta的文件
                $arrDataFiles = $daoMvDataFile->getByMetaIDAndType($metaID, MvDataFile::FILE_TYPE_CLICK);
                // var_dump($arrDataFiles);

                // 检验是否已经完成了
                $bolReady = true;
                if (!empty($arrDataFiles)) {
                    foreach ($arrDataFiles as $oneDataFile) {
                        if ($oneDataFile['status'] != 1) {
                            $bolReady = false;
                            break;
                        }
                    }
                }
                if ($bolReady === false) {
                    printf("%s meta file [%s] not ready\n", $taskName, $oneMeta['file_name']);
                    continue;
                }

                // 处理
                $this->_arrSendDataFile = array();
                if (!empty($arrDataFiles)) {
                    foreach ($arrDataFiles as $oneDataFile) {
                        $inFile = Config::item('mediav_click_res').$oneDataFile['file_name'];
                        // var_dump($inFile);
                        $inFh = fopen($inFile, 'r');
                        if (!$inFh) {
                            printf("%s can not open file[%s]", $taskName, $inFile);
                            continue;
                        }

                        $outFile = Config::item('mediav_send_data_ptah').$oneDataFile['file_name'];
                        $outFh = fopen($outFile, 'w');
                        if (!$outFh) {
                            printf("%s can not open file[%s]", $taskName, $outFile);
                            fclose($inFh);
                            continue;
                        }


                        while (false !== ($strLine = fgets($inFh, 102400))) {
                            $strLine = trim($strLine);
                            if ($strLine=='') {
                                continue;
                            }
                            $arrLineData = json_decode($strLine, true);
                            if ($arrLineData === false) {
                                printf("%s invalid data, file[%s] line[%s]\n", $taskName, $oneFile['file_name'], $strLine);
                                continue;
                            }

                            if ($arrLineData['diff_type'] != self::DIFF_TYPE_SAME) {
                                // print_r($arrLineData);
                                $arrOut = array(
                                    'mediav',                               // 固定值
                                    '0',                                    // 展现数 0
                                    '1',                                    // 点击数 1
                                    $arrLineData['ad_plan_id'],             // 计划id
                                    $arrLineData['ad_group_id'],            // 组id
                                    $arrLineData['ad_advert_id'],           // 创意id
                                    $arrLineData['ad_user_id'],             // 用户id
                                    $arrLineData['recordid'],               // record id for mediav
                                    round($arrLineData['bidPrice']*100, 0), // mediav charge cost
                                    round(100*$arrLineData['price'], 0),    // cost
                                    $arrLineData['diff_type'],              // drop reason
                                    $arrLineData['kw_md5'],                 // keyword hash
                                    $arrLineData['area_key'],               // city
                                    strtotime($arrLineData['create_time']), // 原来日志里的时间戳
                                    1,                                      // valid 固定为1
                                );
                                fwrite($outFh, implode(chr(1), $arrOut)."\n");
                            }
                        }
                        fclose($inFh);
                        fclose($outFh);

                        $this->_processMediavDataFile($oneDataFile['file_name']);

                    }
                }
                // 写metafile
                $strOut = '';
                if (!empty($this->_arrSendDataFile)) {
                    foreach ($this->_arrSendDataFile as $_fn => $_md5) {
                        $strOut .= sprintf("%s  %s\n", $_md5, $_fn);
                    }
                }
                $metaFilePath = Config::item('mediav_send_data_ptah').$oneMeta['file_name'];
                file_put_contents($metaFilePath, $strOut);

                // 标记已经完成
                $daoMvMetaFile->updateStatusByName($oneMeta['file_name'], 2);

                printf("%s %s done\n", $taskName, $oneMeta['file_name']);
                $totalMeta++;
            }
        } while(false);

        $endTime = time();
        printf("%s begin at %s, end at %s, %d metafiles processd\n",
            $taskName,
            date('Y-m-d H:i:s', $beginTime),
            date('Y-m-d H:i:s', $endTime),
            $totalMeta
        );
    }

    /**
     * 生成入 mv_stats_{date} 表的数据文件
     * done
     */
    public function actionProcessMvStatsFile () {
        $beginTime = time();
        $this->_task_name = $taskName = sprintf("[ProcessMvStatsFile %s]", date('Ymd-His', $beginTime));
        printf("%s begin\n", $taskName);

        $clickFilePath = Config::item('mediav_click_res');
        $dirFh = opendir($clickFilePath);
        if (!$dirFh) {
            printf("%s can not open dir[%s]\n", $taskName, $clickFilePath);
            return ;
        }

        $finishFileDir = Config::item('mediav_click_second');
        $arrFiles = array();
        while (($_oneFile = readdir($dirFh)) !== false) {
            if ($_oneFile == '.' || $_oneFile=='..') {
                continue;
            }
            if (is_dir($clickFilePath.$_oneFile)) {
                continue;
            }
            if (substr($_oneFile, -3, 3)=='.ok') {
                continue;
            }
            $okFile = $clickFilePath.$_oneFile.".ok";
            if (!file_exists($okFile)) {
                continue;
            }

            $finishFile = $finishFileDir.$_oneFile.'.ok';
            if (file_exists($finishFile)) {
                continue;
            }
            $arrFiles[] = $_oneFile;
        }

        $totalFileNum = 0;
        if (!empty($arrFiles)) {
            foreach ($arrFiles as $_oneFile) {
                // 转换
                $ret = $this->_convertResToStatsClick($_oneFile);
                if ($ret) {
                    $totalFileNum++;
                }
            }
        }

        $endTime = time();
        printf("%s begin at %s, end at %s, %d file processd\n",
            $taskName,
            date('Y-m-d H:i:s', $beginTime),
            date('Y-m-d H:i:s', $endTime),
            $totalFileNum
        );
    }

    // 成功返回 true
    protected function _convertResToStatsClick($fileName) {
        $finishFileDir = Config::item('mediav_click_second');
        $finishFile = $finishFileDir.$fileName.'.ok';
        if (file_exists($finishFile)) {
            printf("%s file [%s] already processd\n", $this->_task_name, $fileName);
            return false;
        }
        $inFile = Config::item('mediav_click_res').$fileName;
        $inFh = fopen($inFile, 'r');
        if (!$inFh) {
            printf("%s can not open file[%s]\n", $this->_task_name, $inFile);
            return false;
        }
        $outFile = $finishFileDir.$fileName;
        $outFh = fopen($outFile, 'w');
        if (!$outFh) {
            printf("%s can not open file[%s]\n", $this->_task_name, $outFile);
            fclose($inFh);
            return false;
        }
        while (false !== ($strLine = fgets($inFh, 102400)) ) {
            $strLine = trim($strLine);
            if ($strLine == '') {
                continue;
            }
            $arrData = json_decode($strLine, true);
            if (false === $arrData) {
                printf("%s invalid data, file[%s] line[%s]\n", $this->_task_name, $inFile, $strLine);
                continue;
            }
            if ($arrData['result'] != self::COST_SUCCESS) {
                continue;
            }

            $arrOut = array(
                'ad_user_id'    => $arrData['ad_user_id'],
                'views'         => 0,
                'clicks'        => (int)$arrData['clicks'],
                'costs'         => (float)$arrData['price'],
                'create_time'   => $arrData['create_time'],
                'update_time'   => $arrData['create_time'],

                '_db_id_'       => $arrData['ad_user_id'],
                '_table_'       => date('Ymd',  strtotime($arrData['create_time'])),
            );
            fwrite($outFh, json_encode($arrOut)."\n");
        }


        fclose($inFh);
        fclose($outFh);
        touch($finishFile);
        printf("%s file [%s] process done\n", $this->_task_name, $outFile);
        return true;
    }

    /**
     * 生成入 mv_click_log_{date} 表的数据文件
     * done
     */
    public function actionProcessMvClicklogFile () {
        $beginTime = time();
        $this->_task_name = $taskName = sprintf("[ProcessMvClicklogFile %s]", date('Ymd-His', $beginTime));
        printf("%s begin\n", $taskName);

        $clickFilePath = Config::item('mediav_click_res');
        $dirFh = opendir($clickFilePath);
        if (!$dirFh) {
            printf("%s can not open dir[%s]\n", $taskName, $clickFilePath);
            return ;
        }

        $finishFileDir = Config::item('mediav_click_log');
        $arrFiles = array();
        while (($_oneFile = readdir($dirFh)) !== false) {
            if ($_oneFile == '.' || $_oneFile=='..') {
                continue;
            }
            if (is_dir($clickFilePath.$_oneFile)) {
                continue;
            }
            if (substr($_oneFile, -3, 3)=='.ok') {
                continue;
            }
            $okFile = $clickFilePath.$_oneFile.".ok";
            if (!file_exists($okFile)) {
                continue;
            }

            $finishFile = $finishFileDir.$_oneFile.'.ok';
            if (file_exists($finishFile)) {
                continue;
            }
            // printf("%s\n", $_oneFile);
            $arrFiles[] = $_oneFile;
        }
        // var_dump($arrFiles);

        $totalFileNum = 0;
        if (!empty($arrFiles)) {
            foreach ($arrFiles as $_oneFile) {
                // 转换
                $ret = $this->_convertResToClicklog($_oneFile);
                if ($ret) {
                    $totalFileNum++;
                }
            }
        }

        $endTime = time();
        printf("%s begin at %s, end at %s, %d file processd\n",
            $taskName,
            date('Y-m-d H:i:s', $beginTime),
            date('Y-m-d H:i:s', $endTime),
            $totalFileNum
        );
    }

    // 成功返回 true
    protected function _convertResToClicklog($fileName) {
        $finishFileDir = Config::item('mediav_click_log');
        $finishFile = $finishFileDir.$fileName.'.ok';
        if (file_exists($finishFile)) {
            printf("%s file [%s] already processd\n", $this->_task_name, $fileName);
            return false;
        }
        $inFile = Config::item('mediav_click_res').$fileName;
        $inFh = fopen($inFile, 'r');
        if (!$inFh) {
            printf("%s can not open file[%s]\n", $this->_task_name, $inFile);
            return false;
        }
        $outFile = $finishFileDir.$fileName;
        $outFh = fopen($outFile, 'w');
        if (!$outFh) {
            printf("%s can not open file[%s]\n", $this->_task_name, $outFile);
            fclose($inFh);
            return false;
        }
        while (false !== ($strLine = fgets($inFh, 102400)) ) {
            $strLine = trim($strLine);
            if ($strLine == '') {
                continue;
            }
            $arrData = json_decode($strLine, true);
            if (false === $arrData) {
                printf("%s invalid data, file[%s] line[%s]\n", $this->_task_name, $inFile, $strLine);
                continue;
            }

            $arrOut = array(
                'file_name'     => $fileName,
                'line_num'      => (int)$arrData['line_num'],
                'ad_user_id'    => $arrData['ad_user_id'],
                'clicks'        => $arrData['clicks'],
                'req_price'     => $arrData['bidPrice'],
                'real_price'    => $arrData['price'],
                'diff_type'     => $arrData['diff_type'],
                'reduce_price'  => 0,
                'create_time'   => $arrData['create_time'],
                'update_time'   => $arrData['create_time'],

                '_table_'       => date('Ymd', strtotime($arrData['create_time'])),
            );
            fwrite($outFh, json_encode($arrOut)."\n");
        }


        fclose($inFh);
        fclose($outFh);
        touch($finishFile);
        printf("%s file [%s] process done\n", $this->_task_name, $outFile);
        return true;
    }

    /**
     * 同步给mediav的文件并转移
     * done
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

    // 处理点击日志
    // 成功返回true
    protected function _processOneClickFile ($fileName) {
        $dataFile   = Config::item('mediav_click_path').$fileName;
        $okFile     = $dataFile.'.ok';
        $finishFile = Config::item('mediav_click_res').$fileName.'.ok';
        if (!file_exists($dataFile)) {
            printf("%s datafile[%s] not exists\n", $this->_task_name, $dataFile);
            return false;
        }
        if (!file_exists($okFile)) {
            printf("%s datafile[%s] not ready\n", $this->_task_name, $dataFile);
            return false;
        }
        if (file_exists($finishFile)) {
            printf("%s datafile[%s] already processed\n", $this->_task_name, $dataFile);
            return false;
        }
        // lock
        if (false == $this->_lock('click', $fileName)) {
            printf("%s get lock faild, prefix[%s] fname[%s]\n", $this->_task_name, 'click', $fileName);
            return false;
        }
        $fh = fopen($dataFile, 'r');
        if (!$fh) {
            printf("%s datafile[%s] can not open\n", $this->_task_name, $dataFile);
            $this->_unlock('click', $fileName);
            return false;
        }

        $outFile = Config::item('mediav_click_res').$fileName;
        $outFh = fopen($outFile, 'w');
        if (!$outFh) {
            printf("%s outputfile[%s] can not open\n", $this->_task_name, $outFile);
            $this->_unlock('click', $fileName);
            return false;
        }

        $intLineNum = 1;
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

            $ad_user_id = (int)$arrData[6];
            if($ad_user_id<=0){
                continue;
            }
            $data = array(
                'click_id'      => 'media_v',   // 固定
                'clicks'        => (int)$arrData[2],
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
                'line_num'      => $intLineNum++,
                'recordid'      => $arrData[7],
                'kw_md5'        => $arrData[9],
                // 辅助变量
                '_db_id_'       => (int)$arrData[6],
                'result'        => (int)self::COST_SUCCESS, // 计费是否成功及失败类型
                'diff_type'     => (int)self::DIFF_TYPE_SAME,  // 计费是否和360的一样
            );
            $this->_balance = $this->_quota = 0;


            list($area_fid,$area_id)=explode(",",$arrData[10]);
            $time = time();
            $detail=array(
                'click_id'=>substr(md5($arrData[6].$arrData[10]. $arrData[8] .$arrData[11]. php_uname('n').posix_getpid() . microtime(true)), 8, 16),
                'click_time'=>$arrData[11],
                'ad_user_id'=>(int)$arrData[6],
                'keyword'=>$arrData[9],
                'area_fid'=>$area_fid,
                'area_id'=>$area_id,
                'price'=>$data['bidPrice'],
                'create_date'=>date('Y-m-d', $arrData[11]),
                'cid'=>self::MEDIAV_CHANNEL_ID,
                'pid'=>self::MEDIAV_PLACE_ID,
                'ver'=>'mediav',
                'create_time'=>$time,
                'update_time'=>$time,
                'reduce_price'=>0,
                'pos'=>$data['line_num'],
                'apitype'=>0,
                'type'=>1,
                'cheat_type'=>0,
                'source_type'=>1,
                'source_system'=>4,
                'extension'=>0,
                'status'=>0,
                );

            // 计费流程

            //$cal_res=$this->_caculatePrice($data, $arrData[11]);
            //开始计算费用
            $ret=ComQuota::cost($detail['ad_user_id'],0,(float)$detail['price'],(int)$detail['click_time'],$detail['click_id'],'mv',$detail['ver']);
            $detail['reduce_price']=$detail['price']-$ret['real_cost'];
            $data['price'] = $ret['real_cost'];
            Utility::log(__CLASS__,"COST_RET",array($detail['click_id'],$ret));
            if(!($ret['result']===true))
            {
                $data['result'] = self::COST_TIMEOUT;
                $data['diff_type'] |= self::DIFF_TYPE_TIME_OUT;
                Utility::sendAlert(__CLASS__,"MV_NEED_REQUEUE_ERROR",json_encode(array($data,$ret)));
                sleep(5);
            }
            else
            {
                if($ret['offline_type']==-1)
                {
                    $data['result'] = self::COST_TIMEOUT;
                    $data['diff_type'] |= self::DIFF_TYPE_TIME_OUT;
                }
                else if($ret['offline_type']==1)
                {
                    $data['result'] = self::COST_USER_BALANCE;
                    $data['diff_type'] |= self::DIFF_TYPE_BALANCE;
                }
                else if($ret['offline_type']==2)
                {
                    $data['result'] = self::COST_USER_QUOTA_FULL;
                    $data['diff_type'] |= self::DIFF_TYPE_USER_QUOTA;
                }
            }

            fwrite($outFh, json_encode($data)."\n");

            $offlineType = $ret['offline_type'];

            $detail['extension'] = $offlineType;
            ComAdDetail::insertDetail($detail, $arrData[11]);
	    //写quota redis 数据
            $list_arr=array();
            $list_arr['click_id']=$detail['click_id'];
            $list_arr['get_sign']='';
            $list_arr['click_time']=$detail['click_time'];
            $list_arr['view_id']='';
            $list_arr['view_time']='';
            $list_arr['ip']='';
            $list_arr['mid']='';
            $list_arr['ad_user_id']=(int) $detail['ad_user_id'];
            $list_arr['ad_advert_id']=0;
            $list_arr['ad_group_id']= 0;
            $list_arr['ad_plan_id']= 0;
            $list_arr['ls']='';
            $list_arr['src']='';
            $list_arr['area_fid']=$detail['area_fid'];
            $list_arr['area_id']=$detail['area_id'];
            $list_arr['price']=(float) $detail['price'];
            $list_arr['bidprice']=(float) $detail['price'];
            $list_arr['create_date']=$detail['create_date'];
            $list_arr['cid']=(int) $detail['cid'];
            $list_arr['pid']=(int) $detail['pid'];
            $list_arr['ver']='mediav';
            $list_arr['create_time']=$detail['create_time'];
            $list_arr['update_time']=$detail['update_time'];
            $list_arr['sub_ver']='';
            $list_arr['sub_data']='';
            $list_arr['tag_id']='';
            $list_arr['apitype']=0;
            $list_arr['type']=1;
            $list_arr['cheat_type']=0;
            $list_arr['source_type']=1;
            $list_arr['source_system']=4;
            $list_arr['status']=0;
            //$this->push_quota_click_redis($list_arr);
            $this->sendClickInfo($list_arr,$ret);
            // 发送撞线
            // rmq 中的格式
            // {"mid":"2a255202653bddd8d9904b4200e2cd2e","msg_src":"esc_mediavv2","time":1414483788,"logid":"ESC_141448378814003286","exchange":"ex_overlimit_offline","content":{"advert_id":0,"ad_group_id":0,"ad_plan_id":0,"ad_user_id":289932195,"offline_type":1,"ad_type":"mediav","time":1414483788}}

            if ($offlineType >0 && !isset($ret['no_need_offline']) ) {
                $curTime = gettimeofday();
                $logID = sprintf("ESC_TO_EMQAPI_MV_%d%06d%02d", $curTime['sec'], $curTime['usec'], mt_rand(0, 99));
                $mqData = array(
                    'advert_id'         => 0, // 为保持消息格式一致，置0
                    'ad_group_id'       => 0, // 为保持消息格式一致，置0
                    'ad_plan_id'        => 0, // 为保持消息格式一致，置0
                    'ad_user_id'        => $data['ad_user_id'],
                    'offline_type'      => $offlineType,
                    'ad_type'           => 'mediav',
                    'time'              => time(),
                    'balance'           => $ret['balance'],
                    'userQuota'         => $ret['userQuota'],
                    'planQuota'         => 0,
                    'needOfflineLog'    => $ret['needOfflineLog'],
                );

                static   $emq;
                $emq=new ComEMQ('emq_audit');
                $emq->exchangeName='ex_overlimit_offline';
                $emq->logid=time();
                $emq->send($mqData,0);
                Utility::log(__CLASS__,__FUNCTION__,$mqData);
            }
        }

        fclose($fh);
        fclose($outFh);
        touch($finishFile);
        $this->_unlock('click', $fileName);
        printf("%s process click file [%s] success\n", $this->_task_name, $fileName);
        return true;
    }

    // 处理展现日志 成功返回true
    protected function _processOneViewFile ($fileName) {
        $dataFile   = Config::item('mediav_view_path').$fileName;
        $okFile     = $dataFile.'.ok';
        $finishFile = Config::item('mediav_view_second').$fileName.'.ok';
        if (!file_exists($dataFile)) {
            printf("%s datafile[%s] not exists\n", $this->_task_name, $dataFile);
            return false;
        }
        if (!file_exists($okFile)) {
            printf("%s datafile[%s] not ready\n", $this->_task_name, $dataFile);
            return false;
        }
        if (file_exists($finishFile)) {
            printf("%s datafile[%s] already processed\n", $this->_task_name, $dataFile);
            return false;
        }
        $fh = fopen($dataFile, 'r');
        if (!$fh) {
            printf("%s datafile[%s] can not open\n", $this->_task_name, $dataFile);
            return false;
        }

        $outData = array();
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
             *  3  pid             广告计划id 二期上线后此数据无效果，但保留格式
             *  4  gid             广告组id 二期上线后此数据无效果，但保留格式
             *  5  aid             广告创意id 二期上线后此数据无效果，但保留格式
             *  6  uid             广告主用户id
             *  7  recordid        mediav 的记录id
             *  8  cost            消费(整数单位为分)
             *  9  keyword         hash，不需要
             * 10  city            地域id
             * 11  now             打点时时间戳
             * 12  valid           是否作弊
             */
            $arrData = explode(chr(1), $strLine);
            if ($arrData[12]==0) { // 被过滤的
                continue;
            }
            if ($arrData[1] == 0) { // 过滤展现数为0的
                continue;
            }
            $ad_user_id = (int)$arrData[6];
            if($ad_user_id<=0){
                continue;
            }
            $key = sprintf("%d-%s", (int)$arrData[6], date('Ymd'));
            $clickDateTime = date('Y-m-d H:i:s', $arrData[11]);
            if (!isset($outData[$key])) {
                $outData[$key] = array(
                    'ad_user_id'    => (int)$arrData[6],
                    'device_type'   => 1,
                    'views'         => 0,
                    'clicks'        => 0,
                    'costs'         => 0,
                    'create_time'   => $clickDateTime,
                    'update_time'   => $clickDateTime,
                    '_db_id_'       => (int)$arrData[6],
                    '_table_'       => date('Ymd', $arrData[11]),
                );
            }
            $outData[$key]['views'] += (int)$arrData[1];
            if ($outData[$key]['update_time'] < $clickDateTime) {
                $outData[$key]['update_time'] = $clickDateTime;
            }
        }
        fclose($fh);

        $outputFile = Config::item('mediav_view_second').$fileName;
        $outputFh = fopen($outputFile, 'w');
        if (!$outputFh) {
            printf("%s outputFile[%s] can not open\n", $this->_task_name, $outputFile);
            return ;
        }
        if (!empty($outData)) {
            foreach ($outData as $key => $oneData) {
                fwrite($outputFh, json_encode($oneData)."\n");
            }
        }
        fclose($outputFh);
        touch($finishFile);

        printf("%s process file[%s] done\n", $this->_task_name, $dataFile);
        return true;
    }

    protected function _lock($prefix, $fname) {
        $lockPath = Config::item('mediav_lock_path');
        $strMutexFile = sprintf('%s%s_%s.lock', $lockPath, $prefix, $fname);
        $resFile = fopen($strMutexFile, 'a');
        $bolRet = false;
        if ($resFile) {
            $bolRet = flock($resFile, LOCK_EX | LOCK_NB);
            if ($bolRet) {
                $key = sprintf("%s_%s", $prefix, $fname);
                $this->_resMutexFile[$key] = $resFile;
            } else {
                fclose($resFile);
            }
        }

        return $bolRet;
    }

    protected function _unlock($prefix, $fname) {
        $key = sprintf("%s_%s", $prefix, $fname);
        if (isset($this->_resMutexFile[$key])) {
            fclose($this->_resMutexFile[$key]);
            unset($this->_resMutexFile[$key]);
        }
        $lockPath = Config::item('mediav_lock_path');
        $strMutexFile = sprintf('%s%s_%s.lock', $lockPath, $prefix, $fname);
        @unlink($strMutexFile);
    }

    // 计费 true 成功 false 失败
    protected function _caculatePrice (&$data, $createTime) {
        // for test
        // $data['price'] = $data['bidPrice'];
        // return true;
        // 如果超过两点不再计费，避免影响结算
        $curDate = date('Y-m-d');
        if ($curDate!=$data['create_date']) {
            $curTime = time();
            // 为了有时间执行 mv 点击入 ad_click_log 库，时间限制收缩到 1:30
            if ($curTime<$createTime || $curTime - strtotime($data['create_date']) > 86400+3600+1800) {
                $data['result'] = self::COST_TIMEOUT;
                $data['diff_type'] |= self::DIFF_TYPE_TIME_OUT;
                return false;
            }
        }
        $userID = $data['ad_user_id'];

        // 获取用户限额
        $userMVQuota = ComAdQuotaV2::getUserMediavQuota($userID);
        if (false === $userMVQuota) { // 没有帐户预算
            $data['result'] = self::COST_USER_QUOTA;
            $data['diff_type'] |= self::DIFF_TYPE_INVALID;
            return false;
        }
        $userDJQuotaInfo = ComAdQuotaV2::getUserDJQuotaInfo($userID);
        if (!isset($userDJQuotaInfo['balance'])) {
            $data['result'] = self::COST_USER_QUOTA;
            $data['diff_type'] |= self::DIFF_TYPE_INVALID;
            return false;
        }
        $userBalance = $userDJQuotaInfo['balance'];

        // 获取用户消费情况
        $userMVCost = ComAdQuotaV2::getUserMediavCost(date('j', $createTime), $userID);
        if ($userMVCost === false) {
            //连接redis失败
            $data['result'] = self::COST_SYSTEM_ERROR;
            $data['diff_type'] |= self::DIFF_TYPE_INVALID;
            return false;
        }

        // 检查预算

        if ($userMVQuota!=0 && round($userMVCost- $userMVQuota * 1.1, 2)>=0) {
            $data['result'] = self::COST_USER_QUOTA_FULL;
            $data['diff_type'] |= self::DIFF_TYPE_USER_QUOTA;
            $this->_quota = $userMVQuota;
            return false;
        }
        // 检查余额
        $userDJCostInfo = ComAdQuotaV2::getUserDJCostInfo(date('j', $createTime), $userID);
        $userDJCost = isset($userDJCostInfo['cost']) ? $userDJCostInfo['cost'] : 0;
        if ($userBalance - $userDJCost - $userMVCost <= 0) {
            $clickPrice = round($userBalance - $userDJCost - $userMVCost, 2);
            $data['result'] = self::COST_USER_BALANCE;
            $data['diff_type'] |= self::DIFF_TYPE_BALANCE;
            $this->_balance = $userBalance;
            return false;
        }

        // 计费应扣费用
        $clickPrice = number_format($data['bidPrice'], 2, '.', '');

        if ($userMVQuota>0 && $clickPrice > number_format($userMVQuota - $userMVCost, 2, '.', '')) {
            if ($clickPrice > number_format($userMVQuota*1.1 - $userMVCost, 2, '.', '')) {
                $clickPrice = number_format($userMVQuota*1.1 - $userMVCost, 2, '.', '');
            }

            $data['diff_type'] |= self::DIFF_TYPE_USER_QUOTA;
            $this->_quota = $userMVQuota;
            if ($clickPrice<=0) {
                return false;
            }
        }
        if ($clickPrice > $userBalance - $userDJCost - $userMVCost) {
            $clickPrice = number_format($userBalance - $userDJCost - $userMVCost, 2, '.', '');
            $data['diff_type'] |= self::DIFF_TYPE_BALANCE;
            $this->_balance = $userBalance;
        }

        if ($clickPrice<0) { // 保险一点
            $clickPrice = 0;
        }
        $data['price'] = $clickPrice;

        $userMVCost = number_format($userMVCost + $clickPrice, 2, '.', '');
        ComAdQuotaV2::setUserMediavCost(date('j', $createTime), $userID, $userMVCost);
        $userCost['click_id']=$data['click_id'];
        $userCost['click_price']=$clickPrice;
        $userCost['process_id']=getmypid();
        $userCost['userMVCost']=$userMVCost;
        $userCost['userID']=$userID;

        $comBineLog = date('YmdHis') . "\t" . "esc_user_cost_log_mv" . "\t" . json_encode($userCost);
        ComAdLog::write($comBineLog, '/dev/shm/user_cost_log');
        return true;
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

    private function push_quota_click_redis($list)
    {
        static $redis;
        $redis = new ComRedis('esc_monitor_redis', 0);
        $arr=array(
            'get_sign'=> $list['get_sign'],
            'view_id' => $list['view_id'],
            'ip' => $list['ip'],
            'type' => $list['type']==1?'click':'view',
            'now' => (int) $list['click_time'],
            'view_time' =>(int) $list['view_time'],
            'apitype' =>  $list['apitype'],
            'pid' => (int) $list['ad_plan_id'],
            'place' => $list['location'],
            'pos' => (int) $list['pos'],
            'gid' => (int) $list['ad_group_id'],
            'aid' => (int) $list['ad_advert_id'],
            'uid' => (int) $list['ad_user_id'],
            'price' => (float) $list['price'],
            'mid' => (string) $list['mid'],
            'city_id' => $list['area_fid'] . ',' . $list['area_id'],
            'keyword' => $list['keyword'],
            'query' => $list['query'],
            'matchtype' => 0,//这个暂时没有
            'click_id' => $list['click_id'],
            'channel_id' => (int) $list['cid'],
            'place_id' => (int) $list['pid'],
            'ls' => (string) $list['ls'],
            'src' => (string) $list['src'],
            'guid' => '',
            'site' => '',
            'ver' => $list['ver'],
            'subver' => $list['sub_ver'],
            'subdata' => $list['sub_data'],
            'sub_ad_info' =>$list['sub_ad_info'],
            'buckettest' => 0,
            'source_type'=>(int) $list['source_type'],
        );
        $redis->rpush("open_ad_v1:stats:",json_encode($arr));
    }

    protected function sendClickInfo($list,$ret)
    {

        $data=array(
            'get_sign'=> $list['get_sign'],
            'view_id' => $list['view_id'],
            'ip' => $list['ip'],
            'type' => $list['type']==1?'click':'view',
            'now' => (int) $list['click_time'],
            'view_time' =>(int) $list['view_time'],
            'apitype' =>  $list['apitype'],
            'pid' => (int) $list['ad_plan_id'],
            'place' => $list['location'],
            'pos' => (int) $list['pos'],
            'gid' => (int) $list['ad_group_id'],
            'aid' => (int) $list['ad_advert_id'],
            'uid' => (int) $list['ad_user_id'],
            'price' => (float) $list['price'],
            'mid' => (string) $list['mid'],
            'city_id' => $list['area_fid'] . ',' . $list['area_id'],
            'keyword' => $list['keyword'],
            'query' => $list['query'],
            'matchtype' => 0,//这个暂时没有
            'click_id' => $list['click_id'],
            'channel_id' => (int) $list['cid'],
            'place_id' => (int) $list['pid'],
            'ls' => (string) $list['ls'],
            'src' => (string) $list['src'],
            'guid' => '',
            'site' => '',
            'ver' => $list['ver'],
            'subver' => $list['sub_ver'],
            'subdata' => $list['sub_data'],
            'sub_ad_info' =>$list['sub_ad_info'],
            'buckettest' => 0,
            'source_type'=>(int) $list['source_type'],
            'file_click'=>1,
        );

        static $emq;
        $emq=new ComEMQ('emq_esc');
        $emq->exchangeName='click_info';
        $emq->logid=Utility::getLoggerID('ESC');
        $data['clickPrice']=$ret['real_cost'];
        $data['cost_info']=$ret;
        $data['settleTime']  = time();
        $res = $emq->send($data);
        Utility::log(__CLASS__,__FUNCTION__,$res);
    }

}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
