<?php
ini_set('memory_limit','6144M');

class ComCollectQueue
{
    protected $redis;
    protected $prefixQueueKey;
    protected $prefixDataKey;
    protected $prefixFlagKey;

    public function __construct()
    {
        //$this->redis = Yii::app()->collect;
        $this->prefixQueueKey= Config::item('redisKey') . 'collect_queue:';
        $this->prefixDataKey= Config::item('redisKey') . 'collect_data:';
        $this->prefixFlagKey= Config::item('redisKey') . 'collect_flag:';
        //$this->yestodayKey= Config::item('redisKey') . 'collect_yestoday_queue:';
    }

    public function collectStatistic($data, $flagKey)
    {
        //$flagKey的格式为:13031918-5-10.16.15.217-0
        $flagArr = explode("-", $flagKey);
        $fileName = Yii::app()->params['statsLog'] . $flagKey;
        $fh = fopen($fileName, "w");
        foreach($data as $value){
            fwrite($fh, serialize($value) . "\n");
        }
        fclose($fh);
        shell_exec("/bin/gzip -f $fileName");
        return ;
    }

    public function writeToCollect($flagKey, $data)
    {
        $fileName = Config::item('statsLog') . $flagKey;
        ComAdLog::write($data, $fileName);
        return ;
    }

    public function gzipCollect($flagKey)
    {
        $fileName = Config::item('statsLog') . $flagKey;
        shell_exec('/bin/gzip -f ' . $fileName);
        return ;
    }

    public function setFlag($flagKey, $value) {
        //$flagKey的格式为:13031918-5-10.16.15.217-0
        $flagArr = explode("-", $flagKey);
        if ($id = StatsFileLog::model()->getIdByFlagkey($flagKey)) {
            return StatsFileLog::model()->updateStatusById($id, $value);
        } else {
            $arr = array(
                'hour_str' => $flagArr[0],
                'time_str' => sprintf("%d%02d", $flagArr[0], $flagArr[1]),
                'm_inter' => $flagArr[1],
                'ip' => $flagArr[2],
                'sid' => $flagArr[3],
                'hash_value' => '',
                'status' => $value,
                'update_time' => date('Y-m-d H:i:s'),
            );
            $flag = Yii::app()->stats_db->createCommand()->insert(StatsFileLog::model()->tableName(), $arr);
            return $flag;
        }
    }

    public function pushData($data, $sid)
    {
        $ip = trim(`/sbin/ifconfig | /bin/grep eth1 -A 1 | /bin/grep "inet addr" | /bin/awk -F"inet addr:" '{print $2}' | awk '{print $1}'`);
        if (!empty($data)) {
            foreach($data as $row) {
                $k = $this->prefixDataKey . "-" . date('Ymd', $row['data_time']) . "-{$row['aid']}-{$row['channel_id']}-{$row['place_id']}-{$ip}:{$sid}"; 
                $this->redis->setex($k, 86400, serialize($row)); //设置1 days
                $key = $this->prefixQueueKey . "{$ip}:{$sid}"; 
                if (!$this->redis->hExists($key, $k)) {
                    $this->redis->hset($key, $k, 'ad');
                }
            }
        }
    }

    /**
     * add by kangle
     *
     * 2013-07-30 update
     *
     * rsync file
     *
     */
    public function rsyncFile($t=false) {
        echo "rsyncFile process begin at " . date('Y-m-d H:i:s') . "\n";
        $statsFileLog = StatsFileLog::model();
        $statisticedQueue = $statsFileLog->getQueue(StatsFileLog::STATUS_STATISTICED);
        if (!$statisticedQueue)
            $statisticedQueue = array();
        foreach ($statisticedQueue as $row) {
            $fileName = "{$row['hour_str']}-{$row['m_inter']}-{$row['ip']}-{$row['sid']}.gz"; 
            $targetFile = Config::item('statsCollectLog') . $fileName;
            shell_exec("/bin/ls " . Config::item('statsCollectLog') . " | /bin/grep {$row['hour_str']}-{$row['m_inter']}-{$row['ip']}-{$row['sid']}  | /usr/bin/xargs -I{} /bin/rm -f {} ");
            $res = shell_exec("/usr/bin/rsync -avP {$row['ip']}::stats_log/{$fileName} $targetFile --delay-updates --timeout=60");
            //rsync过程中失败
            if ($res === null) {
                if ($row['try_times'] < 3) {
                    $statsFileLog->updateById(array('try_times' => $row['try_times'] + 1), $row['id']);
                } else {
                    $statsFileLog->updateById(array('status' => 20), $row['id']);
                }
                continue;
            }
            //文件是否存在
            if (!file_exists($targetFile)) {
                if ($row['try_times'] < 3) {
                    $statsFileLog->updateById(array('try_times' => $row['try_times'] + 1), $row['id']);
                } else {
                    $statsFileLog->updateById(array('status' => 21), $row['id']);
                }
                continue;
            }
            if (md5_file($targetFile) != $row['hash_value']) {
                if ($row['try_times'] < 3) {
                    $statsFileLog->updateById(array('try_times' => $row['try_times'] + 1), $row['id']);
                } else {
                    $statsFileLog->updateById(array('status' => 22), $row['id']);
                }
                continue;
            }
            $res = shell_exec("/bin/gzip -ldf {$targetFile}");
            if ($res !== null) {
                $statsFileLog->updateStatusById($row['id'], StatsFileLog::STATUS_RSYNCED);
            } else {
                if ($row['try_times'] < 3) {
                    $statsFileLog->updateById(array('try_times' => $row['try_times'] + 1), $row['id']);
                } else {
                    $statsFileLog->updateById(array('status' => 23), $row['id']);
                }
                continue;
            }
        }
        echo "rsyncFile process end at " . date('Y-m-d H:i:s') . "\n";
        return ;
    }

    /**
     * 汇总日志里的数据并入库
     * 单进程在跑
     * add by kangle
     */
    public function collectSum($t=false)
    {
        ini_set('memory_limit','6144M');
        $updateFlag = array(
            date('Y-m-d') => false,
            date('Y-m-d', strtotime('-1 days')) => false,
        );
        $yestodayFlag = false;
        $todayFlag = false;
        $limitMemory = Utility::return_bytes(ini_get('memory_limit'));
        $statsFileLog = StatsFileLog::model();
        $cur_time = time() - 600;
        if ($t !== false && $t >= 0 && $t <= 11) {
            $mInter = $t;
        } else {
            $mInter = Utility::minuteInter(date('i', $cur_time)) ; //取上两个时段的数据
        }
        $hh = date('ymdH', $cur_time);
        echo date('YmdH',$cur_time) . "_{$mInter} begin at " . date('Y-m-d H:i:s') . ".....\n";
        $rsyncedQueue = $statsFileLog->getQueue(StatsFileLog::STATUS_RSYNCED, $mInter);
        $dataArr = array();

        //collectedQueue存已处理过的，内存溢出，或者其它原因中段不一定所有的rsyncedQueue都能执行的
        $collectedQueue = array();
        if (!empty($rsyncedQueue)) {
            foreach($rsyncedQueue as $value){
                if ($value['ip'] == StatsFileLog::IP_ERROR && $value['sid'] == StatsFileLog::SID_ERROR){
                    $this->calErrorLog($value['hour_str'], $value['m_inter']);
                }
            }
            foreach($rsyncedQueue as $id => $value) {
                if (memory_get_usage() > $limitMemory * 0.3)
                    break;
                //$statsFileLog->updateStatusById($id, StatsFileLog::STATUS_COLLECTING);
                $fileName = Yii::app()->params['statsCollectLog'] . "{$value['hour_str']}-{$value['m_inter']}-{$value['ip']}-{$value['sid']}.gz"; 
                if (file_exists($fileName)) {
                    $content = trim(`/bin/zcat $fileName`);
                    if ($content){
                        $content = explode("\n", $content);
                        //$content = unserialize($content);
                        if (!empty($content)) {
                            foreach($content as $data){
                                $data = unserialize($data);
                                $keyStr = $data['aid'] . '-' . $data['channel_id'] . '-' . $data['place_id'] . '-' . date('Y-m-d', $data['data_time']);
                                //只是为了核对数据用，对完数据记得删除
                                //ComAdLog::write(array($keyStr, $data['view']), 'collectToDbLog' . date('ymdH'));
                                if (isset($dataArr[$keyStr])){
                                    $dataArr[$keyStr] = $this->mergeNodeArr($dataArr[$keyStr], $data);
                                }else
                                    $dataArr[$keyStr] = $data;
                                $data = null;
                            }
                        }
                    }
                    $collectedQueue[$id] = $rsyncedQueue[$id];
                }
            }
        }
        $trans = Yii::app()->stats_db->beginTransaction();
        try {
            foreach($collectedQueue as $key => $null){
                $statsFileLog->updateStatusById($key, StatsFileLog::STATUS_COLLECTED, true);
            }
            if (!empty($dataArr)){
                $dataArrKey = "{$hh}-{$mInter}-" . StatsFileLog::IP_ERROR . "-" . StatsFileLog::SID_ERROR;
                $dataArrFileName = Yii::app()->params['statsCollectLog'] . $dataArrKey;
                $dataArrFw = fopen($dataArrFileName, 'w');
                foreach($dataArr as $data){
                    $createDate = date('Y-m-d', $data['data_time']);
                    if (isset($updateFlag[$createDate]))
                        $updateFlag[$createDate] = time();
                    fwrite($dataArrFw, serialize($data) . "\n");
                }
                fclose($dataArrFw);
                shell_exec("/bin/gzip -f $dataArrFileName");
                $this->setFlag($dataArrKey, StatsFileLog::STATUS_RSYNCED);
                $trans->commit();
            } else {
                $trans->commit();
                return ;
            }
        }catch(Exception $e) {
            var_dump($e->getMessage());
            $trans->rollback();
            return ;
        }
        //记下某一天最后更新时间
        foreach ($updateFlag as $date => $time) {
            if ($time !== false)
                shell_exec("/bin/echo $time > " . Yii::app()->params['statsCollectLog'] . $date);
        }

        $dataFlagFileName = Yii::app()->params['statsCollectLog'] . "{$hh}-{$mInter}-127.0.0.1-dataFlag";
        $dataFlagFw = fopen($dataFlagFileName, 'w');
        $stats = new EdcStats();
        $areaStats = new EdcStatsArea();
        $interStats = new EdcStatsInterest();
        $mKeyword = new EdcStatsKeyword();
        $execute = 0;
        $status = 0;
        $pidArr = array();
		$span = 40000;
		$timeArr = array();
		for ($i=0; $i<count($dataArr); $i=$i+$span) {
			if ($i + $span > count($dataArr))
				$lenght = count($dataArr) - $i;
			else
				$lenght = $span;
			$arr = array_slice($dataArr, $i, $lenght, true);


			$pid = pcntl_fork();
			if ($pid == -1){
				die('fork failure!');
			}elseif ($pid){
				//这里是父进程, 记得重连一下db
				Yii::app()->stats_db->setActive(false);
				Yii::app()->stats_db->setActive(true);
				$execute++;
				$pidArr[] = $pid;
				if ($execute >= Config::item('maxCollectToDbProcess')){
					$pid=pcntl_wait($status);
					$execute--;
				}
			} else {
				Yii::app()->stats_db->setActive(false);
				Yii::app()->stats_db->setActive(true);
				$pid = getmypid();
				$timeArr[$pid] = time();
				foreach($arr as $keyStr => $data) {
					if (empty($data))
						continue;
					$trans = Yii::app()->stats_db->beginTransaction();
					try {
						$row = array(
							'ad_advert_id' => $data['aid'],
							'ad_plan_id' => $data['pid'],
							'ad_group_id' => $data['gid'],
							'ad_user_id' => $data['uid'],
							'create_date' => date('Y-m-d', $data['data_time']),
							'last_update_time' => $data['last_time'],
							'views' => intval($data['view']),
							'clicks' => intval($data['click']),
							'trans' => isset($data['trans'])? intval($data['trans']):0,
							'total_cost' => floatval($data['amount']),
							'ad_channel_id' => intval($data['channel_id']),
							'ad_place_id' => intval($data['place_id']),
							'data_source' => 0,
							'admin_user_id' => 0,
							'status' => 0,
						);
						$res = $stats->addIncr($row);
						if (isset($data['area'])) {
							foreach($data['area'] as $k => $value) {
								$list = explode(',', $k);
								$row = array(
									'ad_group_id' => $data['gid'],
									'ad_user_id' => $data['uid'],
									'ad_plan_id' => $data['pid'],
									'area_key' => addslashes($k),
									'clicks' => isset($value['click'])? $value['click']:0,
									'views' => isset($value['view'])? $value['view']:0,
									'trans' => isset($data['trans'])? intval($data['trans']):0,
									'costs' => isset($value['cost'])? number_format($value['cost'],2,'.',''):0,
									'create_date' => date('Y-m-d', $data['data_time']),
								);
								if (count($list) == 2) {
									if ($list[0] == 10001 && $list[1] == 10001) {
										$row['area_id'] = 0;
										$row['area_fid'] = 0;
									} elseif ($list[0] != 10001 && $list[1] == 10001) {
										$row['area_id'] = 0;
										$row['area_fid'] = (int)$list[0];
									} else {
										$row['area_id'] = (int)$list[1];
										$row['area_fid'] = (int)$list[0];
									}
								} else {
									$row['area_id'] = 0;
									$row['area_fid'] = 0;
								}
								$res = $areaStats->updateIncr($row);

								if (!$res) {
									ComAdLog::write($row, Yii::app()->runtimePath . '/collectAreaDbFailure' . date('ymd') . '.log');
								}
								$logData = array(
									date('Y-m-d H:i:s'),
									$k,
									);
								$logData = array_merge($logData, $row);
								ComAdLog::write($logData, 'collectAreatodb.log');
							}
						}
						if (isset($data['inter'])) {
							foreach($data['inter'] as $k => $value) {
								$row = array(
									'ad_group_id' => $data['gid'],
									'ad_user_id' => $data['uid'],
									'ad_plan_id' => $data['pid'],
									'inter_id' => addslashes($k),
									'clicks' => isset($value['click'])? $value['click']:0,
									'views' => isset($value['view'])? $value['view']:0,
									'costs' => isset($value['cost'])? number_format($value['cost'],2,'.',''):0,
									'trans' => isset($data['trans'])? intval($data['trans']):0,
									'create_date' => date('Y-m-d', $data['data_time']),
								);
								if (!$interStats->updateIncr($row)) {
									ComAdLog::write($row, Yii::app()->runtimePath . '/collectInterestDbFailure' . date('ymd') . '.log');
								}
								$logData = array(
									date('Y-m-d H:i:s'),
									$k,
									);
								$logData = array_merge($logData, $row);
								ComAdLog::write($logData, 'collectInteresttodb.log');
							}
						}
						if (isset($data['kw'])) {
							foreach($data['kw'] as $k => $value) {
								$row = array(
									'ad_group_id' => $data['gid'],
									'ad_user_id' => $data['uid'],
									'ad_plan_id' => $data['pid'],
									'keyword' => addslashes($k),
									'clicks' => isset($value['click'])? $value['click']:0,
									'views' => isset($value['view'])? $value['view']:0,
									'costs' => isset($value['cost'])? number_format($value['cost'],2,'.',''):0,
									'trans' => isset($data['trans'])? intval($data['trans']):0,
									'create_date' => date('Y-m-d', $data['data_time']? $data['data_time']:$data['last_time']),
									'ad_keyword_id' => 0,
								);
								if (!$mKeyword->updateIncr($row)) {
									ComAdLog::write($row, Yii::app()->runtimePath . '/collectKeywordDb' . date('ymd') . '.log');
								}
								$logData = array(
									date('Y-m-d H:i:s'),
									$k,
									);
								$logData = array_merge($logData, $row);
								ComAdLog::write($logData, 'collectKeywordtodb.log');
							}
						}
						$trans->commit();
						fwrite($dataFlagFw, $keyStr . "\n");
					}catch(Exception $e) {
						echo "[exception]----";
						print_r($e->getMessage());
						echo "\n";
						$trans->rollback();
					}
				}
				if (isset($pidArr[$pid]))
					unset($pidArr[$pid]);
				$execute--;
				echo $pid . "--- cost " . (time() - $timeArr[$pid]) . " second...\n";
				exit;
			}
		}
        while (!empty($pidArr)) {
            $pid = pcntl_wait($status);
            if ($pid == -1) {
                //echo "parent 异常\n";
                break;
            }
        }
        fclose($dataFlagFw);
        $this->calErrorLog($hh, $mInter);
        echo date('YmdH',$cur_time) . "_{$mInter} end at " . date('Y-m-d H:i:s') . "\n";
        return true;
    }

    public function calErrorLog($hh, $mInter){
        $dataArrKey = "{$hh}-{$mInter}-" . StatsFileLog::IP_ERROR . "-" . StatsFileLog::SID_ERROR;
        $dataArrFileName = Yii::app()->params['statsCollectLog'] . $dataArrKey;
        $dataFlagFileName = Yii::app()->params['statsCollectLog'] . "{$hh}-{$mInter}-127.0.0.1-dataFlag";
        if (file_exists($dataArrFileName . ".gz") && file_exists($dataFlagFileName)){
            if (file_exists($dataArrFileName))
                shell_exec("/bin/rm -f {$dataArrFileName}");
            $dataFlagFw = fopen($dataFlagFileName, 'r');
            $dataFlagArr = array();
            while(!feof($dataFlagFw)){
                $keyStr = trim(fgets($dataFlagFw));
                if ($keyStr){
                    $dataFlagArr[$keyStr] = 1;
                }
            }
            fclose($dataFlagFw);

            $dataArr = array();
            $content = trim(shell_exec("/bin/zcat {$dataArrFileName}.gz"));
            if ($content) {
                $content = explode("\n", $content);
                if (is_array($content)) {
                    foreach($content as $data) {
                        $data = unserialize(trim($data));
                        if (!$data)
                            continue;
                        $keyStr = $data['aid'] . '-' . $data['channel_id'] . '-' . $data['place_id'] . '-' . date('Y-m-d', $data['data_time']);
                        if (!isset($dataFlagArr[$keyStr])){
                            $dataArr[] = serialize($data);
                        }
                    }

                }
            }




            //shell_exec("/bin/gzip -df {$dataArrFileName}.gz");
            //$dataArrFw = fopen($dataArrFileName, 'r');
            //while(!feof($dataArrFw)){
                //$data = unserialize(trim(fgets($dataArrFw)));
                //if ($data){
                    //$keyStr = $data['aid'] . '-' . $data['place_id'] . '-' . date('Y-m-d', $data['data_time']);
                    //if (!isset($dataFlagArr[$keyStr])){
                        //$dataArr[] = serialize($data);
                    //}
                //}
            //}
            //fclose($dataArrFw);

            if (!empty($dataArr)){
                $dataArrFw = fopen($dataArrFileName, 'w');
                foreach($dataArr as $row){
                    fwrite($dataArrFw, $row . "\n");
                }
                fclose($dataArrFw);
                shell_exec("/bin/gzip -f $dataArrFileName");
                shell_exec("/bin/rm -f $dataFlagFileName");
            } else {
                if ($this->setFlag($dataArrKey, StatsFileLog::STATUS_COLLECTED)) {
                    shell_exec("/bin/rm -f {$dataArrFileName}");
                    shell_exec("/bin/rm -f {$dataFlagFileName}");
                }
            }
        }
        return true;
    }

    public function mergeNodeArr($nodeData, $content)
    {
        if (empty($nodeData) && !empty($content)) {
            $nodeData = $content;
            return $nodeData;
        }
        if (isset($nodeData['view']) && isset($content['view']) && $content['view'] > 0)
            $nodeData['view'] += $content['view'];

        if (isset($nodeData['click']) && isset($content['click']) && $content['click'] > 0)
            $nodeData['click'] += $content['click'];

        if (isset($nodeData['trans']) && isset($content['trans']) && $content['trans'] > 0)
            $nodeData['trans'] += $content['trans'];

        if (isset($nodeData['amount']) && isset($content['amount']) && $content['amount'] > 0)
            $nodeData['amount'] += $content['amount'];

        if (isset($content['last_time']))
            if (!(isset($nodeData['last_time']) && $nodeData['last_time'] > $content['last_time']))
                $nodeData['last_time'] = $content['last_time'];

        if (!empty($content['area'])) {
            foreach($content['area'] as $key => $value) {
                if (isset($nodeData['area'][$key]) && is_array($value)) {
                    foreach($value as $k => $v) {
                        if (isset($nodeData['area'][$key][$k])) {
                            $nodeData['area'][$key][$k] += $v; 
                        }else {
                            $nodeData['area'][$key][$k] = $v; 
                        }   
                    }   
                } else {
                    $nodeData['area'][$key] = $value;
                }   
            }   
        }   

        if (!empty($content['inter'])) {
            foreach($content['inter'] as $key => $value) {
                if (isset($nodeData['inter'][$key]) && is_array($value)) {
                    foreach($value as $k => $v) {
                        if (isset($nodeData['inter'][$key][$k])) {
                            $nodeData['inter'][$key][$k] += $v; 
                        }else {
                            $nodeData['inter'][$key][$k] = $v; 
                        }   
                    }   
                } else {
                    $nodeData['inter'][$key] = $value;
                }   
            }   
        }   

        if (!empty($content['kw'])) {
            foreach($content['kw'] as $key => $value) {
                if (isset($nodeData['kw'][$key]) && is_array($value)) {
                    foreach($value as $k => $v) {
                        if (isset($nodeData['kw'][$key][$k])) {
                            $nodeData['kw'][$key][$k] += $v; 
                        }else {
                            $nodeData['kw'][$key][$k] = $v; 
                        }   
                    }   
                } else {
                    $nodeData['kw'][$key] = $value;
                }   
            }   
        }   


        return $nodeData;
    }
}

?>
