<?php

/**
 * Author: dengchao@360.cn
 *
 * Last modified: 2013-05-7 11:37
 *
 * Filename: statsreport.php
 *
 * Description: 统计form表，按 广告位时间分组入库。 
 *
 */
include __DIR__ . '/CommonCommand.php'; 
set_time_limit(0);
class StatsReportCommand extends CommonCommand
{
    private  $eDate,$sDate,$pageSize,$user_category_main,$dataConfig;
    protected $_arrUserRouter = array();
    protected $_arrUserName = array();
    public function __construct() {
        $this->eDate = date("Y-m-d",time());
        $this->sDate = date('Y-m-d',strtotime('-1 day')); 
        $this->pageSize = 20000; 
        $this->user_category_main = Yii::app()->params['user_category'];
        $this->dataConfig = Config::item('user_category');
    }    
    public function actionTest()
    {
        return ;
        print_r(Yii::app()->db_form);
        return ;
    }
    /**
     * @desc 频道广告位报告
     *
     */
    public function actionInsertForm()
    {
       //$formObj = Yii::app()->db_form;
	$AdPlace = new AdPlace();
        $PlaceInfo = $AdPlace->getList('status>0',0,1000);//列出所有广告位ID;
	 $baseInfo= array();
        if($PlaceInfo)
        {
            foreach($PlaceInfo as $key=>$val)
            {
              $adPlaceIdArr[] = $val['id'];//广告位id
              $baseInfo[$val['id']]['placeid'] = $val['id'];
              $baseInfo[$val['id']]['channelid'] = $val['ad_channel_id'];
              $baseInfo[$val['id']]['placename'] = $val['name'];
            }
            if(isset( $adPlaceIdArr) &&  $adPlaceIdArr)
            {
                $pidStr = implode($adPlaceIdArr, ',');
                $groupcount = $this->getCountGroupIdByPid($pidStr);
               if($groupcount>0)
               {
                    for($gi=0;$gi<=$groupcount;$gi+=$this->pageSize)
                    {
                        $groupInfo = $this->getGroupIdByPid($pidStr,$gi);
                        foreach($groupInfo as $gkey=>$gval)
                        {
                               $baseInfo[$gval['ad_place_id']]['groupid'][] = $gval['id'];
                               $baseInfo[$gval['ad_place_id']]['planid'] = $gval['ad_plan_id'];
                               $baseInfo[$gval['ad_place_id']]['groupname'] = $gval['title'];

                        }
                        if(isset($baseInfo) && $baseInfo)
                        {
                            $this->mainPrg($baseInfo);//入库主程序
                        }
                        echo '第'.$gi.'批已完成!'."\r\n";
                    }
               }    

            }

        }

    }
    /**
     * @ 创意报表数据插入form表
     * 
     * 
    **/
   public function actionAdvertInsertForm()
   {
       $count = $this->getCountStats();
       $mid = ($count['mid'])?$count['mid']:0;
       $xid = $count['xid'];
       $insertC = 0;
       for($i=$mid;$i<=$xid;$i+=$this->pageSize)
       {
          //echo $i."\r\n";
         $baseInfoS = $this->getStatsInfo($i);
         if($baseInfoS) 
         {    //$baseInfo[] = $baseInfoS;
             foreach($baseInfoS as $val)
             {       
                 $insertC++;
                 echo "正在插入第".$insertC."条\r\n";
                 $this->insertForm('ad_advert_report',$val);
             }
         }      
        }
   }
   /**
    * @desc 取统计信息
    * @param int $name 开始条数
    */
   private function getStatsInfo($current)
   {
        $maxid = $current+$this->pageSize;
        $data =$adAdvertIdArr=$userIdArr =$planIdArr=$channelIdArr=$placeIdArr= array();
      /*  $sql = "select a.* from (select ad_advert_id,ad_user_id,ad_place_id,ad_group_id,ad_channel_id,sum(clicks) clicks,sum(views) views,sum(total_cost) cost,SUM(trans) trans from ad_stats where "
        ."create_date>='{$this->sDate}' and create_date<='{$this->eDate}'"
        ." group by ad_advert_id) a limit $current,$this->pageSize";  */
        $sql = "select ad_advert_id,ad_user_id,ad_place_id,ad_group_id,ad_channel_id,ad_plan_id, clicks,views, total_cost as cost,trans,create_date from ad_stats where id>=$current and id<$maxid and create_date>='{$this->sDate}' and create_date<'{$this->eDate}'";
        echo $sql."\r\n";
        $rows =  Yii::app()->db_center->createCommand($sql)->queryAll($sql);
        //echo "主键time".time()."\r\n";
        if($rows)
        {
            foreach($rows as $key=>$val)
            {
                $adAdvertIdArr[] = $val['ad_advert_id'];
                $channelIdArr[] = $val['ad_channel_id'];
                $placeIdArr[]  = $val['ad_place_id'];  
                $userIdArr[] =$val['ad_user_id'];
                $planIdArr[] = $val['ad_plan_id'];            
                $dataInfo[$key] = $val;
            }
            echo "创意前时间:".time()."\r\n";
            $this->setAdvertInfo($dataInfo,$adAdvertIdArr);
            echo "用户前时间:".time()."\r\n";
            $this->setUserInfo($dataInfo,$userIdArr);
             echo "计划前时间:".time()."\r\n";
            $this->setPlanInfo($dataInfo,$planIdArr);
             echo "频道前时间:".time()."\r\n";
            $this->setChannelInfo($dataInfo,$channelIdArr);
             echo "广告位前时间:".time()."\r\n";
            $this->setPlaceInfo($dataInfo,$placeIdArr);
            return $dataInfo;
        }
   }
   private function setAdvertInfo(&$dataInfo,$adAdvertIdArr)
   {
        if(isset($adAdvertIdArr) && $adAdvertIdArr )
        {
           $adAdvertIdArr = array_unique($adAdvertIdArr);
           $adAdvertIdStr = implode(',', $adAdvertIdArr);
          //创意相关信息
           $advertInfo =  $this->getAdvertInfo($adAdvertIdStr);
           if($advertInfo)
           {
                    foreach($dataInfo as $key=>$val)
                    {
                        if(isset($advertInfo[$val['id']]) && $advertInfo[$val['id']] )
                        {
                            $dataInfo[$key]['caption'] =  $advertInfo[$val['id']]['caption'];
                            $dataInfo[$key]['ad_type'] =   $advertInfo[$val['id']]['ad_type'];
                        }    
                    }    
           }
        }
   }
   private function setUserInfo(&$dataInfo,$userIdArr)
   {
       if(isset($userIdArr) && $userIdArr)
        {
            $userIdArr = array_unique($userIdArr);
            $userIdStr = implode(',',$userIdArr);
            $userInfo = $this->getUserInfo($userIdStr);
            if($userInfo && $dataInfo)
            {
                foreach($dataInfo as $key=>$val)
                {
                    if(isset($userInfo[$val['ad_user_id']]) && $userInfo[$val['ad_user_id']])
                    {
                      $dataInfo[$key]['user_name'] =  $userInfo[$val['ad_user_id']]['user_name'];
                      $dataInfo[$key]['company_name'] =  $userInfo[$val['ad_user_id']]['company_name'];
                      $dataInfo[$key]['client_category'] =  $userInfo[$val['ad_user_id']]['client_category'];
                      $dataInfo[$key]['client_category_name'] =  isset($this->dataConfig['client'][$userInfo[$val['ad_user_id']]['client_category']]) ? $this->dataConfig['client'][ $userInfo[$val['ad_user_id']]['client_category']] : '';
                      $dataInfo[$key]['signed_category'] =  $userInfo[$val['ad_user_id']]['signed_category'];
                      $dataInfo[$key]['signed_category_name'] =  isset($this->dataConfig['signed'][$userInfo[$val['ad_user_id']]['signed_category']]) ? $this->dataConfig['signed'][$userInfo[$val['ad_user_id']]['signed_category']] : '';;
                      $dataInfo[$key]['industry_category'] =  $userInfo[$val['ad_user_id']]['industry_category'];
                      $dataInfo[$key]['industry_category_name'] =  isset(Yii::app()->params['industry'][$userInfo[$val['ad_user_id']]['industry_category']]) ?Yii::app()->params['industry'][$userInfo[$val['ad_user_id']]['industry_category']] : '';; 
                    }
                }    
            }
        }//end
   }
   private function setPlanInfo(&$dataInfo,$planIdArr)
   {
        if(isset($planIdArr) && $planIdArr)
        {
            $planIdArr = array_unique($planIdArr);               
            $planIdStr = implode(',',$planIdArr);
            $planInfo = $this->getPlanInfo($planIdStr);
        }
         if($dataInfo && $planInfo)
        {
            foreach($dataInfo as $key=>$val)
            {
                 if(isset($planInfo[$val['ad_plan_id']]) && $planInfo[$val['ad_plan_id']] ) 
                 {
                    $dataInfo[$key]['ad_plan_name'] = $planInfo[$val['ad_plan_id']] ['title'];
                    //$dataInfo[$key]['ad_plan_id'] = $planInfo[$val['ad_plan_id']] ['id'];
                    $dataInfo[$key]['start_date'] = $planInfo[$val['ad_plan_id']] ['start_date'];
                    $dataInfo[$key]['end_date'] = $planInfo[$val['ad_plan_id']] ['end_date'];
                 }
            }   

        }
   }
   private function setChannelInfo(&$dataInfo,$channelIdArr)
   {
                //频道相关信息
         if(isset($channelIdArr) && $channelIdArr )
         {
            $channelIdArr = array_unique($channelIdArr);                      
            $channelIdStr = implode(',', $channelIdArr); 
            $channelInfo =$this->getChannelInfo($channelIdStr);
         }   
         if($channelInfo && $dataInfo)
         {
            foreach($dataInfo as $key=>$val)
            {
                   if(isset($channelInfo[$val['ad_channel_id']]) && $channelInfo[$val['ad_channel_id']])
                   {
                      $dataInfo[$key]['ad_channel_name'] =  $channelInfo[$val['ad_channel_id']]['name'];
                   }
            } 
         }
   }
   private function setPlaceInfo(&$dataInfo,$placeIdArr)
   {
        if(isset($placeIdArr) && $placeIdArr )
       {
          $placeIdArr = array_unique($placeIdArr);  
          $placeIdStr = implode(',', $placeIdArr); 
          $placeInfo =$this->getPlaceInfo($placeIdStr);
       }
       if($placeInfo && $dataInfo)
       {
            foreach($dataInfo as $key=>$val)
            {
               if(isset($placeInfo[$val['ad_place_id']]) && $placeInfo[$val['ad_place_id']])
               {
                 $dataInfo[$key]['ad_place_name'] =$placeInfo[$val['ad_place_id']]['name'];
               }
            }  
      }

   }
   private function getPlaceInfo($idstr)
   {
       $key = Utility::getAdRedisKey(__CLASS__.__FUNCTION__,$idstr.'place');
       $data = Yii::app()->redis->get($key);
       if($data)
       {
           return unserialize($data);
       }
       $sql = "select id,name from ad_place where id in($idstr)";
       $rowsTmp =  Yii::app()->db_center->createCommand($sql)->queryAll(); 
       if($rowsTmp)
       {
           foreach($rowsTmp as $key=>$val)
           {
               $rows[$val['id']] = $val;
           }    
       }
       Yii::app()->redis->setex($key,36000, serialize($rows));       
       return $rows;
   }
   private function getChannelInfo($idstr)
   {   
       $key = Utility::getAdRedisKey(__CLASS__.__FUNCTION__,$idstr.'channel');
       $data = Yii::app()->redis->get($key);
       if($data)
       {
           return unserialize($data);
       }       
       $sql = "select id,name from ad_channel where id in($idstr)";
       $rowsTmp =  Yii::app()->db_center->createCommand($sql)->queryAll();
       if($rowsTmp)
       {
           foreach($rowsTmp as $key=>$val)
           {
               $rows[$val['id']] = $val;
           }    
       }    
       Yii::app()->redis->setex($key,36000, serialize($rows));
       return  $rows;
   }
   /**
    * @通过planid查询信息
    * @param str $idstr 主键串
    */
   private function getPlanInfo($idstr)
   {
       $key = Utility::getAdRedisKey(__CLASS__.__FUNCTION__,$idstr.'plan');
       $data = Yii::app()->redis->get($key);
       if($data)
       {
           return unserialize($data);
       }
       $sql = "select id,title,start_date,end_date from ad_plan where id in($idstr)";
       $rowsTmp =  Yii::app()->db_center->createCommand($sql)->queryAll();
       if($rowsTmp)
       {
           foreach($rowsTmp as $key=>$val)
           {
               $rows[$val['id']] = $val;
           }   
       }   
        Yii::app()->redis->setex($key,36000, serialize($rows));
       return $rows;
        //print_r($rowsTmp);
   }
   /**
    * @通过用户id查询用户信息
    * @param str $idstr 用户id字符串
    */
   private function getUserInfo($idstr)
   {
       if(!$idstr)return;
       $key = Utility::getAdRedisKey(__CLASS__.__FUNCTION__,$idstr.'a');
       $data = Yii::app()->redis->get($key);
       if($data)
       {
           return unserialize($data);
       }
       $sql = "select id,user_name,industry_category,client_category,signed_category,company_name from ad_user where id in($idstr)";
       $rowsTmp =  Yii::app()->db_center->createCommand($sql)->queryAll();
       if($rowsTmp)
       {
           foreach($rowsTmp as $key=>$val)
           {
               $rows[$val['id']] = $val;
           }    
       }
        Yii::app()->redis->setex($key,36000, serialize($rows));
       return $rows;
      // print_r($rowsTmp);
   }
   /**
    * @desc 通过创意Id查询信息
    */
   private function getAdvertInfo($idstr)
   {
       $key = Utility::getAdRedisKey(__CLASS__.__FUNCTION__,$idstr);
       $data = Yii::app()->redis->get($key);
       if($data && 0)
       {
           return unserialize($data);
       }       
       $sql = "select id,caption,ad_user_id,ad_type,ad_group_id,ad_plan_id from ad_advert where id in($idstr)";
       $rowsTmp =  Yii::app()->db_center->createCommand($sql)->queryAll();
       if($rowsTmp)
       { 
           foreach($rowsTmp  as $val)
           {
              $rows[$val['id']] = $val;
           }
       }
         Yii::app()->redis->setex($key,36000, serialize($rows));
       return $rows;
   }
   /**
    *@desc 统计记录数
    */
   private function getCountStats()
   {
        $sql = "select min(id) as mid,max(id) as xid from ad_stats where create_date>='{$this->sDate}' and create_date<'{$this->eDate}'" ;
        echo $sql."\r\n";
        $cmd = Yii::app()->db_center->createCommand($sql);
        $count = $cmd->queryRow();
        return $count;
   }
    /**
     * @deprecated since version number
     * @param type $baseInfo 
     */
    private function mainPrg($baseInfo)
    {
          $sDate = date("Y-m-d",time());
          $eDate = date('Y-m-d',strtotime('- 1 day'));
          foreach($baseInfo as $val)
          {
            //print_r($val['groupid']);  
	    if($val['groupid'] && is_array($val['groupid']))
              {
               $gCount = $this->getCountByGroupId($val['groupid'], $sDate, $eDate);
                for($i=0;$i<=$gCount;$i+=$this->pageSize)
                {
                $data = $this->getReportDataByGroupId($val['groupid'], $sDate, $eDate,$i);         
                if($data)
                {
                    foreach($data as $dval)
                    {
                        $inserInfo['ad_channel_id'] = $val['channelid'];
                        $inserInfo['ad_group_id'] = implode(',', $val['groupid']);
                        $inserInfo['ad_plan_id'] = $val['planid'];
                        $inserInfo['ad_place_id'] = $val['placeid'];
                        $inserInfo['ad_group_name'] = $val['groupname'];
                        $inserInfo['ad_place_name'] = $val['placename'];
                        $inserInfo['clicks'] = $dval['clicks'] ;
                        $inserInfo['views'] = $dval['views'];
                        $inserInfo['trans'] = $dval['trans'];
                        $inserInfo['cost'] = $dval['cost'];
                        $inserInfo['click_percent'] = $dval['click_percent'];
                        $inserInfo['click_cost'] = $dval['click_cost'];
                        $inserInfo['trans_percent'] = $dval['transclick'];
                        $inserInfo['costtrans'] = $dval['costtrans'];
                        $inserInfo['view_percent'] = ($dval['views']>0)? number_format( $dval['cost'] / $dval['views'] * 1000, 2, '.', ''):0;
                        $inserInfo['create_date'] = $dval['create_date'];
                        //处理插入
                        $sInfo = Yii::app()->db_form->createCommand("select * from ad_channel_place_report where ad_place_id={$val['placeid']} and create_date='{$dval['create_date']}'")->queryRow();
                        if(!$sInfo)
                        {$this->insertForm('ad_channel_place_report',$inserInfo);}
                        else{
                           Yii::app()->db_form->createCommand()->update("ad_channel_place_report",$inserInfo,"id=:id",array(":id"=>$sInfo['id']));
                        }
                    }
                }   
                 }

              }
          }
    }
    private function insertForm($table,$data)
    {
        if (Yii::app()->db_form->createCommand()->insert($table, $data)) {
            return Yii::app()->db_form->createCommand('SELECT LAST_INSERT_ID()')->queryScalar();
        }
        return false;
    }
    /**
     * @deprecated since version number
     * @author dengchao do
     */
    private function getReportDataByGroupId($groupid,$sdate,$edate,$start=0)
    {
        $adStatsObj = new AdStats();
        $where = " AND create_date>='{$edate}' AND create_date<'{$sdate}' ";
        if (is_array($groupid)) {
            $where = " ad_group_id in (" . implode(',', $groupid) . ") "
                    . $where . " AND status IN(1,2) GROUP BY create_date";
        } else {
            $where = " ad_group_id=" . intval($groupid) . $where . " AND status IN(1,2) GROUP BY create_date";
        }
        $sql = "SELECT SUM(clicks) as clicks, SUM(views) as views, SUM(trans) as trans, ROUND(SUM(FLOOR(total_cost*100)/100),2) as cost, create_date FROM ad_stats  WHERE {$where} limit $start,$this->pageSize";
        $rows = Yii::app()->db_center->createCommand($sql)->queryAll();
        if ($rows) {
            foreach ($rows as $key => $row) {
                $row['transclick'] = $adStatsObj->getClickPercent($row['trans'], $row['clicks']);
                $row['costtrans'] = $adStatsObj->getClickPrice($row['cost'], $row['trans']);
                $row['click_percent'] = $adStatsObj->getClickPercent($row['clicks'], $row['views']);
                $row['click_cost'] = $adStatsObj->getClickPrice($row['clicks'], $row['cost']);
                $result[] = $row;
            }
        }
        return $result;
    }
    /**
     *@desc 按组count便于分段执行
     * 
     **/
    private function getCountByGroupId($groupid,$sdate,$edate)
    {
        $adStatsObj = new AdStats();
        $where = " AND create_date>='{$edate}' AND create_date<'{$sdate}' ";
        if (is_array($groupid)) {
            $where = " ad_group_id in (" . implode(',', $groupid) . ") "
                    . $where . " AND status IN(1,2) GROUP BY create_date";
        } else {
            $where = " ad_group_id=" . intval($groupid) . $where . " AND status IN(1,2) GROUP BY create_date";
        }
        $sql = "select count(1) as acount from (select create_date from ad_stats where {$where}) a";
        $cmd = Yii::app()->db_center->createCommand($sql);
        $count = $cmd->queryRow();
        return $count['acount'];  
    }
    /**
     * @desc 通过placeid查询组ID
     * @param str $name Description placeid字符串
     * @author dengchao Doe <dengchao@360.cn>
     * **/
    private function getGroupIdByPid($pidstr,$start=0)
    {
       $key = Utility::getAdRedisKey(__CLASS__.__FUNCTION__,$pidstr.$start);
       $data = Yii::app()->redis->get($key);
       if($data)
       {
           return unserialize($data);
       }          
        $sql = "select `id`,`title`,ad_plan_id,ad_place_id from ad_group where ad_place_id in ($pidstr) limit $start,$this->pageSize";
        $rows = Yii::app()->db_center->createCommand($sql)->queryAll();
        Yii::app()->redis->setex($key,36000, serialize($rows));
        return $rows;
    }
    private function getCountGroupIdByPid($pidstr)
    {
       $key = Utility::getAdRedisKey(__CLASS__.__FUNCTION__,$pidstr);
       $data = Yii::app()->redis->get($key);
       if($data)
       {
           return unserialize($data);
       }          
       $sql = "select count(1) as acount from ad_group where ad_place_id in ($pidstr)";
       $rows = Yii::app()->db_center->createCommand($sql)->queryRow();
       Yii::app()->redis->setex($key,36000, serialize($rows['acount']));
       return $rows['acount'];
    }        

    /**
     * add by kangle
     *
     * 2013-07-16
     *
     * 通过用户提交的定制报告计划生成task
     *
     */
    public function actionAssignBookTask()
    {
        $today = date('Y-m-d');
        $Houre = date('H');
        $imin = date('i');
        $sql = "select * from " . AdStatsBookReport::model()->tableName() . " where status = " . AdStatsBookReport::STATUS_START ; 
        $rows = Yii::app()->db_book_report->createCommand($sql)->queryAll();
        foreach ($rows as $row) {
            if (AdStatsBookReport::BOOKCYCLE_WEEK == $row['book_cycle'] && date('w') != 1)
                continue;
            if (AdStatsBookReport::BOOKCYCLE_MONTH == $row['book_cycle'] && date('d') != 1)
                continue;
            if (AdStatsBookReport::BOOKCYCLE_SINGLE != $row['book_cycle'] && !($Houre == 8 && $imin == 0))
                continue;
            $transaction = Yii::app()->db_book_report->beginTransaction();
            try {
                $dateArr = $this->getCalDate($row['select_date']);
                $arr = array(
                    'ad_user_id' => $row['ad_user_id'],
                    'start_date' => $dateArr['start_date'],
                    'end_date' => $dateArr['end_date'],
                    'data' => $row['data'],
                    'file_type' => $row['file_type'],
                    'create_date' => $today,
                    'create_time' => date('Y-m-d H:i:s'),
                    'update_time' => date('Y-m-d H:i:s'),
                    'download_key' => '',
                    'issend'=>0,
                    'parent_id' => $row['id'],
                    'type' => $this->getTypeByData($row),
                    'status' => AdStatsBookReportTask::STATUS_START,
                );
                Yii::app()->db_book_report->createCommand()->insert('ad_stats_book_report_task', $arr);
                if (AdStatsBookReport::BOOKCYCLE_SINGLE == $row['book_cycle']) {
                    $updateArr['status'] = AdStatsBookReport::STATUS_FINISH;
                    Yii::app()->db_book_report->createCommand()->update('ad_stats_book_report', $updateArr, 'id=' . (int)$row['id']);
                }
                $transaction->commit();
            } catch (LogicException $e) {
                $transaction->rollback();
                print_r($e->getMessage() . "\n");
                continue;
            } catch (Exception $e) {
                $transaction->rollback();
                print_r($e->getMessage() . "\n");
                continue;
            }
            ComAdLog::write($arr, 'statsAssignBookTask');
        }
        echo date('Y-m-d H:i:s') . "ok--------\n";
        return ;
    }

    /**
     * add by kangle
     *
     * 2013-07-16
     *
     * 这个方法是用来判断mysql查还是hadoop查
     *
     * 查询时间大于3个月，小于1年的走hadoop，1年之前的不提供查询
     *
     */
    public function getTypeByData($row)
    {
        if (isset($row['data']) && ($data=json_decode($row['data'], true)) && isset($data['table_name']) && isset($row['select_date'])) {
            $hadoopTableArr = array(
                'ad_stats_area_group_report', 'ad_stats_area_plan_report', 'ad_stats_report_keyword','ad_stats_neighbor_report', 'ad_stats_advert_report', 'ad_stats_group_report','ad_stats_plan_report','ad_stats_area_report_province','ad_stats_biyi_report',
            ); 
            if (!in_array($data['table_name'], $hadoopTableArr))
                return AdStatsBookReport::TYPE_MYSQL;

            if (!strpos($row['select_date'], ','))
                return AdStatsBookReport::TYPE_MYSQL;
            else {
                $dateArr = explode(',', $row['select_date']);
                if (strtotime($dateArr[0]) >= strtotime('-3 month')) {
                    return AdStatsBookReport::TYPE_MYSQL;
                } elseif (strtotime($dateArr[0]) >= strtotime('-1 year')) {
                    return AdStatsBookReport::TYPE_HADOOP;
                } else {
                  //  throw new LogicException("select date earlier than 1 year.");
                    return AdStatsBookReport::TYPE_HADOOP;
                }
            }
        } else {
            throw new LogicException("params error");
        }
        return ;
    }

    /**
     * add by kangle
     *
     * 2013-07-17
     *
     * 这个方法是把昨天、前天、上个月转换成开始时间和结束时间
     *
     */
    public function getCalDate($selectDate)
    {
        switch ($selectDate) {
            case AdStatsBookReport::SELECTDATE_YESTODAY :
                $endDate = $startDate = date('Y-m-d', strtotime('-1 day'));
                break;
            case AdStatsBookReport::SELECTDATE_BEFOREYESTODAY :
                $endDate = $startDate = date('Y-m-d', strtotime('-2 day'));
                break;
            case AdStatsBookReport::SELECTDATE_LAST7DAY :
                $startDate = date('Y-m-d', strtotime('-7 day'));
                $endDate = date('Y-m-d', strtotime('-1 day'));
                break;
            case AdStatsBookReport::SELECTDATE_LASTWEEK :
                $startDate = date("Y-m-d", mktime(0, 0 , 0, date('m'), date('d') - date('w') - 6, date('Y'))); 
                $endDate = date("Y-m-d", mktime(0, 0 , 0, date('m'), date('d')-date('w'), date('Y'))); 
                break;
            case AdStatsBookReport::SELECTDATE_CURRENTMONTH :
                $startDate = date('Y-m-01', strtotime('-1 day'));
                $endDate = date('Y-m-t', strtotime('-1 day'));
                break;
            case AdStatsBookReport::SELECTDATE_LASTMONTH :
                $startDate = date("Y-m-01", mktime(0, 0 , 0, date('m') - 1, date('d'), date('Y'))); 
                $endDate = date("Y-m-t", mktime(0, 0 , 0, date('m') - 1, date('d'), date('Y'))); 
                break;
            case AdStatsBookReport::SELECTDATE_EASTMONTH :
                $endDate = date('Y-m-d', strtotime('-1 day')); 
                $startDate = date('Y-m-d', strtotime("-1 month",  strtotime($endDate)));
                break;            
            default :
                $dateArr = explode(',', $selectDate);
                if (!$dateArr || count($dateArr) != 2)
                    throw new LogicException("select date - '{$selectDate}' is not in rule...");
                if (strtotime($dateArr[0]) > strtotime('-5 year') && strtotime($dateArr[1]) <= time()) {
                    $startDate = $dateArr[0];
                    $endDate = $dateArr[1];
                } else {
                    throw new LogicException("select date - '{$selectDate}' is not in rule.");
                }
        }
        return array('start_date' => $startDate, 'end_date' => $endDate);
    }

    /**
     * add by kangle
     *
     * 2013-07-19
     *
     * mysql去处理定制任务
     *
     */
    public function actionCalBookReport()
    {
        //只允许一个process在执行
        $mypid  = getmypid();
        $syscmd = "ps -ef | grep -i 'statsreport CalBookReport' | grep -v grep |grep -v vim |grep -v vi |grep -v defunct |grep -v '/bin/sh'| grep -v {$mypid} | wc -l";
        $cmd = @popen($syscmd, 'r');
        $num = @fread($cmd, 512);
        $num += 0;
        @pclose($cmd);
        if ($num >= 1) {
            echo date('Y-m-d H:i:s') . "\tstatscallbookreport process is running, exit this process\n";
            return ;
        }
        $res = array();
        ini_set('memory_limit', '20480M');
        $sql = "select * from " . AdStatsBookReportTask::model()->tableName() . " where create_date='" . date('Y-m-d') 
            . "' and type=" . AdStatsBookReport::TYPE_MYSQL . " and status=" . AdStatsBookReportTask::STATUS_START . " limit 100";
        $rows = Yii::app()->db_book_report->createCommand($sql)->queryAll();
        
        $cassandra = new ComCassandra();
        foreach ($rows as $key => $row) {
            try {
                $data = json_decode($row['data'], true);
                if($data['table_name']=="ad_stats_keyword_report"){
                        $data['table_name']='ad_stats_report_keyword';
                }
                if($data['table_name']=="ad_stats_report_plan"){
                        $data['table_name']='ad_stats_plan_report';
                }
                if($data['table_name']=="ad_stats_report_group"){
                        $data['table_name']='ad_stats_group_report';
                }
                if($data['table_name']=="ad_stats_report_advert"){
                        $data['table_name']='ad_stats_advert_report';
                }
                if($data['table_name']=="ad_stats_area_report_group"){
                        $data['table_name']='ad_stats_area_group_report';
                }
                if($data['table_name']=="ad_stats_area_report_plan"){
                        $data['table_name']='ad_stats_area_plan_report';
                }
                $sql = "select {$data['select']} from {$data['table_name']} where create_date>='{$row['start_date']}' and create_date<='{$row['end_date']}' and ad_user_id='{$row['ad_user_id']}'";
                if (isset($data['where']) && $data['where'] != '')
                    $sql .= "and {$data['where']}";
                if (isset($data['group']) && $data['group'] != '')
                    $sql = $sql  . " group by {$data['group']} ";
                if (isset($data['order']) && $data['order'] != '')
                    $sql = $sql . " order by {$data['order']} ";
                $dbStr = 'db_' . $data['table_name'];
                if($data['table_name']=="ad_stats_neighbor_report"){ //比邻统计表与关键词统计表
                    $dbStr = 'db_ad_stats_area_group_report';
                }
                if($data['table_name']=="ad_stats_biyi_report"){ //比翼表与创意表一个库
                    $dbStr = 'db_ad_stats_biyi_report';
                }
                if($data['table_name']=="mediav_stats_area_group_report" || $data['table_name']=="ad_stats_mediav_report" || $data['table_name']=="ad_app_stats_report" ){ //比翼表与创意表一个库
                    $dbStr = 'db_ad_stats_area_group_report';
                }
                //地域关键词另类处理
                if($data['table_name']=="report_keyword_area"){
                    for($iTab=0;$iTab<64;$iTab++){
                          //$iTabS = ($iTab<10) ? '0'.$iTab : $iTab;
                          $newTab = $data['table_name']."_".$iTab;
                          $Nsql = str_replace($data['table_name'],$newTab, $sql);
                          $newTab='';
                          $res_tmp = Yii::app()->db_report->createCommand($Nsql)->queryAll();     
                          if(!empty($res_tmp))
                              $res = array_merge($res,$res_tmp);
                    }    
                }else{
                  $res = Yii::app()->$dbStr->createCommand($sql)->queryAll();
                }
                if (!$res || empty($res)) {
                    $res = array();
                }
                if($data['table_name']=="ad_stats_interest_report"){
                  foreach ($res as $k => $value) {
                      foreach ($value as $vk=>$vvalue){
                          if($vk=='costs'){
                              $res[$k]['total_cost'] = $value['costs'];
                              unset($res[$k][$vk]);
                          }
                      }
                  }
                }
                foreach ($res as $k => $value) {
                    foreach ($data['ext_filed'] as $field => $ruleArr) {
                        if ($ruleArr[1] == '/') {
                            $res[$k][$field] = ($value[$ruleArr[2]] == 0) ? '' : round($value[$ruleArr[0]] / $value[$ruleArr[2]], 4);
                            if (isset($ruleArr[3]) && $ruleArr[3]){
                                $res[$k][$field] = ($res[$k][$field] * 100) . "%";
                            }
                        }
                    }
                        if(isset($value['ad_group_name']) && (!$value['ad_group_name'] || $value['ad_group_name']=='NULL' ) ){
                            $djBranchDB = $this->getDjBranchDB($row['ad_user_id']);
                            $groupTitle = $djBranchDB->createCommand("select title from ad_group where id='{$value['ad_group_id']}'")->queryScalar();
                            $res[$k]['ad_group_name'] = $groupTitle;
                        }
                        
                        if(isset($value['ad_plan_name']) && (!$value['ad_plan_name'] || $value['ad_plan_name']=='NULL' )){
                            $djBranchDB = $this->getDjBranchDB($row['ad_user_id']);
                            $planTitle = $djBranchDB->createCommand("select title from ad_plan where id='{$value['ad_plan_id']}'")->queryScalar();
                            $res[$k]['ad_plan_name'] = $planTitle;
                        }
                        if(isset($value['caption']) && (!$value['caption'] || $value['caption']=='NULL' )){
                            $djBranchDB = $this->getDjBranchDB($row['ad_user_id']);
                            $advertTitle = $djBranchDB->createCommand("select caption from ad_advert where id='{$value['ad_advert_id']}'")->queryScalar();
                            $res[$k]['caption'] = $advertTitle;
                        }
                        if(isset($value['zilian']) && (!$value['zilian'] || $value['zilian']=='NULL' )){
                            $djBranchDB = $this->getDjBranchDB($row['ad_user_id']);
                            $advertTitle = $djBranchDB->createCommand("select ad_type,caption from ad_advert where id='{$value['sub_id']}'")->queryRow();
                            $res[$k]['zilian'] = $advertTitle['caption'];
                            if($advertTitle['ad_type']==11 || $advertTitle['ad_type']==16)
                            {
                               $res[$k]['zilian'] = '图片';
                            }
                        }
                        if(isset($value['description1']) && (!($value['description1']) || $value['description1']=='NULL' || $value['description1']=='\\\\N')){
                            $djBranchDB = $this->getDjBranchDB($row['ad_user_id']);
                            $advertDesc = $djBranchDB->createCommand("select description from ad_advert where id='{$value['ad_advert_id']}'")->queryScalar();
                            $advertDescArr = explode("^",$advertDesc);
                            $res[$k]['description1'] = $advertDescArr[0];
                            $res[$k]["description2"] = $advertDescArr[1];
                        }
                        if($data['table_name']=='ad_stats_area_group_report' || $data['table_name']=='ad_stats_area_plan_report' || $data['table_name']=='ad_stats_area_report_province' ){ //地域时单取省份名
                            $areaInfo = Yii::app()->db_center->createCommand("select area_name from ad_areas where area_id='{$res[$k]['province_id']}'")->queryRow();
                            $res[$k]["province_name"] = $areaInfo['area_name'];
                        }
                        $res[$k]["click_cost"] = round($res[$k]["click_cost"],2);
                }
                $content = join("\t", $data['title']) . "\n";
                $t_views = $t_clicks = $t_click_percents = $t_total_cost = $t_click_costs = 0;
                foreach ($res as $value) {
                    $arr = array();
                    foreach ($data['title'] as $k => $v) {
                        $arr[] = $value[$k];
                    }
                    $content .= join("\t", $arr) . "\n";
                    $t_views          += $value['views'];
                    $t_clicks         += $value['clicks'];
                    $t_click_percents += $value['click_percent'];
                    $t_total_cost     += $value['total_cost'];
                    $t_click_costs    += $value['click_cost'];
                }
                $content .= "\r\n\r\n";
                $content .= join("\t", array('汇总','','总展示次数','总点击次数','平均点击率','总费用','平均点击费用')) . "\n";
                if ($t_views == 0) {
                    $t_views = $t_clicks = $t_avg = 0;
                }else{
                    $t_avg = round(($t_clicks/$t_views),2) * 100;
                }
                if($t_clicks == 0){
                    $t_costAvg = 0;
                }else{
                    $t_costAvg = round(($t_total_cost/$t_clicks),2);
                }
                $content .= join("\t", array('','',$t_views,$t_clicks,$t_avg. "%",$t_total_cost,$t_costAvg)) . "\n";
                $hash = md5("{$key}-{$row['ad_user_id']}-" . microtime(true))."yasuo";
                $arr = array(
                    'download_key' => $hash,
                    'update_time' => date('Y-m-d H:i:s'),
                    'status' => AdStatsBookReportTask::STATUS_FINISH,
                );
                $cassandra->save($hash, gzcompress($content));
                Yii::app()->db_book_report->createCommand()->update(AdStatsBookReportTask::model()->tableName(), $arr, 'id=' . (int)$row['id']);
            } catch (exception $e) {
                $arr = array(
                    'failure_times' => $row['failure_times'] + 1,
                    'update_time' => date('Y-m-d H:i:s'),
                );
                if ($arr['failure_times'] >= 3)
                    $arr['status'] = AdStatsBookReportTask::STATUS_FAILURE;
                Yii::app()->db_book_report->createCommand()->update(AdStatsBookReportTask::model()->tableName(), $arr, 'id=' . (int)$row['id']);
                print_r(date('Y-m-d H:i:s') . "\t" . $e->getMessage() . "\n");
                $logData = array(
                    'data' => json_encode($row),
                    'error_info' => $e->getMessage(),
                    'error_strace' => $e->getTraceAsString(),
                );
                ComAdLog::write($logData, 'calBookReportError');
            }
        }
        echo date('Y-m-d H:i:s') . "ok--------\n";
        return ;
    }
     public function getDjBranchDB($uid)
    {
        $centerDB = DbConnectionManager::getDjCenterDB();
        $daoDbRouter    = new DbRouter();
        $daoDbRouter->setDB($centerDB);
        $dbID = $daoDbRouter->getRouter($uid);
        $djBranchDB = DbConnectionManager::getDjBranchDB($dbID);
        return $djBranchDB;
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
    protected function getUserName($userID){
      if(!isset($this->_arrUserName[$userID])){
            $dbCenter = DbConnectionManager::getDjCenterDB();
            $sql = "select user_name,email,status from ad_user where id=:user_id";
            $cmd = $dbCenter->createCommand($sql);
            $cmd->bindParam(':user_id', $userID);
            $ret = $cmd->queryRow();
            $this->_arrUserName[$userID]['username'] = isset($ret['user_name']) ? $ret['user_name'] : false; 
            $this->_arrUserName[$userID]['email'] = isset($ret['email']) ? $ret['email'] : false; 
             $this->_arrUserName[$userID]['status'] = isset($ret['status']) ? $ret['status'] : false;
        }
        return  $this->_arrUserName[$userID];
    }
    /**
     * @用户定制报表生成后发送邮件到客户
     * 每分钟都去检测
     */
    public function actionSendReport($date=false)
    {
        $title = '360点睛营销效果评估报告';
        $timeUnit = "不分日";
        $cassandra = new ComCassandra();
        $bookObj =  Yii::app()->db_book_report;
        $taskInfoSql = "select * from ad_stats_book_report_task where create_date='{$date}' and issend=0 and download_key!=''  limit 100"; 
        //$taskInfoSql = "select * from ad_stats_book_report_task where id=63";
        $taskInfo = Yii::app()->db_book_report->createCommand($taskInfoSql)->queryAll();
        $TaskIdStr=''; //记录已经更新成1的任务
        if(empty($taskInfo)){
            echo "无要发送报告";
            exit;
        }
        try {
        foreach($taskInfo as $val){
            $updateTaskSql = "update ad_stats_book_report_task set issend=1 where id={$val['id']} limit 1";
            echo $updateTaskSql;
            $TaskIdStr .=$val['id']."-";
            Yii::app()->db_book_report->createCommand($updateTaskSql)->execute();
            $dataInfo = json_decode($val['data'], true);
            if(strpos($dataInfo['select'], 'create_date')!==false){
                $timeUnit='分日';
            }
            $bookInfoSql =  "select * from ad_stats_book_report where id={$val['parent_id']}";
            $bookInfo = Yii::app()->db_book_report->createCommand($bookInfoSql)->queryRow();
            //$bookInfo['email'] = "iwannakiss@sina.com";
            //$bookInfo['issend'] = 1;
            if(!$bookInfo['email'] && !$bookInfo['issend']){
                echo "无邮箱";
               continue; //没写邮件的也没指定发送到注册账号不发
            }
            // 获取用户路由
            $userInfo = $this->getUserName($bookInfo['ad_user_id']);
            $userName = $userInfo['username'];
            $userEmail = $userInfo['email'];
            $userStatus = $userInfo['status'];
            if($userStatus==-4){ ///账户注销不会发邮件
                continue;
            }
            if(trim($bookInfo['email']) || $bookInfo['issend']){ //邮箱用分号分开，如果选定发送到注册账号则加入数组
               $mailArr = explode(';', $bookInfo['email']);
               if($bookInfo['issend']){
                  array_push($mailArr, $userEmail);
                  $mailArr  =  array_unique ($mailArr);
               }
            }else{
                continue;
            }
            $titleTime = $this->setTimeName($bookInfo['select_date'],$val['create_date'],'-');
            $title = $titleTime. $dataInfo['ceng'] . $dataInfo['baogao'];
            $body = "尊敬的点睛用户，您好：<br>您的账户".$userName." 定制的".$title." 已生成，请下载附件查看报告详情。<br>报告相关信息如下：<br>报告时间范围：{$titleTime}<br>报告层级：{$dataInfo['ceng']}<br>报告类型：{$dataInfo['baogao']}<br>时间单位：".$timeUnit."<br><br>非常感谢您对点睛的支持，如有任何问题请查看<a href='http://e.360.cn/static/help/list.html?3266479857265949'>点睛帮助</a> 。如需查看更多点睛推广信息，请登录<a href='http://e.360.cn'>点睛平台</a> ";
            try{
                    $content = $cassandra->get($val['download_key']);
                    if((substr($val['download_key'],-5))=='yasuo'){
                       $content = gzuncompress($content);
                    }
                }catch(Exception $e){
                    //e $e->getMessage();
                } 
                if(empty($mailArr)){
                        continue;;
                }
                if ($bookInfo['file_type'] == 1) {
                    $arr = array_filter(explode("\n", $content));
                    //$this->convertArr($arr);
                    foreach ($mailArr as $mval){
                        if(trim($mval)){
                           $respons = $this->writeReportToCsv($arr,$bookInfo['ad_user_id'], $title,$body,$mval);
                           $respons = json_decode($respons,true);
                           if(!isset($respons['result'])){
                               Yii::app()->curl->run("http://10.108.68.121:888/notice/notice.php?s=定制发邮件失败&c=sendfailid".$val['id']."email:".$mval.$respons['error']['message']."&g=e_formcheck_new");
                           }
                        } 
                        }
                } else {
                  foreach ($mailArr as $mval){  
                    if(trim($mval)){
                        $respons = $this->writeReportToTxt($content,$bookInfo['ad_user_id'], $title,$body,$mval);
                        $respons = json_decode($respons,true);
                        if(!isset($respons['result'])){
                               Yii::app()->curl->run("http://10.108.68.121:888/notice/notice.php?s=定制发邮件失败&c=sendfailid".$val['id']."email:".$mval.$respons['error']['message']."&g=e_formcheck_new");
                        }
                    }
                  }
                }                
            //echo $body."\r\n";
            
        }
      } catch (Exception $e){
           echo "taskid:".$TaskIdStr.'未发送成功，但状态已更新!'; //这块增加报警
           echo $e->getMessage();
           exit;
      }
        //$emailObj->send_attachment_mail($title, $body, 'iwannakiss@sina.com',array(), '/data/stor/stats/ad_stats_biyi_click_20140416');
    }
    private function writeReportToCsv($rows,$user_id, $filename,$body,$email='') {
        //header('Content-Type: text/csv; charset=gbk');
        $emailObj = new ComEmail();
        $fname = '/tmp/' . $filename . '.csv';
        $baseDir = "/data/log/statsLog/sendreport/";
        if(!is_dir($baseDir.$user_id."/")){
            mkdir($baseDir.$user_id."/");
        }
        $fname =  $baseDir.$user_id."/". $filename . '.csv';
        $fp = fopen($fname, 'w');
        $title = $rows[0];
        $title = mb_convert_encoding($title, 'GBK','UTF-8');
        $title = explode("\t", $title);
        fputcsv($fp, $title);
        unset($rows[0]);
        foreach ($rows as $dk => $row) {
            $data = array();
            $row = mb_convert_encoding($row, 'GBK','UTF-8');
            $row = explode("\t",$row);
            foreach ($title as $key => $value) {
                if (isset($row[$key])) {
                    if($row[$key]=='全国' || $row[$key]=='全部'){
                        $row[$key]='其他';
                    }
                    if($row[$key]=='NULL'){
                        $row[$key]='';
                    }                    
                    $data[] = $row[$key];
                } else {
                    $data[] = '';
                }
            }
            fputcsv($fp, $data);
        }
        fclose($fp);
        try {
            if (is_file($fname)) {
               $respons= $emailObj->send_attachment_mail($filename, $body, $email,array(), $fname);
               sleep(10);//发送间隔不能小于5s
                if (!unlink($fname)) {
                    throw new Exception("delete" . $fname . " file faild!");
                }
                return $respons;
            }
        } catch (Exception $e) {
            Yii::log('失败: ' . $e->getMessage(), CLogger::LEVEL_ERROR, '效果评估报表下载');
        }        
    }    
    private function writeReportToTxt($contents, $user_id, $filename,$body,$email='') {
        $emailObj = new ComEmail();
        $contents = str_replace("\n", "\r\n", $contents);
        $contents = preg_replace("/\t(全部|全国)\t/", "\t其他\t", $contents);
        $contents = mb_convert_encoding($contents, 'GBK','UTF-8');
        $fname = '/tmp/' . time() . '.txt';
        $baseDir = "/data/log/statsLog/sendreport/";
        if(!is_dir($baseDir.$user_id."/")){
            mkdir($baseDir.$user_id."/");
        }
        $fname =  $baseDir.$user_id."/". $filename . '.txt';
        $fp = fopen($fname, 'w');
        fputs($fp, $contents);
        fclose($fp);
        try {
            if (is_file($fname)) {
                $respons= $emailObj->send_attachment_mail($filename, $body, $email,array(), $fname);
                sleep(10);//发送间隔不能小于5s
                if (!unlink($fname)) {
                    throw new Exception("delete" . $fname . " file faild!");
                }
                return $respons;
            }
        } catch (Exception $e) {
            Yii::log('失败: ' . $e->getMessage(), CLogger::LEVEL_ERROR, '效果评估报表下载');
        }
    }
    /**
     *@设置下载文件时文件时间命名
     *@param string $date 时间
     *@return string 返回字符串 
     */ 
    private function setTimeName($date,$datetime,$split='-')
    {
        $datetime = strtotime($datetime);
        $dateFormat = "Y{$split}m{$split}d";
        //时间选择对应
        if ($date == 1) {//昨天
            $str = date($dateFormat, strtotime("-1 day",$datetime)); 
        } elseif ($date == 2) {//前天
            $str = date($dateFormat, strtotime("-2 days",$datetime));
        } elseif ($date == 7) {//最近七天
            $str = date($dateFormat, strtotime("-7 days",$datetime)).'-'.date($dateFormat, strtotime('-1 day',$datetime));
        } elseif ($date== 8) {
            $data['start_date'] = date("Y{$split}m", strtotime("-1 month",$datetime)) . "{$split}01";
            $data['end_date'] = date($dateFormat, strtotime("+1 month -1 day", strtotime($data['start_date'])));
            $str = $data['start_date'].'-'.$data['end_date'];
        }        
         elseif ($date == 9) {//上周
            $str= date($dateFormat, mktime(0, 0, 0, date('m',$datetime), date('d',$datetime) - date('w',$datetime) - 6, date('Y',$datetime))).'-'.date("Y-m-d", mktime(0, 0, 0, date('m',$datetime), date('d',$datetime) - date('w',$datetime), date('Y',$datetime)));
        } elseif ($date == 10) {//本月
            $str = date("Y{$split}m{$split}01").'-'.date($dateFormat,$datetime);
        } elseif(strpos($date,',')!==false){
            $date = str_replace('-',$split,$date);
            $str = str_replace(',','-', $date);
        }
        return $str;
    }  
}
?>
