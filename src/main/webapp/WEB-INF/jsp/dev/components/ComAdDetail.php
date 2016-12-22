<?php
/**
 * @author guichenglin@360.cn
 */
class ComAdDetail {

    /**
     * 最后一次错误，若方法返回false,使用ComAudit::$lastErr可以获取错误原因
     * @var string
     */
    const STATUS_NORMAL     = 0; // 正常点击
    const STATUS_CHEAT      = 1; // 作弊待处理
    const STATUS_DONE       = 2; // 作弊返款成功
    const STATUS_TIMEOUT    = 3; // 已经结算，不再返款
    const STATUS_FAIL       = 4; // 作弊处理失败，超出重试次数
    const STATUS_AMONG      = 5; //作弊处理中间状态，还需后续处理各个维度的统计
    const STATUS_EXCEPTION       = -1; // 结算异常，去除的点击
    const VER_SOU = 'sou';
    const VER_GUESS = 'guess';
    const VER_GOODS = 'goods';

    public static $lastErr="";
    private static $pixTableName = 'click_detail_';

    /**
     * 统一的日志ID，用于连贯性检查
     * @var string
     */
    public static $logid="";

    /**
     * @param time 时间戳
     */
    public static function getTableName($time)
    {
        if (!is_numeric($time)) {
            throw new Exception('com_ad_detail.getTableName time is fail! mast unixtime!');
        }
        // 时间小于一个月或者大于今天的都取今天的时间
        if ($time <= strtotime('-10 year') || $time >= time()) {
            $time = time();
        }

        $selectYmd = date('Ymd', $time);
        $tableName = self::$pixTableName . $selectYmd;

        // echo date('Ymd His') . "\t".getmypid()."\t$tableName\n";

        return $tableName;
    }

    public static function getDetailByClickId($click_id,$click_time)
    {
    	$tableName = self::getTableName($click_time);
    	$sql="select * from {$tableName} where click_id='{$click_id}'";
    	return Yii::app()->db_click_log->createCommand($sql)->queryRow();
    }
    public static function insertDetail($data, $click_time){
        $db=Yii::app()->db_click_log;
        foreach ($data as $k=>&$v)
        {
            if (is_string($v))
            {
                $v = "'" . mysql_escape_string($v) . "'";
            }
            else if(is_array($v))
            {
            	$v="'" . mysql_escape_string(@json_encode($v)) . "'";
            }
            else if(is_null($v)) {
            	$v='null';
            }
            else if(empty($v) && !($v===0))
            {
                $v="''";
            }
        }

        $tableName = self::getTableName($click_time);
        $sql="insert ignore into $tableName (".implode(",",array_keys($data)).") values(".implode(',',array_values($data)).")";
        $ret=$db->createCommand($sql)->execute();
        if($ret!=1)
            Utility::log(__CLASS__,__FUNCTION__,array($sql, $ret));
        return $ret;
    }

    /**
     * 批量添加数据
     */
    public static function insertDetails($lists)
    {
        if (empty($lists)) {
            return false;
        }

        try {
            $datas = array();
            foreach($lists as $key => &$list) {
                $tableName = self::getTableName($list['click_time']);
                $datas[$tableName][] = $list;
            }
            unset($list);
            foreach($datas as $tableName => $datalist) {
                $sql = "insert ignore into $tableName (".implode(",",array_keys($datalist[0])).") values ";
                $values = array();
                foreach($datalist as $data) {
                    foreach($data as $k => &$v) {
                        if(is_string($v)) {
                            $v = "'" . mysql_escape_string($v) . "'";
                        } else if (is_array($v)) {
                            $v="'" . mysql_escape_string(@json_encode($v)) . "'";
                        } else if (is_null($v)) {
                            $v='null';
                        } else if (empty($v) && !($v===0)) {
                            $v = "''";
                        }
                    }
                    $values[] = "(".implode(',',array_values($data)).")";
                }
                $sql .= implode(',', $values);
                $ret=Yii::app()->db_click_log->createCommand($sql)->execute();
                // echo "$sql ret:$ret\n";
            }
        } catch (Exception $e) {
            Utility::sendAlert(__CLASS__,__FUNCTION__,$e->getMessage());
            return false;
        }

        return true;
    }

    //'操作类型，1添加，2反作弊，3结算,4余额不够，减金额 等'
    //from:来源 1 esc系统
    public static function insertOperateLog($click_id, $operate_type, $extension='', $operate_from=1)
    {
        return;//这个表放弃使用
        $sql="insert into operate_log (click_id,create_time,operate_type,operate_from,extension) values('{$click_id}',".time().",'{$operate_type}','{$operate_from}','".mysql_escape_string($extension)."')";
        $ret=Yii::app()->db_click_log->createCommand($sql)->execute();
    }

    public static function updateDetail($update_field,$click_id, $click_time)
    {
        $up=array();
        foreach($update_field as $k=>$v)
        {
            if(is_null($v))
            {
                $up[]="`".$k."`=null";
            }
            else if(is_float($v) || is_int($v)|| is_numeric($v))
            {
                $up[]="`".$k."`=$v";
            }
            else
            {
                $up[]="`".$k."`='".mysql_escape_string($v)."'";
            }
        }

        $tableName = self::getTableName($click_time);
        $sql="update $tableName set ".implode(",", $up)." where click_id='".$click_id."'";
        $ret=Yii::app()->db_click_log->createCommand($sql)->execute();
        // echo "$sql ret:$ret\n";
        return $ret;
    }

    /**
     * 根据id更新数据
     * @param int $id
     * @param array $data
     * @author jinggaunwgen@360.cn  20140224
     */
    public function updateById($id, $data, $click_time)
    {
        $tableName = self::getTableName($click_time);
        $command = Yii::app()->db_click_log->createCommand();
        return $command->update($tableName, $data, 'id=:id', array(':id' => $id));
    }

    /**
     * 更新函数
     * @param array $data   数据
     * @param array $where  条件
     * @param string $cond   绑定关系
     * @param array $time   时间
     * 调用例子：ComAdDetail::updateDetails(array('status'=>1, 'time'=>2), array(':id' => $id), 'id=:id and id=:id', 1428863609);
     */
    public function updateDetails($data, $where, $cond, $time)
    {
        $tableName = self::getTableName($time);
        $db = Yii::app()->db_click_log;
        return $db->createCommand()->update($tableName, $data, $cond, $where);
    }

    /**
     * 根据日期还有状态获取用户相关的消费数据
     * @param date $date 2014-09-04
     * @param tinyint $deal_status 是否已经结算了 1已经结算了，0未结算
     * @return array
     * @author jingguangwen@360.cn  2015-06-17
     */
    public static function getUserCostByDay($date, $deal_status,$uid="")
    {
        $tableName = self::getTableName(strtotime($date));

        $sql = sprintf("select ad_user_id as `uid`, sum(price-reduce_price) as `cost`, max(id) as max_click_id from %s where create_date='%s' and status not in (-1,2) and cheat_type  not  in (2,3) and deal_status=%d and price != reduce_price ".($uid?" and ad_user_id ={$uid}":"")." group by ad_user_id", $tableName, $date, $deal_status);

        return Yii::app()->db_click_log->createCommand($sql)->queryAll();
    }
    /**
     * 通过ClickID修改状态
     * @param int clickId       ID
     * @param int status        状态
     * @param int where_status  状态
     * @param int date          时间戳，根据这个分表的
     * @return bool 2015-06-17
     */
    public function updateStatusByClickId($clickId, $status, $where_status=null, $click_time)
    {
        $tableName = self::getTableName($click_time);

        $db = Yii::app()->db_click_log;
        $update_time = time();

        if (is_null($where_status))
        {
            return $db->createCommand()->update($tableName, array('status'=>$status,'update_time' => $update_time), 'click_id=:clickId', array(":clickId" => $clickId));
        }
        else
        {
            $where_status = intval($where_status);
            return $db->createCommand()->update($tableName, array('status'=>$status,'update_time' => $update_time), 'click_id=:clickId and status='.$where_status, array(":clickId" => $clickId));
        }

        return false;
    }
    /**
     * 查询接口
     * $query = array(
     *      'select' => * | id,status   // 查询的列
     *      'limit' => 0                // LIMIT
     *      'order' => 0                // ORDER BY
     *      'offset' => 100             // OFFSET
     *      'table_alias' => 'cd'       // click_detail别名
     *      'join' => array('table' => 'table a', 'on'=>'a.id=p.id')    table:join的表名，on:条件
     *      'where' => array('cond' => 'status=:status', 'value' => array(':status' => 0))
     *      'time' => 1428863609 //表示查询某天的
     *      'query' => row | all | count
     * );
     */
    public function queryDetails($query)
    {
        $db = Yii::app()->db_click_log->createCommand();

        $time = isset($query['time']) ? $query['time'] : time();
        $db->from(self::getTableName($time));

        $tableName  = isset($query['select']) ? $query['select'] : '*';
        $tableAlias = isset($query['table_alias']) ? $query['table_alias'] : '';
        $db->select("$tableName $tableAlias");

        $limit = isset($query['limit']) ? $query['limit'] : 1000;
        $db->limit($limit);

        if (isset($query['offset'])) $db->limit($query['offset']);

        if (isset($query['where']['cond']) && isset($query['query']['value'])) {
            // $db->where('status=:status', array(':status'=>0));
            $db->where($query['where']['cond'], $query['query']['value']);
        }

        if (isset($query['join']['table']) && isset($query['join']['on'])) {
            $db->join($query['join']['table'], $query['join']['on']);
        }

        if (isset($query['query'])) {
            $query = $query['query'];
            switch ($query) {
                case 'all':
                    return $db->queryAll();
                    break;
                case 'row':
                    return $db->queryRow();
                    break;
                case 'count':
                    return $db->queryScalar();
                    break;
            }

        }

        throw new Exception('query type is fail!');
    }

    /**
     * 根据SQL查询，通过时间转换SQL的表名
     * @param string $sql
     * @param int $time
     * @param string $queryType row | all | scalar
     */
    public static function queryBySql($sql, $time, $queryType='all')
    {
        $tableName = self::getTableName($time);
        $sql = str_ireplace('click_detail', $tableName, $sql);

        $db = Yii::app()->db_click_log->createCommand($sql);
        switch ($queryType) {
            case 'all':
                return $db->queryAll();
                break;
            case 'row':
                return $db->queryRow();
                break;
            case 'scalar':
                return $db->queryScalar();
                break;
            case 'exec':
                return $db->execute();
                break;
        }

        throw new Exception('query type is fail!');
    }
}