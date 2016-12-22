<?php
/**
 * ClickLog定制查询 
 * 
 * @author dongdapeng@360.cn
 * @link http://www.360.cn/
 * @date 2015-07-02
 * @filecoding UTF-8 
 * @copyright (c) 2015 360.cn, Inc. All Rights Reserved
 */
class AdepQueryClickLog
{

    static private $db;
    static protected $dbString = 'db_stats';
    static private $_instance = null;

	public static function model()
	{
        if (is_null(self::$_instance)) {
            self::$_instance = new self();
        }
        return self::$_instance;
	}

    public function setDB($db) {
        self::$db = $db;
    }

    public function getDbConnection()
    {
        if (self::$db !== null)
            return self::$db;
        else {
            $dbString = self::$dbString;
            self::$db = Yii::app()->$dbString;
            return self::$db;
        }
    }

    public function getTableCount($table, $arrConds)
    {
        $sql = "SELECT count(1) as num FROM " . $table . " WHERE 1";
        $conds = $this->getConds($arrConds);
        if ($conds) {
            $sql .=' AND '.implode(' and ',$conds);
        }
        $arrRet = self::$db->createCommand($sql)->queryRow();
        if ( $arrRet === false ) {
            return false;
        }
        $intCount = isset($arrRet['num']) ? $arrRet['num'] : 0;
        return $intCount;
    }

    public function getConds($arrConds)
    {
        $searchs = array(
            'id' => array('id', '>'),
            'status'  =>array('status', 'NOT IN'),
            'ad_user_id'  =>array('ad_user_id', '='),
            'ad_plan_id'  =>array('ad_plan_id', '='),
            'ad_advert_id'  =>array('ad_advert_id', '='),
            'ad_group_id'  =>array('ad_group_id', '='),
            'keyword'  =>array('keyword', 'like'),
        );
        $conds = array();
        $condxs= '';
        foreach ($searchs as $key=>$val) {
            if ( !isset($arrConds[$key]) ) {
                continue;
            }
            if(is_array($arrConds[$key])){
                foreach($arrConds[$key] as $k=>$v){
                    $condxs .= self::$db->quoteValue($v).',';
                }
                $condxs = substr($condxs, 0, -1); 
                $conds[]=$val[0].' '.$val[1].' ('.$condxs.')';
            }else{
                if($val[1]=='like'){
                    $rawconds[$key]='%'.$rawconds[$key].'%';
                }
                $conds[]=$val[0].' '.$val[1].' '.self::$db->quoteValue($arrConds[$key]);
            }
        }
        return $conds;
    }

    public function getList($arrTables,$arrConds=array(),$intOffset=0, $intLimit=1000)
    {
        $start = $intOffset;

        $intBeforeNum = 0;
        $intGetTotalNum = 0;
        $arrList = array();
        $tableNum = count($arrTables);
        if($tableNum > 1){
            foreach ($arrTables as $table) {
                $intCurrentCount = $this->getTableCount($table, $arrConds);
                if ( $intBeforeNum + $intCurrentCount <= $intOffset ) {
                    $intBeforeNum += $intCurrentCount;
                    continue;
                }
                $intCurrentStart = $intOffset + $intGetTotalNum - $intBeforeNum;
                // 当前表的获取个数
                $intCurrentNum = min(($intCurrentCount-$intCurrentStart), ($intLimit-$intGetTotalNum));//获取个数

                $arrTmpList = $this->getTableList($table, $arrConds, $intCurrentStart, $intCurrentNum);
                $arrList = array_merge($arrList, $arrTmpList);

                $intBeforeNum += $intCurrentCount;
                $intGetTotalNum += $intCurrentNum;
                if ($intGetTotalNum >= $intLimit) {
                    break;
                }
            }
        }else{
            $arrList = $this->getTableList($arrTables[0], $arrConds, $intOffset, $intLimit);
        }
        return $arrList;
    }

    public function getTableList($table, $arrConds=array(), $intOffset=0, $intLimit=1000)
    {
        $sql = "SELECT
            ad_user_id,
            click_id,
            click_time,
            view_id,
            view_time,
            ip,
            mid,
            ad_advert_id,
            ad_group_id,
            ad_plan_id,
            query,
            keyword,
            ls,
            src,
            area_fid,
            area_id,
            price,
            bidprice,
            create_date,
            cid,
            pid,
            app_cid,
            ver,
            deal_status,
            reduce_price,
            pos,
            location,
            tag_id,
            cheat_type, 
            source_type,
            source_system,
            extension,
            update_time
            FROM " . $table . " WHERE 1";


        $conds = $this->getConds($arrConds);
        if ($conds) {
            $sql .=' AND '.implode(' and ',$conds);
        }
        //$sql .= " ORDER BY ID DESC";
        $sql .= " LIMIT {$intOffset} , {$intLimit}";
        $rows = self::$db->createCommand($sql)->queryAll();

        return $rows;
    }
}
