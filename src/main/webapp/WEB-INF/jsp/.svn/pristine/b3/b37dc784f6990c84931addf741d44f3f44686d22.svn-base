<?php
/**
 * HiveToMysql 
 * 
 * @author dongdapeng@360.cn
 * @link http://www.360.cn/
 * @date 2016-02-25
 * @filecoding UTF-8 
 * @copyright (c) 2016 360.cn, Inc. All Rights Reserved
 */
class HiveToMysql
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

    public function quote($val){
        $dbString = self::$dbString;
        return Yii::app()->$dbString->quoteValue($val);
    }

    public function quotes($val){
        $val = array_map(array($this,'quote'),$val);
        return implode(',',$val);
    }

    public function batchInsert(&$sql) {
        return $this->getDbConnection()->createCommand($sql)->execute();
    }
}
