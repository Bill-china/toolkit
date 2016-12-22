<?php

/**
 * This is the model class for table "stats_collect_task".
 *
 * The followings are the available columns in table 'stats_collect_task':
 * @property integer $id
 * @property string $file_name
 * @property integer $type
 * @property string $create_time
 * @property string $update_time
 * @property integer $status
 */
class StatsCollectTask
{
    static private $db;
    protected $dbString = 'db_stats';
    const TYPE_VIEW = 2;
    const TYPE_CLICK = 1;
    const STATUS_OPEN = 1;
    const STATUS_FINISH = 2;
    const STATUS_FAILURE = 3;
    static private $_instance = null;

	public static function model()
	{
        if (is_null(self::$_instance)) {
            self::$_instance = new self();
        }
        return self::$_instance;
	}

    public function getDbConnection()
    {
        if (self::$db !== null)
            return self::$db;
        else {
            $dbString=$this->dbString;
            self::$db=Yii::app()->$dbString;
            return self::$db;
        }
    }

	/**
	 * @return string the associated database table name
	 */
	public function tableName()
	{
		return 'stats_collect_task';
	}


    public function insertData($data)
    {
        return $this->getDbConnection()->createCommand()->insert($this->tableName(), $data);
    }

    public function getTaskByStatusAndType($status, $type=false)
    {
        if (false !== $type)
            $where = " and type=" . (int)$type;
        else 
            $where = "";
        $sql = "select * from " . $this->tableName() . " where status=" . (int)$status . " $where limit 1";
        return $this->getDbConnection()->createCommand($sql)->queryRow();
    }

    public function updateStatusByIdArr($idArr, $status)
    {
        $sql = "update " . $this->tableName() . " set status = " . (int)$status . " where id in ( " . join(',', $idArr) . " )";
        return $this->getDbConnection()->createCommand($sql)->execute();
    }
}
