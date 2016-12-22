<?php

/**
 * This is the model class for table "stats_file_log".
 *
 * The followings are the available columns in table 'stats_file_log':
 * @property integer $id
 * @property integer $hour_str
 * @property integer $m_inter
 * @property string $time_str
 * @property string $ip
 * @property integer $sid
 * @property string $hash_value
 * @property integer $status
 * @property string $create_time
 * @property string $update_time
 */
class StatsFileLog
{
    static private $db;
    protected $dbString = 'db_stats';
	const STATUS_STATISTICED = 1;
	const STATUS_RSYNCED = 3;
	const STATUS_COLLECTED = 5;
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
		return 'stats_file_log';
	}

    public function getQueue($status, $mInter='', $limit=0)
    {
        $sql = 'select * from ' . $this->tableName() . ' where status=:status ';
        if($mInter!=='')
            $sql .= ' and m_inter=' . (int)$mInter;
        if ($limit > 0) {
            $sql .= ' order by id asc limit ' . $limit;
        } else {
            $sql .= ' order by id asc limit 50';
        }
        $cmd = $this->getDbConnection()->createCommand($sql);
        $cmd->bindParam(':status', $status);
        $rows = $cmd->queryAll();
        if(!$rows)
            return false;
        else{
            $queueArr = array();
            foreach($rows as $value)
                $queueArr[$value['id']] = $value;
            return $queueArr;
        }   
    }

    public function updateStatusById($id, $status, $trans=false)
    {
        try{
            $dbStatus = self::$db->getAttribute(PDO::ATTR_SERVER_INFO);
            if($dbStatus == 'MySQL server has gone away'){
                self::$db->setActive(false);
                self::$db->setActive(true);
            }   
            $flag = self::$db->createCommand()->update($this->tableName(), array('status'=>$status, 'update_time'=>date('Y-m-d H:i:s')), 'id=:id', array(':id'=>$id));
            return $flag;
        }catch(Exception $e){
            print_r($e->getMessage());
            return false;
        }
    }

    public function updateById($arr, $id)
    {
        return $this->getDbConnection()->createCommand()->update($this->tableName(), $arr, 'id=:id', array(':id' => $id));
    }

    public function updateStatusByIdArr($idArr, $status)
    {
        $this->getDbConnection()->setActive(false);
        $this->getDbConnection()->setActive(true);
        $sql = "update " . $this->tableName() . " set status = " . (int)$status . " where id in ( " . join(',', $idArr) . " )";
        return $this->getDbConnection()->createCommand($sql)->execute();
    }
}
