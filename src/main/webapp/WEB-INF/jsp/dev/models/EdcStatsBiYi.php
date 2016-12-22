<?php
/**
 * Author: Kangle.Liu - liukangle@360.cn
 *
 * Last modified: 2013-09-11 16:22
 *
 * Filename: EdcStatsBiYi.php
 *
 * Description: 
 *
 */
class EdcStatsBiYi 
{
    static private $db;
    protected $dbString = 'db_stats';
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
		return 'ad_stats_biyi';
	}

    public function getByGroupIDSubID ($groupID, $subID, $create_date) {
        $tableName = $this->tableName() . "_click_" . date('Ymd', strtotime($create_date));
        $sql = sprintf("select * from %s where ad_group_id=:ad_group_id and sub_id=:sub_id", $tableName);
        $cmd = $this->getDbConnection()->createCommand($sql);
        $cmd->bindParam(':ad_group_id', $groupID);
        $cmd->bindParam(':sub_id', $subID);
        return $cmd->queryRow();
    }

    public function cheatClickRefund ($id, $price, $createDate) {
        $tableName = $this->tableName(). '_click_'.date('Ymd', strtotime($createDate));
        $sql = sprintf("update %s set clicks=clicks-1, costs=costs-:price where id=:id", $tableName);
        $cmd = $this->getDbConnection()->createCommand($sql);
        $cmd->bindParam(':price', $price);
        $cmd->bindParam(':id', $id);
        return $cmd->execute();
    }

    public function insertRow($tableName, $data) {
        if ($this->getDbConnection()->createCommand()->insert($tableName, $data)) {
            return $this->getDbConnection()->createCommand('SELECT LAST_INSERT_ID()')->queryScalar();
        }
        return false;
    }

    /**
     * 根据sub_id更新数据
     * @param array $data
     * @return boolean
     * @author jingguangwen@360.cn  20140402
     */
    public function updateByData($data)
    {
        if (!isset($data['create_date']) || !isset($data['ad_group_id']) || !isset($data['sub_id'])) {
            return false;
        }
        $tableName = $this->tableName() . "_click_" . date('Ymd', strtotime($data['create_date']));
    	
        $sql = "update " . $tableName . " set clicks=" . (int)$data['clicks']
        . ",costs=" . $data['costs']
        . ", update_time='" . date('Y-m-d H:i:s') . "'";
        $sql .= " where ad_group_id=:groupId and sub_id=:sub_id  limit 1";
        $cmd = $this->getDbConnection()->createCommand($sql);
        $cmd->bindParam(':sub_id', $data['sub_id'], PDO::PARAM_INT);
        $cmd->bindParam(':groupId', $data['ad_group_id'], PDO::PARAM_INT);
        return $cmd->execute();
         
    }
    /**
     * 根据sub_id更新作弊数据
     * @param array $data
     * @return boolean
     * @author jingguangwen@360.cn  20140402
     */
    public function cheatUpdateByData($data)
    {
        if (!isset($data['create_date']) || !isset($data['ad_group_id']) || !isset($data['sub_id']) ) {
            return false;
        }
    	$tableName = $this->tableName() . "_click_" . date('Ymd', strtotime($data['create_date']));
    	 
    	$sql = "update " . $tableName . " set clicks=clicks-" . (int)$data['clicks']
    	. ",costs=costs-" . $data['costs']
    	. ", update_time='" . date('Y-m-d H:i:s') . "'";
    	$sql .= " where ad_group_id=:groupId and sub_id=:sub_id  limit 1";
    	$cmd = $this->getDbConnection()->createCommand($sql);
    	$cmd->bindParam(':sub_id', $data['sub_id'], PDO::PARAM_INT);
    	$cmd->bindParam(':groupId', $data['ad_group_id'], PDO::PARAM_INT);
    	return $cmd->execute();
    	 
    }
    /**
     * 存入数据
     * @param array $data
     * @return boolean
     * @author jingguangwen@360.cn  20140402
     */
    public function insertData($data)
    {
       
        $tableName = $this->tableName() . "_click_" . date('Ymd', strtotime($data['create_date']));
         
        if ($this->getDbConnection()->createCommand()->insert($tableName, $data)) {
            return $this->getDbConnection()->createCommand('SELECT LAST_INSERT_ID()')->queryScalar();
        }
        return false;
         
    }


    /**
     * 更新展示数据
     * @author jingguangwen@360.cn
     * 2014-04-02
     *
     */
    public function updateViews($data)
    {
        if (!isset($data['create_date']) || !isset($data['ad_group_id']) || !isset($data['sub_id'])) {
            return false;
        }
        $tableName = $this->tableName() . "_click_" . date('Ymd', strtotime($data['create_date']));
        $sql = "select id from {$tableName} where ad_group_id=:groupId and sub_id =:sub_id";
        $cmd = $this->getDbConnection()->createCommand($sql);
        $cmd->bindParam(':sub_id', $data['sub_id'], PDO::PARAM_INT);
        $cmd->bindParam(':groupId', $data['ad_group_id'], PDO::PARAM_INT);
        $id = $cmd->queryScalar();
        if (!$id) {
            return $this->getDbConnection()->createCommand()->insert($tableName, $data);
        } else {
            $sql = "update {$tableName} set views=views+" . (int)$data['views'] . ", update_time='" 
                . $data['update_time'] . "' where id=" . (int)$id . " limit 1";
            return $this->getDbConnection()->createCommand($sql)->execute();
        }
    }
}
