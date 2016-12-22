<?php
/**
 * Author: Kangle.Liu - liukangle@360.cn
 *
 * Last modified: 2013-09-11 16:22
 *
 * Filename: EdcStatsKeyword.php
 *
 * Description: 
 *
 */
class EdcStatsNeighbor 
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
		return 'ad_stats_neighbor';
	}

    public function insertRow($tableName, $data) {
        if ($this->getDbConnection()->createCommand()->insert($tableName, $data)) {
            return $this->getDbConnection()->createCommand('SELECT LAST_INSERT_ID()')->queryScalar();
        }
        return false;
    }

    public function getByGroupKeyword($tableName, $groupId, $keyword)
    {
        $sql = "select id from " . $tableName . " where ad_group_id=:groupId and keyword=:keyword";
        $cmd = $this->getDbConnection()->createCommand($sql);
        $cmd->bindParam(':groupId', $groupId);
        $cmd->bindParam(':keyword', $keyword, PDO::PARAM_STR);
        return $cmd->queryScalar();
    }

    public function updateIncr($data)
    {
        $tableName = $this->tableName() . "_click_" . date('Ymd', strtotime($data['create_date']));
        if ($id = $this->getByGroupKeyword($tableName, $data['ad_group_id'], $data['keyword'])) {
            $sql = "update " . $tableName . " set clicks=clicks+" . (int)$data['clicks']
                . " ,views=views+" . (int)$data['views'] . ",costs=costs+" . $data['costs']
                . ", update_time='" . date('Y-m-d H:i:s') . "'";
            $sql .= " where id=" . $id . " limit 1";

            return $this->getDbConnection()->createCommand($sql)->execute();
        } else {
            return $this->insertRow($tableName, $data);
        }
    }

    public function getByGroupIDAndKeyword ($groupID, $keyword, $createDate) {
        $tableName = $this->tableName() . '_click_' . date('Ymd', strtotime($createDate));
        $sql = sprintf("select * from %s where ad_group_id=:ad_group_id and keyword=:keyword", $tableName);
        $cmd = $this->getDbConnection()->createCommand($sql);
        $cmd->bindParam(':ad_group_id', $groupID, PDO::PARAM_INT);
        $cmd->bindParam(':keyword', $keyword, PDO::PARAM_STR);
        return $cmd->queryRow();
    }


    public function getByDateAndKeyword($ad_group_id,$keyword,$create_date) {
        if (empty($ad_group_id) || empty($create_date) || empty($keyword)) {
            return false;       
        }
        $tableName = $this->tableName() . "_click_" . date('Ymd', strtotime($create_date));
        $sql = "select * from $tableName where ad_group_id=" . (int)$ad_group_id
             . " and keyword=:keyword";
        $cmd = $this->getDbConnection()->createCommand($sql);
        $cmd->bindParam(':keyword', $keyword, PDO::PARAM_STR);
        return $cmd->queryRow();
    }

    public function cheatClickRefund($id, $price, $create_date)
    {
        $tableName = $this->tableName() . "_click_" . date('Ymd', strtotime($create_date));
        $sql = "update " . $tableName . " set clicks=clicks-1, costs=costs-:price "
            . " where id=:id";
        $cmd = $this->getDbConnection()->createCommand($sql);
        $cmd->bindParam(':id', $id);
        $cmd->bindParam(':price', $price);

        return $cmd->execute();
    }

    /**
     * 根据数组条件 ad_group_id与keyword更新数据
     * @param array $data
     * @return boolean
     * @author jingguangwen@360.cn  20140227
     */
    public function updateByData($data)
    {
        if (!isset($data['create_date']) || !isset($data['ad_group_id']) || !isset($data['keyword'])) {
            return false;
        }
        $tableName = $this->tableName() . "_click_" . date('Ymd', strtotime($data['create_date']));
    	
        $sql = "update " . $tableName . " set clicks=" . (int)$data['clicks']
        . ",costs=" . $data['costs']
        . ", update_time='" . date('Y-m-d H:i:s') . "'";
        $sql .= " where ad_group_id=:groupId and keyword=:keyword  limit 1";
        $cmd = $this->getDbConnection()->createCommand($sql);
        $cmd->bindParam(':groupId', $data['ad_group_id']);
        $cmd->bindParam(':keyword', $data['keyword'], PDO::PARAM_STR);
        
        return $cmd->execute();
         
    }
    /**
     * 根据数组条件 ad_group_id与keyword更新作弊数据
     * @param array $data
     * @return boolean
     * @author jingguangwen@360.cn  20140318
     */
    public function cheatUpdateByData($data)
    {
    	if (!isset($data['create_date']) || !isset($data['ad_group_id']) || !isset($data['keyword'])) {
    		return false;
    	}
    	$tableName = $this->tableName() . "_click_" . date('Ymd', strtotime($data['create_date']));
    	 
    	$sql = "update " . $tableName . " set clicks=clicks-" . (int)$data['clicks']
    	. ",costs=costs-" . $data['costs']
    	. ", update_time='" . date('Y-m-d H:i:s') . "'";
    	$sql .= " where ad_group_id=:groupId and keyword=:keyword  limit 1";
    	$cmd = $this->getDbConnection()->createCommand($sql);
    	$cmd->bindParam(':groupId', $data['ad_group_id']);
    	$cmd->bindParam(':keyword', $data['keyword'], PDO::PARAM_STR);
    
    	return $cmd->execute();
    	 
    }
    /**
     * 存入数据
     * @param array $data
     * @return boolean
     * @author jingguangwen@360.cn  20140227
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
     *
     * add by kangle
     *
     * 2014-02-27
     *
     */
    public function updateViews($data)
    {
        if (!isset($data['create_date']) || !isset($data['ad_group_id']) || !isset($data['keyword'])) {
            return false;
        }
        $tableName = $this->tableName() . "_click_" . date('Ymd', strtotime($data['create_date']));
        $sql = "select id from {$tableName} where ad_group_id =:groupId and keyword=:keyword";
        $cmd = $this->getDbConnection()->createCommand($sql);
        $cmd->bindParam(':groupId', $data['ad_group_id'], PDO::PARAM_INT);
        $cmd->bindParam(':keyword', $data['keyword'], PDO::PARAM_STR);
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
