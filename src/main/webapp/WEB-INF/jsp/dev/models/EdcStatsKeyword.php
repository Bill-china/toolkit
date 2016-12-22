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
class EdcStatsKeyword 
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
		return 'ad_stats_keyword';
	}

    public function insertRow($tableName, $data) {
        if ($this->getDbConnection()->createCommand()->insert($tableName, $data)) {
            return $this->getDbConnection()->createCommand('SELECT LAST_INSERT_ID()')->queryScalar();
        }
        return false;
    }

    public function getByGroupKeyword($tableName, $groupId, $keyword, $type)
    {
        $sql = "select id from " . $tableName . " where ad_group_id=:groupId and keyword=:keyword and type=:type";
        $cmd = $this->getDbConnection()->createCommand($sql);
        $cmd->bindParam(':groupId', $groupId);
        $cmd->bindParam(':keyword', $keyword, PDO::PARAM_STR);
        $cmd->bindParam(':type', $type);
        return $cmd->queryScalar();
    }

    public function updateIncr($data)
    {
        $tableName = $this->tableName() . "_click_" . date('Ymd', strtotime($data['create_date']));
        if ($id = $this->getByGroupKeyword($tableName, $data['ad_group_id'], $data['keyword'], $data['type'])) {
            $sql = "update " . $tableName . " set clicks=clicks+" . (int)$data['clicks']
                . " ,views=views+" . (int)$data['views'] . ",costs=costs+" . $data['costs']
                . ", update_time='" . date('Y-m-d H:i:s') . "'";
            $sql .= " where id=" . $id . " limit 1";

            return $this->getDbConnection()->createCommand($sql)->execute();
        } else {
            return $this->insertRow($tableName, $data);
        }
    }


    public function getByDateAndKeyword($ad_group_id,$keyword,$create_date) {
        if (empty($ad_group_id) || empty($create_date) || empty($keyword)) {
            return false;       
        }
        $tableName = $this->tableName() . "_click_" . date('Ymd', strtotime($create_date));
        $sql = "select * from $tableName where ad_group_id=" . (int)$ad_group_id
             . " and keyword=:keyword and clicks>0";
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

	public function fixDataUpdate($id, $clicks, $costs, $tableName)
	{
		$sql = "update $tableName set clicks= " . (int)$clicks . ", costs=" . (float)$costs . " where id = $id limit 1";
		echo $sql . "\n";
		return ;
	}

	public function fixDataInsert($key, $clicks, $costs, $tableName)
	{
		$keyArr = explode('_', $key);
		$sql = "update $tableName set clicks= " . (int)$clicks . ", costs=" . (float)$costs
		   	. " where ad_group_id=" . $keyArr[0] . ", keyword='" . addslashes($keyArr[1]) . "' limit 1";
		echo $sql . "\n";
		return ;
	}
}
