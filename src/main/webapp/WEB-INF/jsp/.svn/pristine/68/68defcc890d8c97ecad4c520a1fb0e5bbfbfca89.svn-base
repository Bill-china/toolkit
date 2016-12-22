<?php
/**
 * Author: Kangle.Liu - liukangle@360.cn
 *
 * Last modified: 2013-09-11 16:01
 *
 * Filename: EdcStats.php
 *
 * Description: ad_stats表的model，不再继承CActiveRecord 
 *
 */
class EdcStats
{
    static private $db;
    protected $dbString = 'db_stats';
    const STATUS_STATED = 1;
    const TYPE_ClICK = 1;
    const TYPE_VIEW = 2;
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
    public function tableName() {
        return 'ad_stats';
    }

    public function addIncr($data)
    {
        if (!isset($data['data_source']))
            $data['data_source'] = 0;
        if (!isset($data['last_update_time']))
            $data['last_update_time'] = 0;
        if (!isset($data['trans']))
            $data['trans'] = 0;
        if (!isset($data['admin_user_id']))
            $data['admin_user_id'] = 0;
        if (!isset($data['type']))
            $data['type'] = 0;
        if (!isset($data['status']))
            $data['status'] = 0;
        $tableName = $this->tableName() . '_click_' . date('Ymd', strtotime($data['create_date']));
        $sql = "INSERT INTO {$tableName} (`clicks`, `views`, `total_cost`, `trans`, `status`, `create_date`,"
            . " `ad_group_id`, `ad_plan_id`, `ad_advert_id`, `ad_user_id`, `ad_channel_id`, `ad_place_id`,"
            . " `last_update_time`, `data_source`, `create_time`, `update_time`, `type`) values("
            . " {$data['clicks']}, {$data['views']}, {$data['total_cost']}, {$data['trans']}, {$data['status']}, '{$data['create_date']}',"
            . " '{$data['ad_group_id']}', '{$data['ad_plan_id']}', '{$data['ad_advert_id']}', '{$data['ad_user_id']}', {$data['ad_channel_id']}, {$data['ad_place_id']}, "
            . " {$data['last_update_time']}, {$data['data_source']}, '{$data['create_time']}', '{$data['update_time']}', '{$data['type']}') ON DUPLICATE KEY "
            . " UPDATE `clicks`=`clicks`+{$data['clicks']}, `views`=`views`+{$data['views']}, `total_cost`=`total_cost`+{$data['total_cost']}";
        return $this->getDbConnection()->createCommand($sql)->execute();
    }

    public function getRow($tableName, $advertId, $channelId, $placeId, $type)
    {
        $sql = "SELECT * FROM $tableName WHERE ad_advert_id=:advertId AND ad_channel_id=:channelId AND ad_place_id=:placeId AND type=:type";
        $command = $this->getDbConnection()->createCommand($sql);
        $command->bindParam(":advertId", $advertId, PDO::PARAM_INT);
        $command->bindParam(":channelId", $channelId, PDO::PARAM_INT);
        $command->bindParam(":placeId", $placeId, PDO::PARAM_INT);
        $command->bindParam(":type", $type, PDO::PARAM_INT);
        return $command->queryRow();
    }

    public function updateRow($tableName, $id, $data)
    {
        $sql = "UPDATE " . $tableName . " SET ";
        if (isset($data['views'])) {
            $sql .= "views=views+" . intval($data['views']) . ',';
        }
        if (isset($data['clicks'])) {
            $sql .= "clicks=clicks+" . intval($data['clicks']) . ',';
        }
        if (isset($data['total_cost'])) {
            $sql .= "total_cost=total_cost+" . number_format($data['total_cost'], 2, '.', '') . ',';
        }
        $sql .= ' `update_time`="' . date('Y-m-d H:i:s') . '" WHERE id=:id limit 1';
        $command = $this->getDbConnection()->createCommand($sql);
        $command->bindParam(":id", $id, PDO::PARAM_INT);
        return $command->execute();
    }

    public function insertRow($tableName, $data)
    {
        if ($this->getDbConnection()->createCommand()->insert($tableName, $data)) {
            return $this->getDbConnection()->createCommand('SELECT LAST_INSERT_ID()')->queryScalar();
        }
        return false;
    }


    public function getByAdvertIdAndDate($ad_advert_id, $create_date)
    {
        $tableName = $this->tableName() . '_click_' . date('Ymd', strtotime($create_date));
        $ad_advert_id = intval($ad_advert_id);
        if (empty($ad_advert_id) || empty($create_date)) {
            return false;
        }
        $sql = "select * from $tableName where ad_advert_id=" . $ad_advert_id . " and clicks>0";
        $cmd = $this->getDbConnection()->createCommand($sql);
        return $cmd->queryRow();
    }

    public function cheatClickRefund($id, $price, $create_date)
    {
        $tableName = $this->tableName() . '_click_' . date('Ymd', strtotime($create_date));
        $sql = "update " . $tableName . " set clicks=clicks-1, total_cost=total_cost-:price "
            . " where id=:id";
        $cmd = $this->getDbConnection()->createCommand($sql);
        $cmd->bindParam(':id', $id);
        $cmd->bindParam(':price', $price);

        return $cmd->execute();
    }

	public function fixDataUpdate($id, $clicks, $total_cost, $tableName)
	{
		$sql = "update $tableName set clicks= " . (int)$clicks . ", total_cost=" . (float)$total_cost . " where id = $id limit 1";
		echo $sql . "\n";
		return ;
	}

	public function fixDataInsert($key, $clicks, $total_cost, $tableName)
	{
		$keyArr = explode('_', $key);
		$sql = "update $tableName set clicks= " . (int)$clicks . ", total_cost=" . (float)$total_cost
		   	. " where ad_advert_id=" . $keyArr[0] . " and ad_channel_id=" . $keyArr[1] . " and ad_place_id=" . $keyArr[2] . " limit 1";
		echo $sql . "\n";
		return ;
	}
	public function getByAdvertIdAndDateAndChannelAndPlace($ad_advert_id, $create_date,$ad_channel_id,$ad_place_id)
	{
		$tableName = $this->tableName() . '_click_' . date('Ymd', strtotime($create_date));
		$ad_advert_id = intval($ad_advert_id);
		$ad_channel_id = intval($ad_channel_id);
		$ad_place_id = intval($ad_place_id);
		if (empty($ad_advert_id) || empty($create_date)) {
			return false; 
		}
		$sql = "select * from $tableName where ad_advert_id=" . $ad_advert_id . " and ad_channel_id=".$ad_channel_id."  and ad_place_id=".$ad_place_id." and clicks>0";
		$cmd = $this->getDbConnection()->createCommand($sql);
		return $cmd->queryRow();
	}

    public function getUserCostByDay($date, $status) {
        $tableName = $this->tableName() . '_click_' . date('Ymd', strtotime($date));
        $sql = sprintf("select ad_user_id as `uid`, sum(total_cost) as `cost` from %s where type=1 and status=%d group by ad_user_id",
            $tableName, $status
        );
        $db = $this->getDbConnection();
        $cmd = $db->createCommand($sql);
        return $cmd->queryAll();
    }

    public function setSettled($userID, $date) {
        $tableName = $this->tableName() . '_click_' . date('Ymd', strtotime($date));
        $sql = sprintf('update %s set status=1 where ad_user_id=:ad_user_id and status=0', $tableName);
        $db = $this->getDbConnection();
        $cmd = $db->createCommand($sql);
        $cmd->bindParam(':ad_user_id', $userID, PDO::PARAM_INT);
        return $cmd->execute();
    }
    public function setAllSettled($date) {
    	$tableName = $this->tableName() . '_click_' . date('Ymd', strtotime($date));
    	$sql = sprintf('update %s set status=1 where  status=0', $tableName);
    	$db = $this->getDbConnection();
    	$cmd = $db->createCommand($sql);
    	return $cmd->execute();
    }
}
