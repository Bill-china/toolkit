<?php
/**
 * Author: Kangle.Liu - liukangle@360.cn
 *
 * Last modified: 2013-10-22 18:12
 *
 * Filename: AdClickLog.php
 *
 * Description: 
 *
 */
class AdClickLog
{
    static private $db;
    static protected $dbString = 'db_stats';

    const STATUS_NORMAL     = 0; // 正常点击
    const STATUS_CHEAT      = 1; // 作弊待处理
    const STATUS_DONE       = 2; // 作弊返款成功
    const STATUS_TIMEOUT    = 3; // 已经结算，不再返款
    const STATUS_FAIL       = 4; // 作弊处理失败，超出重试次数
    const STATUS_AMONG      = 5; // 作弊处理中间状态，还需后续处理各个维度的统计
    const STATUS_EXCEPTION       = -1; // 结算异常，去除的点击
    const VER_SOU = 'sou';
    const VER_GUESS = 'guess';
    const VER_GOODS = 'goods';
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

    /**
     * @return string the associated database table name
     */
    public function tableName()
    {
        return 'ad_click_log';
    }

    public function getCheatLog($num)
    {
        $num = intval($num);
        if ($num <= 0) {
            return array();
        }
        $date = date('Y-m-d', strtotime('-10 hour'));
        $sql = "select * from " . $this->tableName() . " where create_date >='{$date}' and status=" . self::STATUS_CHEAT . " limit " . $num;
        $rows = $this->getDbConnection()->createCommand($sql)->queryAll();
        if (!$rows)
            return array();
        else
            return $rows;
    }

    public function updateStatusByClickId($clickId,$status,$where_status=null) {
        if (is_null($where_status)) {
            return self::$db->createCommand()->update($this->tableName(), array('status'=>$status,'update_time' => date('Y-m-d H;i:s',time())), 'click_id=:clickId', array(":clickId" => $clickId));
        } else {
            return self::$db->createCommand()->update($this->tableName(), array('status'=>$status,'update_time' => date('Y-m-d H;i:s',time())), 'click_id=:clickId and status='.intval($where_status), array(":clickId" => $clickId));
        }
        

    }
    /**
     * 根据id更新数据
     * @param int $id
     * @param array $data
     * @author jinggaunwgen@360.cn  20140224
     */
    public function updateById($id, $data) {
    	$command = self::$db->createCommand();
    	return $command->update($this->tableName(), $data, 'id=:id', array(':id' => $id));
    }

	/**
	 * add by kangle
	 *
	 * 2014-03-03
	 *
	 */
	public function getInfoByAdvertKey($key, $date)
	{
		$keyArr = explode('_', $key);
		$sql = "select * from " . $this->tableName() . " where create_date=:date and ad_advert_id"
			. "=:aid and ad_channel_id=:cid and ad_place_id=:pid limit 1";
		$cmd = $this->getDbConnection()->createCommand($sql);
		$cmd->bindParam(':date', $date, PDO::PARAM_STR);
		$cmd->bindParam(':aid', $keyArr[0], PDO::PARAM_STR);
		$cmd->bindParam(':cid', $keyArr[1], PDO::PARAM_STR);
		$cmd->bindParam(':pid', $keyArr[2], PDO::PARAM_STR);
		return $cmd->queryRow();
	}
	/**
	 * 根据日期还有状态获取用户相关的消费数据
	 * @param date $date 2014-09-04
	 * @param tinyint $deal_status 是否已经结算了 1已经结算了，0未结算
	 * @return array
	 * @author jingguangwen@360.cn  2014-09-04
	 */
	public function getUserCostByDay($date, $deal_status) {
		$tableName = $this->tableName();
		$sql = sprintf("select ad_user_id as `uid`, sum(price) as `cost`, max(id) as max_click_id from %s where create_date='%s' and status not in (-1,2) and deal_status=%d group by ad_user_id",
				$tableName, $date, $deal_status
		);
		$db = $this->getDbConnection();
		$cmd = $db->createCommand($sql);
		return $cmd->queryAll();
	}
	/**
	 * 结算完毕修改状态
	 * @param int $userID
	 * @param date $date
	 * @return bool
	 * @author jingguangwen@360.cn 2014-09-04
	 */
	public function setSettled($userID, $date, $max_click_id) {
		$tableName = $this->tableName();
		$sql = sprintf('update %s set deal_status=1 where create_date=:create_date and ad_user_id=:ad_user_id and deal_status=0 and id <='.intval($max_click_id), $tableName);
		$db = $this->getDbConnection();
		$cmd = $db->createCommand($sql);
		$cmd->bindParam(':ad_user_id', $userID, PDO::PARAM_INT);
		$cmd->bindParam(':create_date', $date, PDO::PARAM_STR);
		return $cmd->execute();
	}
	public function insertRow($tableName, $data)
	{
		if ($this->getDbConnection()->createCommand()->insert($tableName, $data)) {
			return $this->getDbConnection()->createCommand('SELECT LAST_INSERT_ID()')->queryScalar();
		}
		return false;
	}
}
