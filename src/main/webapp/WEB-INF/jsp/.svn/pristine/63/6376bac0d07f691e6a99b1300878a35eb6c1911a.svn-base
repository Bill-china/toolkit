<?php
/**
 * Author: jingguangwen@360.cn
 *
 * Last modified: 2015-04-20
 *
 * Filename: ClickDetail.php
 *
 * Description:
 *
 */
class ClickDetail
{
    static private $db;
    static protected $dbString = 'db_stats';

    const STATUS_NORMAL     = 0; // 正常点击
    const STATUS_CHEAT      = 1; // 作弊待处理
    const STATUS_DONE       = 2; // 作弊返款成功
    const STATUS_TIMEOUT    = 3; // 已经结算，不再返款
    const STATUS_FAIL       = 4; // 作弊处理失败，超出重试次数
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
        return 'click_detail';
    }


    public function updateStatusByClickId($clickId,$status,$where_status=null) {
        if (is_null($where_status)) {
            return self::$db->createCommand()->update($this->tableName(), array('status'=>$status,'update_time' => time()), 'click_id=:clickId', array(":clickId" => $clickId));
        } else {
            return self::$db->createCommand()->update($this->tableName(), array('status'=>$status,'update_time' => time()), 'click_id=:clickId and status='.intval($where_status), array(":clickId" => $clickId));
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
	 * 根据日期还有状态获取用户相关的消费数据
	 * @param date $date 2014-09-04
	 * @param tinyint $deal_status 是否已经结算了 1已经结算了，0未结算
	 * @return array
	 * @author jingguangwen@360.cn  2014-09-04
	 */
	public function getUserCostByDay($date, $deal_status) {
		$tableName = $this->tableName();
		$sql = sprintf("select ad_user_id as `uid`, sum(price-reduce_price) as `cost`, max(id) as max_click_id from %s where create_date='%s' and status not in (-1,2) and cheat_type  not  in (2,3) and deal_status=%d and price != reduce_price group by ad_user_id",
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
