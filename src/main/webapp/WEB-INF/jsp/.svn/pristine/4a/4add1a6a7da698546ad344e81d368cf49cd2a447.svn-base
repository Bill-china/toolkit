<?php
/**
 * Author: Kangle.Liu - liukangle@360.cn
 *
 * Last modified: 2013-10-22 18:12
 *
 * Filename: MvClickLog.php
 *
 * Description: 
 *
 */
class MvClickLog
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

    /**
     * @return string the associated database table name
     */
    public function tableName()
    {
        return 'mv_click_log';
    }

	/**
	 * 结算完毕修改状态
	 * @param int $userID
	 * @param date $date
	 * @return bool
	 * @author jingguangwen@360.cn 2014-09-04
	 */
	public function setSettled($userID, $date, $max_click_id) {
	    if (empty($userID) || empty($date) || empty($max_click_id)) {
	        return false;
	    }
		$tableName = $this->tableName()."_" . date('Ymd', strtotime($date));
		$sql = sprintf('update %s set deal_status=1 where  ad_user_id=:ad_user_id and deal_status=0 and id <='.intval($max_click_id), $tableName);
		$db = $this->getDbConnection();
		$cmd = $db->createCommand($sql);
		$cmd->bindParam(':ad_user_id', $userID, PDO::PARAM_INT);
		return $cmd->execute();
	}
}
