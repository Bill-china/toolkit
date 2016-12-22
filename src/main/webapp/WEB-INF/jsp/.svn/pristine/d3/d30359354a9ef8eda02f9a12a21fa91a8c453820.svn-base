<?php
class AdApp {

    static private $db;
    protected $dbString = 'db_stats';
    static private $_instance = null;

    public static function model() {
        if (is_null(self::$_instance)) {
            self::$_instance = new self();
        }
        return self::$_instance;
    }

    public function setDB($db) {
        self::$db = $db;
    }

    public function getDbConnection() {
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
        return 'ad_app';
    }

    public function getByDateAndAreaKey($ad_group_id, $area_key, $create_date,$ad_channel_id,$location_id,$place_id,$ad_plan_id,$ad_advert_id,$click_type) {
        if ((empty($ad_group_id)&&empty($ad_plan_id)&&empty($ad_advert_id)&&empty($click_type)) || empty($create_date)) {
            return false;
        }
        $tableName = $this->tableName() . "_" . date('Ymd', strtotime($create_date));
        list($fid, $id) = explode(",", $area_key);
        if ($fid == 10001) {
            $fid = $id = 0;
        } else if ($id==10001) {
            $fid = (int)$fid;
            $id = 0;
        } else {
            $fid = (int)$fid;
            $id = (int)$id;
        }
        $sql = sprintf(
            "select * from %s where ad_group_id=%d and area_fid=%d and area_id=%d and ad_channel_id=%d and location_id=%d and place_id=%d  and clicks>0 and ad_plan_id=%d and ad_advert_id=%d  and click_type=%d",
            $tableName, $ad_group_id, $fid, $id, $ad_channel_id, $location_id, $place_id,$ad_plan_id,$ad_advert_id,$click_type
        );

        $cmd = $this->getDbConnection()->createCommand($sql);
        return $cmd->queryRow();
    }

    public function cheatClickRefund($id, $price, $create_date) {
        $tableName = $this->tableName() . "_" . date('Ymd', strtotime($create_date));
        $sql = "update " . $tableName . " set clicks=clicks-1, costs=costs-:price "
            . " where id=:id";
        $cmd = $this->getDbConnection()->createCommand($sql);
        $cmd->bindParam(':id', $id);
        $cmd->bindParam(':price', $price);

        return $cmd->execute();
    }
}
