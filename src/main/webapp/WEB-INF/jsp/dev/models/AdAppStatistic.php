<?php
class AdAppStatistic {

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
        return 'ad_app_statistic';
    }

    public function getByDateAndPlanKey($create_date, $ad_plan_id, $ad_channel_id,$place_id,$req_src) {
        if (empty($create_date)) {
            return false;
        }
        $tableName = $this->tableName() . "_" . date('Ymd', strtotime($create_date));
        $sql = sprintf(
            "select * from %s where ad_plan_id=%d and ad_channel_id=%d and clicks>0 and ad_place_id=%d and req_src='%s' ",
            $tableName, $ad_plan_id, $ad_channel_id, $place_id, $req_src
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
