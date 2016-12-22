<?php
/**
 *
 */
class Plan {

    protected $_db = null;

    public function setDB($db) {
        $this->_db = $db;
    }

    protected function getDB() {
        if (is_null($this->_db)) {
            throw new Exception("db is null", 1);
        }
        return $this->_db;
    }

    public function tableName () {
        return 'ad_plan';
    }

    public function getWorkingPlanList($userID, $time) {
        $sql = sprintf("select id, exp_amt, third_party from %s where ad_user_id=:ad_user_id and status=1 and start_date<=:time and end_date>=:time",
            $this->tableName()
        );
        $db = $this->getDB();
        $cmd = $db->createCommand($sql);
        $cmd->bindParam(':ad_user_id', $userID, PDO::PARAM_INT);
        $cmd->bindParam(':time', $time, PDO::PARAM_INT);
        return $cmd->queryAll();
    }

}