<?php
/**
 * 
 */
class DbRouter {
    
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
        return 'edc_db_router';
    }

    public function getRouter($userID) {
        $sql = sprintf("select db_id from %s where user_id=:user_id",
            $this->tableName()
        );
        $db = $this->getDB();
        $cmd = $db->createCommand($sql);
        $cmd->bindParam(':user_id', $userID, PDO::PARAM_INT);
        $ret = $cmd->queryRow();
        return isset($ret['db_id']) ? $ret['db_id'] : false;
    }

}