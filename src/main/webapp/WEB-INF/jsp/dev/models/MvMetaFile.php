<?php

class MvMetaFile {

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
        return 'mv_meta_files';
    }

    public function addNewFile($filename) {
        $curTime = date('Y-m-d H:i:s');
        $arrData = array(
            'file_name'     => $filename,
            'status'        => 0,
            'create_time'   => $curTime,
            'update_time'   => $curTime,
        );

        $db = $this->getDB();
        try {
            $db->createCommand()->insert($this->tableName(), $arrData);
        } catch (Exception $e) {
            return false;
        }
        return $db->getLastInsertID();
    }

    public function updateStatusByName ($filename, $status) {
        $curTime = date('Y-m-d H:i:s');
        $sql = sprintf('update %s set status=:status, update_time=:update_time where file_name=:file_name', $this->tableName());
        $db = $this->getDB();
        $cmd = $db->createCommand($sql);
        $cmd->bindParam(':status',      $status,    PDO::PARAM_INT);
        $cmd->bindParam(':file_name',   $filename,  PDO::PARAM_STR);
        $cmd->bindParam(':update_time', $curTime,   PDO::PARAM_STR);
        return $cmd->execute();
    }

    public function getByStatus ($status) {
        $sql = sprintf('select * from %s where status=:status', $this->tableName());
        $db = $this->getDB();
        $cmd = $db->createCommand($sql);
        $cmd->bindParam(':status', $status, PDO::PARAM_INT);
        return $cmd->queryAll();
    }


}