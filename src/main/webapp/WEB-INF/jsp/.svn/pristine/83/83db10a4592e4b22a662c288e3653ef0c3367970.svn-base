<?php
class MvDataFile {

    const FILE_TYPE_CLICK   = 0;
    const FILE_TYPE_VIEW    = 1;

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
        return 'mv_data_files';
    }

    public function addNewFile($filename, $md5, $metaID, $type) {
        $curTime = date('Y-m-d H:i:s');
        $arrData = array(
            'meta_file_id'  => $metaID,
            'file_name'     => $filename,
            'type'          => $type,
            'md5sum'        => $md5,
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

    public function getByMetaIDAndType ($metaID, $type) {
        $sql = sprintf('select * from %s where meta_file_id=:meta_file_id and type=:type', $this->tableName());
        $db = $this->getDB();
        $cmd = $db->createCommand($sql);
        $cmd->bindParam(':meta_file_id', $metaID,   PDO::PARAM_INT);
        $cmd->bindParam(':type',         $type,     PDO::PARAM_INT);
        return $cmd->queryAll();
    }

}
