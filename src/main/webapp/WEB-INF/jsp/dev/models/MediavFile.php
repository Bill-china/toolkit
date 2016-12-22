<?php
/**
 * CREATE TABLE `file_mediav` (
 *     `id` int(11) UNSIGNED NOT NULL AUTO_INCREMENT,
 *
 *     `time_str` char(16) NOT NULL COMMENT '时间，精确到分, 201408061005',
 *     `file_name` char(255) NOT NULL COMMENT '数据文件名称',
 *     `md5sum` char(40) NOT NULL COMMENT 'md5值',
 *     `status` tinyint(4) NOT NULL DEFAULT 0 COMMENT '状态 0 存在 1 已删除',
 *     `download_number` int(10) NOT NULL DEFAULT 0 COMMENT '下载次数',
 *     `create_time` int(11) NOT NULL COMMENT '创建时间',
 *     `update_time` int(11) NOT NULL COMMENT '最后更新时间',
 *
 *     PRIMARY KEY (`id`),
 *     UNIQUE KEY `idx_timt_name` (`time_str`,`file_name`),
 *     KEY `idx_status` (`time_str`, `status`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 */
class MediavFile {

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
        return 'file_mediav';
    }

    public function insert($timeStr, $fileName, $md5sum) {
        $insertData = array(
            'time_str'          => $timeStr,
            'file_name'         => $fileName,
            'md5sum'            => $md5sum,
            'status'            => 0, // 已生成
            'download_number'   => 0,
            'create_time'       => time(),
            'update_time'       => time(),
        );
        $db = $this->getDB();
        $db->createCommand()->insert($this->tableName(), $insertData);
        return $db->getLastInsertId();
    }

    public function getByTimeFname($timeStr, $fileName) {
        $sql = sprintf('select * from %s where time_str=:time_str and file_name=:file_name', $this->tableName());
        $db = $this->getDB();
        $cmd = $db->createCommand($sql);
        $cmd->bindParam(':time_str', $timeStr, PDO::PARAM_STR);
        $cmd->bindParam(':file_name', $fileName, PDO::PARAM_STR);
        return $cmd->queryRow();
    }

}