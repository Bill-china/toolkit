<?php
class MqMessageLog {

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
        return 'mq_message_log';
    }

    public function addLog($exchange, $content) {
        $insertData = array(
            'exchange'      => $exchange,
            'routing_key'   => '',
            'content'       => json_encode($content),
            'create_time'   => date('Y-m-d H:i:s'),
        );
        $db = $this->getDB();
        $cmd = $db->createCommand();
        return $cmd->insert($this->tableName(), $insertData);
    }
}
