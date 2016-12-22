<?php
/**
 * CREATE TABLE `ad_user_third_quota` (
 * `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增id',
 * `ad_user_id` bigint(20) NOT NULL COMMENT '用户ID',
 * `create_date` date not null COMMENT '生效日期',
 * `type` int not null DEFAULT 0 COMMENT '预算类型 0 mediaV 1 以后扩展再说',
 * `ad_user_quota` decimal(10,2) not null COMMENT '每日预算',
 * `rate` int not null DEFAULT 0 COMMENT '预算比例 0-100',
 * `method` int not null COMMENT '预算计算方法 0 按预算 1 按最近七天消费 2 按用户余额',
 * `create_time` datetime not null COMMENT '创建时间',
 * `update_time` datetime not null COMMENT '更新时间',
 * PRIMARY KEY (`id`),
 * UNIQUE KEY `idx_user_date_type` (`ad_user_id`, `create_date`, `type`),
 * key `ad_user_id` (`ad_user_id`),
 * key `create_date` (`create_date`)
 * 
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='用户第三方广告预算表';
 *
 */
class UserQuotaThird {

    protected $_db = null;

    const TYPE_MEDIV = 0;

    const METHOD_PERSENT = 0; // 按用户预算
    const METHOD_CONSUME = 1; // 按以前消费
    const METHOD_BALANCE = 2; // 按用户余额


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
        return 'ad_user_third_quota';
    }

    public function addMediav($userID, $date, $data) {
        $curTime = time();
        $sql = sprintf(
             "insert into %s "
            ."(ad_user_id, create_date, type, ad_user_quota, rate, method, create_time, update_time) "
            ."values "
            ."(:ad_user_id, '%s', %d, :ad_user_quota, :rate, :method, '%s', '%s')",
            $this->tableName(), $date, self::TYPE_MEDIV, date('Y-m-d H:i:s', $curTime), date('Y-m-d H:i:s', $curTime)
        );
        $db = $this->getDB();
        $cmd = $db->createCommand($sql);
        $cmd->bindParam(':ad_user_id', $userID, PDO::PARAM_INT);
        $cmd->bindParam(':ad_user_quota', $data['ad_user_quota'], PDO::PARAM_STR);
        $cmd->bindParam(':rate', $data['rate'], PDO::PARAM_INT);
        $cmd->bindParam(':method', $data['method'], PDO::PARAM_INT);
        $cmd->execute();
        return $db->getLastInsertId();
    }

    public function insertUpdateMediav($userID, $date, $data) {
        $curTime        = time();
        $curDateTime    = date("Y-m-d H:i:s", $curTime);
        $sql = sprintf(
             "insert into %s "
            ."(ad_user_id, create_date, type, ad_user_quota, rate, method, create_time, update_time) "
            ."values "
            ."(:ad_user_id, '%s', %d, :ad_user_quota, :rate, :method, '%s', '%s') "
            ."ON DUPLICATE KEY UPDATE ad_user_quota=:ad_user_quota, rate=:rate, method=:method, update_time='%s'",
            $this->tableName(), $date, self::TYPE_MEDIV, $curDateTime, $curDateTime, $curDateTime 
        );
        $db = $this->getDB();
        $cmd = $db->createCommand($sql);
        $cmd->bindParam(':ad_user_id', $userID, PDO::PARAM_INT);
        $cmd->bindParam(':ad_user_quota', $data['ad_user_quota'], PDO::PARAM_STR);
        $cmd->bindParam(':rate', $data['rate'], PDO::PARAM_INT);
        $cmd->bindParam(':method', $data['method'], PDO::PARAM_INT);
        $cmd->execute();
        return $db->getLastInsertId();
    }

    public function getMediavQuotaAll ($date) {
        $sql = sprintf('select ad_user_id as uid, ad_user_quota as quota from %s where create_date=:create_date', $this->tableName());
        $db = $this->getDB();
        $cmd = $db->createCommand($sql);
        $cmd->bindParam(':create_date', $date, PDO::PARAM_STR);
        $_tmp = $cmd->queryAll();
        if (empty($_tmp)) {
            return array();
        }
        $arrRet = array();
        foreach ($_tmp as $oneItem) {
            $arrRet[$oneItem['uid']] = $oneItem['quota'];
        }
        return $arrRet;
    }
}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */

