<?php
/**
 *
 * CREATE TABLE `ad_user_ext` (
 *   `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增id',
 *   `ad_user_id` bigint(20) NOT NULL COMMENT '用户ID',
 *   `user_rebate` tinyint(4) NOT NULL DEFAULT '0' COMMENT '用户返点比例0-100',
 *   `update_time` int(10) unsigned NOT NULL COMMENT '数据添加或者更新时间',
 *   `is_img` tinyint(1) NOT NULL DEFAULT '0' COMMENT '是否开启比邻',
 *   `web_scan` varchar(255) NOT NULL DEFAULT '',
 *   `third_party` int(11) NOT NULL DEFAULT '0' COMMENT '第三方投放，第一位media v，第二位搜索联盟',
 *   PRIMARY KEY (`id`),
 *   UNIQUE KEY `ad_user_id` (`ad_user_id`) USING BTREE
 * ) ENGINE=InnoDB AUTO_INCREMENT=84 DEFAULT CHARSET=utf8 COMMENT='用户扩展表'
 */
class UserExt {

    const THIRD_PARTY_MEDIAV = 1;

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
        return 'ad_user_ext';
    }

    public function getUserInfoByID ($userID) {
        $sql = sprintf('select * from %s where ad_user_id=%d',
            $this->tableName(), $userID
        );
        $db = $this->getDB();
        $cmd = $db->createCommand($sql);
        return $cmd->queryRow($userID);
    }

    public function isOpenMediav ($userID) {
        $sql = sprintf('select * from %s where ad_user_id=%d',
            $this->tableName(), $userID
        );
        $db = $this->getDB();
        $cmd = $db->createCommand($sql);
        $ret = $cmd->queryRow($userID);
        return isset($ret['third_party']) ? (bool)($ret['third_party'] & self::THIRD_PARTY_MEDIAV) : false;
    }
    public function getUserThirdPartyAll () {
        $sql = 'select ad_user_id, third_party from '.$this->tableName();
        $db = $this->getDB();
        $_ret = $db->createCommand($sql)->queryAll();
        if (empty($_ret)) {
            return array();
        }
        $arrRet = array();
        foreach ($_ret as $_oneRow) {
            $arrRet[$_oneRow['ad_user_id']] = array(
                'is_media_v'    => (bool)($_oneRow['third_party'] & self::THIRD_PARTY_MEDIAV),
            );
        }
        return $arrRet;
    }

}

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
