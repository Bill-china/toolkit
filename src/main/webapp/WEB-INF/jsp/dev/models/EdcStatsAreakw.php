<?php
/**
 * CREATE TABLE `area_keyword_20141009` (
 *   `id` bigint(20) NOT NULL AUTO_INCREMENT,
 *   `ad_user_id` bigint(20) NOT NULL COMMENT '用户id',
 *   `ad_plan_id` int(11) NOT NULL COMMENT '广告计划id',
 *   `ad_group_id` int(11) NOT NULL COMMENT '组id',
 *   `ad_keyword_id` bigint(11) NOT NULL DEFAULT '0',
 *   `area_fid` int(11) NOT NULL COMMENT '省id',
 *   `area_id` int(11) NOT NULL COMMENT '市id',
 *   `keyword` varchar(128) NOT NULL COMMENT '关键词',
 *
 *   `views` int(11) DEFAULT NULL COMMENT '展现数',
 *   `clicks` int(11) DEFAULT NULL COMMENT '点击数',
 *   `costs` decimal(12,2) DEFAULT NULL COMMENT '消费金额',
 *   `create_time` datetime NOT NULL COMMENT '创建时间',
 *   `update_time` datetime NOT NULL COMMENT '更新时间',
 *
 *   PRIMARY KEY (`id`),
 *   KEY `idx_uid` (`ad_user_id`),
 *   UNIQUE KEY `idx_gid_city_kw` (`ad_group_id`, `area_fid`, `area_id`, `keyword`)
 * )ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='关键词地域统计表';
*/
class EdcStatsAreakw {

    static private $db;

    static private $_instance = null;

    public function setDB($db) {
        self::$db = $db;
    }

    public function getDbConnection() {
        return self::$db;
    }

    public function tableName() {
        return 'ad_click_area_keyword';
    }

    public function getByGidAreadAndKeyword ($groupID, $areaFid, $areaID, $keyword, $createDate) {
        $tableName = $this->tableName() . '_' . date('Ymd', strtotime($createDate));
        $sql = sprintf("select * from %s where ad_group_id=:ad_group_id and area_fid=:area_fid and area_id=:area_id and keyword=:keyword", $tableName);
        $cmd = $this->getDbConnection()->createCommand($sql);

        $cmd->bindParam(':ad_group_id', $groupID,   PDO::PARAM_INT);
        $cmd->bindParam(':area_fid',    $areaFid,   PDO::PARAM_INT);
        $cmd->bindParam(':area_id',     $areaID,    PDO::PARAM_INT);
        $cmd->bindParam(':keyword',     $keyword,   PDO::PARAM_STR);

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
