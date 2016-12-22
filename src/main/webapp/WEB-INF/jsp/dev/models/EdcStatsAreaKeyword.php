<?php
/**
 * Created by PhpStorm.
 * User: dongpingan
 * Date: 2016/6/21
 * Time: 9:50
 */

class EdcStatsAreaKeyword {
    private static $db;
    protected $dbString = 'db_esc_stat';

    public function tableName() {
        return 'ad_stats_area_keyword_click_';
    }

    public function setDB($db) {
        self::$db = $db;
    }

    public function insertRows($rows,$date) {
        if(is_null(self::$db) || empty($rows) || !is_array($rows)) {
            return 0;
        }
        $tableName = $this->tableName()  . date('Ymd', strtotime($date));

        $sql = 'INSERT INTO ' . $tableName . ' (ad_group_id,ad_plan_id,ad_user_id,keyword,area_fid,area_id,clicks,costs,create_date,create_time) VALUES  ';

        foreach($rows as $row) {
            $sql .= "('" .
                $row['ad_group_id'] . "','" .
                $row['ad_plan_id'] . "','" .
                $row['ad_user_id'] . "','" .
                mysql_escape_string($row['keyword']) . "','" .
                $row['area_fid'] . "','" .
                $row['area_id'] . "','" .
                $row['clicks'] . "','" .
                $row['costs'] . "','" .
                $row['create_date'] . "','" .
                $row['create_time'] . "')," ;
        }
        $sql = trim($sql,',');
        self::$db->createCommand($sql)->execute();
        return self::$db->createCommand('SELECT LAST_INSERT_ID()')->queryScalar();
    }

}