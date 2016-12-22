<?php
class MoveCommand extends CConsoleCommand
{
    /**
     * 将click_detail表中数据迁移到分天的数据表中
     */
    public function actionMoveClickDetailLog($days_ago="15")
    {
        self::logger(__CLASS__, __FUNCTION__, 'start');
        set_time_limit(0);
        $time = strtotime("-$days_ago days");
        $date = date('Y-m-d', $time);
        $db = Yii::app()->db_click_log;
        $tableName = 'click_detail_' . date('Ymd', $time);
        $todayTableName = 'click_detail_' . date('Ymd', time());
        self::logger(__CLASS__, __FUNCTION__, 'table name : ' . $tableName);
        while($datas = $db->createCommand("select * from $todayTableName where create_date='{$date}' limit 500")->queryAll())
        {
            $ids = $this->insertDetail($tableName, $datas);
            $db->createCommand("delete from $todayTableName where id in (".implode(",",$ids).")")->execute();
        }

        self::logger(__CLASS__, __FUNCTION__, 'done');
    }

    private function insertDetail($tableName, $datas)
    {
        $sql = "insert ignore into $tableName (".implode(",",array_keys($datas[0])).") values ";
        $values = array();
        $ids = array();
        foreach($datas as $data)
        {
            foreach ($data as $k => &$v)
            {
                if (is_string($v)) {
                    $v = "'" . mysql_escape_string($v) . "'";
                }
                else if(is_array($v)) {
                    $v="'" . mysql_escape_string(@json_encode($v)) . "'";
                } else if(is_null($v)) {
                    $v='null';
                } else if(empty($v) && !($v===0)) {
                    $v="''";
                }
            }
            $values[] = "(".implode(',',array_values($data)).")";
            $ids[] = $data['id'];
        }

        $sql .= implode(",",$values);
        $ret = Yii::app()->db_click_log->createCommand($sql)->execute();
        self::logger(__CLASS__, __FUNCTION__, 'ret:'.json_encode($ret));
        return $ids;
    }

    public static function logger($class, $func, $content)
    {
        if (is_array($content)) {
            $content = json_encode($content);
        }
        $pid = getmypid();

        echo date('Ymd His') . "\t$class\t$func\tpid::$pid\t$content\n";
    }
}

