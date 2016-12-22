<?php
/**
 *
 */
class UserChargeLog {

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
        return 'ad_user_charge_log';
    }

    public function getByUIDAndDate($userID, $date) {
        $sql = "SELECT * FROM " . $this->tableName() . " WHERE ad_user_id=:user_id AND create_date=:date";
        $db = $this->getDB();
        $cmd = $db->createCommand($sql);
        $cmd->bindParam(':user_id', $userID, PDO::PARAM_INT);
        $cmd->bindParam(':date', $date, PDO::PARAM_STR);
        return $cmd->queryRow();
    }

    public function updateCostIncr ($userID, $cost, $balance, $date) {
        $cost = floatval($cost);
        $db = $this->getDB();
        if ($row = $this->getByUIDAndDate($userID, $date)) {
            $sql = sprintf("update %s set cost=cost+:cost, balance=balance-:cost where ad_user_id=:ad_user_id and create_date=:create_date",
                $this->tableName()
            );
            $cmd = $db->createCommand($sql);
            $cmd->bindParam(':cost', $cost);
            $cmd->bindParam(':ad_user_id', $userID);
            $cmd->bindParam(':create_date', $date);
            $res =  $cmd->execute();

        } else{
            //再减去当天的结算前充值
            $finish_time_start = strtotime(date('Y-m-d'));
            $charge_sql = sprintf("select  ad_user_id,sum(amount) as money from  ad_topup_log  where ad_user_id=%d and finish_time>%d and  status  =1 and pay_type  not  in (-3,-20) ", $userID,$finish_time_start);
            $charge_arr  = $db->createCommand($charge_sql)->queryRow();
            $charge_money = 0;
            if(!empty($charge_arr)){
                $charge_money = $charge_arr['money'];
            }
            $insertData = array(
                'ad_user_id'    => $userID,
                'cost'          => $cost,
                'balance'       => round($balance - $cost - $charge_money ,2),
                'create_date'   => $date,
            );
            $res = $db->createCommand()->insert($this->tableName(), $insertData);
        }
        //今天如果有数据，也需要更新
        $sql_tooday = sprintf("update %s set balance=balance-:cost where ad_user_id=:ad_user_id and create_date > :create_date limit  1",
            $this->tableName()
        );
        $cmd = $db->createCommand($sql_tooday);
        $cmd->bindParam(':cost', $cost);
        $cmd->bindParam(':ad_user_id', $userID);
        $cmd->bindParam(':create_date', $date);
        $cmd->execute();
        return $res;
    }

    public function getUserCharge ($date) {
        $sql = sprintf('select ad_user_id, cost from %s where create_date=:create_date', $this->tableName() );
        $db = $this->getDB();
        $cmd = $db->createCommand($sql);
        $cmd->bindParam(':create_date', $date, PDO::PARAM_STR);
        return $cmd->queryAll();
    }

    // 获取用户在一段时间内的平均消费
    public function getUserOldChargeAverage($userID, $startDate, $endDate, $n) {
        $sql = sprintf('select cost from %s where ad_user_id=:ad_user_id and create_date>=:start_date and create_date<=:end_date', $this->tableName());
        $db = $this->getDB();
        $cmd = $db->createCommand($sql);
        $cmd->bindParam(':ad_user_id', $userID, PDO::PARAM_INT);
        $cmd->bindParam(':start_date', $startDate, PDO::PARAM_STR);
        $cmd->bindParam(':end_date', $endDate, PDO::PARAM_STR);
        $ret = $cmd->queryAll();
        $total = 0;
        // var_dump($ret);
        if (!empty($ret)) {
            foreach ($ret as $_one) {
                $total += $_one['cost'];
            }
        }
        // printf("total : %s", $total);
        return $total/$n;
    }


    public function updateProductCost ($userID, $cost, $date,$type) {

        if(!in_array($type, array(1,2,3,4))){
            return 0;
        }
        $key = '';
        switch ($type) {
            case 1:
                $key = 'cost_dianjing';
                break;
            case 2:
                $key = 'cost_app';
                break;
            case 3:
                $key = 'cost_ruyi';
                break;
            case 4:
                $key = 'cost_show';
                break;

        }
        $cost = floatval($cost);
        $db = Yii::app()->db_center;

        //今天如果有数据，也需要更新
        $sql = sprintf("update %s set %s=:cost where ad_user_id=:ad_user_id and create_date = :create_date limit  1",
            $this->tableName(),$key
        );
        $cmd = $db->createCommand($sql);
        $cmd->bindParam(':cost', $cost);
        $cmd->bindParam(':ad_user_id', $userID);
        $cmd->bindParam(':create_date', $date);
        return $cmd->execute();
    }

}

