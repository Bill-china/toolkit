<?php
/**
 *
 */
class User {

    const STATUS_AUDITED = 1; // 已审核
    const STATUS_NOCASH  = 2; // 余额不足

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
        return 'ad_user';
    }

    public function getValidUser($lastUid, $limit) {
        $sql = sprintf("select id, balance, day_quota, mv_quota, client_category from %s where id>:id and status in (%d, %d) limit :limit",
            $this->tableName(),
            self::STATUS_AUDITED,
            self::STATUS_NOCASH
        );
        $db = $this->getDB();
        $cmd = $db->createCommand($sql);
        $cmd->bindParam(':id', $lastUid, PDO::PARAM_INT);
        $cmd->bindParam(':limit', $limit, PDO::PARAM_INT);
        return $cmd->queryAll();
    }

    public function getInvalidUser($lastUid, $limit) {
        $sql = sprintf("select id, client_category from %s where id>:id and status not in (%d, %d) limit :limit",
            $this->tableName(),
            self::STATUS_AUDITED,
            self::STATUS_NOCASH
        );
        $db = $this->getDB();
        $cmd = $db->createCommand($sql);
        $cmd->bindParam(':id', $lastUid, PDO::PARAM_INT);
        $cmd->bindParam(':limit', $limit, PDO::PARAM_INT);
        return $cmd->queryAll();
    }

    public function getUserList ($lastUid, $limit) {
        $sql = sprintf("select id, status, client_category from %s where id>:id limit :limit",
            $this->tableName()
        );
        $db = $this->getDB();
        $cmd = $db->createCommand($sql);
        $cmd->bindParam(':id', $lastUid, PDO::PARAM_INT);
        $cmd->bindParam(':limit', $limit, PDO::PARAM_INT);
        return $cmd->queryAll();
    }

    public function getInfoByID ($userID) {
        $sql = sprintf("select * from %s where id=:id", $this->tableName());
        $db = $this->getDB();
        $cmd = $db->createCommand($sql);
        $cmd->bindParam(':id', $userID, PDO::PARAM_INT);
        return $cmd->queryRow();
    }

    public function updateCostByUserId ($userID, $amount) {
        $amount = floatval($amount);
        $sql = sprintf('update %s set exp_amt=exp_amt+%s, balance=balance-%s where id=:ad_user_id',
            $this->tableName(), $amount, $amount
        );
        $db = $this->getDB();
        $cmd = $db->createCommand($sql);
        $cmd->bindParam(':ad_user_id', $userID, PDO::PARAM_INT);
        return $cmd->execute();
    }

    public function updateUserStataus ($userID, $status) {
        $sql = sprintf ('update %s set status=:status where id=:ad_user_id', $this->tableName());
        $db = $this->getDB();
        $cmd = $db->createCommand($sql);
        $cmd->bindParam(':status', $status, PDO::PARAM_INT);
        $cmd->bindParam(':ad_user_id', $userID, PDO::PARAM_INT);
        return $cmd->execute();
    }
    /**
     * 获取存在限额变更以及有效转无效等的账户信息
     * @param  [int] $lastUid [起始数]
     * @param  [int] $limit   [条数]
     * @return [array]          [array]
     * @author jingguangwen@360.cn
     */
    public function getQuotaUser($lastUid, $limit,$status=array()) {
        $sql = sprintf("select id, balance, day_quota, mv_quota,status,quota_dianjing,quota_ruyi,quota_app from %s where id>:id  order by  id  asc limit :limit",
            $this->tableName()
        );
        if($status)
        $sql = sprintf("select id, balance, day_quota, mv_quota,status,quota_dianjing,quota_ruyi,quota_app from %s where id>:id and status in (".implode(",",$status).")  order by  id  asc limit :limit",
            $this->tableName()
        );
        $db = $this->getDB();
        $cmd = $db->createCommand($sql);
        $cmd->bindParam(':id', $lastUid, PDO::PARAM_INT);
        $cmd->bindParam(':limit', $limit, PDO::PARAM_INT);
        return $cmd->queryAll();
    }
    /**
     * [根据账户id查询限额信息]
     * @param  [int] $ad_user_id [账户id]
     * @return [array]             [返回数据]
     * @author jingguangwen@360.cn
     */
    public static function getQuotaInfoByUserId($ad_user_id) {
        $sql = sprintf("select id, balance, day_quota, mv_quota,status from %s where id=:id",
            $this->tableName()
        );
        $cmd = Yii::app()->db_center->createCommand($sql);
        $cmd->bindParam(':id', $ad_user_id, PDO::PARAM_INT);
        return $cmd->queryRow();
    }

    /**
     * 结算完毕之后校验账户账户
     * @param  [int] $ad_user_id [账户id]
     * @return [bole]
     * @author jingguangwen@360.cn
     */
    public function  checkUserStatus($ad_user_id)
    {
        $ad_user_id = intval($ad_user_id);
        $sql = "select id,balance,status from ad_user where id=".$ad_user_id;
        $user_info_arr = Yii::app()->db_center->createCommand($sql)->queryRow();
        if(empty($user_info_arr)){
            return false;
        }
        $user_status_change = null;
        // 更新用户状态
        if ($user_info_arr['balance']>0) {
            if ($user_info_arr['status']==2) { // unlock
                $this->updateUserStataus($ad_user_id, 1);
                $user_status_change = 1;
            }else if ($user_info_arr['status']==-2) { // 待审核
                $this->updateUserStataus($ad_user_id, 0);
                $user_status_change = 0;
            }
        } else {
            if ($user_info_arr['status']==1) { // lock
                $this->updateUserStataus($ad_user_id, 2);
                $user_status_change = 2;
            }
        }
        if (isset($user_status_change)) {

            //写消息通知crm用户状态变更为有效状态
            $content = array(
                'msg_type' => 'update',
                'msg_src' => 'esc_crontab',
                'msg_id' => '',
                'time' => time(),
                'content' => array(
                    'ad_user_id' => $ad_user_id,
                    'data' => array(
                        'ad_user_id' => $ad_user_id,
                        'status' => $user_status_change,
                        'old_status' => $user_info_arr['status'],
                        'user_balance' => $user_info_arr['balance'],
                    ),
                ),
            );
            //写emq消息
            $insertData = array(
                'exchange'      => 'ex_user_status_change',
                'routing_key'   => '',
                'content'       => json_encode($content),
                'create_time'   => date('Y-m-d H:i:s'),
            );

            return Yii::app()->db_center->createCommand()->insert('mq_message_log', $insertData);
        }
        return true;
    }
}