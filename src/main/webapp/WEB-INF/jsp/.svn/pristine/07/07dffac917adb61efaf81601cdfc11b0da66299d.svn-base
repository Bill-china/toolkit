<?php
/**
 * Author: Kangle.Liu - liukangle@360.cn
 *
 * Last modified: 2013-09-11 16:27
 *
 * Filename: EdcStatsArea.php
 *
 * Description: 
 *
 */
class EdcMvStatsArea
{
    static private $db;
    protected $dbString = 'db_stats';
    static private $_instance = null;

    public static function model()
    {
        if (is_null(self::$_instance)) {
            self::$_instance = new self();
        }
        return self::$_instance;
    }

    public function setDB($db) {
        self::$db = $db;
    }

    public function getDbConnection()
    {
        if (self::$db !== null)
            return self::$db;
        else {
            $dbString=$this->dbString;
            self::$db=Yii::app()->$dbString;
            return self::$db;
        }
    }

    /**
     * @return string the associated database table name
     */
    public function tableName()
    {
        return 'mediav_stats_area';
    }

    public function insertRow($tableName, $data) {
        if ($this->getDbConnection()->createCommand()->insert($tableName, $data)) {
            return $this->getDbConnection()->createCommand('SELECT LAST_INSERT_ID()')->queryScalar();
        }
        return false;
    }
    /**
     * 根据click_log数据信息解析地域统计信息
     * @param array $user_ids_arr
     * @param array $result_arr
     * @param array $mv_result_arr
     * @author jingguangwen@360.cn  2914-09-11
     */
    public static function getClikResult($click_log_arrs, &$result_arr, &$mv_result_arr) {
        
        if (!empty($click_log_arrs)) {
            foreach ($click_log_arrs as $click_arr) {
                $area_key = $click_arr['area_key'];
                $ad_group_id = $click_arr['ad_group_id'];
                $ad_plan_id = $click_arr['ad_plan_id'];
                $ad_user_id = $click_arr['ad_user_id'];
                $price = $click_arr['price'];
                $ver = $click_arr['ver'];
                //解析$area_key
                $arr = explode(",", $area_key);
                $area_fid = $area_id = 0;
                if (count($arr)>1) {
                    if ($arr[0] == 10001 && $arr[1] == 10001) {
                        $area_fid = 0;
                        $area_id = 0;
                    } elseif ($arr[0] != 10001 && $arr[1] == 10001) {
                        $area_fid = (int)$arr[0];
                        $area_id = 0;
                    } else {
                        $area_fid = (int)$arr[0];
                        $area_id = (int)$arr[1];
                    }
                };
                if ($ver == 'mediav') {
                    if (!isset($mv_result_arr[$ad_group_id][$area_fid][$area_id])) {
        
                        $mv_result_arr[$ad_group_id][$area_fid][$area_id] = array(
                            'area_id' => $area_id,
                            'area_fid' => $area_fid,
                            'ad_group_id' => $ad_group_id,
                            'ad_plan_id' => $ad_plan_id,
                            'ad_user_id' => $ad_user_id,
                            'costs' => $price,
                            'clicks' => 1,
                            'area_key' => $area_key,
                        );
                    }else {
        
                        $mv_result_arr[$ad_group_id][$area_fid][$area_id]['costs'] = round($mv_result_arr[$ad_group_id][$area_fid][$area_id]['costs']+$price,2);
                        $mv_result_arr[$ad_group_id][$area_fid][$area_id]['clicks'] = $mv_result_arr[$ad_group_id][$area_fid][$area_id]['clicks']+1;
                    }
                }else {
                    if (!isset($result_arr[$ad_group_id][$area_fid][$area_id])) {
        
                        $result_arr[$ad_group_id][$area_fid][$area_id] = array(
                            'area_id' => $area_id,
                            'area_fid' => $area_fid,
                            'ad_group_id' => $ad_group_id,
                            'ad_plan_id' => $ad_plan_id,
                            'ad_user_id' => $ad_user_id,
                            'costs' => $price,
                            'clicks' => 1,
                            'area_key' => $area_key,
                        );
                    }else {
        
                        $result_arr[$ad_group_id][$area_fid][$area_id]['costs'] = round($result_arr[$ad_group_id][$area_fid][$area_id]['costs']+$price,2);
                        $result_arr[$ad_group_id][$area_fid][$area_id]['clicks'] = $result_arr[$ad_group_id][$area_fid][$area_id]['clicks']+1;
                    }
                }
            }
        }
    }
}
