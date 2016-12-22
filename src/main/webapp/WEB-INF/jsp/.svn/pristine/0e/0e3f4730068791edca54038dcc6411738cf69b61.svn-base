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
class EdcStatsArea
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
        return 'ad_stats_area';
    }

    public function insertRow($tableName, $data) {
        if ($this->getDbConnection()->createCommand()->insert($tableName, $data)) {
            return $this->getDbConnection()->createCommand('SELECT LAST_INSERT_ID()')->queryScalar();
        }
        return false;
    }

    public function getByAreaKey($tableName, $areaKey, $groupId, $type) {
        $sql = "select id from " . $tableName . " where ad_group_id=:groupId and area_key=:areaKey and type=:type";
        $cmd = $this->getDbConnection()->createCommand($sql);
        $cmd->bindParam(':groupId', $groupId);
        $cmd->bindParam(':areaKey', $areaKey, PDO::PARAM_STR);
        $cmd->bindParam(':type', $type);
        return $cmd->queryScalar();
    }



    public function updateIncr($data) {
        $tableName = $this->tableName() . "_click_" . date('Ymd', strtotime($data['create_date']));
        $sql = "INSERT INTO {$tableName} (`area_id`, `area_fid`, `ad_group_id`, `ad_plan_id`, `ad_user_id`, `clicks`,"
            . " `views`, `costs`, `trans`, `create_date`, `area_key`, `create_time`, `update_time`, `type`) values("
            . " {$data['area_id']}, {$data['area_fid']}, {$data['ad_group_id']}, {$data['ad_plan_id']}, {$data['ad_user_id']},"
            . " {$data['clicks']}, {$data['views']}, {$data['costs']}, {$data['trans']}, '{$data['create_date']}', "
            . " '{$data['area_key']}', '{$data['create_time']}', '{$data['update_time']}', '{$data['type']}') ON DUPLICATE KEY"
            . " UPDATE `clicks`=`clicks`+{$data['clicks']}, `views`=`views`+{$data['views']}, `costs`=`costs`+{$data['costs']}";
        return $this->getDbConnection()->createCommand($sql)->execute();
    }

    /*public function updateIncr($data)
    {
        $tableName = $this->tableName() . "_click_" . date('Ymd', strtotime($data['create_date']));
        if ($id = $this->getByAreaKey($tableName, $data['area_key'], $data['ad_group_id'], $data['type'])) {
            $sql = "update " . $tableName . " set clicks=clicks+" . (int)$data['clicks']
                . " ,views=views+" . (int)$data['views'] . ",costs=costs+" . $data['costs']
                . ", update_time='" . date('Y-m-d H:i:s') . "'";
            $sql .= " where id=" . $id . " limit 1";

            return $this->getDbConnection()->createCommand($sql)->execute();
        } else {
            return $this->insertRow($tableName, $data);
        }
    }*/

    public function getByDateAndAreaKey($ad_group_id,$area_key,$create_date)
    {
        if (empty($ad_group_id) || empty($create_date)) {
            return false;
        }
        $tableName = $this->tableName() . "_click_" . date('Ymd', strtotime($create_date));
        $sql = "select * from $tableName where ad_group_id=" . (int)$ad_group_id
             . " and area_key=:areaKey and clicks>0";
        $cmd = $this->getDbConnection()->createCommand($sql);
        $cmd->bindParam(':areaKey', $area_key, PDO::PARAM_STR);
        return $cmd->queryRow();
    }

    public function cheatClickRefund($id, $price, $create_date)
    {
        $tableName = $this->tableName() . "_click_" . date('Ymd', strtotime($create_date));
        $sql = "update " . $tableName . " set clicks=clicks-1, costs=costs-:price "
            . " where id=:id";
        $cmd = $this->getDbConnection()->createCommand($sql);
        $cmd->bindParam(':id', $id);
        $cmd->bindParam(':price', $price);

        return $cmd->execute();
    }

	public function fixDataUpdate($id, $clicks, $costs, $tableName)
	{
		$sql = "update $tableName set clicks= " . (int)$clicks . ", costs=" . (float)$costs . " where id = $id limit 1";
		echo $sql . "\n";
		return ;
	}

	public function fixDataInsert($key, $clicks, $costs, $tableName)
	{
		$keyArr = explode('_', $key);
		$sql = "update $tableName set clicks= " . (int)$clicks . ", costs=" . (float)$costs
		   	. " where ad_group_id=" . $keyArr[0] . ", area_key='" . addslashes($keyArr[1]) . "' limit 1";
		echo $sql . "\n";
		return ;
	}
	/**
	 * 根据click_log数据信息解析地域统计信息
	 * @param array $user_ids_arr
	 * @param array $result_arr
	 * @author jingguangwen@360.cn  2914-09-11
	 */
	public static function getClikResult($click_log_arrs, &$result_arr, &$app_result_arr, &$app_statistic_arr,$type="area") {

		if (!empty($click_log_arrs)) {

			foreach ($click_log_arrs as $click_arr) {
				//$area_key = $click_arr['area_key'];
                $area_key = $click_arr['area_fid'].','.$click_arr['area_id'];
				$ad_group_id = intval($click_arr['ad_group_id']);
				$ad_plan_id = $click_arr['ad_plan_id'];
				$ad_user_id = $click_arr['ad_user_id'];
				$price = round($click_arr['price']-$click_arr['reduce_price'],2);
				$ver = $click_arr['ver'];
				$app_id = 0;
                $source_type = intval($click_arr['source_type']);

                //jingguangwen  2015-03-18 add
                $ad_channel_id = intval($click_arr['cid']);
                $ad_place_id = intval($click_arr['pid']);
                $req_src = $click_arr['src'];

                if(empty($req_src)){
                    $req_src = "";
                }

                //布尔二期新加
                $location_id = intval($click_arr['app_cid']);
                $ad_advert_id= intval($click_arr['ad_advert_id']);
                $click_type= intval($click_arr['type']);//sou表示area类型：1物理定位，2搜索意图

                $source_system = intval($click_arr['source_system']);

                if($source_system == 5){
                    $source_system = 3;
                }else if($source_system == 6 || $source_system == 7){
                    $source_system = 2;
                } else {
                    $source_system = 1;
                }



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
                if ($type == 'area') {
                    $plat_type = 1;
                    if($click_arr['cid'] == 29 && $click_arr['pid'] == 238) {
                        $plat_type = 3;
                    }

                    $result_arr_key= $ad_group_id."|".$area_fid."|".$area_id."|".$source_type."|".$click_type ."|".$plat_type;

    				if (!isset($result_arr[$result_arr_key])) {

    					$result_arr[$result_arr_key] = array(
    						'area_id' => $area_id,
    						'area_fid' => $area_fid,
    						'ad_group_id' => $ad_group_id,
    						'ad_plan_id' => $ad_plan_id,
    						'ad_user_id' => $ad_user_id,
                            'area_type' => $click_type,
                            'plat_type'=>$plat_type,
    						'costs' => $price,
    						'clicks' => 1,
    						'area_key' => $area_key,
                            'source_type' => $source_type,
    					);
    				}else {

    					$result_arr[$result_arr_key]['costs'] = round($result_arr[$result_arr_key]['costs']+$price,2);
    					$result_arr[$result_arr_key]['clicks'] = $result_arr[$result_arr_key]['clicks']+1;
    				}

                } else if($type == 'shouzhu'){

                    $app_result_arr_key= $ad_group_id."|".$ad_channel_id."|".$area_fid."|".$area_id."|".$location_id."|".$ad_place_id."|".$ad_plan_id."|".$ad_advert_id."|".$click_type;
    				if (!isset($app_result_arr[$app_result_arr_key])) {

    					$app_result_arr[$app_result_arr_key] = array(
							'area_id' => $area_id,
							'area_fid' => $area_fid,
							'ad_group_id' => $ad_group_id,
                            'ad_channel_id' => $ad_channel_id,
							'ad_plan_id' => $ad_plan_id,
							'ad_user_id' => $ad_user_id,
							'costs' => $price,
							'clicks' => 1,
							'area_key' => $area_key,
    					    'app_id' => $app_id,
                            'location_id' => $location_id,
                            'place_id' => $ad_place_id,

                            'click_type' => $click_type,
                            'ad_advert_id' => $ad_advert_id,
                            'source_system' => $source_system,
    					);
    				}else {

    					$app_result_arr[$app_result_arr_key]['costs'] = round($app_result_arr[$app_result_arr_key]['costs']+$price,2);
    					$app_result_arr[$app_result_arr_key]['clicks'] = $app_result_arr[$app_result_arr_key]['clicks']+1;
    				}
                    //APP_ADSTATISTIC jingguangwen 2015-03-18 add
                    //
                    $app_statistic_arr_key= $ad_plan_id."|".$ad_channel_id."|".$ad_place_id."|".$req_src;

                    if (!isset($app_statistic_arr[$app_statistic_arr_key])) {

                        $app_statistic_arr[$app_statistic_arr_key] = array(

                            'ad_plan_id' => $ad_plan_id,
                            'ad_user_id' => $ad_user_id,
                            'ad_channel_id' => $ad_channel_id,
                            'ad_place_id' => $ad_place_id,
                            'req_src' => $req_src,
                            'costs' => $price,
                            'clicks' => 1,
                        );
                    }else {

                        $app_statistic_arr[$app_statistic_arr_key]['costs'] = round($app_statistic_arr[$app_statistic_arr_key]['costs']+$price,2);
                        $app_statistic_arr[$app_statistic_arr_key]['clicks'] = $app_statistic_arr[$app_statistic_arr_key]['clicks']+1;
                    }
				}

			}

        }
      }

}
