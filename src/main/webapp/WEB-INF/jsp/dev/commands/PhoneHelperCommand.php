<?php
/**
 * 统计手机助手投放的一些信息，自动发送邮件
 * 短期需求
 */
class PhoneHelperCommand extends CConsoleCommand {

    protected $_task_name = '';

    protected $_db_ad_material = null;

    const PHONE_HELPER_MOBILE_CHANNEL = 33; // 手机助手移动端

    const PH_FREE_ZONE_PLACE        = 163; // 手助免流量专区
    const PH_MOBLE_SEARCH_PLACE     = 164; // 手助移动搜索
    const PH_FREE_WIFI_PLACE        = 269; // 免费WIFI免流量专区
    const PH_FREE_BROWSER_PLACE     = 270; // 手机浏览器免流量专区

    protected $_place_map = array(
        self::PH_FREE_ZONE_PLACE        => '手助免流量专区',
        self::PH_MOBLE_SEARCH_PLACE     => '手助移动搜索',
        self::PH_FREE_WIFI_PLACE        => '免费WIFI免流量专区',
        self::PH_FREE_BROWSER_PLACE     => '手机浏览器免流量专区',
    );

    /**
     * 从ad_stats表及物料库取出数据，然后整合再发送邮件。
     */
    public function actionStatic($date) {
        $beginTime = time();
        $this->_task_name = $task_name = sprintf("[PnoeHelper_Static %s]", date('Ymd_His', $beginTime));
        printf("%s begin at %s\n", $task_name, date('Y-m-d H:i:s', $beginTime));

        $this->_db_ad_material = DbConnectionManager::getDB('ad_material');
        if (!$this->_db_ad_material) {
            printf("%s get _db_ad_material fail\n", $task_name);
            return ;
        }
        $arrAppList = $this->_getAppList();
        // var_dump($arrAppList);

        $arrOutput = array();
        for ($i=1; $i<=4; ++$i) {
            $sql = sprintf(
                "select clicks, total_cost, ad_advert_id, ad_place_id from esc_stats_click_%s where ad_channel_id=%d and ad_place_id in (%d, %d, %d, %d)",
                $date,
                self::PHONE_HELPER_MOBILE_CHANNEL,
                self::PH_FREE_ZONE_PLACE,
                self::PH_MOBLE_SEARCH_PLACE,
                self::PH_FREE_WIFI_PLACE,
                self::PH_FREE_BROWSER_PLACE
            );
            // echo $sql."\n";
            $db = DbConnectionManager::getStatBranchDB($i);
            $cmd = $db->createCommand($sql);
            $tmpRes = $cmd->queryAll();
            // var_dump($tmpRes);
            if (empty($tmpRes)) {
                continue;
            }
            foreach ($tmpRes as $_one_item) {
                $advertID = $_one_item['ad_advert_id'];
                if (!isset($arrAppList[$advertID])) {
                    printf("%s err! can not get app info of advert[%d]\n", $task_name, $advertID);
                    continue;
                }
                $softID = $arrAppList[$advertID]['id'];
                if (!isset($arrOutput[$softID])) {
                    $arrOutput[$softID] = array();
                }

                $placeID = $_one_item['ad_place_id'];
                if (!isset($arrOutput[$softID][$placeID])) {
                    $arrOutput[$softID][$placeID] = array(
                        'name'          => $arrAppList[$advertID]['name'],
                        'download'      => $_one_item['clicks'],
                        'total_cost'    => $_one_item['total_cost'],
                    );
                } else {
                    $arrOutput[$softID][$placeID]['download'] += $_one_item['clicks'];
                    $arrOutput[$softID][$placeID]['total_cost'] = number_format($arrOutput[$softID][$placeID]['total_cost'] + $_one_item['total_cost'], 2, '.', '');
                }
            }
        }
        // var_dump($arrOutput);

        // 转换成html的body
        $table = '<table border="1">'."\n";
        $table .= "<tr>\n";
        $table .= "<th>时间</th><th>应用</th><th>渠道</th> <th>下载量(次)</th> <th>收入(元)</th>\n";
        $table .= "</tr>\n";

        $totalCost = $totalDownload = 0;
        foreach ($arrOutput as $softID => $softInfo) {
            foreach ($softInfo as $placeID => $placeInfo) {
                $table .= sprintf("<tr>\n");
                $table .= sprintf(
                    "<td>%s</td><td>%s</td><td>%s</td><td>%d</td><td>%.2f</td>\n",
                    $date, $placeInfo['name'], $this->_place_map[$placeID], $placeInfo['download'], $placeInfo['total_cost']
                );
                $table .= sprintf("</tr>\n");
                $totalCost = number_format($totalCost+$placeInfo['total_cost'], 2, '.', '');
                $totalDownload += $placeInfo['download'];
            }
        }
        $table .= "</table>\n";
        $header = sprintf("有消费应用数：%d，总下载次数：%d，总收入：%.2f 元。<p>\n", count($arrOutput), $totalDownload, $totalCost);

        $body = $header.$table;
        printf("%s body start \n %s\n%s body end\n", $task_name, $body, $task_name);
        $response = $this->_send_mail('手助免流量渠道查询结果_'.$date, $body);
        printf("%s send email, res[%s]\n", $task_name, $response);

        $endTime = time();
        printf(
            "%s begin at %s, end at %s\n",
            $task_name, date('Y-m-d H:i:s', $beginTime), date('Y-m-d H:i:s', $beginTime)
        );
    }

    protected function _send_mail ($subject, $body) {
        $data = array();
        $toList = array(
            'wangdizhi@360.cn',         // 王迪志
            'pengchengyuan@360.cn',     // 彭程远
            'chenhande@360.cn',         // 陈汉德
            'duxiaoli@360.cn',       // 杜晓丽
            'luoyuan@360.cn',       // 宫英慧
            'yangchunhuan@360.cn',
            'jingguangwen@360.cn',
            'renyajun@360.cn',
        );
        /////////////////////////////
        $from="jingguangwen@alarm.360.cn";
        $response = Utility::sendEmail($from, $toList, $subject, $body);
        return $response;

        ////////////////////////////
        // $ccList = array(
        //     'jingguangwen@360.cn',
        //     'renyajun@360.cn',
        // );
        //
        // $mail = array(
        //     'batch'     => true,
        //     'subject'   => $subject,
        //     'from'      => 'noreply@ucmail.360.cn',
        //     'to'        => $toList,
        //     'cc'        => $ccList,
        //     'body'      => $body,
        //     'format'    => 'html',
        // );
        // $data['mail'] = json_encode($mail);

        // $url = 'http://qms.addops.soft.360.cn:8360/interface/deliver.php';
        // $ch = curl_init();
        // curl_setopt($ch, CURLOPT_URL,               $url);
        // curl_setopt($ch, CURLOPT_POST,              1);
        // curl_setopt($ch, CURLOPT_POSTFIELDS,        $data);
        // curl_setopt($ch, CURLOPT_RETURNTRANSFER,    1);
        // $response = curl_exec($ch);
        // if (curl_errno($ch)) {
        //     printf("%s curl fail, err [%s]\n", curl_error($ch));
        //     return false;
        // }
        // curl_close($ch);
        // return $response;
    }

    protected function _getAppList () {
        $sql = 'select r.advert_id as aid, a.soft_id as soft_id, a.soft_name as name from  ad_ext_relation r inner join ad_ext_assist a on r.ext_id=a.id';
        $cmd = $this->_db_ad_material->createCommand($sql);
        $res = $cmd->queryAll();
        if (!is_array($res) || empty($res)) {
            return false;
        }

        $arrRet = array();
        foreach ($res as $_one_item) {
            $arrRet[$_one_item['aid']] = array(
                'id'   => $_one_item['soft_id'],
                'name' => $_one_item['name'],
            );
        }
        return $arrRet;
    }

    public function actionGetUserOverCost() {

        $beginTime = time();
        $task_name = sprintf("[PnoeHelper_GetUserOverCost %s]", date('Ymd_His', $beginTime));
        printf("%s begin at %s\n", $task_name, date('Y-m-d H:i:s', $beginTime));
        $response = 0;
        $time = time();
        $time_cj = 86400;
        $out_arr = array();
        for ($i=1; $i < 11; $i++) {
            $time_use = $time- $i*$time_cj;
            $date = date('Ymd',$time_use);
            $table_name = "click_detail_".$date;

            $invalid_sql  =  "SELECT  create_date,count(1) as  clicks,sum(reduce_price) as costs  from {$table_name}   where cheat_type  not  in  (2,3) and   status not in (-1,2)  and    extension  in (1,2,3) and  ver!='mediav'   ";
            $valid_sql =     "SELECT  create_date,count(1) as  clicks,sum(price-reduce_price) as  costs from {$table_name}    where cheat_type  not  in  (2,3) and  price!=reduce_price  and status not in (-1,2)  and  ver != 'mediav'   ";

            $invalid_arr = Yii::app()->db_click_log->createCommand($invalid_sql)->queryRow();
            $valid_arr = Yii::app()->db_click_log->createCommand($valid_sql)->queryRow();

            $invalid_clicks  = $invalid_costs = $valid_clicks = $valid_costs = 0;
            $create_date = $date;
            if(!empty($invalid_arr)){
                $invalid_clicks = $invalid_arr['clicks'];
                $invalid_costs = $invalid_arr['costs'];
                $create_date = $invalid_arr['create_date'];
            }
            if(!empty($valid_arr)){
                $valid_clicks = $valid_arr['clicks'];
                $valid_costs = $valid_arr['costs'];
                $create_date = $valid_arr['create_date'];
            }
            $out_arr[] = array(
                'invalid_clicks'=> $invalid_clicks,
                'invalid_costs'=> $invalid_costs,
                'valid_clicks'=> $valid_clicks,
                'valid_costs'=> $valid_costs,
                'create_date'=> $create_date,
            );
        }


        if(!empty($out_arr)){

            $table = '<table border="1">'."\n";
            $table .= "<tr>\n";
            $table .= "<th>日期</th><th>收入</th><th>点击数</th> <th>超投金额</th> <th>超投点击数</th>\n";
            $table .= "</tr>\n";

            foreach ($out_arr as $arr) {
                $table .= sprintf("<tr>\n");
                $table .= sprintf(
                    "<td>%s</td><td>%.2f</td><td>%d</td><td>%.2f</td><td>%d</td>\n",
                    $arr['create_date'], $arr['valid_costs'], $arr['valid_clicks'], $arr['invalid_costs'], $arr['invalid_clicks']
                );
                $table .= sprintf("</tr>\n");
            }
            $table .= "</table>\n";
            $header = sprintf("最近十天的数据统计(不包含mediav的数据)<p>\n");

            $body = $header.$table;
            $response = $this->_send_over_cost_mail('超投金额统计', $body);
        }

        printf("%s send email, res[%s]\n", $task_name, $response);
        $endTime = time();
        printf(
            "%s begin at %s, end at %s\n",
            $task_name, date('Y-m-d H:i:s', $beginTime), date('Y-m-d H:i:s', $endTime)
        );

    }
    protected function _send_over_cost_mail ($subject, $body) {
         $data = array();
         $toList = array(
             'wangyunlong@360.cn',         // 王迪志
             'lintucheng@360.cn',     // 彭程远
             'xiangbibo@360.cn',         // 陈汉德
             'wangguoying@360.cn',       // 杜晓丽
             'liusisi@360.cn',       // 宫英慧
             'yejing@360.cn',
             'dengyu@360.cn',
             'wuhongling@360.cn',
             'kongxiangling@360.cn',
             'renyajun@360.cn',
             'hanzhongteng@360.cn',
             'jingguangwen@360.cn',
	         'hanmingyang@360.cn',
             'hoululu-pd@360.cn',
             'houtianrui@360.cn',
             'liyankun@360.cn',
         );

         $from="jingguangwen@alarm.360.cn";
         $response = Utility::sendEmail($from, $toList, $subject, $body);
         return $response;

        /*$toList = array(
            'wangyunlong@360.cn',         // 王迪志
            'renyajun@360.cn',
            'sangbaisong@360.cn',
            'jingguangwen@360.cn',
            'wangguoying@360.cn',
        );*/

        // $ccList = array(
        //     'jingguangwen@360.cn',
        //     'renyajun@360.cn',
        // );
        // $mail = array(
        //     'batch'     => true,
        //     'subject'   => $subject,
        //     'from'      => 'jingguangwen@alarm.360.cn',
        //     'to'        => $toList,
        //     //'cc'        => $ccList,
        //     'body'      => $body,
        //     'format'    => 'html',
        // );
        // $data['mail'] = json_encode($mail);

        // $url = 'http://qms.addops.soft.360.cn:8360/interface/deliver.php';
        // $ch = curl_init();
        // curl_setopt($ch, CURLOPT_URL,               $url);
        // curl_setopt($ch, CURLOPT_POST,              1);
        // curl_setopt($ch, CURLOPT_POSTFIELDS,        $data);
        // curl_setopt($ch, CURLOPT_RETURNTRANSFER,    1);
        // $response = curl_exec($ch);
        // if (curl_errno($ch)) {
        //     printf("%s curl fail, err [%s]\n", curl_error($ch));
        //     return false;
        // }
        // curl_close($ch);
        // return $response;
    }
}
