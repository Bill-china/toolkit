<?php
/**
 * Utility
 * 工具类
 * @package open 360
 * @version $Id: Utility.php 1873 2012-05-08 07:38:42Z wangguoqiang $
 * @copyright 2005-2011 360.CN All Rights Reserved.
 * @author gaoyuan@360.cn
 */
class Utility
{
    public static $logid='';//全局logid
    /**
     * 把全角空格替换为半角空格，回车换行替换为半角空格，并去掉字符串的首尾空白字符
     * @param string $str
     * @return string
     */
    public static function trim($str) {
        static $bolInitEncoding = true;
        if ($bolInitEncoding) {
            mb_regex_encoding('UTF-8');
            mb_internal_encoding('UTF-8');
            $bolInitEncoding = false;
        }
        $str = mb_ereg_replace('　', ' ', $str);
        $str = str_replace(array("\t", "\n", "\r"), ' ', $str);
        $str = trim($str);
        return $str;
    }

    /**
     * parseWhere
     * 解析where 为 yii update where
     * @param Array $where
     * @param string $operator
     * @return void
     */
    public static function parseWhere(Array $where, $operator = 'and')
    {
        $condition = array();
        foreach ($where as $key => $val)
        {
            if (strpos($key, '%') !== FALSE)
            {
                $key = substr($key, 0, -1);
                $condition[] = array('like', $key . ' = ' . $val);
                continue;
            }
            $condition[] = $key . ' = ' . $val;
        }
        array_unshift($condition, 'and');
        return $condition;
    }
    /**
     * quote
     * 转移字符串，用于sql查询
     * @param mixed $str
     * @return void
     */
    public static function quote($str)
    {
        return "'" . addcslashes($str, "\000\n\r\\'\"\032") . "'";
        //return "'" . addcslashes(str_replace("'", "''", $str), "\000\n\r\\\032") . "'";
    }
    /**
     * getAdRedisKey
     * 获取广告 redis的 key
     * @param mixed $app
     * @param mixed $userId
     * @param string $type
     * @return void
     */
    public static function getAdRedisKey($app, $userId, $type = 'model')
    {
        $key = Config::item('redisKey') . 'ad:' . $type . ':' . $app . ':' . $userId;
        return $key;
    }
    /**
     * 启动xhprof性能检查
     */
    public static function startProfile()
    {
        if (self::_is_load_model('xhprof'))
        {
            xhprof_enable();
        }
    }
    /**
     * 获取profile data
     */
    private static function _getProfileData()
    {
        if (self::_is_load_model('xhprof'))
        {
            $profile_data = xhprof_disable();
            return $profile_data;
        }
        echo "<br/>----@@-_-@@--------<br/>no xhprof module installed<br/>";
    }
    /**
     *
     * 展示profile信息
     * @param unknown_type $type
     */
    public static function renderProfile($type = 'open_360')
    {
        $xhprof_root     = __DIR__ . '/../../htdoc/tools/xhprof';
        $xhprof_root_url = 'http://' .$_SERVER['HTTP_HOST'] .
        	'/tools/xhprof/xhprof_html';
        include_once $xhprof_root . "/xhprof_lib/utils/xhprof_lib.php";
        include_once $xhprof_root . "/xhprof_lib/utils/xhprof_runs.php";
        $profile_data = self::_getProfileData();
        $profile_obj  = new XHProfRuns_Default();
        $run_id       = $profile_obj->save_run($profile_data, $type);
        echo "---------------\n".
        "you can view profile data run at :\n".
        "<a href='" . $xhprof_root_url . "/index.php?run=$run_id&source=$type" .
        	"'" . " target='_blank'>" . $xhprof_root_url .
        	"/index.php?run=$run_id&source=$type</a>\n".
        "---------------\n";
    }
    /**
     * 是否加载相关模块
     * @param string $model_name
     */
    private static function _is_load_model($model_name)
    {
        return get_extension_funcs($model_name);
    }
    /**
     * isDate
     * 判断是否日期
     * @param mixed $str
     * @return void
     */
    public static function isDate($str)
    {
        if (preg_match('/^[0-9]{2,4}-{0,1}[0-9]{1,2}-{0,1}[0-9]{1,2}$/', $str)) {
            return true;
        }
        return false;
        $stamp = strtotime($str);
        if (!is_numeric($stamp))
        {
            return false;
        }
        $month = date('m', $stamp);
        $day   = date('d', $stamp);
        $year  = date('Y', $stamp);
        if (checkdate($month, $day, $year))
        {
            return true;
        }
        return false;
    }

    /**
     * minuteInter
     * 每5分钟同步数据一次
     * 如果需要调整 需同时修改crontab的值
     *
     * @param mixed $minute
     * @return void
     */
    public static function minuteInter($minute)
    {
        return intval($minute/5);
    }

    public static function checkSign($params, $sign, $tokenKey)
    {
        $signStr = '';
        foreach($params as $key => $value) {
            $signStr .= "{$key}={$value}&";
        }
        $signStr .= 'key=' . $tokenKey;
        $signStr = md5($signStr);
        if ($sign == $signStr) {
            return true;
        }
        return false;
    }

    public static function makeSign($params, $tokenKey)
    {
        $signStr = '';
        foreach($params as $key => $value) {
            $signStr .= "{$key}={$value}&";
        }
        $signStr .= 'key=' . $tokenKey;

        return md5($signStr);
    }

    public function dateParse(&$request, $defaultDate = '-1 day')
    {
        $startDate = $endDate = date('Y-m-d', strtotime($defaultDate));
        if ($selectDay = $request->getParam('day_select')) {
            $sdate = $request->getParam('start_date');
            $edate = $request->getParam('end_date');
            if (!Utility::isDate($sdate) || !Utility::isDate($edate)) {
                $sdate = $edate = date('Y-m-d', strtotime("-1 day"));
            }
            if ($selectDay == 'self' && strtotime($sdate) <= strtotime($edate)) {
                $startDate = $sdate;
                $endDate = $edate;
            }
            else {
                $dict = array(
                    1 => '-1 day',
                    2 => '-2 day',
                    3 => '-1 month',
                    4 => '-3 month',
                    5 => '-6 month',
                    7 => '-7 day',
                    );
                $day = isset($dict[$selectDay])? $dict[$selectDay]:'-7 day'; //输入校验
                $startDate = date('Y-m-d', strtotime("{$day}"));
                if ($selectDay == 1 || $selectDay == 2) {
                    $endDate = $startDate;
                }
            }
        }

        if ($selectDay != 'self') {
            $selectDay = (int)$selectDay;
        }
        else {
            $selectDay = 'self';
        }

        return array(
            'startDate' => $startDate,
            'endDate' => $endDate,
            'daySelect' => $selectDay,
        );
    }

    public function urlXss($url)
    {
        if (preg_match('/\<\s*script|\<\s*iframe/i', $url)) {
            return true;
        }
        return false;
    }
    function checkUrl($weburl) {
        return preg_match("/^http:\/\/[A-Za-z0-9]+\.[A-Za-z0-9]+[\/=\?%\-&_~`@[\]\':+!]*([^<>\"])*$/", $weburl);
    }

    /**
     * 截取指定字节长度的字符串，非ASIIC字符算两个字节，如果最后是半个汉字则丢弃
     *
     * @param string $str 字符串
     * @param int $intLen 截取的长度，即字节数
     * @return string 返回的新字符串
     */
    public static function cutStrAsGBK($str, $intLen) {
    	if (strlen($str) <= $intLen) {
    		return $str;
    	}
    	$intTmp = (int) floor($intLen / 2);
    	if (mb_strlen($str, 'UTF-8') <= $intTmp) {
    		return $str;
    	}
    	$strGBK = mb_convert_encoding($str, "GBK", 'UTF-8');
    	$strTmp = mb_strcut($strGBK, 0, $intLen, 'GBK');
    	$strTmp = mb_convert_encoding($strTmp, 'UTF-8', 'GBK');

    	return $strTmp;
    }

    public static function strlenAsGBK($str) {
    	$strGBK = mb_convert_encoding($str, "GBK", 'UTF-8');
    	return strlen($strGBK);
    }

    /**
     * 将数据转换为csv行，和fputcsv类似，不过该函数以字符串形式返回结果
     * @param array|string $mixedData
     * @param string $strDelimiter
     * @param string $strEnclosure
     * @return string
     */
    public static function sputcsv($mixedData, $strDelimiter = ',', $strEnclosure = '"') {
    	if (! is_array($mixedData)) {
    		$mixedData = array(strval($mixedData));
    	}
    	foreach ($mixedData as &$v) {
    		if (strpos($v, $strDelimiter) !== false || strpos($v, $strEnclosure) !== false) {
    			$v = str_replace($strEnclosure, "{$strEnclosure}{$strEnclosure}", $v);
    			$v = "{$strEnclosure}{$v}{$strEnclosure}";
    		}
    	}
    	return implode($strDelimiter, $mixedData);
    }

    public static function cronLog($strMsg, $strLevel = CLogger::LEVEL_INFO) {
    	if (defined('YII_CMD') && YII_CMD) {
    		echo date('Y-m-d H:i:s') . "\t$strMsg\n";
    	} else {
    		Yii::log($strMsg, $strLevel);
    	}
    }

    /**
     * 函数调用的代理，如果调用时有异常，会自动重试$intMaxTry次，每次间隔$intTryInterval微妙，如果
     * 重试指定次数后仍然异常，则抛出原异常
     *
     * @param callable $mixedCallable The callable to be called
     * @param array $arrParam The parameters to be passed to the callback, as an indexed array
     * @param int $intMaxTry 最大重试次数
     * @param int $intTryInterval 每次重试的时间间隔
     * @return mixed
     */
    public static function autoTryCall($mixedCallable, $arrParam, $intMaxTry = 2, $intTryInterval = 100) {
    	if ($intMaxTry < 1) $intMaxTry = 1;
    	if (is_array($mixedCallable)) {
    		$strClassName = is_object($mixedCallable[0]) ? get_class($mixedCallable[0]) : strval($mixedCallable[0]);
    		$strCallMethod = "{$strClassName}::{$mixedCallable[1]}";
    	} else {
    		$strCallMethod = strval($mixedCallable);
    	}
    	for ($i = 1; $i <= $intMaxTry; $i++) {
    		try {
    			return call_user_func_array($mixedCallable, $arrParam);
    		} catch (Exception $e) {
    			Yii::log("{$strCallMethod} call failed({$i}) ".$e->getMessage(), CLogger::LEVEL_WARNING);
    		}
    		usleep($intTryInterval);
    	}
    	$strParam = serialize($arrParam);
    	if (strlen($strParam) > 1024) $strParam = substr($strParam, 0, 1024);
    	Yii::log("{$strCallMethod} call failed  ".$e->getMessage()." param {$strParam}", CLogger::LEVEL_ERROR);
    	throw $e;
    }


    public static function return_bytes($val) {
        $val = trim($val);
        $last = strtolower($val[strlen($val)-1]);
        switch($last) {
            // 自 PHP 5.1.0 起可以使用修饰符 'G'
            case 'g':
                $val *= 1024;
            case 'm':
                $val *= 1024;
            case 'k':
                $val *= 1024;
        }

        return $val;
    }

    public static function edcApiPost($url, $data)
    {
        if (! isset($data['token'])) {
            $data['token'] = sha1(Yii::app()->params['edcApi']['sys_name'].'_'.Yii::app()->controller->id.'_'.Yii::app()->controller->action->id.uniqid());
        }
        if (! isset($data['source'])) {
            $data['source'] = Yii::app()->params['edcApi']['sys_name'].'_'.Yii::app()->controller->id.'_'.Yii::app()->controller->action->id;
        }
        $res = Yii::app()->curl->run(
                Yii::app()->params['edcApi']['url'] . $url,
                false,
                self::_makeClientSign($data)
        );

        $res = json_decode($res, true);
        if (!isset($res['errno'])) $res['errno'] = 1;

        return $res;
    }

    //在客户端生成sign值
    protected static function _makeClientSign($params)
    {
        $edcConfig =  Yii::app()->params['edcApi'];
        $params['appkey'] = $edcConfig['appkey'];
        $params['ver'] = $edcConfig['ver'];
        $params['time_stamp'] = microtime(true);
        $res = array();
        foreach($params as $k=>$v){
            if(!in_array($k,array('sign'))){
                $res[trim($k)] = trim($v);
            }
        }
        $params['sign'] = md5(join('',$res) . $edcConfig['token']);
        return $params;
    }

    /**
     * 写log
     * @param string $content log内容
     * @param string $file_name log文件名字
     * @return boole
     * @author jingguangwen@360.cn 2013-03-29
     */
    public static function writeLog($content,$file_name) {
        if (empty($content) || empty($file_name)) {
            return false;
        }
        $log_dir = '/dev/shm/';
        //文件路径名
        $file = $log_dir.$file_name;
        //写内容
        ComAdLog::write($content,$file);
        return true;

    }

    /*
     * 取str长度，在strlen会把utf8的汉字当三个字符，该方法对所有汉字都算2个字符
     */
    public static function getStrLen($string){
        $pre = '{%';
        $end = '%}';
        $string = str_replace(array('&amp;', '&quot;', '&lt;', '&gt;'), array($pre.'&'.$end, $pre.'"'.$end, $pre.'<'.$end, $pre.'>'.$end), $string);
        $n = $tn = $noc = 0;
        while($n < strlen($string)) {
            $t = ord($string[$n]);
            if($t == 9 || $t == 10 || (32 <= $t && $t <= 126)) {
                $tn = 1; $n++; $noc++;
            } elseif(194 <= $t && $t <= 223) {
                $tn = 2; $n += 2; $noc += 2;
            } elseif(224 <= $t && $t <= 239) {
                $tn = 3; $n += 3; $noc += 2;
            } elseif(240 <= $t && $t <= 247) {
                $tn = 4; $n += 4; $noc += 2;
            } elseif(248 <= $t && $t <= 251) {
                $tn = 5; $n += 5; $noc += 2;
            } elseif($t == 252 || $t == 253) {
                $tn = 6; $n += 6; $noc += 2;
            } else {
                $n++;
            }
        }
        return $noc;
    }

    //查看某一任务执行的个数
    public static function getProcessNum($name)
    {
        $mypid  = getmypid();
        $syscmd = "ps -ef | grep -i '{$name}' | grep -v grep |grep -v vim |grep -v vi |grep -v defunct |grep -v '/bin/sh'| grep -v {$mypid} | wc -l";
        $cmd = @popen($syscmd, 'r');
        $num = @fread($cmd, 512);
        $num += 0;
        @pclose($cmd);
        return $num;
    }

    /**
     * 发送消息
     *
     * @params array    $data       数据
     * @params string   $func       之前函数名
     * @params string   $exchange   类型
     * @params string   $logid      日志id
     */
    private function sendMessage($data, $func, $exchange, $logid)
    {
        $mqData                 = array();
        $mqData                 = $data;
        $mqData['type']         = 'eGoods';
        $mqData['logid']        = $logid;
        $mqData['ip_address']   = isset($_SERVER['HTTP_CLIENTIP']) ? $_SERVER['HTTP_CLIENTIP'] : '';
        $mqData['opt_user_id']  = isset($_SERVER['HTTP_OPTUSERID']) ? $_SERVER['HTTP_OPTUSERID'] : 0;

        $emqResult  = CEmqPublisher::send(
            Yii::app()->params['exchange']["{$exchange}"],
            __CLASS__ . '_' . $func,
            json_encode($mqData),
            $logid,
            Yii::app()->params['emq']
        );

        return $emqResult;
    }

    /**
     * 根据$logid获取请求来源
     * @param string $logid
     */
    public static function mqMsgSource($logid='')
    {
        if (empty($logid)) return $logid;

        if (preg_match("/dianjing|openapi|cron|app|eapi|onekey/i", $logid, $matches)) {
            $source = strtolower($matches[0]);
        } else {
            $source = "";
        }

        return $source;
    }

    public static function apiPost($url, $data = array(), $token=null)
    {
        if (! isset($token)) {
            //$token = sha1(Yii::app()->params['edcApi']['sys_name'].'_'.Yii::app()->controller->id.'_'.Yii::app()->controller->action->id.uniqid());
           $token = sha1(Yii::app()->params['edcApi']['sys_name'].'_'.uniqid());

        }
        $data['logid'] = self::getLoggerID();
        if(strpos($url,"?")===false)
        {
            $url = $url ."?logid=".$data['logid'];
        }
        else
        {
            $url = $url ."&logid=".$data['logid'];
        }
        $headers = array(
                "CLIENTIP: " . Yii::app()->request->userHostAddress,
                'OPTUSERID: ' . array_key_exists('opt_user_id', $data) && isset($data['opt_user_id'])? $data['opt_user_id']:0,
            );
        if (isset($data['opt_user_id'])) {
            unset($data['opt_user_id']);
        }
        Yii::app()->curl->setHeaders($headers);
        $res = Yii::app()->curl->run(
            Yii::app()->params['eApi']['url'] . $url,
            false,
            $data,
            $token
        );

        $res = json_decode($res, true);
        return $res;
    }
    /**
     * logger id
     *
     * @return int
     */
    static function getLoggerID($moduleName='DIANJING')
    {
        if (self::$logid == '') {
            self::$logid = $moduleName.'_'.rand(1, 9) . microtime(true)*10000 . rand(100,999);
        }
        return self::$logid;
    }
        /**
     * 审核库日志
     * @param  [type] $class   类名
     * @param  [type] $method  方法名
     * @param  [type] $content 日志内容，若是array,则会被json_encode
     * @return [type]          null
     */
    public static function log($class,$method,$content)
    {
        if(is_array($content))
        {
            unset($content['logid']);
            $content=json_encode($content);
        }
        $t=explode(" ",microtime());
        Utility::writeLog(date('Y-m-d H:i:s').".".intval(1000*$t[0]). "\t" . self::getLoggerID('ESC') . "\tPID_" . getmypid() ."\t$class\t$method\t$content", 'esc.log');
    }

    /**
     * 发送警报邮件，同样内容，只发送一次。
     * @param string $className    类名
     * @param string $functionName 方法名
     * @param string $content      内容
     */
    public static function sendAlert($className = "", $functionName = "", $content = "",$sendSms=false)
    {
        if($className.$functionName. $content =="")
            return ;
        $md5=md5($className.$functionName.$content);
        self::log(__CLASS__,__FUNCTION__,array($className,$functionName,$content,$sendSms));
        $exists=false;
        try{
            $f="/tmp/mail_".$md5;
            if(file_exists($f))
            {
                if((time()-filemtime($f))<1*60)
                {
                    $exists=true;
                }
                else
                {
                    unlink($f);
                }
            }
            else
            {
                touch($f);
            }
        }catch(Exception $e){
            $exists=false;
        }
        if($exists===true)
            return;

        $ip=`/sbin/ifconfig  | /bin/grep 'inet addr:'| /bin/grep -v '127.0.0.1' | /usr/bin/cut -d: -f2 | /usr/bin/awk 'NR==1 { print $1}'`;
        $table = "<table border=1><tbody><tr><td><b> 统计系统报警邮件  from ip:" .($ip) . " logid:".self::getLoggerID('ESC')." pid:".getmypid()."</b></td></tr>\n";
        $table .= "<tr><td>【{$className}::{$functionName}】</td></tr>\n";
        $table .= "<tr><td>" . str_replace("\n", "<br/>", $content) . "</td></tr>\n";
        $table .= "</tbody></table>\n";
        try {
            $title="统计系统报警邮件" . '-' . date('Y-m-d H:i:s');
            if($sendSms)
            {
                $ch = curl_init("http://10.108.68.121:888/notice/notice.php?s=&c=".urlencode("$className::$functionName ".substr($content,0,100))."&g=e_esc_monitor_smsonly");
                curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
                curl_exec($ch);
                curl_close($ch);
            }
            $ch = curl_init("http://10.108.68.121:888/notice/notice.php?s=".urlencode($title)."&c=".urlencode($table)."&g=e_esc_monitor_mailonly");
            curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
            curl_exec($ch);
            curl_close($ch);

            $config = new CConfiguration(Yii::getPathOfAlias('application.config') . '/redis.php');
            $servers = $config->itemAt('esc_monitor_redis');
            if($servers)
            {
                $alarm_content=array(
                'level'=>$sendSms?3:2,
                'content'=>"php错误：".substr($content,0,1000),
                'time'=>time(),
                );
                $redis = new ComRedis('esc_monitor_redis', 0);
                $redis->rpush("alarm-list",json_encode($alarm_content));
            }
        } catch (Exception $e) {
            /*发邮件异常*/
        }
    }
    /**
     * 邮件发送
     * @param  [string] $subject [标题]
     * @param  [string] $body    [邮件正文]
     * @param  [string] $from    [发送人]
     * @param  [array]  $toList  [收件人]
     * @param  array  $ccList  [抄送人]
     * @return [string]          [返回信息]
     */
    public static function sendMail ($subject, $body, $from, $toList, $ccList=array()) {
        $data = array();
        $mail = array(
            'batch'     => true,
            'subject'   => $subject,
            'from'      => $from,
            'to'        => $toList,
            'cc'        => $ccList,
            'body'      => $body,
            'format'    => 'html',
        );
        $data['mail'] = json_encode($mail);

        $url = 'http://qms.addops.soft.360.cn:8360/interface/deliver.php';
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL,               $url);
        curl_setopt($ch, CURLOPT_POST,              1);
        curl_setopt($ch, CURLOPT_POSTFIELDS,        $data);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER,    1);
        $response = curl_exec($ch);
        if (curl_errno($ch)) {
            printf("%s curl fail, err [%s]\n", curl_error($ch));
            return false;
        }
        curl_close($ch);
        return $response;
    }

    public static function sendEmail($from, $to, $subject, $content){
        $headers = "MIME-Version: 1.0" . "\r\n";
        $subject = "=?UTF-8?B?".base64_encode($subject)."?=";
        $headers .= "Content-type:text/html;charset=utf-8" . "\r\n";
        $headers .= "From:{$from}\r\n";
        $headers .= "Sender:{$from}\r\n";
        if (is_array($to)){
            $to = implode(',', $to);
        }
        if (mail($to, $subject, $content, $headers)){
            return true;
        }
        return false;
    }

    public static function cheatApiPost($postData = array())
    {
        $reqId = md5('cheat'.uniqid());

        $data = array(
                'request_id'=>$reqId,
                'app_key'=>'cheat',
                'data'=>$postData,
                'time_stamp'=>time(),
                );
        $data['sign'] = self::makeCheatSign($data,Yii::app()->params['cheatKey']);
        $res = Yii::app()->curl->post(
                Yii::app()->params['cheatApi']['url'],
                $data
                );
        return json_decode($res, true);
    }

    /**
     * 产生一个sign值
     * data token
     */
    public static function makeCheatSign($params=array(),$token){
        $arg = '';
        if(empty($params) || !is_array($params) || empty($token)) return false;
        $res = array();
        foreach($params as $k=>$v){
            if (!is_array($v)) {
                $res[trim($k)] = trim($v);
            }
        }
        if (isset ($res['sign'])) {
            unset($res['sign']);
        }
        ksort($res);
        reset($res);

        while (list ($key, $val) = each ($res)) {
            $arg.=$key."=".$val."&";
        }
        $prestr = substr($arg,0,count($arg)-2);
        return md5($prestr.$token);
    }

    public static function microtime_float()
    {
        list($usec, $sec) = explode(" ", microtime());
        return ((float)$usec + (float)$sec);
    }

    /**
     * 获取产品线
     * @param  [type]  $data     [description]
     * @param  integer $msg_type [1反作弊；2点击与展现]
     * @return [array]            [product_line：1搜索；2shouzhu；3如意；4mediav;0异常]
     */
    public static function getProductLineInfo($data,$msg_type=2){
        if(1 == $msg_type){
            $channel_id = $data['cid'];
            $place_id = $data['pid'];
        }elseif(2 == $msg_type){
            $channel_id = $data['channel_id'];
            $place_id = $data['place_id'];
        }
        $db_map_info = array(

        );
        //搜索db映射
        if($data['ver'] == 'sou' and ($channel_id != 29  or $place_id != 238)){
            $db_map_info = array(
                'product_quota'=>'sou_quota',
                'product_yesterday_quota'=>'yesterday_sou_quota',
                'product_cost'=>'sou_cost',
                'product_yesterday_cost'=>'yesterday_sou_cost',
                'product_line' => 1
            );
            return $db_map_info;
        }
        //如意
        if($data['ver'] == 'sou' and $channel_id == 29  and $place_id == 238){
            $db_map_info = array(
                'product_quota'=>'ruyi_quota',
                'product_yesterday_quota'=>'yesterday_ruyi_quota',
                'product_cost'=>'ruyi_cost',
                'product_yesterday_cost'=>'yesterday_ruyi_cost',
                'product_line' => 3
            );
            return $db_map_info;
        }
        //布尔
        if($data['ver'] == 'shouzhu'){
            $db_map_info = array(
                'product_quota'=>'app_quota',
                'product_yesterday_quota'=>'yesterday_app_quota',
                'product_cost'=>'app_cost',
                'product_yesterday_cost'=>'yesterday_app_cost',
                'product_line' => 2
            );
            return $db_map_info;
        }
        //展示网络
        if($data['ver'] == 'mediav'){
            $db_map_info = array(
                'product_quota'=>'mv_quota',
                'product_yesterday_quota'=>'yesterday_mv_quota',
                'product_cost'=>'mv_cost',
                'product_yesterday_cost'=>'yesterday_mv_cost',
                'product_line' => 4
            );
            return $db_map_info;
        }
        return $db_map_info;
    }
}
