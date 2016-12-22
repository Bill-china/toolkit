<?php
/*
  异步消息发送类，mqproxy版本 wangguoqiang
  项目升级步骤：
    1. emq配置增加proxy_url app_key
            // emq 配置
        'emq'   => array(
            'host'  => '10.16.15.218',
            'port'  => '5672',
            'user'  => 'pyh',
            'pass'  => 'pyh1234',
            'vhost' => '/pyhtest',
            'proxy_url' => '',
            'app_key' => 'esc',
        ),
 */
class CEmqPublisher {
    public static $connection;
    public static $exchange;
    public static $channel;
    public static $mqConf;

    public static function init($conf) {
        if (!isset($conf['proxy_url']) || !isset($conf['app_key'])) {
            throw new Exception('emq proxy url or app key is not setting');
        }
        self::$mqConf['proxy_url'] = $conf['proxy_url'];
        self::$mqConf['app_key'] = $conf['app_key'];
    }

    public static function send($exchange, $source, $content, $logid, $conf) {
        self::init($conf);
        return self::sendMsg($exchange, $source, $content, $logid);
    }

    public static function sendToSearch($exchange, $data, $conf) {
        self::init($conf);
        if (self::$mqConf == null) {
            throw new Exception("please call [initConf] first\n", 1);
        }

        $jsonData = $data;
        $logData = array(
            'type' => 'log',
            'data' => $jsonData,
        );
        ComAdLog::combineLog(array(date('YmdHis'), 'rabbitmq', json_encode($logData)));

        return self::_publishMsg($exchange, $jsonData, null);
    }

    public static function sendMsg($exchange, $source, $content, $logid) {
        if (self::$mqConf == null) {
            throw new Exception("please call [initConf] first\n", 1);
        }
        $startRunTime = microtime(true);
        $mid = md5($content . microtime(true) . $exchange . $logid . mt_rand());
        $data = array(
            "app_key" => self::$mqConf['app_key'],
            'source' => $source,
            "mid" => $mid,//兼容老的格式 2015-01-08 add
            'msg_src' => $source,//兼容老的格式 2015-01-08 add
            'time' => time(),
            'logid' => $logid,
            'exchange' => $exchange,
            'content' => json_decode($content, true),
        );
        $jsonData = json_encode($data);
        $res = self::_publishMsg($exchange, $jsonData, $mid);
        $logData = array(
            'type' => 'log',
            'data' => $jsonData,
            'res'  => $res,
            'exec_time' => number_format(microtime(true) - $startRunTime, 5)
        );
        ComAdLog::combineLog(array(date('YmdHis'), 'rabbitmq', json_encode($logData)));
        return $mid;
    }

    private static function _publishMsg($exchange, $jsonData, $mid) {
        $retryTimes = 0;

        $queueParms = array(
            'appkey'     => self::$mqConf['app_key'],
            'exchange'   => $exchange,
            'type'       => 'fanout',
            'routingkey' => '',
            'content'    => $jsonData,
            'format'     => 'json',
            'retry'      => $retryTimes,
        );

        while ($retryTimes < 5) { //重试5次
            $retryTimes++;

            $result = Yii::app()->curl->post(self::$mqConf['proxy_url'], $queueParms);
            $result = json_decode($result, true);
            if (!empty($result)) {
                if ($result['Errno'] == 0) {
                    break;
                } else if ($result['Errno'] == 1) {
                    throw new Exception($result['Msg']);
                } else if ($result['Errno'] == 3) {
                    usleep(1000 * 200); //0.2s
                }
            }
            if ($retryTimes == 4) { //重启5次
                ComAdLog::write(array(
                    date("YmdHis"),
                    json_encode($queueParms)), "mqerror.log");
                self::sendAlarmMail('MQ消息发送错误', "exchange:{$exchange}, mid:{$mid}\ndata：" . $jsonData . "\n");
                return empty($result['Errno'])? -1:$result['Errno'];
            }
            usleep(50000);
        }

        return 0;
    }

    private static function output($msg) {
        $sapi_type = php_sapi_name();
        if (substr($sapi_type, 0, 3) == 'cli') {
            echo $msg;
        }
    }

    /**
     * 报警邮件
     *
     * @param  string $title 标题
     * @param  string $title 内容
     *
     * @return bool
     */
    private static function sendAlarmMail($title, $table)
    {
        if ($title == '' || $table == '') {
            return false;
        }

        $mailApi        = 'http://10.108.68.121:888/notice/notice.php';

        $ip = `/sbin/ifconfig  | grep 'inet addr:'| grep -v '127.0.0.1' | cut -d: -f2 | awk 'NR==1 { print $1}'`;
        $data   = array(
            's' => $title."_".$ip,
            'c' => $table,
            'g' => 'e_emq_monitor_smsonly'
        );
        Yii::app()->curl->run($mailApi, false, $data);
        return true;
    }
}

