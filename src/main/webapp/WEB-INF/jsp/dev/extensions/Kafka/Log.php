<?php
/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4 foldmethod=marker: */
// +---------------------------------------------------------------------------
// | SWAN [ $_SWANBR_SLOGAN_$ ]
// +---------------------------------------------------------------------------
// | Copyright $_SWANBR_COPYRIGHT_$
// +---------------------------------------------------------------------------
// | Version  $_SWANBR_VERSION_$
// +---------------------------------------------------------------------------
// | Licensed ( $_SWANBR_LICENSED_URL_$ )
// +---------------------------------------------------------------------------
// | $_SWANBR_WEB_DOMAIN_$
// +---------------------------------------------------------------------------

namespace Kafka;

/**
+------------------------------------------------------------------------------
* Kafka protocol since Kafka v0.8
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class Log
{
    protected $_strFile;
    protected $_arrCache;
    protected $_strFieldSep = "\t";
    protected $_strLineSep = "\n";
    
    public static $logLevel;
    public static $kafkaConfig; 
    
    public static $logItem;
    
    const DEBUG  = 5;
    const INFO   = 4;
    const NOTICE = 3;
    const WARNING = 2;
    const ERROR   = 1;
    
    public function __construct($strFile) {
        $this->_strFile = $strFile;
    }
    
    public function add($params) {
        $this->_arrCache[] = $params;
        if (count($this->_arrCache) > 50) {
            $this->flush();
        }
    }
    
    public function flush() {
        if (empty($this->_arrCache)) {
            return true;
        }
        foreach ($this->_arrCache as &$v) {
            if (is_array($v)) {
                $v = implode($this->_strFieldSep, $v);
            }           
        }
        unset($v);
        $strTmp = implode($this->_strLineSep, $this->_arrCache);
        $resFile = self::_openFile($this->_strFile);
        if (! $resFile) {
            return false;
        }
        $bolRet = self::_writeFile($resFile, "{$strTmp}{$this->_strLineSep}");
        if ($bolRet) {
            $this->_arrCache = null;
        }
        return $bolRet;
    }
    
    protected static function _writeFile($resFile, $strContent, $intCount = 1) {
        if (flock($resFile, LOCK_EX)) {
            fwrite($resFile, $strContent);
            flock($resFile, LOCK_UN);
            fclose($resFile);
            return true;
        }
        
        fclose($resFile);
        if ($intCount > 3) {
            return false;
        }
        $intCount++;
        return self::_writeFile($resFile, $strContent, $intCount);
    }
    
    protected static function _openFile($strFile) {
        if ($strFile[0] == '/') {
            $fp = fopen($strFile, 'a');
        } elseif (defined(LOG_DEBUG)) {
            $fp = fopen(Yii::app()->runtimePath . '/' . $strFile, 'a');
        } else {
            $tmp = explode('_', $strFile);
            $fp = fopen('/dev/shm/e_' . $tmp[0], 'a');
        }
        return $fp;     
    }
    
    protected static function _write($params, $fileName = NULL, $splitChar = "\t")
    {
        $line = $params;
        if (is_array($params)) {
            $line = implode($splitChar, $params);
        }
        if ($fileName == NULL) {
            $fileName = date('ymd-H') . '.log';
        }

        $fp = self::_openFile($fileName);
        if ($fp) {
            self::_writeFile($fp, "$line\n");
        }
    }
    
     /**
     * @param  [type] $class   类名
     * @param  [type] $method  方法名
     * @param  [type] $content 日志内容，若是array,则会被json_encode
     * @return [type]          null
     */
    //public static function write($class,$method,$content)
    public static function write($logLevel,$content)
    {
        if (!self::$kafkaConfig) {
            self::$kafkaConfig = require dirname(__FILE__).'/config/kafka.php';
        }
        $logLevelConf = self::$kafkaConfig['logLevel'];
        $logDir = self::$kafkaConfig['logDir'];
        
        if($logLevel > $logLevelConf){
            return ;
        }
        
        
        $strLogLevel = self::levelToString($logLevel);
        
        $back=debug_backtrace();
        $back=$back[1];
        $class=$back['class'];
        $method=$back['function'];
        
        if(is_array($content))
        {
            $content=json_encode($content);  
        }
        $t=explode(" ",microtime());

        //文件路径名;
        $file = $logDir.self::$logItem;
        //写内容
        self::_write(date('Y-m-d H:i:s').".".intval(1000*$t[0])."\tPID_" . getmypid() ."\t[$strLogLevel]\t$class::$method\t$content",$file);
        
        return true;
    }
    
     /**
     * 发送警报邮件，同样内容，只发送一次。
     * @param string $className    类名
     * @param string $functionName 方法名
     * @param string $content      内容
     */
    public static function alert($className = "", $functionName = "", $content = "",$sendSms=false)
    {
        if($className.$functionName. $content =="")
            return ;
        $md5=md5($className.$functionName.$content);
        self::write(__CLASS__,__FUNCTION__,array($className,$functionName,$content,$sendSms));
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
        $table = "<table border=1><tbody><tr><td><b> KAFKA消息系统报警邮件  from ip:" .($ip) ." pid:".getmypid()."</b></td></tr>\n";
        $table .= "<tr><td>【{$className}::{$functionName}】</td></tr>\n";
        $table .= "<tr><td>" . str_replace("\n", "<br/>", $content) . "</td></tr>\n";
        $table .= "</tbody></table>\n";
        try {
            $title="KAFKA消息系统报警邮件" . '-' . date('Y-m-d H:i:s');
            if($sendSms)
            {
                $ch = curl_init("http://10.108.68.121:888/notice/notice.php?s=&c=".urlencode("$className::$functionName ".substr($content,0,100))."&g=e_audit_monitor_smsonly");
                curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
                curl_exec($ch);
                curl_close($ch);
            }
            $ch = curl_init("http://10.108.68.121:888/notice/notice.php?s=".urlencode($title)."&c=".urlencode($table)."&g=e_audit_monitor_mailonly");
            curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
            curl_exec($ch);
            curl_close($ch);
        } catch (Exception $e) {
            /*发邮件异常*/
        }
    }
    
    public function levelToString($logLevel){
        $ret = '[unknow]';
        switch ($logLevel){
            case self::DEBUG:
                $ret = 'DEBUG';
                break;
            case self::INFO:
                $ret = 'INFO';
                break;
            case self::NOTICE:
                $ret = 'NOTICE';
                break;
            case self::WARNING:
                $ret = 'WARNING';
                break;
            case self::ERROR:
                $ret = 'ERROR';
                break;
        }
        return $ret;
    }
    
    public function __destruct() {
        if (! empty($this->_arrCache)) {
            $this->flush();
        }
    }
}
