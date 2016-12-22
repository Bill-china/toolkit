<?php
/**
 * 存储日志统计字段数据
 */
class LogData {

    protected static $_logID = '';

    protected static $_logData = array();

    protected static $_loged = false;

    public static function isLog() {
        return self::$_loged;
    }

    public static function setLoged () {
        self::$_loged = true;
    }

    public static function clearLoged () {
        self::$_loged = false;
    }

    public static function getLogID () {
        if (self::$_logID=='') {
            if (isset($_POST['logid'])) {
                self::$_logID = $_POST['logid'];
            } else {
                $requestTime = gettimeofday();
                self::$_logID = $requestTime['sec'] . sprintf('%06d', $requestTime['usec']);
                self::$_logID = 'ESC_'.self::$_logID;
            }
        }
        return self::$_logID;
    }

    public static function addNode ($strKey, $strVal) {
        $strKey = strtolower($strKey);
        if (is_string($strVal)) {
            //$strVal = rawurlencode($strVal);
            $strVal = $strVal;
        }
        self::$_logData[$strKey] = $strVal;
    }

    public static function dataToString() {
        if (empty(self::$_logData)) {
            return 'Data[]';
        }
        $strOut = '';
        foreach (self::$_logData as $_k => $_v) {
            $strOut .= $_k.'='.$_v.' ';
        }
        return 'Data['.rtrim($strOut).']';
    }
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
