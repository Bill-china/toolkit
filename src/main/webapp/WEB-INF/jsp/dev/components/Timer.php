<?php
/**
 * 计时器
 **/
class Timer {

    protected static $_timerData    = array();

    protected static $_curTime      = null;

    public static function getCurTime() {
        if (is_null(self::$_curTime)) {
            self::$_curTime = time();
        }
        return self::$_curTime;
    }

    public static function start($processName) {
        self::$_timerData[$processName]['start'] = self::$_timerData[$processName]['end'] = gettimeofday();
    }

    public static function end($processName) {
        self::$_timerData[$processName]['end'] = gettimeofday();
    }

    public static function toString() {
        $executeTimes = self::_caculate();
        if (empty($executeTimes)) {
            return 'Timer[]';
        }
        $strRet = '';
        foreach ($executeTimes as $processName => $executeTime) {
            $strRet .= $processName.':'.$executeTime.' ';
        }
        return 'Timer['.rtrim($strRet).']';
    }

    protected static function _caculate() {
        if (empty(self::$_timerData)) {
            return array();
        }
        $executeTimes = array();
        foreach (self::$_timerData as $processName => $timers) {
            if (isset($timers['start'])) {
                $executeTimes[$processName] = self::_caculateTime($timers['start'], $timers['end']);
            }
        }
        return $executeTimes;
    }

    protected static function _caculateTime($timer1, $timer2) {
        return ($timer2['sec'] - $timer1['sec'])*1000*1000 + ($timer2['usec'] - $timer1['usec']);;
    }
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
