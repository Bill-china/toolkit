<?php
/**
 * ComAdLog 
 * 
 * @package open 360
 * @version v1
 * @copyright 2005-2011 360.CN All Rights Reserved.
 * @author wangguoqiang@360.cn 
 */
class ComAdCheat
{

    public $redis;

    private $prefixKey;

    private $switchExpireTime;
    private $usualExpireTime;

    private $dict;
    private $dict_time;
    private $curTime;
    protected $_arrPosMap = array('left' => 'L', 'right' => 'R', 'bottom' => 'B');
    private $expireTime;

    //public function __construct($sid)
    public function __construct($sid)
    {
        //$this->redis = new ComRedis('tongji', $sid);
        $this->prefixKey = Config::item('redisKey') . 'ad_cheat:';
        $this->dict = array();
        $this->dict_time=time();
        $this->curTime = time();
        $this->expireTime = 60;
    }

    public function check(&$param)
    {
        //debug
        if (YII_DEBUG) {
            //return true;
        }
        if ($param['type'] != 'view' && $param['type'] != 'click' && $param['type'] != 'trans') {
            $logData = array(
                date('Y-m-d H:i:s'),
            );
            $logData = array_merge($logData, $param);
            $comBineLog = date('YmdHis') . "\t" . "esc_filter" . "\t" . json_encode($logData);
            ComAdLog::write($comBineLog, '/dev/shm/combineLog');
            return false;
        }
        if(isset($param['try_times']))
            return true;

        //是否检测一分钟过滤，2就不过滤,mediav不过滤
        if ($param['apitype'] == 2 || $param['ver'] == 'mediav') {
            return true;
        }

        $key = $this->prefixKey . $param['aid'];
        if (trim($param['mid'])) {
            $key .= $param['mid'];;
        }
        else {
            $key .= ip2long($param['ip']);
        }
        $key .= $param['type'];

        if(time()-$this->dict_time>$this->expireTime)
        {
            $this->dict=null;
            $this->dict=array();
            $this->dict_time=time();
        }


        if (!isset($this->dict[$key])) {
            $this->dict[$key] = $param['now'];
            $this->write($param);
            return true;
        }

        if ( ($param['now'] - $this->dict[$key]) < $this->expireTime ) {

            $logData = array(
                date('Y-m-d H:i:s'),
            );
            $logData = array_merge($logData, $param);
            $comBineLog = date('YmdHis') . "\t" . "esc_filter" . "\t" . json_encode($logData);
            ComAdLog::write($comBineLog, '/dev/shm/combineLog');

            return false;
        }
        $this->dict[$key] = $param['now'];
        $this->write($param);

        return true;
    }

    public function write(&$param)
    {
        if (isset($param['view_id']) && $param['type'] == 'click' && $param['ver'] == 'sou') {
            $data = array(
                $param['view_id'],
                $param['aid'],
                $param['price'],
                $param['city_id'],
                date('Y-m-d H:i:s'),
            	isset($this->_arrPosMap[$param['place']]) ? $this->_arrPosMap[$param['place']] : '',
            	$param['pos'],
            	$param['matchtype'],
            	$param['ip'],
                $param['click_id'],
                );
            ComAdLog::write($data, 'sou.xs.click.log', chr(1));
        }

        if (isset($param['view_id']) && $param['type'] == 'click' && $param['ver'] == 'goods') {
            $data = array(
                $param['view_id'],
                $param['aid'],
                $param['price'],
                $param['city_id'],
                date('Y-m-d H:i:s'),
                (isset($param['place']) && isset($this->_arrPosMap[$param['place']])) ? $this->_arrPosMap[$param['place']] : '',
                $param['pos'],
                $param['matchtype'],
                $param['ip'],
                $param['click_id'],
                );
            ComAdLog::write($data, 'sou.xs.goodsclick.log', chr(1));
        }
    }

    public function finishClickLog()
    {
        ComAdLog::write('', Config::item('logDir') . '/click.' . date('YmdHi', $this->curTime) . '.log.ok');
        //ComAdLog::write('', Yii::app()->params['logDir'] . '/guess.click.' . date('YmdHi', $this->curTime) . '.log.ok');
    }
}
