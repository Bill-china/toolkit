<?php
/**
 * TController is the customized base controller class for test controllers.
 * All test controller classes for this application should extend from this base class.
 */
class TController extends CController {

    // you can change this url to your own
    protected $_site = 'http://esc.gongwei.com';

    public function __construct($id, $model = NULL) {
        parent::__construct($id, $model);
    }

    /**
     * 默认初始化
     */
    public function beforeAction($action) {
        Timer::start('total');
        LogData::addNode('errno', 0);
        LogData::addNode('msg', 'success');
        LogData::addNode('action_id', Yii::app()->controller->id.'_'.Yii::app()->controller->action->id);
        LogData::addNode('req_time', Timer::getCurTime());
        LogData::addNode('req_ip', isset($_SERVER['REMOTE_ADDR']) ? $_SERVER['REMOTE_ADDR'] : '');
        return true;
    }

    final public function afterAction($action) {
        Timer::end('total');
        $strLog = sprintf('NOTICE %s %s %s %s', LogData::getLogID(), date('Y-m-d H:i:s'), LogData::dataToString(), Timer::toString());
        ComAdLog::write($strLog, 'esc');
        return true;
    }


    protected function _make_sign($strData, $time) {
        $salt = Yii::app()->params['one_box_key'];

        $md5sum = md5($salt.$strData.$time);
        return substr($md5sum, 0, 16);
    }

    protected function _callCurl ($interFaceName, $url, $params, $json_decode=true) {/*{{{*/
        $url = $this->_site.$url;
        $data = Yii::app()->curl->run($url, false, $params);
        $strArr = array();
        foreach ($params as $k => $v) {
            $strArr[] = $k.'='.urlencode($v);
        }
        $str = implode('&', $strArr);
        echo $str."\n";

        // display
        echo "<center> test </center><br/>\n";
        echo "interface Name :".$interFaceName."<p>\n";
        echo "url :".$url."<p>\n";
        echo "input params:<p>\n";
        echo "<pre>\n";
        var_dump($params);
        echo "</pre>\n";
        echo "output result :<p>\n";
        echo "<pre>\n";
        if ($json_decode) {
            $data = json_decode($data, true);
        }
        var_dump($data);
        echo "</pre>\n";
    }/*}}}*/
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
