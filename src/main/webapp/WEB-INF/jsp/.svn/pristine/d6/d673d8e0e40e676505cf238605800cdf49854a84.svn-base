<?php
/**
 * Controller is the customized base controller class.
 * All controller classes for this application should extend from this base class.
 */
class Controller extends CController {
    protected $_errno = 0; // 0 正确 1 请求非法(get) 2 请求超时 3 sign 错误 4 invalid params 5 系统错误(redis) 6 打点超时(点击)

    protected $_msg = 'success'; // 如果有返回数据，则将json值写入

    public function __construct($id, $model = NULL) {
        parent::__construct($id, $model);
    }

    /**
     * 默认初始化
     */
    public function beforeAction($action) {
        Timer::start('total');
        $params = $_POST;

        LogData::addNode('errno', 0);
        LogData::addNode('action_id', Yii::app()->controller->id.'_'.Yii::app()->controller->action->id);
        LogData::addNode('req_time', Timer::getCurTime());
        LogData::addNode('req_ip', isset($_SERVER['REMOTE_ADDR']) ? $_SERVER['REMOTE_ADDR'] : '');

        // check method
        if (!Yii::app()->request->isPostRequest) {
            $this->_errno   = 1;
            $this->_msg     = 'ACCESS DENY';
            return true;
        }

        // check params
        $arrParamRequire = array(
            'time', 'data', 'sign',
        );
        foreach ($arrParamRequire as $_oneParam) {
            if (!isset($params[$_oneParam])) {
                $this->_errno   = 1;
                $this->_msg     = sprintf('need param [%s]!', $_oneParam);
                return true;
            }
            LogData::addNode($_oneParam, $params[$_oneParam]);
        }

        // time
        $reqTime = intval($params['time']);
        $curTime = time();
        if ($curTime - $reqTime > 60) {
            $this->_errno   = 2;
            $this->_msg     = 'require time out';
            return true;
        }

        $sign = $this->_make_sign($params['data'], $params['time']);

        if ($params['sign'] !== $sign) {
            $this->_errno   = 3;
            $this->_msg     = 'sign error';
            return true;
        }
        return true;
    }

    final public function afterAction($action) {
        Timer::end('total');
        LogData::addNode('errno', $this->_errno);
        if ($this->_errno!=0) {
            LogData::addNode('msg', $this->_msg);
        } else {
            LogData::addNode('msg', 'success');
        }
        $strLog = sprintf('NOTICE %s %s %s %s', LogData::getLogID(), date('Y-m-d H:i:s'), LogData::dataToString(), Timer::toString());
        ComAdLog::write($strLog, 'esc');

        // 返回数据
        $outData = array(
            'no'    => $this->_errno,
            'msg'   => $this->_msg,
            'data'  => array(),
        );
        echo json_encode($outData);
        return true;
    }


    protected function _make_sign($strData, $time) {
        $salt = Yii::app()->params['one_box_key'];

        $saltString = $salt.$strData.$time;
        $md5sum = md5($saltString);
        // var_dump($saltString);
        // var_dump($md5sum);
        return substr($md5sum, 0, 16);
    }

}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
