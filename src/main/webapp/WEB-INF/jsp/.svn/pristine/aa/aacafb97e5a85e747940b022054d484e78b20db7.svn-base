<?php
class SiteController extends CController {

    public function beforeAction($action) {
        Timer::start('total');
        LogData::addNode('errno', 0);
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

    public function actionMonitor () {/*{{{*/
        echo 'STATUS OK';
    }/*}}}*/

    public function actionError () {/*{{{*/
        echo 'ACCESS DENY';
    }/*}}}*/

}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
