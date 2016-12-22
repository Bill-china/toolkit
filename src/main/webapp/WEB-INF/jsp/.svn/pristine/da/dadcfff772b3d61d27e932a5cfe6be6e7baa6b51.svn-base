<?php
/**
 * this file is used to test interfaces of OneboxController class
 * do not merge this file to online version
 */
class OneboxtestController extends TController {

    // todo
    public function actionView () {/*{{{*/
        $interFaceName = 'OneboxController::actionView';
        $url           = '/onebox/View';

        $data = array(
            'ip'        => '10.16.15.217',  // 察看广告的用户 ip
            'pid'       => 67340683,        // 广告计划 id
            'gid'       => 67909494,        // 广告组 id
            'aid'       => 69275166,        // 广告创意 id
            'uid'       => 160185657,       // 广告主用户id
            'bidprice'  => 2.3,             // 出价
            'price'     => 2.3,             // 点击计价
            'city_id'   => '16,160',        // 点击地域信息
            'now'       => time(),          // 广告展现时间
            'view_id'   => 'asdks_view',
        );

        $strData = json_encode($data);
        $curTime = time();
        $params        = array(
            'data'          => $strData,
            'time'          => $curTime,
            'sign'          => $this->_make_sign($strData, $curTime),
        );
        $this->_callCurl($interFaceName, $url, $params, false);
    }/*}}}*/

    public function actionClick () {/*{{{*/
        $interFaceName = 'OneboxController::actionView';
        $url           = '/onebox/Click';

        $data = array(
            'pid'       => 67340683,        // 广告计划 id
            'gid'       => 67909494,        // 广告组 id
            'aid'       => 69275166,        // 广告创意 id
            'uid'       => 160185657,       // 广告主用户id
            'bidprice'  => 2.3,             // 点击出价
            'price'     => 2.3,             // 点击计费
            'ip'        => '10.16.15.217',  // 察看广告的用户 ip
            'city_id'   => '16,160',        // 点击地域信息
            'view_time' => time() - 100,    // 广告展现时间
            'now'       => time(),          // 广告点击时间
            'click_id'  => '123456789ABCDEF', // 点击id
            'view_id'   => 'aslkd',
        );

        $strData = json_encode($data);
        $curTime = time();
        $params        = array(
            'data'          => $strData,
            'time'          => $curTime,
            'sign'          => $this->_make_sign($strData, $curTime),
        );
        $this->_callCurl($interFaceName, $url, $params, false);
    }/*}}}*/
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
