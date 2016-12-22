<?php
class OneboxController extends Controller {

    const ONEBOX_CHANNEL_ID = 29;
    const ONEBOX_PLACE_ID   = 200;

    // 点击类打点必须参数
    protected $_click_require_params = array(
        'uid',
        'pid',
        'gid',
        'aid',
        'bidprice',
        'price',
        'ip',
        'city_id',
        'view_time',
        'now',
        'view_id',
        'click_id',
    );

    // 展现类打点必须参数
    protected $_view_require_params = array(
        'uid',
        'pid',
        'gid',
        'aid',
        'bidprice',
        'price',
        'ip',
        'city_id',
        'now',
        'view_id',
    );

    public function actionView () {/*{{{*/
        if ($this->_errno!=0) {
            return ;
        }
        $params = $_POST;

        $data = json_decode($params['data'], true);
        if (false === $data) {
            $this->_errno = 4;
            $this->_msg = sprintf('can not decode data[%s]', $params['data']);
            return ;
        }

        $paramsRequire = $this->_view_require_params;
        foreach ($paramsRequire as $_oneReqParam) {
            if (!isset($data[$_oneReqParam])) {
                $this->_errno = 4;
                $this->_msg = sprintf('need param[%s] of data', $_oneReqParam);
                return ;
            }
        }

        $arrViewData = array(
            // 'view_id'       => 'onebox_view',
            'view_id'       => $data['view_id'],
            'ip'            => $data['ip'],
            'type'          => 'view',
            'now'           => $data['now'],
            'apitype'       => 2, // 不进行一分钟过滤
            'pid'           => $data['pid'],
            // 'place'
            // 'pos'
            'gid'           => $data['gid'],
            'aid'           => $data['aid'],
            'uid'           => $data['uid'],
            'bidprice'      => $data['price'], // 以后改进
            'price'         => $data['price'],
            'city_id'       => $data['city_id'],
            'mid'           => '',
            'ls'            => '',
            'keyword'       => '',
            'query'         => '',
            'place'         => '',
            // 'match_type'
            'channel_id'    => self::ONEBOX_CHANNEL_ID,
            'place_id'      => self::ONEBOX_PLACE_ID,
            'guid'          => '',
            'ver'           => 'one_box',
        );
        $key = Config::item('redisKey').'stats:'; // open_ad_v1:stats:

        $sidFrom = Yii::app()->params['one_box_view_sid_from'];
        $sidTo   = Yii::app()->params['one_box_view_sid_to'];
        $sid = mt_rand($sidFrom, $sidTo);
        LogData::addNode('sid', $sid);
        LogData::addNode('data', json_encode($arrViewData));

        Timer::start('send_to_redis');
        $viewRedis = Yii::app()->loader->statRedis($sid, true);
        $ret = $viewRedis->rpush($key, json_encode($arrViewData));
        Timer::end('send_to_redis');
        if (false === $ret) {
            $this->_errno = 5;
            $this->_msg = 'send data to redis fail';
            return ;
        }
        return ;
    }/*}}}*/

    public function actionClick () {/*{{{*/
        if ($this->_errno!=0) {
            return ;
        }
        $params = $_POST;

        $data = json_decode($params['data'], true);
        if (false === $data) {
            $this->_errno = 4;
            $this->_msg = sprintf('can not decode data[%s]', $params['data']);
            return ;
        }

        $paramsRequire = $this->_click_require_params;
        foreach ($paramsRequire as $_oneReqParam) {
            if (!isset($data[$_oneReqParam])) {
                $this->_errno = 4;
                $this->_msg = sprintf('need param[%s] of data', $_oneReqParam);
                return ;
            }
        }
        // 过滤下点击时间,超过一小时数据不再计费
        $curTime = time();
        if ($curTime-$data['now']>3600) {
            $this->_errno = 6;
            $this->_msg = sprintf("data time out");
            return ;
        }


        $arrClickData = array(
            // 'view_id'       => 'onebox_view',
            'view_id'       => $data['view_id'],
            'ip'            => $data['ip'],
            'type'          => 'click',
            'now'           => $data['now'],
            'view_time'     => $data['view_time'],
            'apitype'       => 2, // 不会进一分钟过滤
            'pid'           => $data['pid'],
            // 'place'
            // 'pos'
            'gid'           => $data['gid'],
            'aid'           => $data['aid'],
            'uid'           => $data['uid'],
            'bidprice'      => $data['price'], // 以后改进
            'price'         => $data['price'],
            'city_id'       => $data['city_id'],
            'mid'           => '',
            'ls'            => '',
            'keyword'       => '',
            'query'         => '',
            'place'         => '',
            // 'match_type'
            // 'click_id'      => 'onebox_click',
            'click_id'      => $data['click_id'],
            'channel_id'    => self::ONEBOX_CHANNEL_ID,
            'place_id'      => self::ONEBOX_PLACE_ID,
            'guid'          => '',
            'ver'           => 'one_box',
        );
        $key = Config::item('redisKey').'stats:'; // open_ad_v1:stats:
        Timer::start('send_to_redis');
        $clickRedis = Yii::app()->loader->statRedis(0);
        $ret = $clickRedis->rpush($key, json_encode($arrClickData));
        Timer::end('send_to_redis');
        if (false === $ret) {
            $this->_errno = 5;
            $this->_msg = 'send data to redis fail';
            return ;
        }
        return ;
    }/*}}}*/

}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
