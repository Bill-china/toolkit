<?php
class CSmsclient
{
    private $url = 'http://sms.ops.qihoo.net:8360/sms';
    private $appId = 80;
    private $appKey = 'oTIiByLUGEE';

    public function init()
    {
    }

    public function send($mobile, $msg)
    {
        if (!is_array($mobile)) {
            $mobile = array($mobile);
        }
        $data = array();
        foreach ($mobile as $m) {
            $data[] = array('msg' => $msg, 'phone' => $m);
        }
        $post = array('id' => $this->appId, 'key' => $this->appKey, 'data' => json_encode($data));
        $res = Yii::app()->curl->post($this->url, $post);
        $res = json_decode($res, true);
        if ($res['result'] == 0) {
            return true;
        }
        return false;
    }
}

