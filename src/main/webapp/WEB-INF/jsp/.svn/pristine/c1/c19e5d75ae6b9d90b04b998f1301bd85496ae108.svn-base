<?php
/**
 * Created by PhpStorm.
 * User: dongpingan
 * Date: 2016/5/10
 * Time: 16:46
 */
class KeywordRankCommand {
    //关键词增减
    const KEYWORD_OP_ADD = 0;//增加关键词
    const KEYWORD_OP_DEL = 1;//删除关键词
    public static $keywordOP = array(
        self::KEYWORD_OP_ADD,
        self::KEYWORD_OP_DEL,
    );

    const REDIS_RANK_CONF = 'rank';
    /**
     * 同步eapi侧用户关注关键词增、删
     * @param $msg  string  json encode
     *
     * @return string
     */
    public function batchUpdate($msg) {
        //增加消息日志便于排查
        Utility::log(__CLASS__,__FUNCTION__,$msg);
        $res = 'success';
        try {
            $content = json_decode($msg, true);
            if(!$content) {
                throw new Exception("invalid json code");
            }

            //验证消息关键域
            if(!array_key_exists('data', $content) ||
                !is_array($content['data']) ||
                !array_key_exists('type', $content) ||
                !in_array(intval($content['type']), self::$keywordOP) ) {
                throw new Exception("invalid message");
            }

            //更新redis
            $redisKeyword =new ComRedis(self::REDIS_RANK_CONF);
            $errKeyword = array();
            foreach($content['data'] as $message) {
                $key = self::REDIS_RANK_CONF . '_' . md5($message['groupId'] . '_' . $message['keyword']);
                if(intval($content['type']) === self::KEYWORD_OP_ADD) {
                    $op = 'set';
                    $redisRes = $redisKeyword->set($key, '');
                } else {
                    $op = 'del';
                    $redisRes = $redisKeyword->exists($key) ? $redisKeyword->del($key):true;
                }
                if (!$redisRes) {
                    $errKeyword[] = $message;
                }
            }

            //更新出错告警
            if(!empty($errKeyword)) {
                throw new Exception('ranking keyword ' . $op . ' redis error : ' . json_encode($errKeyword));
            }

        } catch (Exception $e) {
            //发生异常告警
            Utility::sendAlert(__CLASS__, __FUNCTION__, $e->getMessage());
            return $e->getMessage();
        }

        return $res;
    }
}