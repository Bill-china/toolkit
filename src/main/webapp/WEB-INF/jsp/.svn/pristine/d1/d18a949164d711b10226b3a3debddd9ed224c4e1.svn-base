<?php

class CLoader
{
    public $class;
    
    public function __construct()
    {
        $this->class = array();
    }

    public function init()
    {
        $this->config = new CConfiguration(Yii::getPathOfAlias('application.config') . '/redis.php');
    }

    public function component($comName, $params = NULL)
    {
        $key = "{$comName}{$params}";
        if (isset($this->class[$key])) {
            return $this->class[$key];
        }
        if ($params !== NULL) {
            $obj = new $comName($params);
        }
        else {
            $obj = new $comName();
        }
        if ($obj) {
            $this->class[$key] = $obj;
            return $obj;
        }
        throw new Exception('不能实例化类: ' . $comName);
    }

    public function statRedis($id, $isNo = false)
    {
        $node = 'tongji';
        if ($isNo === false) {
            $redisNum = count($this->config->itemAt($node));
            //点击增加单独节点
            if ($id > 0 && $redisNum > 1) {
                $redisNum -= 1;
                //$id = (($id % $redisNum) + intval(date('i'))) % $redisNum + 1; 
                $id = (($id % $redisNum) + Utility::minuteInter(date('i'))) % $redisNum + 1; 
            }
            else {
                $id = ($id % $redisNum);
            }
        }

        if (isset($this->class["{$node}{$id}"])) {
            return $this->class["{$node}{$id}"];
        }
        else {
            return $this->class["{$node}{$id}"] = new ComRedis($node, $id);
        }
    }

    public function viewRedis($id)
    {
        $node = 'view_todb_queue';
        if (isset($this->class["{$node}{$id}"])) {
            return $this->class["{$node}{$id}"];
        }
        else {
            return $this->class["{$node}{$id}"] = new ComRedis($node, $id);
        }
    }

    public function limitRedis($id, $isNo = false)
    {
        $node = 'limit';
        if (!is_int($isNo)) {
            $id = $this->limitRedisNode($id);
        }
        if (isset($this->class["{$node}{$id}"])) {
            return $this->class["{$node}{$id}"];
        }
        else {
            return $this->class["{$node}{$id}"] = new ComRedis($node, $id);
        }
    }

    public function limitRedisNode($id)
    {
        $node = 'limit';
        $redisNum = count($this->config->itemAt($node));
        return ($id % $redisNum);
    }

    public function cheatRedis($id, $isNo = false)
    {
        $node = 'cheat';
        if ($isNo === false) {
            $redisNum = count($this->config->itemAt($node));
            $id = ($id % $redisNum);
        }

        if (isset($this->class["{$node}{$id}"])) {
            return $this->class["{$node}{$id}"];
        }
        else {
            return $this->class["{$node}{$id}"] = new ComRedis($node, $id);
        }
    }

    public function queueRedis($id, $isNo = false)
    {
        $node = 'queue';
        if ($isNo === false) {
            $redisNum = count($this->config->itemAt($node));
            $id = ($id % $redisNum);
        }

        if (isset($this->class["{$node}{$id}"])) {
            return $this->class["{$node}{$id}"];
        }
        else {
            return $this->class["{$node}{$id}"] = new ComRedis($node, $id);
        }
    }

    public function redis($node)
    {
        $redisConf = $this->config->itemAt($node);
        if (!isset($redisConf['host'])) {
            throw new Exception("Error Redis Conf: {$node}", 1);            
        }

        if (isset($this->class["{$node}"])) {
            return $this->class["{$node}"];
        }
        else {
            return $this->class["{$node}"] = new ComRedis($node);
        }
    }

}

?>
