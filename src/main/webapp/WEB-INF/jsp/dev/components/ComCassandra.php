<?php

/**
 * ComCassandra 
 * 
 * @package open 360
 * @version v1
 * @copyright 2005-2011 360.CN All Rights Reserved.
 * @author wangguoqiang@360.cn 
 */
class ComCassandra
{

    protected $cassandra;

    public function __construct()
    {
        $this->cassandra = Yii::app()->cassandra;
    }

    public function save($key, $value)
    {
        $this->cassandra->insert($key, $value);
    }

    public function get($key)
    {
        return $this->cassandra->get($key);
    }

    public function output($key, $type)
    {
        try {
            $data = $this->cassandra->get($key);
        }
        catch(Exception $e) {
            header("Content-type: text/html; Charset=utf-8");
            echo "·þÎñÆ÷´íÎó";
            Yii::app()->end();
        }
        switch($type) {
            case 'swf':
                header('Content-Type: application/x-shockwave-flash');
                header('Cache-Control:max-age=' . (strtotime("+1 day")-time()) . ', must-revalidate');
                header('Last-Modified:' . gmdate('D, d M Y H:i:s') . ' GMT+0800');
                header('Expires: ' . gmdate('D, d M Y H:i:s', strtotime('+1 day')) . ' GMT+0800');
                header('Content-Length: ' . strlen($data));
                echo $data;
                break;
            case 'zip':
                $this->compressFile($data, "{$key}.{$type}");
                break;
            case 'rar':
                $this->compressFile($data, "{$key}.{$type}");
                break;
            case 'jpg':
                $this->compressImg($data, "{$key}.{$type}", $type);
                break;
            case 'jpeg':
                $this->compressImg($data, "{$key}.{$type}", $type);
                break;
            case 'gif':
                $this->compressImg($data, "{$key}.{$type}", $type);
                break;
            case 'png':
                $this->compressImg($data, "{$key}.{$type}", $type);
                break;
            default:
                break;
        }
    }

    private function compressFile($data, $filename)
    {
        header('Content-Type: application/octet-stream; charset=utf-8');
        header('Content-Length: ' . strlen($data));
        header('Content-Disposition: attachment;filename='.$filename);
        echo $data;
    }

    private function compressImg($data, $filename, $type)
    {
        header('Content-Type: image/'.$type.'; charset=utf-8');
        header('Content-Length: ' . strlen($data));
        header('Content-Disposition: attachment;filename='.$filename);
        echo $data;
    }
}