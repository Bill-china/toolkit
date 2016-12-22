<?php

class EmqSubscribe
{
    protected $connection;
    protected $channel;

    protected $exchange;
    protected $queue;
    protected $consumerTag;
    protected $routingKey;
    protected $mqConf;
    protected $callback;

    public function __construct($exchange, $queue, $consumerTag, $routingKey, $conf)
    {
        $this->exchange = $exchange;
        $this->queue = $queue;
        $this->consumerTag = $consumerTag;
        $this->routingKey = $routingKey;
        $this->mqConf = $conf;
    }

    public function start($callback)
    {
        $ret = $this->_declareMq($callback);
        if ($ret == false) {
            echo date("Y-m-d H:i:s") . " subscribe fail\n";
            return false;
        }

        echo "customer started host:" . $this->mqConf['host']
            . " port:" . $this->mqConf['port']
            . " vhost:" . $this->mqConf['vhost']
            . "\nexchange:" . $this->exchange
            . "\nqueue:" . $this->queue . "\n";

        // Loop as long as the channel has callbacks registered
        while (count($this->channel->callbacks)) {
            $this->channel->wait();
        }
    }

    private function _declareMq($callback)
    {
        if ($this->connection != null) return true;
        // error_reporting($level);
        // restore_error_handler();
        $i = 5;
        while ($i > 0) {
            $ret = true;
            try {
                $this->connection = new AMQPConnection(
                    $this->mqConf['host'],
                    $this->mqConf['port'],
                    $this->mqConf['user'],
                    $this->mqConf['pass'],
                    $this->mqConf['vhost']);
                $this->channel = $this->connection->channel();
                $this->channel->queue_declare($this->queue, false, true, false, false);
                $this->channel->exchange_declare($this->exchange, 'fanout', false, true, false);
                $this->channel->queue_bind($this->queue, $this->exchange, $this->routingKey);
                if (! empty($callback)) {
                    $this->channel->basic_consume($this->queue, $this->consumerTag, false, false, false, false, $callback);
                }
                break;
            }
            catch (Exception $e) {
                echo "try: " . (6 - $i) . " error: " . $e->getMessage() . "\n";
                $this->connection = null;
                $this->channel = null;
                $ret = false;
            }
            $i--;
            sleep(3);
        }
        // error_reporting($level);
        // restore_error_handler();

        return $ret;
    }

    public function __destruct()
    {
        return;
        if ($this->channel) $this->channel->close();
        if ($this->connection) $this->connection->close();
    }

    protected static function errorHandler()
    {
        throw new Exception("declare mq connection error");
    }
}

?>
