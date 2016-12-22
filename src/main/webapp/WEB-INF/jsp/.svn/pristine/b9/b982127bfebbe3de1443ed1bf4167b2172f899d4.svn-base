<?php
namespace Kafka;

class MyClient {
  // broker_list format: ip:port,ip:port,ip:port
  public function __construct($broker_list, $topic, $partition, $consumer_group) {
    $this->broker_list_ = $broker_list;
    $this->topic_ = $topic;
    $this->partition_ = $partition;
    $this->consumer_group_ = $consumer_group;
    $this->parseBrokerList();
  }

  public function showParameters() {
    printParameter();
  }

  public function setParameter($key, $value) {
    if ($key == 'required_ack') {
      $this->required_ack_ = $value;
    } else if ($key == 'timeout') {
      $this->timeout_ = $value;
    } else if ($key == 'max_bytes') {
      $this->max_bytes_ = $value;
    } else {
      printf("not supported parameter:".$key."\n");
      printParameter();
      exit();
    }
  }

  // 以offset为开始位置开始从kafka拉取消息
  // 返回以多条消息组成的数组，每个数组有key, value等元素
  // 如果失败，返回null
  // 如果没有结果，返回空数组，这里判断是否失败时，注意区别null和空数组
  public function sendFetchRequest($offset,&$error_code) {
    if (!$this->message_conn_) {
      $ret = $this->initMessageConnect();
      if (!$ret) {
        Log::write(Log::ERROR, "initMessageConnect failed");
        return null;
      }
    }
    $encoder = new \Kafka\Protocol\Encoder($this->message_conn_);
    $data = $this->makeFetchRequest($offset);
    $ret = null;
    try {
      $ret = $encoder->fetchRequest($data);
    } catch (\Kafka\Exception $e) {
      Log::write(Log::ERROR, "send fetchRequest failed");
      $this->message_conn_ = null;
      return null;
    }
    if (!$ret) {
      Log::write(Log::ERROR, "fetchRequest is null");
      $this->message_conn_ = null;
      return null;
    }
    $decoder = new \Kafka\Protocol\Decoder($this->message_conn_);
    $result = null;
    try {
      $result = $decoder->fetchResponse();
    } catch (\Kafka\Exception $e) {
      Log::write(Log::ERROR, "decoder fetchResponse() failed,errMsg = {$e->getMessage()}");
      $this->message_conn_ = null;
      return null;
    }
    if (!$result) {
      Log::write(Log::WARNING, "fetchResponse is null");
      $this->message_conn_ = null;
      return null;
    }
    $error_code = $result[$this->topic_][$this->partition_]['error_code'];
    if ($error_code != 0) {
      Log::write(Log::ERROR, "result of fetch response has error:".\Kafka\Protocol\Decoder::getError($error_code));
      $this->message_conn_ = null;
      return null;
    }
    return $result[$this->topic_][$this->partition_]['message_set'];
  }

  // 将指定的offset提交到zookeeper
  // 成功返回true，否则返回false
  public function sendCommitOffsetRequest($offset) {
    if (!$this->offset_conn_) {
      $ret = $this->initOffsetConnect();
      if (!$ret) {
        return false;
      }
    }
    $data = $this->makeCommitOffsetRequest($offset);
    $encoder = new \Kafka\Protocol\Encoder($this->offset_conn_);
    try {
      $encoder->commitOffsetRequest($data);
      Log::write(Log::NOTICE, "topic:{$this->topic_},partition:{$this->partition_}, commit offset to:{$offset}");
    } catch (\Kafka\Exception $e) {
      Log::write(Log::ERROR, "encoder commitOffsetRequest failed");
      $this->offset_conn_ = null;
      return false;
    }
    $decoder = new \Kafka\Protocol\Decoder($this->offset_conn_);
    $result = null;
    try {
      $result = $decoder->commitOffsetResponse();
    } catch (\Kafka\Exception $e) {
      Log::write(Log::ERROR, "decoder commitOffsetResponse failed");
      
      $this->offset_conn_ = null;
      return false;
    }
    $error_code = $result[$this->topic_][$this->partition_]['errCode'];
    if ($error_code != 0) {
      Log::write(Log::ERROR, "commit offset failed, error number:$error_code".\Kafka\Protocol\Decoder::getError($error_code));
      
      $this->offset_conn_ = null;
      return false;
    }
    return true;
  }

  // 从zookeeper获取当前topic, partition的offset
  // 成功获取，返回当前consumer group保存的offset, 否则返回null
  public function sendFetchOffsetRequest(&$errno) {
    if (!$this->offset_conn_) {
      $ret = $this->initOffsetConnect();
      if (!$ret) {
        $errno = -1;
        return null;
      }
    }
    $data = $this->makeFetchOffsetRequest();
    $encoder = new \Kafka\Protocol\Encoder($this->offset_conn_);
    try {
      $encoder->fetchOffsetRequest($data);
    } catch (\Kafka\Exception $e) {
      Log::write(Log::ERROR, "encoder fetchOffsetRequest failed");
      $this->offset_conn_ = null;
      $errno = -1;
      return null;
    }
    $decoder = new \Kafka\Protocol\Decoder($this->offset_conn_);
    $result = null;
    try {
      $result = $decoder->fetchOffsetResponse();
    } catch (\Kafka\Exception $e) {
      Log::write(Log::ERROR, "decoder fetchOffsetResponse failed");
      
      $this->offset_conn_ = null;
      $errno = -1;
      return null;
    }
    $error_code = $result[$this->topic_][$this->partition_]['errCode'];
    if ($error_code != 0) {
      Log::write(Log::ERROR, "response of sendFetchOffsetRequest has error:".\Kafka\Protocol\Decoder::getError($error_code));
      $this->offset_conn_ = null;
      $errno = $error_code;
      return null;
    }
    $offset = $result[$this->topic_][$this->partition_]['offset'];
    $errno = $error_code;
    return $offset;
  }
  
  // 从kafka获取当前message log size等数据
  // time 获取在指定时间之前的offset, 单位ms, -1表示获取最新的offset, -2表示获取最老可用的offset
  // max_number_of_offsets, 需要获取的offsets的数量，默认为1
  // 成功获取，返回这样一个offsets的数组，否则返回flase
  public function sendOffsetRequest($time, $max_number_of_offsets = 1) {
    if (!$this->message_conn_) {
      $ret = $this->initMessageConnect();
      if (!$ret) {
        return false;
      }
    }
    $data = $this->makeOffsetRequest($time, $max_number_of_offsets = 1);
    $encoder = new \Kafka\Protocol\Encoder($this->message_conn_);
    try {
      $encoder->offsetRequest($data);
    } catch (\Kafka\Exception $e) {
      Log::write(Log::ERROR, "encoder offsetRequest failed");
      
      $this->message_conn_ = null;
      return false;
    }
    $decoder = new \Kafka\Protocol\Decoder($this->message_conn_);
    $result = null;
    try {
      $result = $decoder->offsetResponse();
    } catch (\Kafka\Exception $e) {
      Log::write(Log::ERROR, "decoder offsetResponse failed");
      $this->message_conn_ = null;
      return false;
    }
    if (!$result) {
      Log::write(Log::ERROR, "decoder offsetResponse failed");
      return false;
    }
    $error_code = $result[$this->topic_][$this->partition_]['errCode'];
    if ($error_code != 0) {
      Log::write(Log::ERROR, "sendOffsetRequest response has error:".\Kafka\Protocol\Decoder::getError($error_code));
      $this->message_conn_ = null;
      return false;
    }
    return $result[$this->topic_][$this->partition_]['offset'];
  }

  // produce 消息到kafka
  // 输入，要发送的消息的数组
  // 成功produce, 返回true, 否则返回false
  public function sendProduceRequest($messages) {
    if (!$this->message_conn_) {
      $ret = $this->initMessageConnect();
      if (!$ret) {
        return false;
      }
    }
    $data = $this->makeProduceRequest($messages);
    if (!$data) {
      Log::write(Log::ERROR, "messages to send are not valid array");
      $this->message_conn_ = null;
      return false;
    }
    $encoder = new \Kafka\Protocol\Encoder($this->message_conn_);
    try {
      $encoder->produceRequest($data);
    } catch (\Kafka\Exception $e) {
      Log::write(Log::ERROR,"encoder produceRequest failed");
      $this->message_conn_ = null;
      return false;
    }
    $decoder = new \Kafka\Protocol\Decoder($this->message_conn_);
    $result = null;
    try {
      $result = $decoder->produceResponse();
    } catch (\Kafka\Exception $e) {
      Log::write(Log::ERROR, "decoder produceResponse failed");
      $this->message_conn_ = null;
      return false;
    }
    $error_code = $result[$this->topic_][$this->partition_]['errCode'];
    if ($error_code != 0) {
      Log::write(Log::ERROR, "sendProduceRequest response has error:".\Kafka\Protocol\Decoder::getError($error_code));
      $this->message_conn_ = null;
      return false;
    }
    return true;
  }

  private function makeOffsetRequest($time, $max_number_of_offsets) {
    $data = array(
      'data' => array(
        array(
          'topic_name' => $this->topic_,
          'partitions' => array(
              array(
              'partition_id' => $this->partition_,
              'time' => $time,
              'max_offset' => $max_number_of_offsets,
            ),
          ),
        ),
      ),
    );
    return $data;
  }

  private function makeProduceRequest($messages) {
    if (!is_array($messages)) {
      Log::write(Log::ERROR, "produce messages must be array");
      
      return null;
    }
    $payloads = array();
    $payloads['required_ack'] = $this->required_ack_;
    $payloads['timeout'] = $this->timeout_;
    $payloads['data'] = array();
    $payloads['data'][0] = array();
    $payloads['data'][0]['topic_name'] = $this->topic_;
    $payloads['data'][0]['partitions'] = array();
    $payloads['data'][0]['partitions'][0] = array();
    $payloads['data'][0]['partitions'][0]['partition_id'] = $this->partition_;
    $payloads['data'][0]['partitions'][0]['messages'] = array();
    $payloads['data'][0]['partitions'][0]['messages'] = $messages;
    return $payloads;
  }

  private function makeFetchOffsetRequest() {
    $data = array(
      'group_id' => $this->consumer_group_,
      'data' => array(
        array(
          'topic_name' => $this->topic_,
          'partitions' => array(
            array(
              'partition_id' => $this->partition_,
            ),
          ),
        ),
      ),
    );
    return $data;
  }

  private function printParameter() {
    printf("the supported parameters are as follows:\n".
        "required_ack: (int)ack number:".$this->required_ack_."\n".
        "timeout: (int) timeout in ms:".$this->timeout_."\n".
        "max_bytes: (int) receive max bytes in one package:".$this->max_bytes_."\n");
  }

  private function parseBrokerList() {
    $ip_array = explode($this->ip_separator_, $this->broker_list_);
    $i = 0; 
    foreach ($ip_array as $ip_port) {
      $ip_port_array = explode($this->port_separator_, $ip_port);
      if (count($ip_port_array) != 2) {
        Log::write(Log::ERROR, "broker list not legal, the right format is ip:port,ip:port,ip:port...");
        exit;
      }
      $this->ip_port_array_[$i] = array();
      $this->ip_port_array_[$i]['ip'] = $ip_port_array[0];
      $this->ip_port_array_[$i]['port'] = $ip_port_array[1];
      $i++;
    }
  }

  
  private function makeFetchRequest($offset) {
    $data = array(
        'required_ack' => $this->required_ack_,
        'timeout' => $this->timeout_,
        'data' => array(
          array(
            'topic_name' => $this->topic_,
            'partitions' => array(
              array(
                'partition_id' => $this->partition_,
                'offset' => $offset,
                'max_bytes' => $this->max_bytes_,
                ),
              ),
            ),
          ),
        );
    return $data;
  }


  private function makeCommitOffsetRequest($offset) {
    $data = array(
      'group_id' => $this->consumer_group_,
      'data' => array(
        array(
          'topic_name' => $this->topic_,
          'partitions' => array(
            array(
              'partition_id' => $this->partition_,
              'offset' => $offset,
            ),
          ),
        ),
      ),
    );
    return $data;
  }

  private function initOffsetConnect() {
    for ($i = 0; $i < count($this->ip_port_array_); $i++) {
      $ip = $this->ip_port_array_[$i]['ip'];
      $port = $this->ip_port_array_[$i]['port'];
      $this->offset_conn_ = new \Kafka\Socket($ip, $port);
      $ret = $this->offset_conn_->connect();
      if (!$ret) {
        Log::write(Log::ERROR, "connect fail:".$ip.":".$port);
        $this->offset_conn_ = null;
        continue;
      }
      // 获取offset meta
      $ret = $this->getConsumerMetadata();
      if (!$ret) {
        return false; 
      }
      // 连接新的offset coordination
      $this->offset_conn_ = new \Kafka\Socket($this->offset_host_, $this->offset_port_);
      $ret = $this->offset_conn_->connect();
      if (!$ret) {
        Log::write(Log::ERROR, "connect offset coordinator failed");
        $this->offset_conn_ = null;
        return false;
      } else {
        return true;
      }
    }
    $this->offset_conn_ = null;
    return false;
  }

  private function getConsumerMetadata() {
    $encoder = new \Kafka\Protocol\Encoder($this->offset_conn_);
    try {
      $encoder->consumerMetadataRequest($this->consumer_group_);
    } catch (\Kafka\Exception $e) {
      $this->offset_conn_ = null;
      Log::write(Log::ERROR, "send consumerMetadataRequest failed");
      return false;
    }
    $decoder = new \Kafka\Protocol\Decoder($this->offset_conn_);
    $result = null;
    try {
      $result = $decoder->consumerMetadataResponse();
    } catch (\Kafka\Exception $e) {
      Log::write(Log::ERROR, "recv consumerMetadataResponse failed");
      $this->offset_conn_ = null;
      return false;
    }
    $error_code = $result['error_code'];
    if ($error_code != 0) {
      Log::write(Log::ERROR, "get consumer metadata failed:$error_code,".\Kafka\Protocol\Decoder::getError($error_code));
      $this->offset_conn_ = null;
      return false;
    }
    $this->offset_host_ = $result['host'];
    $this->offset_port_ = $result['port'];
    Log::write(Log::DEBUG, "offset host:port:$this->offset_host_,$this->offset_port_");
    return true;
  }

  private function initMessageConnect() {
    for ($i = 0; $i < count($this->ip_port_array_); $i++) {
      $ip = $this->ip_port_array_[$i]['ip'];
      $port = $this->ip_port_array_[$i]['port'];
      $this->message_conn_ = new \Kafka\Socket($ip, $port);
      $ret = $this->message_conn_->connect();
      if (!$ret) {
        $this->message_conn_ = null;
        Log::write(Log::ERROR, "connect $ip:$port failed");
        
        continue;
      }
      // 获取metadata
      if (!$this->getPartionLeader()) {
        Log::write(Log::ERROR, "get broker leader failed");
        $this->message_conn_ = null;
        return false;
      }
      // 发起连接
      $this->message_conn_ = new \Kafka\Socket($this->message_host_, $this->message_port_);
      $ret = $this->message_conn_->connect();
      if (!$ret) {
        Log::write(Log::ERROR, "connect to leader failed");
        $this->message_conn_ = null;
        return false;
      } else {
        return true;
      }
    }
    $this->message_conn_ = null;
    return false;
  }

  // 获取发送或者接收消息的leader broker
  // 成功返回true, 失败返回false
  private function getPartionLeader() {
    $encoder = new \Kafka\Protocol\Encoder($this->message_conn_);
    $data = array($this->topic_);
    try {
      $encoder->metadataRequest($data); //TODO 异常判断, 发送请求
    } catch (\Kafka\Exception $e) {
      Log::write(Log::ERROR, "send metadataRequest failed");
      $this->message_conn_ = null;
      return false;
    }
    $decoder = new \Kafka\Protocol\Decoder($this->message_conn_);
    $result = null;
    try {
      $result = $decoder->metadataResponse(); //TODO 接收结果
    } catch (\Kafka\Exception $e) {
      Log::write(Log::ERROR, "recv metadataResponse failed");
      $this->message_conn_ = null;
      return false;
    }
    if (!$result) {
      Log::write(Log::ERROR, "recv metadataResponse failed");
      $this->message_conn_ = null;
      return false;
    }
    if (!$this->resolveMetadataResult($result)) {
      Log::write(Log::ERROR, "resolveMetadataResult failed");
      $this->message_conn_ = null;
      return false;
    }
    return true;
  }

  private function resolveMetadataResult($result) {
    if (!array_key_exists('topics', $result)) {
      Log::write(Log::ERROR, "key topics not in result");
      return false;
    }
    if (!array_key_exists($this->topic_, $result['topics'])) {
      Log::write(Log::ERROR, "key not in result[topic]");
      return false;
    }
    $topic_info = $result['topics'][$this->topic_];
    if ($topic_info['errCode'] != 0) {
      Log::write(Log::ERROR, "metadata response has error, err code is:".\Kafka\Protocol\Decoder::getError($topic_info['errCode']));
      return false;
    }
    $partition_info = $topic_info['partitions'];
    if (!array_key_exists($this->partition_, $partition_info)) {
      Log::write(Log::ERROR, "topic ".$this->topic_."has no partition:".$this->partition_);
      return false;
    }
    $this_partition = $topic_info['partitions'][$this->partition_];
    if ($this_partition['errCode'] != 0) {
      if ($this_partition['errCode'] != 9) {
        Log::write(Log::ERROR, "metadata response has error, err is:".\Kafka\Protocol\Decoder::getError($this_partition['errCode']));
        return false;
      } else {
        Log::write(Log::WARNING, "metadata response has error, err is:".\Kafka\Protocol\Decoder::getError($this_partition['errCode']));
      }
    }
    $leader_id = $this_partition['leader'];
    if (!array_key_exists('brokers', $result)) {
      Log::write(Log::ERROR, "key brokers not in result");
      return false;
    }
    $broker_array = $result['brokers']; 
    $this->message_host_ = $broker_array[$leader_id]['host'];
    $this->message_port_ = $broker_array[$leader_id]['port'];
    return true;
  }

  private $broker_list_;
  private $ip_port_array_ = array();
  private $message_conn_ = null; // 发送，消费消息的连接
  private $offset_conn_ = null; // 发送offset的连接
  private $topic_;
  private $partition_;
  private $consumer_group_;
  private $message_host_;
  private $message_port_;
  private $offset_host_;
  private $offset_port_;
  private $required_ack_ = 1;
  private $timeout_ = 1000;
  private $max_bytes_ = 10240000; 
  private $ip_separator_ = ',';
  private $port_separator_ = ':';
}
?>
