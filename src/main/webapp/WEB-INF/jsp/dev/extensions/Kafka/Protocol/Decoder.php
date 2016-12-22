<?php
/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4 foldmethod=marker: */
// +---------------------------------------------------------------------------
// | SWAN [ $_SWANBR_SLOGAN_$ ]
// +---------------------------------------------------------------------------
// | Copyright $_SWANBR_COPYRIGHT_$
// +---------------------------------------------------------------------------
// | Version  $_SWANBR_VERSION_$
// +---------------------------------------------------------------------------
// | Licensed ( $_SWANBR_LICENSED_URL_$ )
// +---------------------------------------------------------------------------
// | $_SWANBR_WEB_DOMAIN_$
// +---------------------------------------------------------------------------

namespace Kafka\Protocol;

/**
+------------------------------------------------------------------------------
* Kafka protocol since Kafka v0.8
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class Decoder extends Protocol
{
    private $data_; //要解析的响应包
    private $data_len_ = 0; //响应包的长度
    private $offset_ = 0;
    private function readInt8() {
        $data = substr($this->data_, $this->offset_, 1);
        $res = self::unpack(self::BIT_B8, $data);
        $res = array_shift($res);
        $this->offset_ += 1;
        return $res;
    }
    private function readInt16() {
        $data = substr($this->data_, $this->offset_, 2);
        $res = self::unpack(self::BIT_B16, $data);
        $res = array_shift($res);
        $this->offset_ += 2;
        return $res;
    }
    private function readInt32() {
        $data = substr($this->data_, $this->offset_, 4);
        $res = self::unpack(self::BIT_B32, $data);
        $res = array_shift($res);
        $this->offset_ += 4;
        return $res;
    }
    private function readInt64() {
        $data = substr($this->data_, $this->offset_, 8);
        $res = self::unpack(self::BIT_B64, $data);
        $this->offset_ += 8;
        return $res;
    }
    private function readString() {
        $len = $this->readInt16();
        $data = substr($this->data_, $this->offset_, $len);
        $this->offset_ += $len;
        return $data;
    }
    private function readBytes() {
        $len = $this->readInt32();
        $data = substr($this->data_, $this->offset_, $len);
        $this->offset_ += $len;
        return $data;
    }
    // {{{ functions
    // {{{ public function produceResponse()

    /**
     * decode produce response
     *
     * @param string $data
     * @access public
     * @return array
     */
    public function produceResponse()
    {
        $result = array();
        $dataLen = self::unpack(self::BIT_B32, $this->stream->read(4, true));
        $dataLen = array_shift($dataLen);
        if (!$dataLen) {
            throw new \Kafka\Exception\Protocol('produce response invalid.');
        }
        $data = $this->stream->read($dataLen, true);

        // parse data struct
        $offset = 4;
        $topicCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $topicCount = array_shift($topicCount);
        $offset += 4;
        for ($i = 0; $i < $topicCount; $i++) {
            $topicLen = self::unpack(self::BIT_B16, substr($data, $offset, 2)); // int16 topic name length
            $topicLen = isset($topicLen[1]) ? $topicLen[1] : 0;
            $offset += 2;
            $topicName = substr($data, $offset, $topicLen);
            $offset += $topicLen;
            $partitionCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
            $partitionCount = isset($partitionCount[1]) ? $partitionCount[1] : 0;
            $offset += 4;
            $result[$topicName] = array();
            for ($j = 0; $j < $partitionCount; $j++) {
                $partitionId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
                $offset += 4;
                $errCode = self::unpack(self::BIT_B16, substr($data, $offset, 2));
                $offset += 2;
                $partitionOffset = self::unpack(self::BIT_B64, substr($data, $offset, 8));
                $offset += 8;
                $result[$topicName][$partitionId[1]] = array(
                    'errCode' => $errCode[1],
                    'offset'  => $partitionOffset
                );
            }
        }

        return $result;
    }
    // 从socket接收kafka broker发送过来的数据
    private function recvData() {
        $this->data_ = NULL;
        $this->offset_ = 0;
        $dataLen = self::unpack(self::BIT_B32, $this->stream->read(4, true));
        $dataLen = array_shift($dataLen);
        $this->data_ = $this->stream->read($dataLen, true);
        $this->data_len_ = $dataLen;
    }

    // }}}
    public function consumerMetadataResponse() {
        $this->recvData();
        $array = array();
        $this->readInt32(); //correlation id
        $array['error_code'] = $this->readInt16();
        $array['corelation_id'] = $this->readInt32();
        $array['host'] = $this->readString();
        $array['port'] = $this->readInt32();
        return $array;
    }
    // {{{ public function fetchResponse()

    /**
     * decode fetch response
     *
     * @param string $data
     * @access public
     * @return Iterator
     */
    public function fetchResponse()
    {
        $this->recvData();
        $result = array();
        $this->readInt32(); //correlation id
        $topic_array_size = $this->readInt32();
        for ($i = 0; $i < $topic_array_size; $i++) {
            $topic_name = $this->readString();
            $result[$topic_name] = array();
            $partition_array_size = $this->readInt32();
            for ($j = 0; $j < $partition_array_size; $j++) {
                $partition = $this->readInt32(); 
                $error_code = $this->readInt16();
                $high_water = $this->readInt64();
                $message_set_bytes_num = $this->readInt32();
                $result[$topic_name][$partition] = array();
                $result[$topic_name][$partition]['error_code'] = $error_code;
                $result[$topic_name][$partition]['high_water'] = $high_water;
                $result[$topic_name][$partition]['message_set_size'] = $message_set_bytes_num;
                $result[$topic_name][$partition]['message_set'] = $this->resolveMessageSet($partition);
            }
        }
        return $result;
    }

    private function resolveMessageSet($partition) {
        $message_set = array();
        //print "into resolveMessageSet\n";
        for ($i = 0; ; $i++) {
            if ($this->offset_ + 8 + 4 + 4 + 1 + 1 + 8 > $this->data_len_) {
                break;
            }
            $offset = $this->readInt64();
            $message_size = $this->readInt32();
            if ($message_size < 0) {
                \kafka\Log::write(\Kafka\Log::ERROR, "message_size:$message_size");
            }
            if ($this->offset_ + $message_size > $this->data_len_) {
                break;
            }
            $message = $this->resolveMessage();
            $message['offset'] = $offset;
            $message['partition'] = $partition;
            $message_set[$i] = $message;
        }
        return $message_set;
    }

    private function resolveMessage() {
        $result = array();
        $crc = $this->readInt32();
        $magic = $this->readInt8();
        $attributes = $this->readInt8();
        if ($attributes != 0) {
            \kafka\Log::write(\Kafka\Log::ERROR,"compression:$attributes is not support now, exit");
            exit;
        }
        $key = $this->readBytes();
        $value = $this->readBytes();
        $result['crc'] = $crc;
        $result['magic'] = $magic;
        $result['attributes'] = $attributes;
        $result['key'] = $key;
        $result['value'] = $value;
        //print_r($result);
        return $result;
    }



    // }}}
    // {{{ public function metadataResponse()

    /**
     * decode metadata response
     *
     * @param string $data
     * @access public
     * @return array
     */
    public function metadataResponse()
    {
        $result = array();
        $broker = array();
        $topic = array();
        $dataLen = self::unpack(self::BIT_B32, $this->stream->read(4, true));
        $dataLen = array_shift($dataLen);
        if (!$dataLen) {
            throw new \Kafka\Exception\Protocol('metaData response invalid.');
        }
        $data = $this->stream->read($dataLen, true);
        $offset = 4;
        $brokerCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $brokerCount = isset($brokerCount[1]) ? $brokerCount[1] : 0;
        for ($i = 0; $i < $brokerCount; $i++) {
            $nodeId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
            $nodeId = $nodeId[1];
            $offset += 4;
            $hostNameLen = self::unpack(self::BIT_B16, substr($data, $offset, 2)); // int16 host name length
            $hostNameLen = isset($hostNameLen[1]) ? $hostNameLen[1] : 0;
            $offset += 2;
            $hostName = substr($data, $offset, $hostNameLen);
            $offset += $hostNameLen;
            $port = self::unpack(self::BIT_B32, substr($data, $offset, 4));
            $offset += 4;
            $broker[$nodeId] = array(
                'host' => $hostName,
                'port' => $port[1],
            );
        }

        $topicMetaCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $topicMetaCount = isset($topicMetaCount[1]) ? $topicMetaCount[1] : 0;
        for ($i = 0; $i < $topicMetaCount; $i++) {
            $topicErrCode = self::unpack(self::BIT_B16, substr($data, $offset, 2));
            $offset += 2;
            $topicLen = self::unpack(self::BIT_B16, substr($data, $offset, 2));
            $offset += 2;
            $topicName = substr($data, $offset, $topicLen[1]);
            $offset += $topicLen[1];
            $partitionCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
            $offset += 4;
            $partitionCount = isset($partitionCount[1]) ? $partitionCount[1] : 0;
            $topic[$topicName]['errCode'] = $topicErrCode[1];
            $partitions = array();
            for ($j = 0; $j < $partitionCount; $j++) {
                $partitionErrCode = self::unpack(self::BIT_B16, substr($data, $offset, 2));
                $offset += 2;
                $partitionId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
                $partitionId = isset($partitionId[1]) ? $partitionId[1] : 0;
                $offset += 4;
                $leaderId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
                $offset += 4;
                $repliasCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
                $offset += 4;
                $repliasCount = isset($repliasCount[1]) ? $repliasCount[1] : 0;
                $replias = array();
                for ($z = 0; $z < $repliasCount; $z++) {
                    $repliaId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
                    $offset += 4;
                    $replias[] = $repliaId[1];
                }
                $isrCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
                $offset += 4;
                $isrCount = isset($isrCount[1]) ? $isrCount[1] : 0;
                $isrs = array();
                for ($z = 0; $z < $isrCount; $z++) {
                    $isrId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
                    $offset += 4;
                    $isrs[] = $isrId[1];
                }

                $partitions[$partitionId] = array(
                    'errCode'  => $partitionErrCode[1],
                    'leader'   => $leaderId[1],
                    'replicas' => $replias,
                    'isr'      => $isrs,
                );
            }
            $topic[$topicName]['partitions'] = $partitions;
        }

        $result = array(
            'brokers' => $broker,
            'topics'  => $topic,
        );
        return $result;
    }

    // }}}
    // {{{ public function offsetResponse()

    /**
     * decode offset response
     *
     * @param string $data
     * @access public
     * @return array
     */
    public function offsetResponse()
    {
        $result = array();
        $dataLen = self::unpack(self::BIT_B32, $this->stream->read(4, true));
        $dataLen = array_shift($dataLen);
        if (!$dataLen) {
            throw new \Kafka\Exception\Protocol('offset response invalid.');
        }
        $data = $this->stream->read($dataLen, true);
        $offset = 4;
        $topicCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $topicCount = array_shift($topicCount);
        for ($i = 0; $i < $topicCount; $i++) {
            $topicLen = self::unpack(self::BIT_B16, substr($data, $offset, 2)); // int16 topic name length
            $topicLen = isset($topicLen[1]) ? $topicLen[1] : 0;
            $offset += 2;
            $topicName = substr($data, $offset, $topicLen);
            $offset += $topicLen;
            $partitionCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
            $partitionCount = isset($partitionCount[1]) ? $partitionCount[1] : 0;
            $offset += 4;
            $result[$topicName] = array();
            for ($j = 0; $j < $partitionCount; $j++) {
                $partitionId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
                $offset += 4;
                $errCode     = self::unpack(self::BIT_B16, substr($data, $offset, 2));
                $offset += 2;
                $offsetCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
                $offset += 4;
                $offsetCount = array_shift($offsetCount);
                $offsetArr = array();
                for ($z = 0; $z < $offsetCount; $z++) {
                    $offsetArr[] = self::unpack(self::BIT_B64, substr($data, $offset, 8));
                    $offset += 8;
                }
                $result[$topicName][$partitionId[1]] = array(
                    'errCode' => $errCode[1],
                    'offset'  => $offsetArr
                );
            }
        }
        return $result;
    }

    // }}}
    // {{{ public function commitOffsetResponse()

    /**
     * decode commit offset response
     *
     * @param string $data
     * @access public
     * @return array
     */
    public function commitOffsetResponse()
    {
        $result = array();
        $dataLen = self::unpack(self::BIT_B32, $this->stream->read(4, true));
        $dataLen = array_shift($dataLen);
        if (!$dataLen) {
            throw new \Kafka\Exception\Protocol('commit offset response invalid.');
        }
        $data = $this->stream->read($dataLen, true);
        $offset = 4;
        $topicCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $topicCount = array_shift($topicCount);
        for ($i = 0; $i < $topicCount; $i++) {
            $topicLen = self::unpack(self::BIT_B16, substr($data, $offset, 2)); // int16 topic name length
            $topicLen = isset($topicLen[1]) ? $topicLen[1] : 0;
            $offset += 2;
            $topicName = substr($data, $offset, $topicLen);
            $offset += $topicLen;
            $partitionCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
            $partitionCount = isset($partitionCount[1]) ? $partitionCount[1] : 0;
            $offset += 4;
            $result[$topicName] = array();
            for ($j = 0; $j < $partitionCount; $j++) {
                $partitionId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
                $offset += 4;
                $errCode     = self::unpack(self::BIT_B16, substr($data, $offset, 2));
                $offset += 2;
                $result[$topicName][$partitionId[1]] = array(
                    'errCode' => $errCode[1],
                );
            }
        }
        return $result;
    }

    // }}}
    // {{{ public function fetchOffsetResponse()

    /**
     * decode fetch offset response
     *
     * @param string $data
     * @access public
     * @return array
     */
    public function fetchOffsetResponse()
    {
        $result = array();
        $dataLen = self::unpack(self::BIT_B32, $this->stream->read(4, true));
        $dataLen = array_shift($dataLen);
        if (!$dataLen) {
            throw new \Kafka\Exception\Protocol('fetch offset response invalid.');
        }
        $data = $this->stream->read($dataLen, true);
        $offset = 4;
        $topicCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        $offset += 4;
        $topicCount = array_shift($topicCount);
        for ($i = 0; $i < $topicCount; $i++) {
            $topicLen = self::unpack(self::BIT_B16, substr($data, $offset, 2)); // int16 topic name length
            $topicLen = isset($topicLen[1]) ? $topicLen[1] : 0;
            $offset += 2;
            $topicName = substr($data, $offset, $topicLen);
            $offset += $topicLen;
            $partitionCount = self::unpack(self::BIT_B32, substr($data, $offset, 4));
            $partitionCount = isset($partitionCount[1]) ? $partitionCount[1] : 0;
            $offset += 4;
            $result[$topicName] = array();
            for ($j = 0; $j < $partitionCount; $j++) {
                $partitionId = self::unpack(self::BIT_B32, substr($data, $offset, 4));
                $offset += 4;
                $partitionOffset = self::unpack(self::BIT_B64, substr($data, $offset, 8));
                $offset += 8;
                $metaLen = self::unpack(self::BIT_B16, substr($data, $offset, 2));
                $metaLen = array_shift($metaLen);
                $offset += 2;
                $metaData = '';
                if ($metaLen) {
                    $metaData = substr($data, $offset, $metaLen);
                    $offset += $metaLen;
                }
                $errCode = self::unpack(self::BIT_B16, substr($data, $offset, 2));
                $offset += 2;
                $result[$topicName][$partitionId[1]] = array(
                    'offset'   => $partitionOffset,
                    'metadata' => $metaData,
                    'errCode'  => $errCode[1],
                );
            }
        }
        return $result;
    }

    // }}}
    // {{{ public static function getError()

    /**
     * get error
     *
     * @param integer $errCode
     * @static
     * @access public
     * @return string
     */
    public static function getError($errCode)
    {
        $error = '';
        switch($errCode) {
            case 0:
                $error = 'No error--it worked!';
                break;
            case -1:
                $error = 'An unexpected server error';
                break;
            case 1:
                $error = 'The requested offset is outside the range of offsets maintained by the server for the given topic/partition.';
                break;
            case 2:
                $error = 'This indicates that a message contents does not match its CRC';
                break;
            case 3:
                $error = 'This request is for a topic or partition that does not exist on this broker.';
                break;
            case 4:
                $error = 'The message has a negative size';
                break;
            case 5:
                $error = 'This error is thrown if we are in the middle of a leadership election and there is currently no leader for this partition and hence it is unavailable for writes';
                break;
            case 6:
                $error = 'This error is thrown if the client attempts to send messages to a replica that is not the leader for some partition. It indicates that the clients metadata is out of date.';
                break;
            case 7:
                $error = 'This error is thrown if the request exceeds the user-specified time limit in the request.';
                break;
            case 8:
                $error = 'This is not a client facing error and is used only internally by intra-cluster broker communication.';
                break;
            case 10:
                $error = 'The server has a configurable maximum message size to avoid unbounded memory allocation. This error is thrown if the client attempt to produce a message larger than this maximum.';
                break;
            case 11:
                $error = 'Internal error code for broker-to-broker communication.';
                break;
            case 12:
                $error = 'If you specify a string larger than configured maximum for offset metadata';
                break;
            case 14:
                $error = 'The broker returns this error code for an offset fetch request if it is still loading offsets (after a leader change for that offsets topic partition).';
                break;
            case 15:
                $error = 'The broker returns this error code for consumer metadata requests or offset commit requests if the offsets topic has not yet been created.';
                break;
            case 16:
                $error = 'The broker returns this error code if it receives an offset fetch or commit request for a consumer group that it is not a coordinator for.';
                break;
            default:
                $error = 'Unknown error';
        }

        return $error;
    }

    // }}}
    // }}}
}
