<?php
/**
 * Author: 郭明瑞 <guomingrui@360.cn>
 * 实现高级别的消费者，只需要指定topic以及groupid，即可自动实现partition的均衡分配
 */
namespace Kafka;
include 'MyZookeeper.php';

class HighLevelConsumer {
  // broker_list, kafka broker list, 以逗号(,)分隔的ip:port列表
  // zookeeper_list, 以逗号分隔的ip:port列表，在最后可以加上路径前缀，eg: 127.0.0.1:2181/shgt
  public function __construct($broker_list, $zookeeper_list, $topic, $group_id) {
    $this->broker_list_ = $broker_list;
    $zk_path_prefix = strstr($zookeeper_list, '/');
    if ($zk_path_prefix) {
      $this->zk_path_prefix_ = $zk_path_prefix;
    }
    $zk_list = strstr($zookeeper_list, '/', $before_needle = true);
    if ($zk_list) {
      $this->zk_list_ = $zk_list;
    } else {
      $this->zk_list_ = $zookeeper_list;
    }
    $this->topic_ = $topic;
    $this->group_id_ = $group_id;
  }

  // 消费消息, 返回获取到的消息的数组
  // 1. 提交上次消费的partition的offset
  // 2. 检查consumer是否有变动，有的话，更新自己管理的partitions
  // 3. 轮询的从某个partition取消息进行消费
  public function consume() {
    static $first_consume = true;
    static $last_fetch_partition = -1;
    if ($first_consume) {
      $this->init();
      $first_consume = false;
    } else if ($this->auto_commit_offset_) {
      if ($last_fetch_partition != -1) {
        $current_time = time();
        if ($this->commit_offset_method_ == 2 ||
            !array_key_exists($last_fetch_partition, $this->last_offset_commit_time_) ||
            $current_time - $this->last_offset_commit_time_[$last_fetch_partition] > $this->commit_offset_interval_) {
          Log::write(Log::INFO, "commit offset now, par:$last_fetch_partition");
          $this->commitOffset($last_fetch_partition);
          $this->last_offset_commit_time_[$last_fetch_partition] = $current_time;
        }
      } else {
        Log::write(Log::INFO, "last fetch no message, do not commit offset");
      }
    }
    if (!$this->should_own_partitions_) {
      Log::write(Log::WARNING, "this process manages no partition, maybe the number of consumers is more than partitions");
      sleep($this->check_consumer_interval_);
      $this->checkConsumerChange();
      return array();
    }
    static $last_check_time = 0;
    $current_time = time();
    $should_own = array_diff($this->should_own_partitions_, $this->has_own_partitions_);
    if ($current_time - $last_check_time > $this->check_consumer_interval_ || $should_own) {
      $this->checkConsumerChange();
      $last_check_time = $current_time;
    }
    static $fetch_time = 0;
    $result = $this->fetchMessages($fetch_time, $last_fetch_partition);
    if (!$result) {
      Log::write(Log::INFO, "fetch no result");
      sleep(1);
    }
    return $result;
  }

  // 用于主动的修改某个partition的消费offset, 主要用于消息回放等一些特殊需求
  // 应用也可以在消费之后主动提交offset, 如果主动提交offset，最好设置auto_commit_offset选项为false
  // 注意：
  // 需要将该topic所有的消费者停掉以后，再进行修改，否则修改的值可能被其它消费者覆盖
  // 修改完以后，再启动消费者，将从新修改的offset处开始消费
  // 需要将修改offset的脚本与消费者脚本区别开来，否则每启动一个消费者，都会修改一下offset, 导致混乱
  public function sendCommitOffset($partition, $offset) {
    $myclient = null;
    if (array_key_exists($partition, $this->myclient_map_)) {
      $myclient = $this->myclient_map_[$partition];
    } else {
      $myclient = new \Kafka\MyClient($this->broker_list_, $this->topic_, $partition, $this->group_id_);
    }
    $ret = $myclient->sendCommitOffsetRequest($offset);
    while (!$ret) {
      Log::write(Log::ERROR, "commit offset failed, repeat to commit it");
      sleep(1);
      $ret = $myclient->sendCommitOffsetRequest($offset);
    }
  }
  
  // 设置析构时是否提交offset, 析构时默认是要提交offset的，除非主动设置为不提交
  // true 提交，false不提交。
  public function setWhenExitCommitOffset($is_commit_offset) {
    $this->when_exit_commit_offset_ = $is_commit_offset;
  }

  public function setParameter($key, $value) {
    if ($key == "check_consumer_interval") {
      $this->check_consumer_interval_ = $value;
    } else if ($key == "commit_offset_interval") {
      $this->commit_offset_interval_ = $value;
    } else if ($key == "commit_offset_method") {
      $this->commit_offset_method_ = $value;
    } else if ($key == "auto_commit_offset") {
      $this->auto_commit_offset_ = $value;
    } else {
      Log::write(Log::ERROR, "not supported parameter:$key");
      $this->showParameters();
      exit;
    }
  }

  public function showParameters() {
    printf("the supported parameters are as follows:\n".
      "check_consumer_interval: check consumer change interval(s), default 10\n".
      "commit_offset_interval: commit offset interval(s), default 10\n".
      "commit_offset_method: commit method, by time(1) or each fetch(2), default 1\n".
      "auto_commit_offset: commit offset automatically(true or false), default true\n");
  }

  public function __destruct() {
    if (!$this->when_exit_commit_offset_) {
      return;
    }
    foreach ($this->myclient_map_ as $par => $myclient) {
      $this->commitOffset($par);
    }
  }

  private function commitOffset($par) {
    $offset = $this->offset_array_[$par];
    Log::write(Log::NOTICE, "topic:{$this->topic_},partition:{$par},auto commit offset to:{$offset}");
    $ret = $this->myclient_map_[$par]->sendCommitOffsetRequest($offset);
    while (!$ret) {
      Log::write(Log::ERROR, "commit offset failed, repeat to commit");
      sleep(1);
      $ret = $this->myclient_map_[$par]->sendCommitOffsetRequest($offset);
    }
  }

  private function fetchMessages(&$fetch_time, &$last_fetch_partition) {
    $i = 0;
    $result = array();
    $client_count = count($this->myclient_map_);
    foreach ($this->myclient_map_ as $par => $myclient) {
      if ($fetch_time % $client_count != $i++) {
        continue;
      }
      $result = $myclient->sendFetchRequest($this->offset_array_[$par],$error_code);

      //当当前offset超出有效范围时,自动设置为最老有效offset
      //一般不会出现这种情况，除非消费端长时间未运行，物理数据被kafka自动删除造成 或 offset设置错误造成
      if ($error_code == 1){
          $offsetInfo = $myclient->sendOffsetRequest(-2);
          if (is_numeric($offsetInfo[0])){
              $ret = $myclient->sendCommitOffsetRequest($offsetInfo[0]);
              if ($ret){
                  Log::write(Log::WARNING, "topic:{$this->topic_},partition:{$this->partition_} offset is invalid,auto set to {$offsetInfo[0]}");
                  $this->offset_array_[$par] = $offsetInfo[0];
              }
              
          }
      }
      
      $fetch_time++;
      if ($result) {
        $this->offset_array_[$par] += count($result);
        $last_fetch_partition = $par; 
        break;
      }
    }
    if (!$result) {
      $last_fetch_partition = -1;
    }
    return $result;
  }

  private function checkConsumerChange() {
    $this->should_own_partitions_ = $this->getConsumePartitions();
    $should_abandon = array_diff($this->has_own_partitions_, $this->should_own_partitions_);
    if ($should_abandon) {
      $this->unOwnerRegister($should_abandon);
      $this->unOwnPartitions($should_abandon);
    }
    $should_own = array_diff($this->should_own_partitions_, $this->has_own_partitions_);
    if ($should_own) {
      $own_par = $this->ownRegister($should_own);
      $this->ownPartitions($own_par);
    }
  }

  private function ownPartitions($partition_array) {
    foreach ($partition_array as $partition) {
      $this->has_own_partitions_[] = $partition;
      $this->myclient_map_[$partition] = new \Kafka\MyClient($this->broker_list_,
          $this->topic_, $partition, $this->group_id_);
      $errno = -1;
      $offset = $this->myclient_map_[$partition]->sendFetchOffsetRequest($errno);
      while ($errno != 0) {
        if ($errno == 3) {
          Log::write(Log::WARNING, "no conumser info, topic:$this->topic_,partition:$partition,group:$this->group_id_");
          $result = $this->myclient_map_[$partition]->sendOffsetRequest(-2);
          while (!$result) {
            Log::write(Log::ERROR, "sendOffsetRequest failed, repeat to send");
            sleep(1);
            $result = $this->myclient_map_[$partition]->sendOffsetRequest(-2);
          }
          $this->offset_array_[$partition] = $result[0];
          break;
        }
        Log::write(Log::ERROR, "sendFetchOffsetRequest failed, repeat to fetch offset");
        sleep(1);
        $offset = $this->myclient_map_[$partition]->sendFetchOffsetRequest($errno);
      }
      if ($errno == 0) {
        $this->offset_array_[$partition] = $offset;
      }
    }
  }

  private function unOwnPartitions($partition_array) {
    foreach ($partition_array as $par) {
      unset($this->myclient_map_[$par]);
      $index = array_search($par, $this->has_own_partitions_);
      array_splice($this->has_own_partitions_, $index, 1);
    }
  }

  private function init() {
    $this->zk_client_ = new \MyZookeeper($this->zk_list_);
    $this->registerConsumer();
    $this->should_own_partitions_ = $this->getConsumePartitions();
    $owned_partitions = $this->ownRegister($this->should_own_partitions_);
    $this->ownPartitions($owned_partitions);
  }

  private function unOwnerRegister($partition_array) {
    $dir = $this->zk_path_prefix_ . "/consumers/" . $this->group_id_ . "/owner/".$this->topic_."/";
    foreach ($partition_array as $partition) {
      // 首先提交offset, 然后再放弃对该partition的所有权，避免其他进程获取该partition时重复消费
      $this->commitOffset($partition);
      unset($this->last_offset_commit_time_[$partition]);
      $path = $dir.$partition;
      $value = $this->zk_client_->get($path);
      // 确确实实获取到一个存在的value，但是这个value不是本进程，那么不删除该节点
      // 可能的一种情况是，这个节点中途因为网络等原因中途消失了，而后被另外的节点抢占了
      // 此时如需放弃该节点，就不能删除现在已经属于其他进程的节点了
      if ($value && $value != $this->consumer_id_) {
        Log::write(Log::WARNING, "value $path is not myself:$this->consumer_id_");
        continue;
      }
      $ret = false;
      $ret = $this->zk_client_->delete($path);
      while (!$ret) {
        if (!$this->zk_client_->exists($path)) {
          break;
        }
        sleep(1);
        Log::write(Log::ERROR, "delete $path failed");
        $ret = $this->zk_client_->delete($path);
      }
    }
  }

  // Partition owner registry
  // 返回本次已经添加的partition
  private function ownRegister($partition_array) {
    $dir = $this->zk_path_prefix_ . "/consumers/" . $this->group_id_ . "/owner/".$this->topic_."/";
    $owned_array = array();
    foreach ($partition_array as $partition) {
      $path = $dir.$partition;
      if ($this->zk_client_->exists($path)) {
        Log::write(Log::WARNING, "$this->consumer_id_ want to own $path, but it be owned by other now");
        continue;
      } else {
        $ret = null;
        $ret = $this->zk_client_->create($path, $this->consumer_id_, $params = array(), $flags = \Zookeeper::EPHEMERAL);
        if (!$ret) {
          Log::write(Log::ERROR, "create path $path failed");
        } else {
          $owned_array[] = $partition;
        }
      }
    }
    return $owned_array;
  }

  // 得到本进程该消费的所有partitions
  private function getConsumePartitions() {
    $partition_path = $this->zk_path_prefix_."/brokers/topics/".$this->topic_."/partitions";
    $all_partitions = $this->zk_client_->getChildren($partition_path);
    if (!$all_partitions) {
      Log::write(Log::ERROR, "get all_partitions failed or topic not exists");
      return array();
    }
    sort($all_partitions);
    $partition_count = count($all_partitions);
    $consumer_path = $this->zk_path_prefix_."/consumers/".$this->group_id_."/ids";
    $all_consumers = $this->zk_client_->getChildren($consumer_path);
    if (!$all_consumers) {
      Log::write(Log::ERROR, "get all_consumers failed");
      return array();
    }
    $myself = array_search($this->consumer_id_, $all_consumers);
    if ($myself === false) {
      Log::write(Log::ERROR, "consumer $this->consumer_id_ not exist now");
      $this->registerConsumer();
      $all_consumers = $this->zk_client_->getChildren($consumer_path);
      if (!$all_consumers) {
        Log::write(Log::ERROR, "get all_consumers failed");
        return array();
      }
    }
    $consumer_count = count($all_consumers); 
    $par_each_con = 1;
    if ($consumer_count == 1) {
      return $all_partitions;
    }
    sort($all_consumers);
    $pos = array_search($this->consumer_id_, $all_consumers);
    if ($consumer_count > $partition_count) {
      if ($pos >= $partition_count) {
        return array();
      } else {
        return array($all_partitions[$pos]);
      }
    }
    $par_each_con = 1;
    if ($consumer_count < $partition_count) {
      $par_each_con = floor($partition_count * 1.0 / $consumer_count);
    }
    $rest_partition = $partition_count - $par_each_con * $consumer_count;
    if ($pos < $rest_partition) {
      return array_slice($all_partitions, $pos * ($par_each_con + 1), $par_each_con + 1);
    } else {
      return array_slice($all_partitions, $pos * $par_each_con + $rest_partition, $par_each_con);
    }
    Log::write(Log::ERROR, "getConsumePartitions failed");
    return array();
  }

  private function registerConsumer() {
    $pid = getmypid();
    $hostname = php_uname('n');
    $ip = gethostbyname($hostname);
    $this->consumer_id_ = $ip."_".$pid;
    $path = $this->zk_path_prefix_."/consumers/".$this->group_id_."/ids/".$this->consumer_id_;
    $ret = '';
    $ret = $this->zk_client_->set($path, $this->topic_, $params = array(), $flags = \Zookeeper::EPHEMERAL);
    while ($ret != $path) {
      Log::write(Log::ERROR, "register myself failed");
      sleep(1);
      $ret = $this->zk_client_->set($path, $this->topic_, $params = array(), $flags = \Zookeeper::EPHEMERAL);
    }
  }
  
  private $zk_list_; // 以逗号分隔的ip:port列表
  private $zk_path_prefix_ = ''; // 该kafka集群使用的节点列表可能有前缀
  private $broker_list_; // kafka broker list, 以逗号分隔的ip:port列表
  private $topic_;
  private $group_id_;
  private $zk_client_;
  private $consumer_id_;
  private $myclient_map_ = array(); // 每个partition的client
  private $should_own_partitions_ = array(); // 当前应该管理的所有partition
  private $has_own_partitions_ = array(); // 当前已经管理的所有partition
  private $offset_array_ = array(); // 当前各个partition 的offset
  private $last_offset_commit_time_ = array(); // 每个partition的上次offset提交时间
  private $check_consumer_interval_ = 10; // 检查consumer是否有变动的时间间隔(s)，减小对zookeeper的负载压力
  private $commit_offset_interval_ = 10; // 提交offset的时间间隔(s), 
  private $commit_offset_method_ = 1; // 提交方式，默认按时间间隔提交, 2表示每次fetch之后提交
  private $auto_commit_offset_ = true;  // 默认自动提交offset
  private $when_exit_commit_offset_ = true; // 默认正常析构时，提交offset, 如果设置为false, 那么析构时不提交offset
}

?>

