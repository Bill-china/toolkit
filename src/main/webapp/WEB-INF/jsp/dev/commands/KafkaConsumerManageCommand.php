<?php
/**
 * kafka
 */
include __DIR__ . '/CommonCommand.php';

class KafkaConsumerManageCommand extends CommonCommand {
        
    public function actionGetAllProcessInfo()
    {
        $server_list = $this->_getSupervisorConf();
        if (! empty($server_list)) {
            foreach ($server_list as $node => $mac) {
                $response = $this->_req($node, 'getAllProcessInfo');
                $service = $server_list[$node]['service'];
                foreach ($response as $v){
                        $name = $v['name'];
                        if (substr($name,0,6)!="kafka_"){
                            continue;
                        }
                        $status = $v['state'];
                        /**
                          *  STOPPED (0)
                          *  STARTING (10)
                          *  RUNNING (20)
                          *  BACKOFF (30)
                          *  STOPPING (40)
                          *  EXITED (100)
                          *  FATAL (200)
                          *  UNKNOWN (1000)
                         */
                        if (isset($service[$name])){
                            
                            if ($status === 30 || $status === 200 || $status === 1000){
                                //todo alart
                                echo "start it:".$name."\n";
                                $result = $this->_req($node , 'startProcess' ,array($name , 1)) ;
                                var_dump($result);
                            }elseif ($status === 0 || $status === 40 || $status === 100){
                                //todo start it
                                echo "start it:".$name."\n";
                                $result = $this->_req($node , 'startProcess' ,array($name , 1)) ;
                                var_dump($result);
                            }
                        }elseif ($status === 20){
                            //todo stop it
                            $result = $this->_req($node , 'stopProcess' ,array($name , 1)) ;
                            var_dump($result);
                        }else{
                            echo "shutdown it:".$name." ".$status."\n";
                        }
                }
            }
        }
    }
    
    /**
     * 发送xmlrpc请求
     * @param  [type] $server [description]
     * @param  [type] $method [description]
     * @param  [type] $params [description]
     * @return [type]         [description]
     */
    protected function _req($server , $method , $params = array()){
        $data       = array() ;
        $server_info = $this->_getSupervisorConf($server) ;
        if(empty($server_info)){
            $data['error'] = "Invalid Server " . $server ;
            return $data ;
        }
        $url        = $server_info['host'] ;
        if(isset($server_info['port'])){
            $url   .= ":".$server_info['port'] ;
        }
        if(isset($server_info['url'])){
            $url   .= $server_info['url'] ;
        }
        $method     = "supervisor." . $method ;
        $request    = xmlrpc_encode_request($method , $params);
        $header[]   = "Content-type: text/xml";
        $header[]   = "Content-length: ".strlen($request);
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_TIMEOUT, 10);
        curl_setopt($ch, CURLOPT_HTTPAUTH, CURLAUTH_ANY);
        curl_setopt($ch, CURLOPT_USERPWD, "{$server_info['user']}:{$server_info['pass']}");
        curl_setopt($ch, CURLOPT_HTTPHEADER, $header);
        curl_setopt($ch, CURLOPT_POSTFIELDS, $request);
        $data = curl_exec($ch);
        if (curl_errno($ch)) {
            $data['error'] = curl_error($ch);
        } else {
            $data = xmlrpc_decode($data) ;
        }
        curl_close($ch);
        return $data ;
    }
    
    /**
     * 获取supervisor 服务器配置文件
     * @param  string $server 服务器ip
     * @return mixed         xmlrpc配置信息
     */
    private function _getSupervisorConf($server = ''){
        $result     = array() ;
        $config     = new CConfiguration(Yii::getPathOfAlias('application.config') . '/supervisor.php');
        $server_list= $config->toArray() ;
        if(!empty($server)){
            if(isset($server_list[$server])) $result = $server_list[$server] ;
        } else{
            $result = $server_list ;
        }
        return $result ;
    }
    }
?>
