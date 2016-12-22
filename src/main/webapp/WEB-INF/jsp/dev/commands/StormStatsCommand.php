<?php
class StormStatsCommand extends CConsoleCommand {

    private $child_pids=array();//子进程id

    /**
     * 定时从storm聚合redis拿数据
     * @param  integer $sid [第几个redis，从0开始]
     * @author jingguangwen@360.cn  2016.1.22
     */
    public function actionMultiGetStats($sid=0)
    {
        set_time_limit(0);

        $taskName = sprintf("[MultiGetStats_%s]", date('YmdHis'));
        $beginTime = date('Y-m-d H:i:s');
        printf("task %s begin at %s\n", $taskName, $beginTime);

        $sid = intval($sid);
        //文件锁，确保单进程模式
        $lockDir = Config::item('stats_data_dir');
        $lock_file = $lockDir.'storm_stats_get_'.$sid.'.lock';
        $lock_fp = fopen($lock_file,'w');
        if(!flock($lock_fp, LOCK_NB | LOCK_EX)) {
            printf("task %s lock fail, quit\n", $taskName);
            return ;
        }

        $storm_redis_info = Yii::app()->params['storm_redis_info'];

        //redis数量
        $redis_num      = $storm_redis_info['redis_num'];
        if($sid>=$redis_num){
            printf("task %s sid error, quit\n", $taskName);
            return ;
        }
        //聚合时间
        $merge_time     = $storm_redis_info['merge_time'];
        //可以处理的时间段必须小于这个
        $time_now = floor(date('YmdHis')/$merge_time)*$merge_time;

        $deal_key = "";
        $can_deal_key_arrs = array(

        );


        $redis = new ComRedis('storm_stats', $sid);
        $keys=$redis->keys("storm:*");
        if(empty($keys))
        {
            printf("task %s storm:* empty, quit\n", $taskName);
            return;
        }
        foreach($keys as $k)
        {
                //$k = "rmq:".$this->init_param.":".$send_data['exchange']
                $key_info=explode(":",$k);
                $key_type= $key_info[1];//类型
                $key_time= $key_info[2];//时间段

                if($key_time>=$time_now){
                    continue;
                }
                $can_deal_key_arrs[] = array(
                    'key'=> $k,
                    'key_type'=> $key_type,
                    'num'=> $sid,
                );


        }

        //redis连接关闭
        $redis->close();

        if(!empty($can_deal_key_arrs)){
            foreach ($can_deal_key_arrs as $arr) {
                $key = $arr['key'];
                $key_type = $arr['key_type'];
                $num = $arr['num'];

                $deal_key .= $key.";";
                $this->childGetStats($key,$key_type,$num);
            }
        }

        foreach($this->child_pids as $pid=>$t)
        {
            pcntl_waitpid($pid, $status,WUNTRACED);//等待退出
        }

        flock($lock_fp, LOCK_UN);
        fclose($lock_fp);

        printf("task %s end at %s, deal_key %s success\n", $taskName, date('Y-m-d H:i:s'), $deal_key);


    }

    protected function childGetStats($k,$key_type,$redis_num)
    {
        $pid=pcntl_fork();
        if($pid<0)
        {
            echo "[".date('Y-m-d H:i:s')."] StormStats::MultiGetStats PID_".getmypid()." fork error\n";
            exit();
        }
        else if($pid==0)
        {

            if(empty($k)||empty($key_type)){
                exit();
            }

            $count = 0;
            $data_dir   = Config::item('stats_data_dir').$key_type;
            if(!is_dir($data_dir)){
                mkdir($data_dir, 0777);
            }
            $filename = 'statsLog_'.$k.'_'.date('YmdHis').'_'.$redis_num;
            $f = $data_dir . '/' . $filename;
            $resFile = fopen($f, 'a');

            //delay 目录
            $delay_count = 0;
            $delay_data_dir   = Config::item('stats_data_dir').'delay';
            if(!is_dir($delay_data_dir)){
                mkdir($delay_data_dir, 0777);
            }
            $delay_filename = 'statsLog_'.$k.'_'.$key_type.'_'.date('YmdHis').'_'.$redis_num;
            $delay_f = $delay_data_dir . '/' . $delay_filename;
            $delay_res_file = fopen($delay_f, 'a');


            $redis = new ComRedis('storm_stats', $redis_num);

            //改为hsacn
            $it = null;
            $redis->setOption(Redis::OPT_SCAN, Redis::SCAN_RETRY);
            while($arr_keys = $redis->hscan($k, &$it,'',5000)) {
                foreach($arr_keys as $hkey => $content_json) {
                    $content_arr = json_decode($content_json,true);
                    if(!empty($content_arr)){
                        //判断时间，如果昨天的数据在第二天凌晨以后过来了，则不能再入db，直接写delay.data
                            $content_date = $content_arr['_table_'];
                            $today_date = date('Ymd');
                            //延迟数据
                            if(($today_date > $content_date) && intval(date('H')) > 1 ){

                                fwrite($delay_res_file, json_encode($content_arr) . "\n");
                                $delay_count++;
                            } else {

                                //根据类型写不同目录文件
                                fwrite($resFile, json_encode($content_arr) . "\n");
                                $count++;
                            }

                    }
                    $redis->hdel($k,$hkey);
                }
                $it = null;
            }

            fflush($resFile);
            fclose($resFile);

            if($count==0){
                @unlink($f);
            } else {
                touch($f.'.ok');
            }

            if($delay_count==0){
                @unlink($delay_f);
            }
            //再次查询，如果没有hkeys，则删除对应的key
            $hkeys=$redis->hkeys($k);
            if(empty($hkeys)){
                $redis->del($k);
            }

            echo "[".date('Y-m-d H:i:s')."] StormStats::MultiGetStats PID_".getmypid()." child ".getmypid()." exited;Success deal count {$count};delay_count {$delay_count}\n";
            //posix_kill(getmypid(), 9);
            exit();
        }
        else
        {

            $this->child_pids[$pid]=time();
            echo "[".date('Y-m-d H:i:s')."] StormStats::MultiGetStats PID_".getmypid()." create child $pid \n";
        }
    }
}

