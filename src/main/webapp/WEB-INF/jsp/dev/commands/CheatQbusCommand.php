<?php
include __DIR__ . '/CommonCommand.php';
Yii::import('application.extensions.qbus.*');
class CheatQbusCommand extends CommonCommand {

	public $consumerLog = '/dev/shm/qbus_consumer.log';

	public function actionQbusConsumer($cluster='shbt_priv_sffzb_1',$topic='filterOut2ESC',$group='esc'){

		try{
			$consumer = new QbusConsumer;
			$ret = $consumer->init($cluster, $this->consumerLog,'qbus_consumer.config');
			if ($ret == false){
				throw new Exception("QbusConsumer Failed init");
			}
			$ret = $consumer->subscribeOne($group, $topic);
			if ($ret == false){
				throw new Exception("QbusConsumer Failed subscribe");
			}

			$run=true;
			//kill 信号捕获，正常退出
			pcntl_signal(SIGTERM, function ($signal) {
				global $run;
            	$run=false;
			});

			$consumer->start();
			$msg_info = new QbusMsgContentInfo;

			while ($run)
			{
				if ($consumer->consume($msg_info))
				{
					//echo "topic: ".$msg_info->topic." | msg: ".$msg_info->msg."\n";

					$clickId = $msg_info->msg;
					Utility::log(__CLASS__, __FUNCTION__, $clickId);
					if(!$clickId){
						Utility::log(__CLASS__, __FUNCTION__, "clickId error:".$clickId);
						continue;
					}

					$date = date("Y-m-d");
					//连接修改为每次的短连接
					$cnn = Yii::app()->db_click_log;
                    $click_id = $cnn->quoteValue($clickId);
					$trs = $cnn->beginTransaction();
					$has_commit=0;
					$click=ComAdDetail::queryBySql("select * from click_detail where click_id={$click_id} for update",time(),'row');
					if(!$click){
						$date=date("Y-m-d",strtotime("-1 days"));
						$click=ComAdDetail::queryBySql("select * from click_detail where click_id={$click_id} for update",strtotime("-1 days"),'row');
					}
					if(!$click){
						Utility::log(__CLASS__,"_clickIdSubscriber_no_clickid",$click_id);
					}else{
						//esc计费产品线支持 20160412 jingguangwen add
						$product_line_info = Utility::getProductLineInfo($click,1);
						if(empty($product_line_info)){
							Utility::log(__CLASS__,"ERROR",$click['click_id']." get product_line_info err");
							@$trs->commit();
							return;
						}
						$cheat_price = 0;
						$ret=array();
						$need_search_quota = 0;//传递引擎数据是否需要重新查询quota

						if($click['cheat_type']!=0  || $click['deal_status'] !=0 ||  $click['price'] <= $click['reduce_price'])
						{
							//说明已经处理完成，或者没必要处理
							Utility::log(__CLASS__,"_clickIdSubscriber_no_need",$click);
							$need_search_quota = 1;
						} else{
							//开始计算费用,需退款金额
							$cheat_price = $amount = round($click['price']-$click['reduce_price'], 2);

							$ret=ComQuota::cost((int)$click['ad_user_id'],(int)$click['ad_plan_id'],(0-$amount),(int)$click['click_time'],$click['click_id'],($click['ver']=='mediav'?'mv':'dj'),$click['ver'],$product_line_info);
							Utility::log(__CLASS__,"CHEAT_RET",array($click['click_id'],$ret));

							$reduce_price=$click['reduce_price']-$ret['real_cost'];//之前减少的，加上现在减少的金额
							ComAdDetail::updateDetail(array('status'=>2,'update_time' => time(),'reduce_price'=>$reduce_price,'cheat_type'=>3),$click['click_id'], strtotime($date));

							$mqData = array(
									'ad_user_id'    => $click['ad_user_id'],
									'data'          => array(
										'ad_user_id'      => $click['ad_user_id'],
										'ad_plan_id'      => $click['ad_plan_id'],
										'amount'          => $reduce_price,
										'ver'             => $click['ver'],
										'create_time'     => $click['click_time'],
										'click_id'        => $click['click_id'],
										'cheat_exec_time' => date('Y-m-d H:i:s'),
										'product_line'    => $product_line_info['product_line'],
										),
								       );

							//给消息基线发上下线消息
							//充值和反作弊上线判断消费和限额，达到限额的产品线不发上线
							$costInfo = ComQuota::getUserCost($click['ad_user_id']);
							if (empty($costInfo)) {
								$errorInfo = "uid:{$click['ad_user_id']} getUserCost failed\n";
								Utility::log(__CLASS__,__FUNCTION__,$errorInfo."\tclickId:\t".$click_id);
							}else{
								$productLine = ComMsg::setProductLineOnlineData($costInfo,$mqData['data']['product_line']);
							}

							//如果查库异常，默认发上线
							if(false === $productLine){
								$productLine = array(1,2,3,4);
							}
							//事务提交
							@$trs->commit();
							$has_commit=1;

							$mqData['ad_user_id'] = $click['ad_user_id'];
							$mqData['product_line_arr'] = $productLine;
							$sData = ComMsg::setCheatMsgData($mqData,1,$ret,$click);
							ComMsg::sendMsg(1,$ret,$sData,$click['ad_user_id']);
							//给消息基线发上下线消息结束
						}

						$click['cost_info']=$ret;
						$click['cheat_info']= array(
								'cheat_price'       => $cheat_price,
								'cheat_deal_time'   => time(),
								'need_search_quota' => $need_search_quota,
								);
						$click['product_line_info']= $product_line_info;
						//发送处理各维度消息
						static $emq2;
						$emq2 = new ComEMQ('emq_esc');
						$emq2->exchangeName = "cheat_dimension_reduce";
						$emq2->logid = Utility::$logid;
						$id=$emq2->send($click);
						Utility::log(__CLASS__,"_clickIdSubscriber_done",array($click['click_id'],$id));
						//QBUS数据源
						$comBineLog = date('YmdHis') . "\t" . "esc_cheat" . "\t" . json_encode($click);
						$qbus_file_name = "esc_cheat.combineLog.".date('YmdH');
						ComAdLog::write($comBineLog, '/data/log/e/qbus/'.$qbus_file_name);

					}
					if($has_commit==0){
						@$trs->commit();
					}
                    Yii::app()->db_click_log->setActive(false);
                    Yii::app()->db_quota->setActive(false);
				}else{
					sleep(1);
				}
				pcntl_signal_dispatch(); // 接收到信号时，调用注册的signalHandler()
			}
			$consumer->stop();

		}catch (Exception $ex){
			Utility::sendAlert(__CLASS__,__FUNCTION__,"qbusConsumer fail:".$ex->getMessage()."\t".$ex->getTraceAsString());
			sleep(1);
		}
	}
}
