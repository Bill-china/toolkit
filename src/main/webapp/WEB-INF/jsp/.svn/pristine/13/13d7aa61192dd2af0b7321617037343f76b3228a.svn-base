<?php
/**
 * author renyajun@360.cn
 */
class ErrorHandler extends CErrorHandler
{
    public  function handle($event)
    {   
    	restore_error_handler();
		restore_exception_handler();
		if($event instanceof CExceptionEvent)
		{
			$exception=$event->exception;
			
			if(is_callable(array('Utility','sendAlert')) && strpos($exception->getTraceAsString(),'missingAction')===false && strpos($exception->getMessage(),'无法解析请求')===false)
			{
				Utility::sendAlert(__CLASS__,"Exception",$exception->getFile()." (".$exception->getLine().") \n".$exception->getMessage()."\n".$exception->getTraceAsString());
			}			
		}
		else
		{
			if(is_callable(array('Utility','sendAlert')))
			{
				$trace=debug_backtrace();
				// skip the first 3 stacks as they do not tell the error position
				if(count($trace)>3)
					$trace=array_slice($trace,3);
				$traceString='';
				foreach($trace as $i=>$t)
				{
					if(!isset($t['file']))
						$trace[$i]['file']='unknown';

					if(!isset($t['line']))
						$trace[$i]['line']=0;

					if(!isset($t['function']))
						$trace[$i]['function']='unknown';

					$traceString.="#$i {$trace[$i]['file']}({$trace[$i]['line']}): ";
					if(isset($t['object']) && is_object($t['object']))
						$traceString.=get_class($t['object']).'->';
					$traceString.="{$trace[$i]['function']}()\n";

					unset($trace[$i]['object']);
				}
				Utility::sendAlert(__CLASS__,"Error",$event->file." (".$event->line .") \n".$event->message."\n".$traceString);
			}
		}
		//sleep(1);
		
    }   

}