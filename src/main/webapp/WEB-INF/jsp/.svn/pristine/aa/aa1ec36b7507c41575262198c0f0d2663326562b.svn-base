<?php
/**
 * ComAdLog
 *
 * 
 * @package open 360
 * @version v1
 * @copyright 2005-2011 360.CN All Rights Reserved.
 * @author wangguoqiang@360.cn 
 */
class ComAdLog
{
	protected $_strFile;
	protected $_arrCache;
	protected $_strFieldSep = "\t";
	protected $_strLineSep = "\n";
	
	public function __construct($strFile) {
		$this->_strFile = $strFile;
		Yii::app()->attachEventHandler('onEndRequest',array($this,'flush'));
	}
	
	public function add($params) {
		$this->_arrCache[] = $params;
		if (count($this->_arrCache) > 50) {
			$this->flush();
		}
	}
	
	public function flush() {
		if (empty($this->_arrCache)) {
			return true;
		}
		foreach ($this->_arrCache as &$v) {
			if (is_array($v)) {
				$v = implode($this->_strFieldSep, $v);
			}			
		}
		unset($v);
		$strTmp = implode($this->_strLineSep, $this->_arrCache);
		$resFile = self::_openFile($this->_strFile);
		if (! $resFile) {
			return false;
		}
		$bolRet = self::_writeFile($resFile, "{$strTmp}{$this->_strLineSep}");
		if ($bolRet) {
			$this->_arrCache = null;
		}
		return $bolRet;
	}
	
	protected static function _writeFile($resFile, $strContent, $intCount = 1) {
		if (flock($resFile, LOCK_EX)) {
			fwrite($resFile, $strContent);
			flock($resFile, LOCK_UN);
			fclose($resFile);
			return true;
		}
		
		fclose($resFile);
		if ($intCount > 3) {
			return false;
		}
		$intCount++;
		return self::_writeFile($resFile, $strContent, $intCount);
	}
	
	protected static function _openFile($strFile) {
		if ($strFile[0] == '/') {
			$fp = fopen($strFile, 'a');
		} elseif (defined(LOG_DEBUG)) {
			$fp = fopen(Yii::app()->runtimePath . '/' . $strFile, 'a');
		} else {
			$tmp = explode('_', $strFile);
			$fp = fopen('/dev/shm/e_' . $tmp[0], 'a');
		}
		return $fp;		
	}
	
    public static function write($params, $fileName = NULL, $splitChar = "\t")
    {
        $line = $params;
        if (is_array($params)) {
            $line = implode($splitChar, $params);
        }
        if ($fileName == NULL) {
            $fileName = date('ymd-H') . '.log';
        }

        $fp = self::_openFile($fileName);
        if ($fp) {
        	self::_writeFile($fp, "$line\n");
        }
    }

    public static function combineLog($content)
    {
        $fileName = '/dev/shm/combineLog';
        ComAdLog::write($content, $fileName);
    }

    public function __destruct() {
    	if (! empty($this->_arrCache)) {
    		$this->flush();
    	}
    }
}
