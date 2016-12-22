<?php
/**
 * Author: Kangle.Liu - liukangle@360.cn
 *
 * Last modified: 2013-04-19 23:46
 *
 * Filename: DbConnection.php
 *
 * Description: 
 *
 */

class DbConnection extends CDbConnection
{
    public $type;

    //因为在__contruct()里取不到$this->type，所以把主要代码放到init()里了。
    public function __construct() {
        parent::__construct();
    }

    public function init() {
        $config = new CConfiguration(Yii::getPathOfAlias('application.config') . '/db.php');
		if (!$conf = $config->itemAt($this->type)) {
			$conf = $config->itemAt('db');
		}
        if (empty($conf)) {
            throw new Exception('db config error!');
        }
        foreach ($conf as $k => $v) {
            $this->$k = $v;
        }
        parent::init();
    }

}


