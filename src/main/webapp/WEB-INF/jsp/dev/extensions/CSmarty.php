<?php
//file:webapp/protected/extensions/CSmarty.php
define('SMARTY_SPL_AUTOLOAD', true);
define('SMARTY_DIR', Yii::getPathOfAlias('application.extensions.smarty') . DIRECTORY_SEPARATOR);
require_once(SMARTY_DIR . 'Smarty.class.php');
define('SMARTY_VIEW_DIR', Yii::getPathOfAlias('application') . '/../tmp/');
class CSmarty extends Smarty
{
	const DS = DIRECTORY_SEPARATOR;
	function __construct() 
	{
		parent::__construct();
		$this->template_dir = SMARTY_VIEW_DIR;
		$this->compile_dir = SMARTY_VIEW_DIR . self::DS . '_template_c' . self::DS;
		$this->caching = false;
		$this->cache_dir = SMARTY_VIEW_DIR . self::DS . '_cache' . self::DS;
		$this->config_dir = Yii::getPathOfAlias('application.config');
		$this->left_delimiter  =  '<!--{';
		$this->right_delimiter =  '}-->';
		$this->cache_lifetime = 3600;
		$this->debugging = false;
		$this->use_sub_dirs = true;
		$this->use_include_path = true;
        $this->error_reporting = E_ALL & ~E_NOTICE;
	}

	function init(){
		spl_autoload_unregister('smartyAutoload');
		Yii::registerAutoloader('smartyAutoload');
	}

	public function view($template, $data = NULL, $cache_id = NULL)
	{
		if (!$this->isCached($template, $cache_id)) {
			if (is_array($data)) {
				foreach($data as $key => $value) {
					$this->smarty->assign($key, $value);
				} //end foreach
			}
		}
		$this->smarty->display($template, $cache_id);
	}
}

