<?php

class Config
{
    private static $config;
    private static $adminConf;
    public static function item($name)
    {
        if (!self::$config) {
            self::$config = new CConfiguration(Yii::getPathOfAlias('application.config') . '/config.php');
            return self::$config->itemAt($name);
        }
        else {
            return self::$config->itemAt($name);
        }
    }
    public static function adminItem($name, $admin)
    {
        if (!self::$adminConf) {
            self::$adminConf = new CConfiguration(Yii::getPathOfAlias('application.config') . '/adminConfig.php');
            return self::$adminConf->itemAt($name);
        }
        else {
            return self::$adminConf->itemAt($name);
        }
    }
}
