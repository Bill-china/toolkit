<?php
/*
 * 管理数据库连接的
 */
class DbConnectionManager {

    protected static $_dbConnections    = array();

    protected static $_dbConf           = array();

    public static function init () {
        if (empty(self::$_dbConf)) {
            self::$_dbConf = require dirname(__FILE__).'/../config/db.php';
        }
        return true;
    }

    public static function getStatBranchDB ($dbid, $isPrivate=false) {
        $name = 'db_stat_'.$dbid;
        return self::getDB($name, $isPrivate);
    }

    public static function getRateBranchDB ($dbid, $isPrivate=false) {
        $name = 'db_rate_'.$dbid;
        return self::getDB($name, $isPrivate);
    }

    public static function getDjCenterDB($isPrivate=false) {
        $name = 'db_center';
        return self::getDB($name, $isPrivate);
    }
    public static function getClickLogDB($isPrivate=false) {
        $name = 'click_log';
        return self::getDB($name, $isPrivate);
    }
    public static function getDjBranchDB($dbID, $isPrivate=false) {
        $name = 'dj_bran_'.$dbID;
        return self::getDB($name, $isPrivate);
    }
    public static function getAreakwDB($dbID, $isPrivate=false) {
        $name = 'db_areakw_'.$dbID;
        return self::getDB($name, $isPrivate);
    }
    public static function getMaterialDB($isPrivate=false) {
        $name = 'ad_material';
        return self::getDB($name, $isPrivate);
    }

    public static function getDB ($name='db', $isPrivate=false) {
        self::init();
        try {
            if ($isPrivate) {
                return self::_getRealDB($name);
            } else {
                if (!isset(self::$_dbConnections[$name])) {
                    self::$_dbConnections[$name] = self::_getRealDB($name);
                }
                return self::$_dbConnections[$name];
            }
        } catch (Exception $e) {
            echo "get db [".$name."] fail, err [".$e->getMessage()."]\n";
            return false;
        }
    }

    protected static function _getRealDB ($name) {
        if (!isset(self::$_dbConf[$name])) {
            throw new Exception("has no db config of [".$name."]");
        }
        $_tmpConf   = self::$_dbConf[$name];
        $_tmpDb     = new CDbConnection();
        foreach ($_tmpConf as $k => $v) {
            $_tmpDb->$k = $v;
        }
        $_tmpDb->init();
        return $_tmpDb;
    }
}
/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
