<?php

return array(
    'basePath'      => dirname(__FILE__) . DIRECTORY_SEPARATOR . '..',
    'name'          => 'dianjing stats',
    'runtimePath'   => dirname(__FILE__) . DIRECTORY_SEPARATOR . '../../tmp/_runtime',
    // preloading 'log' component
    'preload'   => array('log'),
    // autoloading model and component classes
    'import'    => array(
        'application.models.*',
        'application.components.*',
    ),
    'language' => 'zh_cn',
    'modules'  => array(),
    // application components
    'components' => array(
        'urlManager' => array(
            'urlFormat' => 'path',
            'rules' => array(
                '<controller:\w+>/<action:\w+>' => '<controller>/<action>',
            ),
        ),
        'errorHandler' => array(
            // use 'site/error' action to display errors
            'class' => 'application.extensions.ErrorHandler',
        ),
        'db_monitor'  => array(
            'class'     => 'application.extensions.DbConnection',
            'type'      => 'db_monitor',
        ),
        'db_stats'  => array(
            'class'     => 'application.extensions.DbConnection',
            'type'      => 'db_stats',
        ),
        'db_stats_shgt' => array(
            'class' => 'application.extensions.DbConnection',
            'type'  => 'db_stats',
        ),
        'db_ad_advert_stats' => array(
            'class' => 'application.extensions.DbConnection',
            'type'  => 'db_advert_stats',
        ),
        'db_ad_plan_stats' => array(
            'class' => 'application.extensions.DbConnection',
            'type'  => 'db_plan_stats',
        ),
        'db_ad_group_stats' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_group_stats',
        ),
        'db_ad_stats_report_advert' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_stats_report_advert',
        ),
        //新创意表配置
        'db_ad_stats_advert_report' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_stats_report_advert',
        ),
        //比翼表配置
        'db_ad_stats_biyi_report' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_stats_biyi_report',
        ),
        'db_ad_stats_report_channel' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_stats_report_channel',
        ),
        'db_ad_stats_report_group' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_stats_report_group',
        ),
        //新组表配置
        'db_ad_stats_group_report' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_stats_report_group',
        ),
        'db_ad_stats_report_plan' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_stats_report_plan',
        ),
        //新计划表配置
        'db_ad_stats_plan_report' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_stats_report_plan',
        ),
        'db_ad_stats_report_plan_channel' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_stats_report_plan_channel',
        ),
        'db_ad_stats_report_user' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_stats_report_user',
        ),
        'db_ad_stats_area_report_group' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_stats_area_report_group',
        ),
        //新地域组表配置
         'db_ad_stats_area_group_report' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_stats_area_report_group',
        ),
        'db_ad_stats_area_report_plan' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_stats_area_report_plan',
        ),
        //新地域计划表配置
        'db_ad_stats_area_plan_report' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_stats_area_report_plan',
        ),
        'db_ad_stats_area_report_province' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_stats_area_report_province',
        ),
        'db_ad_stats_interest_report' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_stats_interest_report',
        ),
        'db_ad_stats_keyword_report' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_stats_keyword_report',
        ),
        //地域关键词库0
        'db_report_0' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_report_0',
        ),
        //地域关键词库1
        'db_report_1' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_report_1',
        ),
        //地域关键词库2
        'db_report_2' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_report_2',
        ),
        //地域关键词库3
        'db_report_3' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_report_3',
        ),
        //新关键表配置
        'db_ad_stats_report_keyword' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_stats_keyword_report',
        ),
        'db_book_report' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_book_report',
        ),
        'db_center' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_center',
        ),
		'db_material' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'ad_material',
        ),
        // 点击日志
        'db_click_log' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'click_log',
        ),
        // front召回日志
        'db_front_log' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'front_log',
        ),
        'db_click_log_slave' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'click_log_slave',
        ),
        'db_stat_1' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_stat_1',
        ),
        'db_stat_2' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_stat_2',
        ),
        'db_stat_3' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_stat_3',
        ),
        'db_stat_4' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_stat_4',
        ),
		'db_userlog' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'userlog',
        ),

        'db_areakw_1' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_areakw_1',
        ),
        'db_areakw_2' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_areakw_2',
        ),
        'db_areakw_3' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_areakw_3',
        ),
        'db_areakw_4' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'db_areakw_4',
        ),
        'db_quota' => array(
             'class' => 'application.extensions.DbConnection',
             'type' => 'ad_quota',
         ),
        'log' => array(
            'class' => 'CLogRouter',
            'routes' => array(
                array(
                    'class' => 'CFileLogRoute',
                    'levels' => 'error, warning, info',
                ),
            ),
        ),
        'smsclient' => array(
            'class' => 'application.extensions.CSmsclient',
        ),
        'redis' => array(
            'class' => 'application.extensions.CRedis',
        ),
        'redis_ad_guess' => array(
            'class' => 'application.extensions.CRedis',
            'host' => 'ad_guess',
        ),
        'loader' => array(
            'class' => 'application.extensions.CLoader',
        ),
        'cassandra' => array(
            'class' => 'application.extensions.CCassandra',
        ),
        'curl' => array(
            'class' => 'application.extensions.Curl',
            'options' => array(
                'timeout' => 60,
                'setOptions' => array(
                    CURLOPT_USERAGENT => 'open_bianxian/curl',
                ),
            ),
        ),
        'db_quota' => array(
            'class' => 'application.extensions.DbConnection',
            'type' => 'ad_quota',
            ),
        'db_rate_1' => array(
                'class' => 'application.extensions.DbConnection',
                'type' => 'db_rate_1',
                ),
        'db_rate_2' => array(
                'class' =>
                'application.extensions.DbConnection',
                'type' => 'db_rate_2',
                ),
        'db_rate_3' => array(
                'class' => 'application.extensions.DbConnection',
                'type' => 'db_rate_3',
             ),
        'db_rate_4' => array(
                'class' => 'application.extensions.DbConnection',
                'type' => 'db_rate_4',
             ),
    ),
    'params' => include "params.php",
);

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
