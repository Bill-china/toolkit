<?php
/**
 * 线上线下一样的配置
 */
return array(
    'logDir'                => '/data/log/statsLog/',
    'redisKey'              => 'open_ad_v1:',
    'cassandra_db'          => 'ad_360_cn',
    'statsLog'              => '/data/log/statsLog/',
    'statsCollectLog'       => '/data/log/stats_collect_log/',
    'statsNeighborViewLog'  => '/data/log/stats_neighbor_view/',
    'statsBiYiViewLog'      => '/data/log/stats_biyi_view/',
    'statsDataDir'          => '/data/log/stats_data/',//给hadoop端生成的stats表数据文件存放位置
    'statlogend'            => '/data/log/e/stats/',
    'budgetLogPath'         => '/data/log/statsLog/budget/',
    'budgetLockPath'        => '/data/log/statsLog/budget/lock/',

    /****************************
     *      mediav相关配置      *
     ****************************/
    //dsp消费同步url
    'mediav_dsp_url' => 'https://api-prod-360.mediav.com/qihu/dspConsumeInMax?format=json',
    // mediav 二期相关配置
    // mv_stats_{data} 表的展现数据文件存储路径
    'mediav_view_second'    => '/data/stor/stats/mediav_view/',
    // mv_stats_{data} 表的点击数据文件存储路径
    'mediav_click_second'   => '/data/stor/stats/mediav_click/',
    // mv_click_log_{date} 表的展现数据文件存储路径
    'mediav_click_log'      => '/data/stor/stats/mediav_click_log/',
    // 计费后的结果文件存储路径
    'mediav_click_res'      => '/data/log/mediav/click_res/',

    // mediav 预算百分比
    'mediav_rate'           => 4,
    // 下载文件的url
    'mediav_url'            => 'http://mediav%3AMediaV%40%21%40%23123@report.dl.mediav.com/qihu/',
    // 'mediav_url'            => 'http://report.dl.mediav.com/qihu/',
    // 存储从mediav 下载下来的数据 meta 和 xz file
    'mediav_log_path'       => '/data/log/mediav/download/',
    // 解压后的点击日志
    'mediav_click_path'     => '/data/log/mediav/click/',
    // 解压后的展现日志
    'mediav_view_path'      => '/data/log/mediav/views/',
    // mediav 结算的实时日志
    'mediav_cost_file_path' => '/data/log/mediav/costs/',
    // 存放回传给mediav的数据
    'mediav_send_data_ptah' => '/data/log/mediav/send_mediav_data/',
    // 存放回传给mediav的历史数据
    'mediav_send_data_his'  => '/data/log/mediav/send_mediav_data_his/',
    // 锁文件存放
    'mediav_lock_path'      => '/data/log/mediav/lock/',
    // 分析后的地域入库数据
    'mediav_area'           => '/data/stor/stats/mediav_area/',
    // 分析后的点击日志入库数据
    'mediav_click'          => '/data/stor/stats/mediav_clicklog/',
    // 分析后的创意入库数据
    'mediav_stats'          => '/data/stor/stats/mediav_creative/',

    'statsServerRoom'   => array(
        'shgt',
        'zwt'
    ),
    'stats_data_dir'                => '/data/stor/stats/',
);

