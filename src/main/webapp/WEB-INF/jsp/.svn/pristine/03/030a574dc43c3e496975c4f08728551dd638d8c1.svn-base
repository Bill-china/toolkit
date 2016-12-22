<?php
error_reporting(E_ALL);
ini_set('display_errors',1);
if(isset($_REQUEST['xhprof_debug']) && in_array('xhprof',get_loaded_extensions())){
    ob_start();
    xhprof_enable(XHPROF_FLAGS_CPU + XHPROF_FLAGS_MEMORY);
    function xhprof_shutdown(){
        $xhprof_root     = dirname(__FILE__) . '/tools/xhprof';
        $xhprof_root_url = 'http://' .$_SERVER['HTTP_HOST'] . '/tools/xhprof/xhprof_html';
        include_once $xhprof_root . "/xhprof_lib/utils/xhprof_lib.php";
        include_once $xhprof_root . "/xhprof_lib/utils/xhprof_runs.php";
        $xhprof_data=xhprof_disable();
        $xhprof_runs = new XHProfRuns_Default();
        $run_id       = $xhprof_runs->save_run($xhprof_data, 'xhprof');
        ob_flush();
        echo "---------------\n".
        "you can view profile data run at :\n".
        "<a href='" . $xhprof_root_url . "/index.php?run=$run_id" .
        "'" . " target='_blank'>" . $xhprof_root_url . 
        "/index.php?run=$run_id</a>\n".
        "---------------\n";
    }
    register_shutdown_function('xhprof_shutdown');
}
