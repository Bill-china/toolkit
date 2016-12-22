<?php
$file = "/data/log/stats_pv/pv.guess." . date('YmdHi', time() - 120);
$rh = fopen($file, 'r');
$redisHost1 = '10.115.112.50';
$redisHost2 = '10.115.105.54';
$redisHost3 = '10.115.105.55';
$redisPortArr = array(17001, 17002, 17003, 17004, 17005, 17006, 17007, 17008);
$redisArr = array();
foreach ($redisPortArr as $key => $port) {
    if ($key < 4)
        $redisHost = $redisHost1;
    elseif ($key < 6)
        $redisHost = $redisHost2;
    elseif ($key < 8)
        $redisHost = $redisHost3;
    $redisArr[$key] = new redis();
    $redisArr[$key]->connect($redisHost, $port);
}
$i = 0;
$redisKey = 'open_ad_v1:stats:';
while (!feof($rh)) {
    $str = trim(fgets($rh));
    $arr = explode("\t", $str);
    if (!$arr || count($arr) != 3)
        continue;
    $data = json_decode($arr[2], true);
    $row = array('isArray' => 1);
    if (!isset($data['ads']) || !is_array($data['ads']))
        continue;
    foreach ($data['ads'] as $key => $values) {
        foreach ($values as $value) {
            $row['data'][] = array(
                'view_id' => $data['pvid'],
                'ip' => $data['ip'],
                'type' => 'view',
                'now' => $data['now'],
                'apitype' => $data['apitype'],
                'pid' => isset($value['pid']) ? $value['pid'] : '',
                'gid' => isset($value['gid']) ? $value['gid'] : '',
                'aid' => $value['aid'],
                'uid' => $value['uid'],
                'price' => $value['cprice'],
                'mid' => $data['mid'],
                'city_id' => $data['province'] . ',' . $data['city'],
                'tag_id' => $key,
                'channel_id' => $data['channel'],
                'place_id' => $data['place'],
                'ver' => 'guess',
            );
        }
    }
    $j = $i++ % 8;
    for ($k = 0; $k < 4; $k++) {
        if ($k == 3) {
            print_r(json_encode($row) . "\n");
            break;
        }
        if ($j < 4)
            $redisHost = $redisHost1;
        elseif ($j < 6)
            $redisHost = $redisHost2;
        elseif ($j < 8)
            $redisHost = $redisHost3;
        $res = $redisArr[$j]->rpush($redisKey, json_encode($row));
        if ($res)
            break;
        $redisArr[$j]->connect($redisHost, $portArr[$j]);
    }
}
echo date('Y-m-d H:i:s') . "\tok\n";
return ;
