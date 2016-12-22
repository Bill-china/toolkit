#!/bin/bash
# 监控3小时前的比翼展现数据文件是否生成
# 部署在 /opt/run/ 路径下，每小时的 10 分钟运行
# 10 * * * * /opt/run/moniter_biyi_view_data_file.sh > /dev/null 2>&1

function sendsms()
{
    smsToken='3be54884918ab235a3ceae5dda9a01d3';
    smsSource='dev';
    smsRoleName='dev_sms_com';
    smsApi='http://mc.ad.360.cn/api.php';
    smsSign=`echo -e -n "${smsToken}|${smsRoleName}" | md5sum |awk '{print $1}'`;
    curl -d "source=dev&sign=${smsSign}&roleName=${smsRoleName}&smsMobile=$1&smsContent=$2"  $smsApi;
}

machine='10.115.112.107'
dateDir='/data/log/stats_biyi_view/'
# dateDir='/home/gongwei-sal/test/'

curTime=`/bin/date +'%Y-%m-%d %H:%M:%S'`
# /data/log/stats_neighbor_view/2014041403.log
dataFile=${dateDir}`/bin/date +'%Y%m%d%H' -d'-3 hours'`.log
# 需要通知的人员 目前是宫伟 井光文
mobileList='13581937912 18611794976'

if [ ! -e "${dataFile}" ]; then
    content="at the time ${curTime}, datefile[${dataFile}] not exists at machine[${machine}]!"
    # echo $content
    for mobile in $mobileList ; do
        sendsms ${mobile} "${content}"
    done
elif [[ ! -s "${dataFile}" ]]; then
    content="at the time ${curTime}, datefile[${dataFile}] at machine[${machine}] is empty!"
    # echo $content
    for mobile in $mobileList ; do
        sendsms ${mobile} "${content}"
    done
fi



