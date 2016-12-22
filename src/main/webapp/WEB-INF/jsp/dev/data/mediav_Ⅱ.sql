-- mediav 二期的数据库表设置
-- 201410201135-lp13dg-100-qihu.c.data
-- 存储mv传来的文件及计费情况，每天一个表，以 2014-11-20的数据来看，一天大概有三万多条记录
CREATE TABLE `mv_click_log` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `file_name` varchar(128) NOT NULL COMMENT '记录此条记录的文件名',
  `line_num` int(10) NOT NULL COMMENT '记录此条记录在文件内的行数',
  `ad_user_id` bigint(20) DEFAULT '0' COMMENT '用户id',
  `clicks` int(10) NOT NULL DEFAULT '0' COMMENT '点击数目',
  `req_price`  decimal(10, 2) NOT NULL COMMENT '请求扣费',
  `real_price` decimal(10, 2) NOT NULL COMMENT '实际扣费',
  `diff_type` int(10) DEFAULT '0' COMMENT '有差异的原因 0 正常 1 超时 2 无效(帐户不存在) 4 超用户预算 16 余额',
  `reduce_price` decimal(10, 2) NOT NULL COMMENT '计费校正 最终消费 real_price - reduce_price',
  `status` int(10) NOT NULL DEFAULT '0' COMMENT '预留字段',

  `create_time` datetime NOT NULL,
  `update_time` datetime NOT NULL,
  `deal_status` tinyint(4) DEFAULT '0' COMMENT '0 未结算未入表ad_click_log; 1 已结算已经入表ad_click_log',

  primary key `id` (`id`),
  UNIQUE INDEX `location` (`file_name`, `line_num`),
  index `ad_user_id` (`ad_user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `mv_stats` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `ad_user_id` bigint(20) DEFAULT '0' COMMENT '用户id',
  `views` int(10) DEFAULT '0' COMMENT '展现数',
  `clicks`  int(10) DEFAULT '0' COMMENT '点击数',
  `costs` decimal(10, 2) NOT NULL COMMENT '消费总额',

  `create_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '最后更新时间',

  primary key `id` (`id`),
  UNIQUE index `ad_user_id` (`ad_user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


-- 存储mv的文件状态
CREATE TABLE `mv_meta_files` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `file_name` varchar(128) NOT NULL COMMENT '记录此条记录的文件名',
  `status` int(10) NOT NULL COMMENT '0 未完成下载 1 对应data文件已下载完成，待处理 2 完成处理',

  `create_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '最后更新时间',

  index `status` (`status`),
  UNIQUE INDEX `file_name` (`file_name`),
  primary key `id` (`id`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- 存储 mv 的数据文件信息
CREATE TABLE `mv_data_files` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `meta_file_id` int(11) unsigned NOT NULL COMMENT '所属的meta file id',
  `file_name` varchar(128) NOT NULL COMMENT '记录此条记录的文件名',
  `type`  int(10) NOT NULL COMMENT '0 点击 1 展现',
  `md5sum` char(40) NOT NULL COMMENT 'md5值',
  `status` int(10) NOT NULL DEFAULT '0' COMMENT '0 未处理 1 已处理',

  `create_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '最后更新时间',

  primary key `id` (`id`),
  UNIQUE INDEX `file_name` (`file_name`),
  index `meta_id` (`meta_file_id`)
)ENGINE=InnoDB DEFAULT CHARSET=utf8;


