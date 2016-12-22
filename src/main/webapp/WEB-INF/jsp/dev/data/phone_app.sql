-- 手机 app 项目的数据表

-- ad_click_log 表中的 sub_ver 字段保存手助广告的 app_id
-- ad_click_log 表中 ver 为 'shouzhu' 的记录为手助广告

-- app统计表
drop table IF EXISTS `ad_app`;

CREATE TABLE `ad_app` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,

  `ad_user_id` bigint(20) DEFAULT '0' COMMENT '用户id',
  `ad_plan_id` int(11) NOT NULL COMMENT '广告计划id',
  `app_id` int(11) NOT NULL COMMENT 'app id',
  `ad_group_id` int(11) NOT NULL COMMENT '广告组id',
  `area_fid` int(11) NOT NULL COMMENT '省级id',
  `area_id` int(11) NOT NULL COMMENT '城市id',
  `area_key` varchar(32) NOT NULL COMMENT '城市 geo id',

  `views` int(11) DEFAULT '0' COMMENT '展现数',
  `clicks` int(11) DEFAULT '0' COMMENT '点击数',
  `costs` decimal(10,2) NOT NULL COMMENT '消费总额',
  `real_costs` decimal(10,2) NOT NULL DEFAULT '0.00' COMMENT '结算后的消费总额',

  `create_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '最后更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_gid_area` (`ad_group_id`, `area_fid`, `area_id`),
  KEY `idx_user` (`ad_user_id`)

) ENGINE=InnoDB DEFAULT CHARSET=utf8;

