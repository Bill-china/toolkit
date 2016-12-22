--搜索意图
--ad_stats_area_click_XXX：
ALTER TABLE ad_stats_area_click_20160218 ADD COLUMN `area_type` TINYINT DEFAULT 1 COMMENT '地域类型:1地理,2意图';
DROP INDEX `idx_groupid_areaid` ON `ad_stats_area_click_20160218`;
CREATE UNIQUE INDEX `idx_groupid_areaid` ON `ad_stats_area_click_20160218` (`ad_group_id`,`area_fid`,`area_id`,`type`,`source_type`,`area_type`);
--esc_stats_area_click_XXX：
ALTER TABLE esc_stats_area_click_20160218 ADD COLUMN `area_type` TINYINT DEFAULT 1 COMMENT '地域类型:1地理,2意图';
DROP INDEX `idx_groupid_areaid` ON `esc_stats_area_click_20160218`;
CREATE UNIQUE INDEX `idx_groupid_areaid` ON `esc_stats_area_click_20160218` (`ad_group_id`,`area_fid`,`area_id`,`type`,`source_type`,`area_type`);

--凤舞二期
--ad_stats_keyword_click_xxxx
ALTER TABLE `ad_stats_keyword_click_20160120` ADD COLUMN `style_id` INT DEFAULT 0 COMMENT '样式类型:0非凤舞' AFTER `keyword`;
DROP INDEX `idx_groupid_keyword` ON `ad_stats_keyword_click_20160120`;
CREATE UNIQUE INDEX `idx_groupid_keyword` ON `ad_stats_keyword_click_20160120` (`ad_group_id`,`keyword`,`style_id`,`type`,`source_type`);
--esc_stats_keyword_click_xxxx
ALTER TABLE `esc_stats_keyword_click_20160120` ADD COLUMN `style_id` INT DEFAULT 0 COMMENT '样式类型:0非凤舞' AFTER `keyword`;
DROP INDEX `idx_groupid_keyword` ON `esc_stats_keyword_click_20160120`;
CREATE UNIQUE INDEX `idx_groupid_keyword` ON `esc_stats_keyword_click_20160120` (`ad_group_id`,`keyword`,`style_id`,`type`,`source_type`);

