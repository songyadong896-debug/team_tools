-- 路线表：存储旅游线路的基本信息
CREATE TABLE `routes` (
                          `id` int unsigned NOT NULL  AUTO_INCREMENT  COMMENT '路线ID',
                          `name` varchar(100)  NOT NULL COMMENT '路线名称',
                          `color` varchar(20)  DEFAULT '#002D28' COMMENT '路线在地图上的显示颜色',
                          `distance` decimal(20,2) DEFAULT '0' COMMENT '路线总距离（米）',
                          `duration` int DEFAULT '0' COMMENT '路线总时长（秒）',
                          `path` json DEFAULT NULL COMMENT '路线路径坐标数组 [[lng,lat],...]',
                          `segments` json DEFAULT NULL COMMENT '路线分段信息（包含每段的距离、时长、路径等）',
                          `created_at` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                          `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                          PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='旅游路线表';

-- 沿途点表：存储路线的必经点（参与路线规划）
CREATE TABLE `waypoints` (
                             `id` int unsigned NOT NULL AUTO_INCREMENT COMMENT '沿途点ID',
                             `route_id` int NOT NULL COMMENT '所属路线ID',
                             `order_num` int NOT NULL COMMENT '序号（决定导航顺序）',
                             `city` varchar(50)  DEFAULT NULL COMMENT '城市名称',
                             `name` varchar(100)  NOT NULL COMMENT '地点名称（用户输入）',
                             `gaode_name` varchar(100)  DEFAULT NULL COMMENT '高德地图匹配的标准名称',
                             `lng` decimal(20,6)  DEFAULT NULL COMMENT '经度',
                             `lat` decimal(20,6)  DEFAULT NULL COMMENT '纬度',
                             `matched` tinyint(1) DEFAULT '0' COMMENT '是否成功匹配到高德POI（0:否,1:是）',
                             `confidence` decimal(3, 2) DEFAULT '0' COMMENT '匹配置信度（0-1）',
                             PRIMARY KEY (`id`),
                             KEY `idx_route_id` (`route_id`),
                             KEY `idx_waypoints_id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='路线沿途点表（参与导航）';

-- 景点表：存储路线相关的景点（仅展示，不参与路线规划）
CREATE TABLE `attractions` (
                               `id` int unsigned NOT NULL AUTO_INCREMENT COMMENT '景点ID',
                               `route_id` int NOT NULL COMMENT '所属路线ID',
                               `city` varchar(50)  DEFAULT NULL COMMENT '城市名称',
                               `name` varchar(100)  NOT NULL COMMENT '景点名称（用户输入）',
                               `gaode_name` varchar(100)  DEFAULT NULL COMMENT '高德地图匹配的标准名称',
                               `lng` decimal(20,6)  DEFAULT NULL COMMENT '经度',
                               `lat` decimal(20,6)  DEFAULT NULL COMMENT '纬度',
                               `matched` tinyint(1) DEFAULT '0' COMMENT '是否成功匹配到高德POI（0:否,1:是）',
                               `confidence` decimal(3,2) DEFAULT '0' COMMENT '匹配置信度（0-1）',
                               PRIMARY KEY (`id`),
                               KEY `idx_route_id` (`route_id`),
                               KEY `idx_attractions_id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='路线景点表（仅展示）';

-- 场站表：存储路线相关的场站（仅展示，不参与路线规划）
CREATE TABLE `stations` (
                            `id` int unsigned NOT NULL AUTO_INCREMENT COMMENT '场站ID',
                            `route_id` int NOT NULL COMMENT '所属路线ID',
                            `city` varchar(50) DEFAULT NULL COMMENT '城市名称',
                            `name` varchar(100) NOT NULL COMMENT '场站名称（用户输入）',
                            `gaode_name` varchar(100) DEFAULT NULL COMMENT '高德地图匹配的标准名称',
                            `lng` decimal(20,6)  DEFAULT NULL COMMENT '经度',
                            `lat` decimal(20,6)  DEFAULT NULL COMMENT '纬度',
                            `matched` tinyint(1) DEFAULT '0' COMMENT '是否成功匹配到高德POI（0:否,1:是）',
                            `confidence` decimal(3,2) DEFAULT '0' COMMENT '匹配置信度（0-1）',
                            PRIMARY KEY (`id`),
                            KEY `idx_route_id` (`route_id`),
                            KEY `idx_stations_id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='路线场站表（仅展示）';

-- 地图配置表：存储高德地图API密钥
CREATE TABLE `map_config` (
                              `id` int unsigned NOT NULL AUTO_INCREMENT COMMENT '配置ID',
                              `amap_key` varchar(100)  DEFAULT NULL COMMENT '高德地图API Key',
                              `amap_security_code` varchar(100)  DEFAULT NULL COMMENT '高德地图安全密钥',
                              `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                              PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='地图配置表';
CREATE TABLE `dashboard_summary` (
                                     `id` int unsigned NOT NULL AUTO_INCREMENT COMMENT '主键ID',
                                     `sheet_name` varchar(100) DEFAULT NULL COMMENT 'Excel表单名称',
                                     `metric_category` varchar(100) DEFAULT NULL COMMENT '指标类别',
                                     `metric_name` varchar(100) DEFAULT NULL COMMENT '指标名称',
                                     `metric_value` varchar(100) DEFAULT NULL COMMENT '指标值',
                                     `target_value` varchar(100) DEFAULT NULL COMMENT '目标值',
                                     `unit` varchar(20) DEFAULT NULL COMMENT '单位',
                                     `extra_data` json DEFAULT NULL COMMENT '额外列数据（JSON格式）',
                                     `update_date` date DEFAULT NULL COMMENT '数据更新日期',
                                     `version` int DEFAULT '1' COMMENT '版本号',
                                     `dt` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '数据时间',
                                     `created_at` timestamp DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                                     PRIMARY KEY (`id`),
                                     KEY `idx_sheet` (`sheet_name`),
                                     KEY `idx_date` (`update_date`),
                                     KEY `idx_dt` (`dt`),
                                     KEY `idx_version` (`version`),
                                     KEY `idx_date_version` (`update_date`,`version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='理想汽车充电网络运营看板数据汇总表';
