SET SEARCH_PATH=datax_web,public;
-- ----------------------------
-- Table structure for job_group
-- ----------------------------
DROP TABLE IF EXISTS job_group;
CREATE TABLE job_group (
  id bigint NOT NULL,
  app_name varchar(64) NOT NULL,
  title varchar(32) NOT NULL,
  "order" bigint NOT NULL DEFAULT 0,
  address_type integer NOT NULL DEFAULT 0,
  address_list varchar(512) NULL DEFAULT NULL,
  PRIMARY KEY (id)
);

CREATE SEQUENCE job_group_id_seq START WITH 2 INCREMENT BY 1 NO minvalue NO maxvalue CACHE 1;

ALTER TABLE job_group ALTER COLUMN id SET DEFAULT nextval('job_group_id_seq');

comment on column job_group.app_name is '执行器AppName';
comment on column job_group.title is '执行器名称';
comment on column job_group.order is '排序';
comment on column job_group.address_type is '执行器地址类型：0=自动注册、1=手动录入';
comment on column job_group.address_list is '执行器地址列表，多地址逗号分隔';

-- ----------------------------
-- Records of job_group
-- ----------------------------
INSERT INTO job_group VALUES (1, 'datax-executor', 'datax执行器', 1, 0, NULL);



-- ----------------------------
-- Table structure for job_info
-- ----------------------------
DROP TABLE IF EXISTS job_info;
CREATE TABLE job_info (
  id bigint NOT NULL,
  job_group bigint NOT NULL,
  job_cron varchar(128) NULL,
  job_desc varchar(255) NOT NULL,
  add_time time NULL DEFAULT NULL,
  update_time timestamp NULL DEFAULT NULL,
  user_id bigint NOT NULL,
  alarm_email varchar(255) NULL DEFAULT NULL,
  executor_route_strategy varchar(50) NULL DEFAULT NULL,
  executor_handler varchar(255) NULL DEFAULT NULL,
  executor_param varchar(512) NULL DEFAULT NULL,
  executor_block_strategy varchar(50) NULL DEFAULT NULL,
  executor_timeout bigint NOT NULL DEFAULT 0,
  executor_fail_retry_count bigint NOT NULL DEFAULT 0,
  glue_type varchar(50) NOT NULL,
  glue_source text NULL,
  glue_remark varchar(128) NULL DEFAULT NULL,
  glue_updatetime timestamp NULL DEFAULT NULL,
  child_jobid varchar(255) NULL DEFAULT NULL,
  trigger_status integer NOT NULL DEFAULT 0,
  trigger_last_time numeric(13,0) NOT NULL DEFAULT 0,
  trigger_next_time numeric(13,0) NOT NULL DEFAULT 0,
  job_json text NULL,
  PRIMARY KEY (id)
);

CREATE SEQUENCE job_info_id_seq START WITH 1 INCREMENT BY 1 NO minvalue NO maxvalue CACHE 1;

ALTER TABLE job_info ALTER COLUMN id SET DEFAULT nextval('job_info_id_seq');

comment on column job_info.job_group is '执行器主键ID';
comment on column job_info.job_cron is '任务执行CRON';
comment on column job_info.user_id is '修改用户';
comment on column job_info.alarm_email is '报警邮件';
comment on column job_info.executor_route_strategy is '执行器路由策略';
comment on column job_info.executor_handler is '执行器任务handler';
comment on column job_info.executor_param is '执行器任务参数';
comment on column job_info.executor_block_strategy is '阻塞处理策略';
comment on column job_info.executor_timeout is '任务执行超时时间，单位秒';
comment on column job_info.executor_fail_retry_count is '失败重试次数';
comment on column job_info.glue_type is 'GLUE类型';
comment on column job_info.glue_source is 'GLUE源代码';
comment on column job_info.glue_remark is 'GLUE备注';
comment on column job_info.glue_updatetime is 'GLUE更新时间';
comment on column job_info.child_jobid is '子任务ID，多个逗号分隔';
comment on column job_info.trigger_status is '调度状态：0-停止，1-运行';
comment on column job_info.trigger_last_time is '上次调度时间';
comment on column job_info.trigger_next_time is '下次调度时间';
comment on column job_info.job_json is 'datax运行脚本';


-- ----------------------------
-- Table structure for job_jdbc_datasource
-- ----------------------------
DROP TABLE IF EXISTS job_jdbc_datasource;
CREATE TABLE job_jdbc_datasource  (
  id bigint NOT NULL,
  datasource_name varchar(200) NOT NULL,
  datasource_group varchar(200) NULL DEFAULT 'Default',
  jdbc_username varchar(100) NULL DEFAULT NULL ,
  jdbc_password varchar(200) NULL DEFAULT NULL,
  jdbc_url varchar(500) NOT NULL,
  jdbc_driver_class varchar(200) NULL DEFAULT NULL,
  status numeric(1,0) NOT NULL DEFAULT 1,
  create_by varchar(20) NULL DEFAULT NULL,
  create_date timestamp NULL DEFAULT CURRENT_TIMESTAMP(0),
  update_by varchar(20) NULL DEFAULT NULL,
  update_date timestamp NULL DEFAULT NULL,
  comments varchar(1000) NULL DEFAULT NULL,
  PRIMARY KEY (id)
);

CREATE SEQUENCE job_jdbc_datasource_id_seq START WITH 1 INCREMENT BY 1 NO minvalue NO maxvalue CACHE 1;

ALTER TABLE job_jdbc_datasource ALTER COLUMN id SET DEFAULT nextval('job_jdbc_datasource_id_seq');

comment on column job_jdbc_datasource.id is '自增主键';
comment on column job_jdbc_datasource.datasource_name is '数据源名称';
comment on column job_jdbc_datasource.datasource_group is '数据源分组';
comment on column job_jdbc_datasource.jdbc_username is '用户名';
comment on column job_jdbc_datasource.jdbc_password is '密码';
comment on column job_jdbc_datasource.jdbc_url is 'jdbc url';
comment on column job_jdbc_datasource.jdbc_driver_class is 'jdbc驱动类';
comment on column job_jdbc_datasource.status is '状态：0删除 1启用 2禁用';
comment on column job_jdbc_datasource.create_by is '创建人';
comment on column job_jdbc_datasource.create_date is '创建时间';
comment on column job_jdbc_datasource.update_by is '更新人';
comment on column job_jdbc_datasource.update_date is '更新时间';
comment on column job_jdbc_datasource.comments is '备注';



-- ----------------------------
-- Table structure for job_lock
-- ----------------------------
DROP TABLE IF EXISTS job_lock;
CREATE TABLE job_lock  (
  lock_name varchar(50) NOT NULL,
  PRIMARY KEY (lock_name)
);

comment on column job_lock.lock_name is '锁名称';

-- ----------------------------
-- Records of job_lock
-- ----------------------------
INSERT INTO job_lock VALUES ('schedule_lock');



-- ----------------------------
-- Table structure for job_log
-- ----------------------------
DROP TABLE IF EXISTS job_log;
CREATE TABLE job_log  (
  id bigint NOT NULL,
  job_group bigint NOT NULL,
  job_id bigint NOT NULL,
  job_desc varchar(255) NULL DEFAULT NULL,
  executor_address varchar(255) NULL DEFAULT NULL,
  executor_handler varchar(255) NULL DEFAULT NULL,
  executor_param varchar(512) NULL DEFAULT NULL,
  executor_sharding_param varchar(20) NULL DEFAULT NULL,
  executor_fail_retry_count bigint NULL DEFAULT 0,
  trigger_time timestamp NULL DEFAULT NULL,
  trigger_code bigint NOT NULL,
  trigger_msg text NULL,
  handle_time timestamp NULL DEFAULT NULL,
  handle_code bigint NOT NULL,
  handle_msg text NULL,
  alarm_status integer NOT NULL DEFAULT 0,
  process_id varchar(20) NULL DEFAULT NULL,
  max_id bigint NULL DEFAULT NULL,
  PRIMARY KEY (id)
);

CREATE SEQUENCE job_log_id_seq START WITH 1 INCREMENT BY 1 NO minvalue NO maxvalue CACHE 1;

ALTER TABLE job_log ALTER COLUMN id SET DEFAULT nextval('job_log_id_seq');

CREATE INDEX I_trigger_time ON job_log (trigger_time);
CREATE INDEX I_handle_code ON job_log (handle_code);

comment on column job_log.job_group is '执行器主键ID';
comment on column job_log.job_id is '任务，主键ID';
comment on column job_log.executor_address is '执行器地址，本次执行的地址';
comment on column job_log.executor_handler is '执行器任务handler';
comment on column job_log.executor_param is '执行器任务参数';
comment on column job_log.executor_sharding_param is '执行器任务分片参数，格式如 1/2';
comment on column job_log.executor_fail_retry_count is '失败重试次数';
comment on column job_log.trigger_time is '调度-时间';
comment on column job_log.trigger_code is '调度-结果';
comment on column job_log.trigger_msg is '调度-日志';
comment on column job_log.handle_time is '执行-时间';
comment on column job_log.handle_code is '执行-状态';
comment on column job_log.handle_msg is '执行-日志';
comment on column job_log.alarm_status is '告警状态：0-默认、1-无需告警、2-告警成功、3-告警失败';
comment on column job_log.process_id is 'datax进程Id';
comment on column job_log.max_id is '增量表max id';

-- ----------------------------
-- Table structure for job_log_report
-- ----------------------------
DROP TABLE IF EXISTS job_log_report;
CREATE TABLE job_log_report  (
  id bigint NOT NULL,
  trigger_day timestamp NULL DEFAULT NULL,
  running_count bigint NOT NULL DEFAULT 0,
  suc_count bigint NOT NULL DEFAULT 0,
  fail_count bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (id)
);

CREATE SEQUENCE job_log_report_id_seq START WITH 28 INCREMENT BY 1 NO minvalue NO maxvalue CACHE 1;

ALTER TABLE job_log_report ALTER COLUMN id SET DEFAULT nextval('job_log_report_id_seq');

CREATE INDEX i_trigger_day ON job_log_report (trigger_day);

comment on column job_log_report.trigger_day is '调度-时间';
comment on column job_log_report.running_count is '运行中-日志数量';
comment on column job_log_report.suc_count is '执行成功-日志数量';
comment on column job_log_report.fail_count is '执行失败-日志数量';


-- ----------------------------
-- Records of job_log_report
-- ----------------------------
INSERT INTO job_log_report VALUES (20, '2019-12-07 00:00:00', 0, 0, 0);
INSERT INTO job_log_report VALUES (21, '2019-12-10 00:00:00', 77, 52, 23);
INSERT INTO job_log_report VALUES (22, '2019-12-11 00:00:00', 9, 2, 11);
INSERT INTO job_log_report VALUES (23, '2019-12-13 00:00:00', 9, 48, 74);
INSERT INTO job_log_report VALUES (24, '2019-12-12 00:00:00', 10, 8, 30);
INSERT INTO job_log_report VALUES (25, '2019-12-14 00:00:00', 78, 45, 66);
INSERT INTO job_log_report VALUES (26, '2019-12-15 00:00:00', 24, 76, 9);
INSERT INTO job_log_report VALUES (27, '2019-12-16 00:00:00', 23, 85, 10);



-- ----------------------------
-- Table structure for job_logglue
-- ----------------------------
DROP TABLE IF EXISTS job_logglue;
CREATE TABLE job_logglue  (
  id bigint NOT NULL,
  job_id bigint NOT NULL,
  glue_type varchar(50) NULL DEFAULT NULL,
  glue_source text NULL,
  glue_remark varchar(128) NOT NULL,
  add_time timestamp NULL DEFAULT NULL,
  update_time timestamp NULL DEFAULT NULL,
  PRIMARY KEY (id)
);

CREATE SEQUENCE job_logglue_id_seq START WITH 1 INCREMENT BY 1 NO minvalue NO maxvalue CACHE 1;

ALTER TABLE job_logglue ALTER COLUMN id SET DEFAULT nextval('job_logglue_id_seq');

comment on column job_logglue.job_id is '任务，主键ID';
comment on column job_logglue.glue_type is 'GLUE类型';
comment on column job_logglue.glue_source is 'GLUE源代码';
comment on column job_logglue.glue_remark is 'GLUE备注';



-- ----------------------------
-- Table structure for job_registry
-- ----------------------------
DROP TABLE IF EXISTS job_registry;
CREATE TABLE job_registry  (
  id bigint NOT NULL,
  registry_group varchar(50) NOT NULL,
  registry_key varchar(191) NOT NULL,
  registry_value varchar(191) NOT NULL,
  update_time timestamp NULL DEFAULT NULL,
  PRIMARY KEY (id)
);

CREATE SEQUENCE job_registry_id_seq START WITH 1 INCREMENT BY 1 NO minvalue NO maxvalue CACHE 1;

ALTER TABLE job_registry ALTER COLUMN id SET DEFAULT nextval('job_registry_id_seq');

CREATE INDEX i_g_k_v ON job_registry (registry_group, registry_key, registry_value);



-- ----------------------------
-- Table structure for job_user
-- ----------------------------
DROP TABLE IF EXISTS job_user;
CREATE TABLE job_user  (
  id bigint NOT NULL,
  username varchar(50) NOT NULL,
  password varchar(100) NOT NULL,
  role varchar(50) NULL DEFAULT NULL,
  permission varchar(255) NULL DEFAULT NULL,
  PRIMARY KEY (id)
);

CREATE SEQUENCE job_user_id_seq START WITH 2 INCREMENT BY 1 NO minvalue NO maxvalue CACHE 1;

ALTER TABLE job_user ALTER COLUMN id SET DEFAULT nextval('job_user_id_seq');

CREATE UNIQUE INDEX i_username ON job_user (username);

-- ----------------------------
-- Records of job_user
-- ----------------------------
INSERT INTO job_user VALUES (1, 'admin', '$2a$10$2KCqRbra0Yn2TwvkZxtfLuWuUP5KyCWsljO/ci5pLD27pqR3TV1vy', 'ROLE_ADMIN', NULL);



/**
v2.1.1脚本更新
*/
ALTER TABLE job_info
ADD COLUMN replace_param VARCHAR(100) NULL DEFAULT NULL,
ADD COLUMN jvm_param VARCHAR(200) NULL DEFAULT NULL,
ADD COLUMN custom_param VARCHAR(200) NULL DEFAULT NULL,
ADD COLUMN time_offset bigint NULL DEFAULT '0';

comment on column job_info.replace_param is '动态参数';
comment on column job_info.jvm_param is 'jvm参数';
comment on column job_info.custom_param is '自定义参数';
comment on column job_info.time_offset is '时间偏移量';



/**
增量改版脚本更新
 */
ALTER TABLE job_info DROP COLUMN time_offset;
ALTER TABLE job_info
ADD COLUMN inc_start_time timestamp NULL DEFAULT NULL;

comment on column job_info.inc_start_time is '增量初始时间';

-- ----------------------------
-- Table structure for job_template
-- ----------------------------
DROP TABLE IF EXISTS job_template;
CREATE TABLE job_template  (
  id bigint NOT NULL,
  job_group bigint NOT NULL,
  job_cron varchar(128) NULL,
  job_desc varchar(255) NOT NULL,
  add_time timestamp NULL DEFAULT NULL,
  update_time timestamp NULL DEFAULT NULL,
  user_id bigint NOT NULL,
  alarm_email varchar(255) NULL DEFAULT NULL,
  executor_route_strategy varchar(50) NULL DEFAULT NULL,
  executor_handler varchar(255) NULL DEFAULT NULL,
  executor_param varchar(512) NULL DEFAULT NULL,
  executor_block_strategy varchar(50)NULL DEFAULT NULL,
  executor_timeout bigint NOT NULL DEFAULT 0,
  executor_fail_retry_count bigint NOT NULL DEFAULT 0,
  glue_type varchar(50) NOT NULL,
  glue_source text NULL,
  glue_remark varchar(128) NULL DEFAULT NULL,
  glue_updatetime timestamp NULL DEFAULT NULL,
  child_jobid varchar(255) NULL DEFAULT NULL,
  trigger_last_time numeric(13,0) NOT NULL DEFAULT 0,
  trigger_next_time numeric(13,0) NOT NULL DEFAULT 0,
  job_json text NULL,
  jvm_param varchar(200) NULL DEFAULT NULL,
  custom_param varchar(200) NULL DEFAULT NULL,
  project_id bigint NULL DEFAULT NULL,
  PRIMARY KEY (id)
);

CREATE SEQUENCE job_template_id_seq START WITH 1 INCREMENT BY 1 NO minvalue NO maxvalue CACHE 1;

ALTER TABLE job_template ALTER COLUMN id SET DEFAULT nextval('job_template_id_seq');

comment on column job_template.job_group is '执行器主键ID';
comment on column job_template.job_cron is '任务执行CRON';
comment on column job_template.user_id is '修改用户';
comment on column job_template.alarm_email is '报警邮件';
comment on column job_template.executor_route_strategy is '执行器路由策略';
comment on column job_template.executor_handler is '执行器任务handler';
comment on column job_template.executor_param is '执行器参数';
comment on column job_template.executor_block_strategy is '阻塞处理策略';
comment on column job_template.executor_timeout is '任务执行超时时间，单位分钟';
comment on column job_template.executor_fail_retry_count is '失败重试次数';
comment on column job_template.glue_type is 'GLUE类型';
comment on column job_template.glue_source is 'GLUE源代码';
comment on column job_template.glue_remark is 'GLUE备注';
comment on column job_template.glue_updatetime is 'GLUE更新时间';
comment on column job_template.child_jobid is '子任务ID，多个逗号分隔';
comment on column job_template.trigger_last_time is '上次调度时间';
comment on column job_template.trigger_next_time is '下次调度时间';
comment on column job_template.job_json is 'datax运行脚本';
comment on column job_template.jvm_param is 'jvm参数';
comment on column job_template.jvm_param is '自定义参数';
comment on column job_template.project_id is '所属项目Id';

/**
添加数据源字段
 */
ALTER TABLE job_jdbc_datasource
ADD COLUMN datasource VARCHAR(45) NOT NULL;

comment on column job_jdbc_datasource.datasource is '数据源';

/**
添加分区字段
 */
ALTER TABLE job_info
ADD COLUMN partition_info VARCHAR(100) NULL DEFAULT NULL;

comment on column job_info.partition_info is '分区信息';


/**
2.1.1版本新增----------------------------------------------------------------------------------------------
 */

/**
最近一次执行状态
 */
ALTER TABLE job_info
ADD COLUMN last_handle_code bigint NULL DEFAULT '0';

comment on column job_info.last_handle_code is '最近一次执行状态:500-失败,502-失败(超时),200-成功,0-未执行,1-执行中';

/**
zookeeper地址
 */
ALTER TABLE job_jdbc_datasource
ADD COLUMN zk_adress VARCHAR(200) NULL DEFAULT NULL;

/**
添加mongodb数据库名字段
 */
ALTER TABLE job_jdbc_datasource
ADD COLUMN database_name VARCHAR(45) NULL DEFAULT NULL;

comment on column job_jdbc_datasource.database_name is '数据库名';

/**
添加执行器资源字段
 */
ALTER TABLE job_registry
ADD COLUMN cpu_usage double precision NULL,
ADD COLUMN memory_usage double precision NULL,
ADD COLUMN load_average double precision NULL;


-- ----------------------------
-- Table structure for job_permission
-- ----------------------------
DROP TABLE IF EXISTS job_permission;
CREATE TABLE job_permission  (
  id bigint NOT NULL,
  name varchar(50) NOT NULL,
  description varchar(11) NULL DEFAULT NULL,
  url varchar(255) NULL DEFAULT NULL,
  pid bigint NULL DEFAULT NULL,
  PRIMARY KEY (id)
);

CREATE SEQUENCE job_permission_id_seq START WITH 1 INCREMENT BY 1 NO minvalue NO maxvalue CACHE 1;

ALTER TABLE job_permission ALTER COLUMN id SET DEFAULT nextval('job_permission_id_seq');

comment on column job_permission.id is '主键';
comment on column job_permission.name is '权限名';
comment on column job_permission.description is '权限描述';



ALTER TABLE job_info
ADD COLUMN replace_param_type varchar(255) NULL;

comment on column job_info.replace_param_type is '增量时间格式';

ALTER TABLE job_info
ADD COLUMN project_id bigint NULL;

comment on column job_info.project_id is '所属项目id';

ALTER TABLE job_info
ADD COLUMN reader_table VARCHAR(255) NULL,
ADD COLUMN primary_key VARCHAR(50) NULL,
ADD COLUMN inc_start_id VARCHAR(20) NULL,
ADD COLUMN increment_type integer NULL DEFAULT 0,
ADD COLUMN datasource_id bigint NULL;

comment on column job_info.reader_table is 'reader表名称';
comment on column job_info.primary_key is '增量表主键';
comment on column job_info.inc_start_id is '所属项目id';
comment on column job_info.increment_type is '增量类型:0-无,1-主键自增,2-时间自增,3-HIVE分区';
comment on column job_info.datasource_id is '数据源id';


CREATE TABLE job_project  (
  id bigint NOT NULL,
  name varchar(100) NULL DEFAULT NULL,
  description varchar(200) NULL DEFAULT NULL,
  user_id bigint NULL DEFAULT NULL,
  flag integer NULL DEFAULT 1,
  create_time timestamp NULL DEFAULT CURRENT_TIMESTAMP(0),
  update_time timestamp NULL DEFAULT CURRENT_TIMESTAMP(0),
  PRIMARY KEY (id)
);

CREATE SEQUENCE job_project_id_seq START WITH 1 INCREMENT BY 1 NO minvalue NO maxvalue CACHE 1;

ALTER TABLE job_project ALTER COLUMN id SET DEFAULT nextval('job_project_id_seq');

comment on column job_project.id is 'key';
comment on column job_project.name is 'project name';
comment on column job_project.user_id is 'creator id';
comment on column job_project.flag is '0 not available, 1 available';
comment on column job_project.create_time is 'create time';
comment on column job_project.update_time is 'update time';

ALTER TABLE job_log ADD job_cron varchar(128) NULL;
COMMENT ON COLUMN job_log.job_cron IS '任务执行CRON';

ALTER TABLE job_info ADD is_big_type int4 NULL DEFAULT 0;
COMMENT ON COLUMN job_info.is_big_type IS '包含大字段: 0否 1是';

CREATE TABLE job_kafka_sync (
	id bigint NOT NULL,
	sync_name varchar(50) NOT NULL,
	create_time timestamp NULL DEFAULT CURRENT_TIMESTAMP(0),
	update_time timestamp NULL DEFAULT CURRENT_TIMESTAMP(0),
	reader_json text NULL,
	writer_json text NULL,
	sync_json text NULL,
	project_id int8 NULL,
	PRIMARY KEY (id)
);

CREATE SEQUENCE job_kafka_sync_id_seq START WITH 1 INCREMENT BY 1 NO minvalue NO maxvalue CACHE 1;

ALTER TABLE job_kafka_sync ALTER COLUMN id SET DEFAULT nextval('job_kafka_sync_id_seq');

CREATE TABLE job_data_type_mapping (
	id bigint NOT NULL,
	datasource varchar(45) NOT NULL,
	reader_data_type varchar(40) NOT NULL,
	writer_data_type varchar(40) NOT NULL,
	create_time timestamp NULL DEFAULT CURRENT_TIMESTAMP(0),
	update_time timestamp NULL DEFAULT CURRENT_TIMESTAMP(0),
	is_retain_value int2 NULL DEFAULT 0,
	PRIMARY KEY (id)
);

CREATE SEQUENCE job_data_type_mapping_id_seq START WITH 50 INCREMENT BY 1 NO minvalue NO maxvalue CACHE 1;

ALTER TABLE job_data_type_mapping ALTER COLUMN id SET DEFAULT nextval('job_data_type_mapping_id_seq');

COMMENT ON COLUMN job_data_type_mapping.datasource IS '数据源';
COMMENT ON COLUMN job_data_type_mapping.reader_data_type IS '源端数据类型';
COMMENT ON COLUMN job_data_type_mapping.writer_data_type IS '目标端数据类型';
COMMENT ON COLUMN job_data_type_mapping.is_retain_value IS '是否保留括号中的值: 0不保留 1保留';

CREATE TABLE job_data_contrast (
	id bigint NOT NULL,
	task_name varchar(50) NOT NULL,
	reader_datasource_id int8 NOT NULL,
	writer_datasource_id int8 NOT NULL,
	reader_schema varchar(50) NOT NULL,
	writer_schema varchar(50) NOT NULL,
	create_time timestamp NULL DEFAULT CURRENT_TIMESTAMP(0),
	update_time timestamp NULL DEFAULT CURRENT_TIMESTAMP(0),
	meta_type varchar(10) NULL,
	PRIMARY KEY (id)
);

CREATE SEQUENCE job_data_contrast_id_seq START WITH 1 INCREMENT BY 1 NO minvalue NO maxvalue CACHE 1;

ALTER TABLE job_data_contrast ALTER COLUMN id SET DEFAULT nextval('job_data_contrast_id_seq');

COMMENT ON COLUMN job_data_contrast.task_name IS '任务名称';
COMMENT ON COLUMN job_data_contrast.reader_datasource_id IS '源端数据源id';
COMMENT ON COLUMN job_data_contrast.writer_datasource_id IS '目标端数据源id';
COMMENT ON COLUMN job_data_contrast.reader_schema IS '源端schema';
COMMENT ON COLUMN job_data_contrast.writer_schema IS '目标端schema';
COMMENT ON COLUMN job_data_contrast.meta_type IS '对比类型:0-数据对比,2-元数据对比';

CREATE TABLE job_data_contrast_details (
	id bigint NOT NULL,
	task_id int8 NOT NULL,
	reader_table varchar(100) NULL,
	writer_table varchar(100) NULL,
	reader_record_rows int4 NOT NULL DEFAULT 0,
	writer_record_rows int4 NOT NULL DEFAULT 0,
	is_correct int2 NOT NULL DEFAULT 0,
	create_time timestamp NULL DEFAULT CURRENT_TIMESTAMP(0),
	update_time timestamp NULL DEFAULT CURRENT_TIMESTAMP(0),
	reader_info text NULL,
	writer_info text NULL,
	mapping_info text NULL,
	meta_type_id varchar(10) NULL,
	PRIMARY KEY (id)
);

CREATE SEQUENCE job_data_contrast_details_id_seq START WITH 1 INCREMENT BY 1 NO minvalue NO maxvalue CACHE 1;

ALTER TABLE job_data_contrast_details ALTER COLUMN id SET DEFAULT nextval('job_data_contrast_details_id_seq');

COMMENT ON COLUMN job_data_contrast_details.task_id IS '任务id';
COMMENT ON COLUMN job_data_contrast_details.reader_table IS '源端对象';
COMMENT ON COLUMN job_data_contrast_details.writer_table IS '目标端对象';
COMMENT ON COLUMN job_data_contrast_details.reader_record_rows IS '源端表记录数';
COMMENT ON COLUMN job_data_contrast_details.writer_record_rows IS '目标端表记录数';
COMMENT ON COLUMN job_data_contrast_details.is_correct IS '对比结果是否一致: 0一致 1不一致';
COMMENT ON COLUMN job_data_contrast_details.reader_info IS '源端对象相关信息';
COMMENT ON COLUMN job_data_contrast_details.writer_info IS '目标端对象相关信息';
COMMENT ON COLUMN job_data_contrast_details.mapping_info IS '类型映射相关信息';
COMMENT ON COLUMN job_data_contrast_details.meta_type_id IS '元数据类型id:1-自定义类型,2-表结构,3-约束,4-索引,5-函数,6-存储过程7-视图,8-触发器9-Package,10-序列';


INSERT INTO datax_web.job_data_type_mapping
(id, datasource, reader_data_type, writer_data_type, create_time, update_time, is_retain_value)
VALUES(1, 'mysql', 'int(.*?) unsigned(\s+zerofill)?', 'bigint', '2024-04-28 12:00:03.000', '2024-04-28 12:00:03.000', 1);
INSERT INTO datax_web.job_data_type_mapping
(id, datasource, reader_data_type, writer_data_type, create_time, update_time, is_retain_value)
VALUES(2, 'mysql', 'smallint(.*?) unsigned(\s+zerofill)?', 'integer', '2024-04-28 12:00:03.000', '2024-04-28 12:00:03.000', 1);
INSERT INTO datax_web.job_data_type_mapping
(id, datasource, reader_data_type, writer_data_type, create_time, update_time, is_retain_value)
VALUES(3, 'mysql', 'mediumint(.*?) unsigned(\s+zerofill)?', 'integer', '2024-04-28 12:00:03.000', '2024-04-28 12:00:03.000', 1);
INSERT INTO datax_web.job_data_type_mapping
(id, datasource, reader_data_type, writer_data_type, create_time, update_time, is_retain_value)
VALUES(4, 'mysql', 'bigint(.*?) unsigned(\s+zerofill)?', 'numeric', '2024-04-28 12:00:03.000', '2024-04-28 12:00:03.000', 1);
INSERT INTO datax_web.job_data_type_mapping
(id, datasource, reader_data_type, writer_data_type, create_time, update_time, is_retain_value)
VALUES(6, 'mysql', 'tinyint(.*?) unsigned(\s+zerofill)?', 'smallint', '2024-04-28 12:00:03.000', '2024-04-28 12:00:03.000', 1);
INSERT INTO datax_web.job_data_type_mapping
(id, datasource, reader_data_type, writer_data_type, create_time, update_time, is_retain_value)
VALUES(7, 'mysql', 'mediumint', 'integer', '2024-04-28 12:00:03.000', '2024-04-28 12:00:03.000', 0);
INSERT INTO datax_web.job_data_type_mapping
(id, datasource, reader_data_type, writer_data_type, create_time, update_time, is_retain_value)
VALUES(8, 'mysql', 'tinyint', 'smallint', '2024-04-28 12:00:03.000', '2024-04-28 12:00:03.000', 0);
INSERT INTO datax_web.job_data_type_mapping
(id, datasource, reader_data_type, writer_data_type, create_time, update_time, is_retain_value)
VALUES(9, 'mysql', 'bigint', 'bigint', '2024-04-28 12:00:03.000', '2024-04-28 12:00:03.000', 0);
INSERT INTO datax_web.job_data_type_mapping
(id, datasource, reader_data_type, writer_data_type, create_time, update_time, is_retain_value)
VALUES(11, 'mysql', 'tinytext', 'text', '2024-04-28 12:00:03.000', '2024-04-28 12:00:03.000', 0);
INSERT INTO datax_web.job_data_type_mapping
(id, datasource, reader_data_type, writer_data_type, create_time, update_time, is_retain_value)
VALUES(12, 'mysql', 'mediumtext', 'text', '2024-04-28 12:00:03.000', '2024-04-28 12:00:03.000', 0);
INSERT INTO datax_web.job_data_type_mapping
(id, datasource, reader_data_type, writer_data_type, create_time, update_time, is_retain_value)
VALUES(13, 'mysql', 'longtext', 'text', '2024-04-28 12:00:03.000', '2024-04-28 12:00:03.000', 0);
INSERT INTO datax_web.job_data_type_mapping
(id, datasource, reader_data_type, writer_data_type, create_time, update_time, is_retain_value)
VALUES(14, 'mysql', 'varbinary', 'bytea', '2024-04-28 12:00:03.000', '2024-04-28 12:00:03.000', 0);
INSERT INTO datax_web.job_data_type_mapping
(id, datasource, reader_data_type, writer_data_type, create_time, update_time, is_retain_value)
VALUES(15, 'mysql', 'binary', 'bytea', '2024-04-28 12:00:03.000', '2024-04-28 12:00:03.000', 0);
INSERT INTO datax_web.job_data_type_mapping
(id, datasource, reader_data_type, writer_data_type, create_time, update_time, is_retain_value)
VALUES(16, 'mysql', 'tinyblob', 'bytea', '2024-04-28 12:00:03.000', '2024-04-28 12:00:03.000', 0);
INSERT INTO datax_web.job_data_type_mapping
(id, datasource, reader_data_type, writer_data_type, create_time, update_time, is_retain_value)
VALUES(17, 'mysql', 'blob', 'bytea', '2024-04-28 12:00:03.000', '2024-04-28 12:00:03.000', 0);
INSERT INTO datax_web.job_data_type_mapping
(id, datasource, reader_data_type, writer_data_type, create_time, update_time, is_retain_value)
VALUES(18, 'mysql', 'mediumblob', 'bytea', '2024-04-28 12:00:03.000', '2024-04-28 12:00:03.000', 0);
INSERT INTO datax_web.job_data_type_mapping
(id, datasource, reader_data_type, writer_data_type, create_time, update_time, is_retain_value)
VALUES(19, 'mysql', 'longblob', 'bytea', '2024-04-28 12:00:03.000', '2024-04-28 12:00:03.000', 0);
INSERT INTO datax_web.job_data_type_mapping
(id, datasource, reader_data_type, writer_data_type, create_time, update_time, is_retain_value)
VALUES(20, 'mysql', 'enum', 'text', '2024-04-28 12:00:03.000', '2024-04-28 12:00:03.000', 0);
INSERT INTO datax_web.job_data_type_mapping
(id, datasource, reader_data_type, writer_data_type, create_time, update_time, is_retain_value)
VALUES(21, 'mysql', 'set', 'text', '2024-04-28 12:00:03.000', '2024-04-28 12:00:03.000', 0);
INSERT INTO datax_web.job_data_type_mapping
(id, datasource, reader_data_type, writer_data_type, create_time, update_time, is_retain_value)
VALUES(22, 'mysql', 'year', 'smallint', '2024-04-28 12:00:03.000', '2024-04-28 12:00:03.000', 0);
INSERT INTO datax_web.job_data_type_mapping
(id, datasource, reader_data_type, writer_data_type, create_time, update_time, is_retain_value)
VALUES(23, 'mysql', 'multipolygon', 'geometry', '2024-04-28 12:00:03.000', '2024-04-28 12:00:03.000', 0);
INSERT INTO datax_web.job_data_type_mapping
(id, datasource, reader_data_type, writer_data_type, create_time, update_time, is_retain_value)
VALUES(24, 'mysql', 'bit', 'bit varying', '2024-04-28 12:00:03.000', '2024-04-28 12:00:03.000', 1);
INSERT INTO datax_web.job_data_type_mapping
(id, datasource, reader_data_type, writer_data_type, create_time, update_time, is_retain_value)
VALUES(25, 'mysql', 'double', 'double precision', '2024-04-28 12:00:03.000', '2024-04-28 12:00:03.000', 0);
INSERT INTO datax_web.job_data_type_mapping
(id, datasource, reader_data_type, writer_data_type, create_time, update_time, is_retain_value)
VALUES(26, 'mysql', 'float', 'double precision', '2024-04-28 12:00:03.000', '2024-04-28 12:00:03.000', 0);
INSERT INTO datax_web.job_data_type_mapping
(id, datasource, reader_data_type, writer_data_type, create_time, update_time, is_retain_value)
VALUES(27, 'mysql', 'smallint', 'smallint', '2024-04-28 12:00:03.000', '2024-04-28 12:00:03.000', 0);
INSERT INTO datax_web.job_data_type_mapping
(id, datasource, reader_data_type, writer_data_type, create_time, update_time, is_retain_value)
VALUES(28, 'mysql', 'int', 'int', '2024-04-28 12:00:03.000', '2024-04-28 12:00:03.000', 0);
INSERT INTO datax_web.job_data_type_mapping
(id, datasource, reader_data_type, writer_data_type, create_time, update_time, is_retain_value)
VALUES(29, 'mysql', 'datetime', 'timestamp without time zone', '2024-04-28 12:00:03.000', '2024-04-28 12:00:03.000', 0);

ALTER TABLE job_info ADD file_path varchar(200) NULL;
COMMENT ON COLUMN job_info.file_path IS '脚本文件存放路径';

ALTER TABLE job_project ADD running_status int2 NULL DEFAULT 0;
COMMENT ON COLUMN job_project.running_status IS '运行状态: 0 未启动,1 运行中, 2 异常终止, 3 已完成';
ALTER TABLE job_project ADD trigger_status int2 NULL DEFAULT 0;
COMMENT ON COLUMN job_project.trigger_status IS '调度状态: 0 停止, 1 启动';
ALTER TABLE job_project ADD logger_file_path varchar(300) NULL;
COMMENT ON COLUMN job_project.logger_file_path IS '日志文件存放路径';
ALTER TABLE job_project ADD reader_datasource_id int4 NULL;
COMMENT ON COLUMN job_project.reader_datasource_id IS '源端数据源id';
ALTER TABLE job_project ADD writer_datasource_id int4 NULL;
COMMENT ON COLUMN job_project.writer_datasource_id IS '目标端数据源id';
ALTER TABLE job_project ADD reader_schema varchar(50) NULL;
COMMENT ON COLUMN job_project.reader_schema IS '需要迁移的schema';
