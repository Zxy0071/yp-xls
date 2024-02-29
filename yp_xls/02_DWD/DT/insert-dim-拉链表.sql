--拉链表
--店铺
--0.ODS抽取新增/变更数据
--1.首次初始化
INSERT overwrite TABLE yp_dwd.dim_store PARTITION (start_date)
select 
	id,
	user_id,
	store_avatar,
	address_info,
	name,
	store_phone,
	province_id,
	city_id,
	area_id,
	mb_title_img,
	store_description,
	notice,
	is_pay_bond,
	trade_area_id,
	delivery_method,
	origin_price,
	free_price,
	store_type,
	store_label,
	search_key,
	end_time,
	start_time,
	operating_status,
	create_user,
	create_time,
	update_user,
	update_time,
	is_valid,
	state,
	idcard,
	deposit_amount,
	delivery_config_id,
	aip_user_id,
	search_name,
	automatic_order,
	is_primary,
	parent_store_id,
	'9999-99-99' end_date,
	dt as start_date
from yp_ods.t_store;

--2.重建临时表
DROP TABLE if EXISTS yp_dwd.dim_store_tmp;
CREATE TABLE yp_dwd.dim_store_tmp(
  id string COMMENT '主键', 
  user_id string, 
  store_avatar string COMMENT '店铺头像', 
  address_info string COMMENT '店铺详细地址', 
  name string COMMENT '店铺名称', 
  store_phone string COMMENT '联系电话', 
  province_id int COMMENT '店铺所在省份ID', 
  city_id int COMMENT '店铺所在城市ID', 
  area_id int COMMENT '店铺所在县ID', 
  mb_title_img string COMMENT '手机店铺 页头背景图', 
  store_description string COMMENT '店铺描述', 
  notice string COMMENT '店铺公告', 
  is_pay_bond tinyint COMMENT '是否有交过保证金 1：是0：否', 
  trade_area_id string COMMENT '归属商圈ID', 
  delivery_method tinyint COMMENT '配送方式  1 ：自提 ；3 ：自提加配送均可\; 2 : 商家配送', 
  origin_price decimal(36,2), 
  free_price decimal(36,2), 
  store_type int COMMENT '店铺类型 22天街网店 23实体店 24直营店铺 33会员专区店', 
  store_label string COMMENT '店铺logo', 
  search_key string COMMENT '店铺搜索关键字', 
  end_time string COMMENT '营业结束时间', 
  start_time string COMMENT '营业开始时间', 
  operating_status tinyint COMMENT '营业状态  0 ：未营业 ；1 ：正在营业', 
  create_user string, 
  create_time string, 
  update_user string, 
  update_time string, 
  is_valid tinyint COMMENT '0关闭，1开启，3店铺申请中', 
  state string COMMENT '可使用的支付类型:MONEY金钱支付\;CASHCOUPON现金券支付', 
  idcard string COMMENT '身份证', 
  deposit_amount decimal(36,2) COMMENT '商圈认购费用总额', 
  delivery_config_id string COMMENT '配送配置表关联ID', 
  aip_user_id string COMMENT '通联支付标识ID', 
  search_name string COMMENT '模糊搜索名称字段:名称_+真实名称', 
  automatic_order tinyint COMMENT '是否开启自动接单功能 1：是  0 ：否', 
  is_primary tinyint COMMENT '是否是总店 1: 是 2: 不是', 
  parent_store_id string COMMENT '父级店铺的id，只有当is_primary类型为2时有效',
  end_date string COMMENT '拉链结束日期')
COMMENT '店铺表'
partitioned by (start_date string)
row format delimited fields terminated by '\t'
stored as orc 
tblproperties ('orc.compress' = 'SNAPPY');

--3.开始合并新旧数据to临时表
INSERT overwrite TABLE yp_dwd.dim_store_tmp PARTITION(start_date)
SELECT * FROM 
	(
--      一、update表更新的数据
		SELECT 
			            id,
			user_id,
			store_avatar,
			address_info,
			name,
			store_phone,
			province_id,
			city_id,
			area_id,
			mb_title_img,
			store_description,
			notice,
			is_pay_bond,
			trade_area_id,
			delivery_method,
			origin_price,
			free_price,
			store_type,
			store_label,
			search_key,
			end_time,
			start_time,
			operating_status,
			create_user,
			create_time,
			update_user,
			update_time,
			is_valid,
			state,
			idcard,
			deposit_amount,
			delivery_config_id,
			aip_user_id,
			search_name,
			automatic_order,
			is_primary,
			parent_store_id,
			'9999-99-99' end_date,
			'2021-03-04' start_date
		FROM yp_ods.t_store
		WHERE dt='2021-03-04'
	UNION ALL
--      二、历史拉链表数据，并根据update判断更新end_date有效期
		select
          	s.id,
			s.user_id,
			s.store_avatar,
			s.address_info,
			s.name,
			s.store_phone,
			s.province_id,
			s.city_id,
			s.area_id,
			s.mb_title_img,
			s.store_description,
			s.notice,
			s.is_pay_bond,
			s.trade_area_id,
			s.delivery_method,
			s.origin_price,
			s.free_price,
			s.store_type,
			s.store_label,
			s.search_key,
			s.end_time,
			s.start_time,
			s.operating_status,
			s.create_user,
			s.create_time,
			s.update_user,
			s.update_time,
			s.is_valid,
			s.state,
			s.idcard,
			s.deposit_amount,
			s.delivery_config_id,
			s.aip_user_id,
			s.search_name,
			s.automatic_order,
			s.is_primary,
			s.parent_store_id,
          	--3、更新end_date：如果没有匹配到变更数据，或者当前已经是无效的历史数据，则保留原始end_date过期时间；
            --  否则变更end_date时间为前天（昨天之前有效）
          	if(up.id is null or s.end_date<'9999-12-31', s.end_date, date_add('2021-03-04',-1)) end_date,
          	s.start_date
        from yp_dwd.dim_store s 
        -- 用来做3的判断
        left join
            (
                select
                id
                from yp_ods.t_store
                where dt='2021-03-04'
            ) up on s.id=up.id
        --4、时间限制：历史表中30天之内的数据才有可能变更，结果会按照所属分区进行覆盖插入
        where s.start_date >= date_add('2021-03-04',-30)
	) his
ORDER BY his.id, his.start_date;

--4.临时表覆盖拉链表
INSERT overwrite TABLE yp_dwd.dim_store PARTITION(start_date)
SELECT * FROM yp_dwd.dim_store_tmp;

--5.查询表数据
SELECT * FROM yp_dwd.dim_store;


--商圈
--步骤同店铺（此处省略，只进行初始化）
INSERT overwrite TABLE yp_dwd.dim_trade_area PARTITION(start_date)
SELECT 
	id,
	user_id,
	user_allinpay_id,
	trade_avatar,
	name,
	notice,
	distric_province_id,
	distric_city_id,
	distric_area_id,
	address,
	radius,
	mb_title_img,
	deposit_amount,
	hava_deposit,
	state,
	search_key,
	create_user,
	create_time,
	update_user,
	update_time,
	is_valid,
	'9999-99-99' end_date,
	dt as start_date
FROM yp_ods.t_trade_area;



--地址信息表（拉链表）
INSERT overwrite TABLE yp_dwd.dim_location PARTITION(start_date)
SELECT
	id,
	type,
	correlation_id,
	address,
	latitude,
	longitude,
	street_number,
	street,
	district,
	city,
	province,
	business,
	create_user,
	create_time,
	update_user,
	update_time,
	is_valid,
	adcode,
	'9999-99-99' end_date,
	dt as start_date
FROM yp_ods.t_location;


--商品SKU表（拉链表）
INSERT overwrite TABLE yp_dwd.dim_goods PARTITION(start_date)
SELECT
	id,
	store_id,
	class_id,
	store_class_id,
	brand_id,
	goods_name,
	goods_specification,
	search_name,
	goods_sort,
	goods_market_price,
	goods_price,
	goods_promotion_price,
	goods_storage,
	goods_limit_num,
	goods_unit,
	goods_state,
	goods_verify,
	activity_type,
	discount,
	seckill_begin_time,
	seckill_end_time,
	seckill_total_pay_num,
	seckill_total_num,
	seckill_price,
	top_it,
	create_user,
	create_time,
	update_user,
	update_time,
	is_valid,
	'9999-99-99' end_date,
	dt as start_date
FROM
yp_ods.t_goods;


--商品分类（拉链表）
INSERT overwrite TABLE yp_dwd.dim_goods_class PARTITION(start_date)
SELECT
	id,
	store_id,
	class_id,
	name,
	parent_id,
	level,
	is_parent_node,
	background_img,
	img,
	keywords,
	title,
	sort,
	note,
	url,
	is_use,
	create_user,
	create_time,
	update_user,
	update_time,
	is_valid,
	'9999-99-99' end_date,
	dt as start_date
FROM yp_ods.t_goods_class;


--品牌表（拉链表）
INSERT overwrite TABLE yp_dwd.dim_brand PARTITION(start_date)
SELECT
	id,
	store_id,
	brand_pt_id,
	brand_name,
	brand_image,
	initial,
	sort,
	is_use,
	goods_state,
	create_user,
	create_time,
	update_user,
	update_time,
	is_valid,
	'9999-99-99' end_date,
	dt as start_date
FROM yp_ods.t_brand;



