--分区
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.created.files=150000;
--hive压缩
set hive.exec.compress.intermediate=true;
set hive.exec.compress.output=true;
--写入时压缩生效
set hive.exec.orc.compression.strategy=COMPRESSION;
--===========全量覆盖===========
--区域字典表
INSERT overwrite TABLE yp_dwd.dim_district
select * from yp_ods.t_district
WHERE code IS NOT NULL AND name IS NOT NULL;

--时间维度表
INSERT overwrite TABLE yp_dwd.dim_date
select * from yp_ods.t_date;


--===========拉链表===========
--店铺
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

--商圈
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


