--动态分区配置
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.created.files=150000;
--hive压缩
set hive.exec.compress.intermediate=true;
set hive.exec.compress.output=true;
--写入时压缩生效
set hive.exec.orc.compression.strategy=COMPRESSION;


----------订单明细宽表----------------------------------
insert into table yp_dwb.dwb_order_detail partition(dt)
select
o.id as order_id,
o.order_num ,
o.buyer_id ,
o.store_id ,
o.order_from,
o.order_state ,
o.create_date ,
o.finnshed_time ,
o.is_settlement ,
o.is_delete ,
o.evaluation_state ,
o.way ,
o.is_stock_up ,

d.order_amount ,
d.discount_amount ,
d.goods_amount ,
d.is_delivery ,
d.buyer_notes ,
d.pay_time ,
d.receive_time ,
d.delivery_begin_time ,
d.arrive_store_time ,
d.arrive_time ,
d.create_user ,
d.create_time ,
d.update_user ,
d.update_time ,
d.is_valid ,

g.group_id ,
g.is_pay ,

p.order_pay_amount  as group_pay_amount,

r.id as refund_id,
r.apply_date ,
r.refund_reason ,
r.refund_amount ,
r.refund_state ,

s.id as settle_id,
s.settlement_amount ,
s.dispatcher_user_id ,
s.dispatcher_money ,
s.circle_master_user_id ,
s.circle_master_money ,
s.plat_fee ,
s.store_money ,
s.status ,
s.settle_time ,

e.id as evaluation_id,
e.user_id  as evaluation_user_id,
e.geval_scores ,
e.geval_scores_speed ,
e.geval_scores_service ,
e.geval_isanony ,
e.create_time  as  evaluation_time,

i.id  as delievery_id,
i.dispatcher_order_state ,
i.delivery_fee ,
i.distance ,
i.dispatcher_code ,
i.receiver_name ,
i.receiver_phone ,
i.sender_name,
i.sender_phone ,
i.create_time  as delievery_create_time,

goods.id  as order_goods_id,
goods.goods_id ,
goods.buy_num ,
goods.goods_price ,
goods.total_price ,
goods.goods_name ,
goods.goods_specification ,
goods.goods_type ,
goods.goods_brokerage ,
goods.is_refund  as is_goods_refund,
substring(o.create_time,0,10) as dt
from (select  * from yp_dwd.fact_shop_order where end_date='9999-99-99')  o
	left join yp_dwd.fact_shop_order_address_detail d on o.id = d.id and d.end_date = '9999-99-99'
	left join yp_dwd.fact_shop_order_group  g on  g.order_id  = o.id and g.end_date = '9999-99-99'
	left join yp_dwd.fact_order_pay  p on p.group_id = g.group_id
	left join yp_dwd.fact_refund_order r on r.order_id  = o.id and r.end_date = '9999-99-99'
	left join yp_dwd.fact_order_settle s on s.order_id = o.id  and s.end_date = '9999-99-99'
	left join yp_dwd.fact_shop_order_goods_details  goods on goods.order_id  = o.id and goods.end_date = '9999-99-99'
	left join yp_dwd.fact_goods_evaluation  e on e.order_id  = o.id and  e.is_valid = 1
	left join yp_dwd.fact_order_delievery_item  i on i.shop_order_id  = o.id  and i.is_valid  = 1;


-------------------------------------------
----------店铺明细宽表----------------------------------
--动态分区配置
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.created.files=150000;
--hive压缩
set hive.exec.compress.intermediate=true;
set hive.exec.compress.output=true;
--写入时压缩生效
set hive.exec.orc.compression.strategy=COMPRESSION;

insert into table yp_dwb.dwb_shop_detail partition(dt)
SELECT
	-- 店铺相关字段
	s.id ,
	s.address_info ,
	s.name  as store_name,
	s.is_pay_bond ,
	s.trade_area_id ,
	s.delivery_method ,
	s.store_type ,
	s.is_primary ,
	s.parent_store_id ,
	-- 商圈相关字段
	t.name  as trade_area_name,
	-- 区域信息
	d3.id  as province_id,
	d2.id as city_id,
	d1.id  as area_id,
	d3.name  as province_name,
	d2.name as city_name,
	d1.name as area_name,
	SUBSTRING(s.create_time , 1,10) as dt
from (select * from yp_dwd.dim_store where  end_date = '9999-99-99')   s
	left join yp_dwd.dim_trade_area   t on  s.trade_area_id = t.id and t.end_date = '9999-99-99'
	left join yp_dwd.dim_location  l on l.correlation_id = s.id and  l.`type` = 2 and l.end_date = '9999-99-99'
	left join yp_dwd.dim_district d1 on l.adcode  = d1.id
	left join yp_dwd.dim_district d2 on d1.pid = d2.id
	left join yp_dwd.dim_district d3 on d2.pid = d3.id;



---------------------------------------------
-----------商品明细宽表-----------------------
insert into table yp_dwb.dwb_goods_detail  partition(dt)
select
g.id,
g.store_id,
g.class_id,
g.store_class_id,
g.brand_id,
g.goods_name,
g.goods_specification,
g.search_name,
g.goods_sort,
g.goods_market_price,
g.goods_price,
g.goods_promotion_price,
g.goods_storage,
g.goods_limit_num,
g.goods_unit,
g.goods_state,
g.goods_verify,
g.activity_type,
g.discount,
g.seckill_begin_time,
g.seckill_end_time,
g.seckill_total_pay_num,
g.seckill_total_num,
g.seckill_price,
g.top_it,
g.create_user,
g.create_time,
g.update_user,
g.update_time,
g.is_valid,

CASE class1.level WHEN 3
		THEN class1.id
		ELSE NULL
		END as min_class_id,
	CASE class1.level WHEN 3
		THEN class1.name
		ELSE NULL
		END as min_class_name,
	CASE WHEN class1.level=2
		THEN class1.id
		WHEN class2.level=2
		THEN class2.id
		ELSE NULL
		END as mid_class_id,
	CASE WHEN class1.level=2
		THEN class1.name
		WHEN class2.level=2
		THEN class2.name
		ELSE NULL
		END as mid_class_name,
	CASE WHEN class1.level=1
		THEN class1.id
		WHEN class2.level=1
		THEN class2.id
		WHEN class3.level=1
		THEN class3.id
		ELSE NULL
		END as max_class_id,
	CASE WHEN class1.level=1
		THEN class1.name
		WHEN class2.level=1
		THEN class2.name
		WHEN class3.level=1
		THEN class3.name
		ELSE NULL
		END as max_class_name,
b.brand_name ,
SUBSTRING(g.create_time,1,10) as  dt
from  yp_dwd.dim_goods   g
	left join yp_dwd.dim_brand  b on g.brand_id  = b.id  and b.end_date  = '9999-99-99'
	left join yp_dwd.dim_goods_class  class1 on g.store_class_id  = class1.id  and class1.end_date = '9999-99-99'
	left join yp_dwd.dim_goods_class  class2 on class1.parent_id  = class2.id  and class2.end_date = '9999-99-99'
	left join yp_dwd.dim_goods_class  class3 on class2.parent_id  = class3.id and class3.end_date = '9999-99-99';
