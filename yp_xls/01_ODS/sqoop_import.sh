/usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
--username root \
--password 123456 \
--query "select * from t_district where 1=1 and  \$CONDITIONS" \
--hcatalog-database yp_ods \
--hcatalog-table t_district \
-m 1

/usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
--username root \
--password 123456 \
--query "select * from t_date where 1=1 and  \$CONDITIONS" \
--hcatalog-database yp_ods \
--hcatalog-table t_date \
-m 1

/usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
--username root \
--password 123456 \
--query "select *, '2021-11-29' as dt from t_goods_evaluation where 1=1 and create_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59' and  \$CONDITIONS" \
--hcatalog-database yp_ods \
--hcatalog-table t_goods_evaluation \
-m 1

/usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
--username root \
--password 123456 \
--query "select *, '2021-11-29' as dt from t_goods_evaluation_detail where 1=1 and create_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59' and  \$CONDITIONS" \
--hcatalog-database yp_ods \
--hcatalog-table t_goods_evaluation_detail \
-m 1

/usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
--username root \
--password 123456 \
--query "select *, '2021-11-29' as dt from t_order_delievery_item where 1=1 and create_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59' and  \$CONDITIONS" \
--hcatalog-database yp_ods \
--hcatalog-table t_order_delievery_item \
-m 1

/usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
--username root \
--password 123456 \
--query "select *, '2021-11-29' as dt from t_user_login where 1=1 and  \$CONDITIONS" \
--hcatalog-database yp_ods \
--hcatalog-table t_user_login \
-m 1

/usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
--username root \
--password 123456 \
--query "select *, '2021-11-29' as dt from t_trade_record where 1=1 and create_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59' and  \$CONDITIONS" \
--hcatalog-database yp_ods \
--hcatalog-table t_trade_record \
-m 1

/usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
--username root \
--password 123456 \
--query "select *, '2021-11-29' as dt from t_store where 1=1 and (create_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') or (update_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') and  \$CONDITIONS" \
--hcatalog-database yp_ods \
--hcatalog-table t_store \
-m 1

/usr/bin/sqoop import -Dorg.apache.sqoop.splitter.allow_text_splitter=true -Dorg.apache.sqoop.db.type=mysql \
--connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
--username root \
--password 123456 \
--query "select *, '2021-11-29' as dt from t_trade_area where 1=1 and (create_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') or (update_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') and  \$CONDITIONS order by id" \
--hcatalog-database yp_ods \
--hcatalog-table t_trade_area \
-m 1

/usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
--username root \
--password 123456 \
--query "select *, '2021-11-29' as dt from t_location where 1=1 and (create_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') or (update_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') and  \$CONDITIONS" \
--hcatalog-database yp_ods \
--hcatalog-table t_location \
-m 1

/usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
--username root \
--password 123456 \
--query "select *, '2021-11-29' as dt from t_goods where 1=1 and (create_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') or (update_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') and  \$CONDITIONS" \
--hcatalog-database yp_ods \
--hcatalog-table t_goods \
-m 1

/usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
--username root \
--password 123456 \
--query "select *, '2021-11-29' as dt from t_goods_class where 1=1 and (create_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') or (update_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') and  \$CONDITIONS" \
--hcatalog-database yp_ods \
--hcatalog-table t_goods_class \
-m 1

/usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
--username root \
--password 123456 \
--query "select *, '2021-11-29' as dt from t_brand where 1=1 and (create_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') or (update_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') and  \$CONDITIONS" \
--hcatalog-database yp_ods \
--hcatalog-table t_brand \
-m 1

/usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
--username root \
--password 123456 \
--query "select *, '2021-11-29' as dt from t_shop_order where 1=1 and (create_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') or (update_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') and  \$CONDITIONS" \
--hcatalog-database yp_ods \
--hcatalog-table t_shop_order \
-m 1

/usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
--username root \
--password 123456 \
--query "select *, '2021-11-29' as dt from t_shop_order_address_detail where 1=1 and (create_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') or (update_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') and  \$CONDITIONS" \
--hcatalog-database yp_ods \
--hcatalog-table t_shop_order_address_detail \
-m 1

/usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
--username root \
--password 123456 \
--query "select *, '2021-11-29' as dt from t_order_settle where 1=1 and (create_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') or (update_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') and  \$CONDITIONS" \
--hcatalog-database yp_ods \
--hcatalog-table t_order_settle \
-m 1

/usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
--username root \
--password 123456 \
--query "select *, '2021-11-29' as dt from t_refund_order where 1=1 and (create_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') or (update_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') and  \$CONDITIONS" \
--hcatalog-database yp_ods \
--hcatalog-table t_refund_order \
-m 1

/usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
--username root \
--password 123456 \
--query "select *, '2021-11-29' as dt from t_shop_order_group where 1=1 and (create_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') or (update_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') and  \$CONDITIONS" \
--hcatalog-database yp_ods \
--hcatalog-table t_shop_order_group \
-m 1

/usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
--username root \
--password 123456 \
--query "select *, '2021-11-29' as dt from t_order_pay where 1=1 and (create_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') or (update_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') and  \$CONDITIONS" \
--hcatalog-database yp_ods \
--hcatalog-table t_order_pay \
-m 1

/usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
--username root \
--password 123456 \
--query "select *, '2021-11-29' as dt from t_shop_order_goods_details where 1=1 and (create_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') or (update_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') and  \$CONDITIONS" \
--hcatalog-database yp_ods \
--hcatalog-table t_shop_order_goods_details \
-m 1

/usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
--username root \
--password 123456 \
--query "select *, '2021-11-29' as dt from t_shop_cart where 1=1 and (create_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') or (update_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') and  \$CONDITIONS" \
--hcatalog-database yp_ods \
--hcatalog-table t_shop_cart \
-m 1

/usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
--username root \
--password 123456 \
--query "select *, '2021-11-29' as dt from t_store_collect where 1=1 and (create_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') or (update_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') and  \$CONDITIONS" \
--hcatalog-database yp_ods \
--hcatalog-table t_store_collect \
-m 1

/usr/bin/sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
--connect 'jdbc:mysql://192.168.88.80:3306/yipin?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true' \
--username root \
--password 123456 \
--query "select *, '2021-11-29' as dt from t_goods_collect where 1=1 and (create_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') or (update_time between '2010-01-01 00:00:00' and '2021-11-29 23:59:59') and  \$CONDITIONS" \
--hcatalog-database yp_ods \
--hcatalog-table t_goods_collect \
-m 1