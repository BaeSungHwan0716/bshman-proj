from airflow import DAG
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

with DAG(
    'mysql_to_bigquery_pipeline',
    default_args=default_args,
    description='런드리고 airflow 데이터과제',
    schedule_interval='0 * * * *',  # 한 시간 단위로 스케줄링
    start_date=days_ago(1),
    catchup=False,
) as dag:

    start = DummyOperator(
        task_id='start',
    )

    product_order = MySQLToGCSOperator(
        task_id='mysql_product_order',
        mysql_conn_id='mysql_default',
        sql="""
        SELECT
               id
             , parent_id
             , product_description
             , status_type
             , order_type
             , order_price
             , cancel_datetime
             , cancel_price
             , user_id
             , order_datetime
             , create_datetime
             , modified_datetime
        FROM product_order 
        where 1=1
          and date_format(create_datetime,'%Y%m%d%H') between date_format(create_datetime - interval '1' hour,'%Y%m%d%H%i') and ,date_format(create_datetime - interval '1' MINUTE,'%Y%m%d%H%i')
        """,
        bucket_name='ods_bucket',
        filename='product_order_{{ ts_nodash }}.json',
        export_format='json'
    )

    product_order_items = MySQLToGCSOperator(
        task_id='mysql_product_order_items',
        mysql_conn_id='mysql_default',
        sql="""
        SELECT
              id
            , parent_product_order_item_id
            , product_order_id
            , parent_product_order_id
            , product_id
            , order_type
            , status_type
            , order_price
            , quantity
            , cancel_datetime
            , cancel_quantity
            , cancel_price
            , create_datetime
            , modified_datetime
        FROM product_order_items 
        where 1=1
          and date_format(create_datetime,'%Y%m%d%H') between date_format(create_datetime - interval '1' hour,'%Y%m%d%H%i') and ,date_format(create_datetime - interval '1' MINUTE,'%Y%m%d%H%i')
        """,
        bucket_name='ods_bucket',
        filename='product_order_items_{{ ts_nodash }}.json',
        export_format='json'
    )

    product_order_items_sku = MySQLToGCSOperator(
        task_id='mysql_product_order_items_sku',
        mysql_conn_id='mysql_default',
        sql="""
        SELECT
              product_order_items_id
            , sku_id
            , order_price
            , cancel_price
            , quantity
            , bundle_count
            , create_datetime
            , modified_datetime
        FROM product_order_items_sku 
        where 1=1
          and date_format(create_datetime,'%Y%m%d%H') between date_format(create_datetime - interval '1' hour,'%Y%m%d%H%i') and ,date_format(create_datetime - interval '1' MINUTE,'%Y%m%d%H%i')
        """,
        bucket_name='ods_bucket',
        filename='product_order_items_sku_{{ ts_nodash }}.json',
        export_format='json'
    )

    product = MySQLToGCSOperator(
        task_id='mysql_product',
        mysql_conn_id='mysql_default',
        sql="""
        SELECT
             id
           , brand_name
           , name
           , original_price
           , sell_price
           , sale_start_datetime
           , slae_end_datetime
           , discount_rate
           , tax_rate
           , create_datetime
           , modified_datetime
        FROM product 
        where 1=1
          and date_format(create_datetime,'%Y%m%d%H') between date_format(create_datetime - interval '1' hour,'%Y%m%d%H%i') and ,date_format(create_datetime - interval '1' MINUTE,'%Y%m%d%H%i')
        """,
        bucket_name='ods_bucket',
        filename='product_{{ ts_nodash }}.json',
        export_format='json'
    )

    sku = MySQLToGCSOperator(
        task_id='mysql_sku',
        mysql_conn_id='mysql_default',
        sql="""
        SELECT
            id
           , varcode
           , erp_no
           , original_price
           , create_datetime
           , modified_datetime
        FROM sku 
        where 1=1
          and date_format(create_datetime,'%Y%m%d%H') between date_format(create_datetime - interval '1' hour,'%Y%m%d%H%i') and ,date_format(create_datetime - interval '1' MINUTE,'%Y%m%d%H%i')
        """,
        bucket_name='ods_bucket',
        filename='sku_{{ ts_nodash }}.json',
        export_format='json'
    )

    user = MySQLToGCSOperator(
        task_id='mysql_user',
        mysql_conn_id='mysql_default',
        sql="""
        SELECT
            id
           , gender
           , address
           , last_visit_at
           , create_datetime
           , modified_datetime
        FROM user 
        where 1=1
          and date_format(create_datetime,'%Y%m%d%H') between date_format(create_datetime - interval '1' hour,'%Y%m%d%H%i') and ,date_format(create_datetime - interval '1' MINUTE,'%Y%m%d%H%i')
        """,
        bucket_name='ods_bucket',
        filename='user_{{ ts_nodash }}.json',
        export_format='json'
    )


    load_product_order_to_bigquery = GCSToBigQueryOperator(
        task_id='load_product_order_to_bigquery',
        bucket='ods_bucket',
        source_objects=['product_order_{{ ts_nodash }}.json'],
        destination_project_dataset_table='life.goes.product_order',
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_APPEND'
    )

    load_product_order_items_to_bigquery = GCSToBigQueryOperator(
        task_id='load_product_order_items_to_bigquery',
        bucket='ods_bucket',
        source_objects=['product_order_items_{{ ts_nodash }}.json'],
        destination_project_dataset_table='life.goes.product_order_items',
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_APPEND'
    )

    load_product_order_items_sku_to_bigquery = GCSToBigQueryOperator(
        task_id='load_product_order_items_sku_to_bigquery',
        bucket='ods_bucket',
        source_objects=['product_order_items_sku_{{ ts_nodash }}.json'],
        destination_project_dataset_table='life.goes.product_order_items_sku',
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_APPEND'
    )

    load_product_to_bigquery = GCSToBigQueryOperator(
        task_id='load_product_to_bigquery',
        bucket='ods_bucket',
        source_objects=['product_{{ ts_nodash }}.json'],
        destination_project_dataset_table='life.goes.product',
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_APPEND'
    )

    load_sku_to_bigquery = GCSToBigQueryOperator(
        task_id='load_sku_to_bigquery',
        bucket='ods_bucket',
        source_objects=['sku_{{ ts_nodash }}.json'],
        destination_project_dataset_table='life.goes.sku',
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_APPEND'
    )

    load_user_to_bigquery = GCSToBigQueryOperator(
        task_id='load_user_to_bigquery',
        bucket='ods_bucket',
        source_objects=['user_{{ ts_nodash }}.json'],
        destination_project_dataset_table='life.goes.user',
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_APPEND'
    )

    end = DummyOperator(
        task_id='end',
    )

    start >> [product_order, product_order_items. product_order_items_sku, product, sku, user] >> [load_product_order_to_bigquery, load_product_order_items_to_bigquery, load_product_order_items_sku_to_bigquery, load_product_to_bigquery, load_sku_to_bigquery, load_user_to_bigquery] >> end
