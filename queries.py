# will contain all the necessary SQL queries to be required while performing sanity

cpq_query1 = """
select distinct order_no,extn_ba_order_no,order_date from ssfspr.YFS_ORDER_HEADER where trunc(ORDER_DATE) = CURRENT DATE - 1 days and draft_order_flag = 'N';
"""

cpq_query2 = """
select TOTAL,(select description from ssfspr.yfs_status where process_type_key='ORDER_FULFILLMENT' and EXTN_STATUS=STATUS)
from (select count(order_no) as TOTAL,extn_status from ssfspr.YFS_ORDER_HEADER where trunc(ORDER_DATE) = CURRENT DATE
group by EXTN_STATUS);
"""

cpq_query3 = """
select order_no, detail_description from ssfspr.yfs_inbox where trunc(generated_on) = CURRENT DATE and detail_description like 'INSIGHT Order Submission Failure%';
"""

espr_query1 = """
select distinct order_no from BA_ES_DATA where trunc(ORDER_DATE) = CURRENT DATE - 1 days;
"""

def format_pdf_export_query(orders: list):
    query = f"""
    select extn_ba_order_no,order_no,order_date from ssfspr.YFS_ORDER_HEADER where order_no in 
        ({','.join([f"'{current_order}'" for current_order in orders])});"""
    return query

def generate_espr_query(days: int):
    return f'select distinct order_no from BA_ES_DATA where trunc(ORDER_DATE) = CURRENT DATE - {days} days;'

def generate_cpq_query(days: int):
    return f"select distinct order_no,extn_ba_order_no,order_date from ssfspr.YFS_ORDER_HEADER where trunc(ORDER_DATE) = CURRENT DATE - {days} days and draft_order_flag = 'N';"