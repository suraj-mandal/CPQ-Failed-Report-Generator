import ibm_db
import logging
from connection import generate_cpq_cursor, generate_espr_cursor
from queries import generate_cpq_query, format_pdf_export_query, generate_espr_query
from operators import generate_difference
import time
import xlsxwriter
import sys
from datetime import datetime


def run(excel_loc='sample.xlsx', days=1):
    # performing the operation on CPQ DB first

    # configuring the logging file
    logging.basicConfig(filename=f'failed_pdf_script_run_{str(datetime.now().strftime("%H_%M_%S"))}.log',
                        filemode='w',
                        format='%(asctime)s - %(levelname)s - %(message)s')

    logger = logging.getLogger()

    logger.setLevel(logging.INFO)

    logger.info(f'Connecting to CPQ DB on {datetime.now()}')
    try:
        cpq_c = generate_cpq_cursor()
        logger.info(f'Connected successfully to CPQ DB')
    except Exception as e:
        logger.error(f'Could not connect to CPQ DB - {e}')
        sys.exit(-1)

    cpq_order_set = set()
    espr_order_set = set()

    # fetching orders from cpq db
    logger.info('Fetching orders from CPQ DB')

    cpq_query = generate_cpq_query(days)
    espr_query = generate_espr_query(days)

    cpq_orders_stmt = ibm_db.exec_immediate(cpq_c, cpq_query)

    while ibm_db.fetch_row(cpq_orders_stmt) is True:
        order_no = ibm_db.result(cpq_orders_stmt, 0)
        cpq_order_set.add(order_no)

    logger.info('Successfully fetched data from CPQ DB')

    time.sleep(2)
    # ESPR DB part
    logger.info('Connecting to ESPR DB')
    try:
        espr_c = generate_espr_cursor()
        logger.info('Connected successfully to ESPR DB')
    except Exception as e:
        logger.error(f'Could not connect to ESPR DB - {e}')
        sys.exit(-1)

    logger.info('Fetching orders from ESPR DB')

    espr_stmt = ibm_db.exec_immediate(espr_c, espr_query)

    while ibm_db.fetch_row(espr_stmt) is True:
        order_no = ibm_db.result(espr_stmt, 0)
        espr_order_set.add(order_no)

    logger.info('Successfully fetched data from ESPR DB')

    print()
    time.sleep(2)

    logger.info('Processing failed Orders')

    failed_orders = generate_difference(cpq_order_set, espr_order_set)

    failed_order_sql = format_pdf_export_query(failed_orders)

    failed_order_stmt = ibm_db.exec_immediate(cpq_c, failed_order_sql)

    logger.info(f'Total CPQ orders: {len(cpq_order_set)}')
    logger.info(f'Total ESPR orders: {len(espr_order_set)}')

    logger.info('Fetching failed orders and writing to excel sheet')

    workbook = xlsxwriter.Workbook(excel_loc)
    worksheet = workbook.add_worksheet()

    row, col = 1, 0
    worksheet.write_row('A1', ('BA Extn Order No.', 'Order No.', 'Date'))

    while ibm_db.fetch_row(failed_order_stmt) is True:
        ba_order_no = ibm_db.result(failed_order_stmt, 0)
        order_no = ibm_db.result(failed_order_stmt, 1)
        date = ibm_db.result(failed_order_stmt, 2)

        worksheet.write(row, col, f'{str(ba_order_no)}')
        worksheet.write(row, col + 1, f'{str(order_no)}')
        worksheet.write(row, col + 2, f'{str(date)}')

        row += 1

    workbook.close()

    logger.info('Successfully fetched orders and wrote to excel sheet')
    # closing the connection

    logger.info('Closing connection to CPQ DB')
    ibm_db.close(cpq_c)
    logger.info('Closing connection to ESPR DB')
    ibm_db.close(espr_c)

    logger.info('Done')


if __name__ == '__main__':
    n = len(sys.argv)
    if n < 2:
        run()
    elif n == 2:
        run(sys.argv[1])
    else:
        run(sys.argv[1], int(sys.argv[2]))
