from google.cloud import bigtable
from google.cloud.bigtable import column_family, row_filters
from google.cloud import bigquery

BT_PROJECT = 'gcp-project'
BT_INSTANCE = 'instance'
BT_TABLE = 'transactions'

BQ_PROJECT = 'gcp-project'
BQ_DATASET = 'dataset'
BQ_TABLE = 'transactions'
COLUMN_FAMILY_ID = 'cf1'

COLUMNS = [
    'timestamp',
    'device_id',
    'merchant_name',
    'total_amount']


def bt_to_bq_http(request):
    request_json = request.get_json()

    if request.method == 'POST':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }

    headers = {
        'Access-Control-Allow-Origin': '*'
    }

    bt_row = read_from_bt(request_json)
    bq_row = bt_row_bq_row(bt_row)
    insert_succeeded = write_to_bq(bq_row)
    return ('Insertion %s' % ('succeeded' if insert_succeeded else 'failed'), 200, headers)


def read_from_bt(request):
    bt_client = bigtable.Client(project=BT_PROJECT, admin=True)
    instance = bt_client.instance(BT_INSTANCE)
    table = instance.table(BT_TABLE)

    max_versions_rule = column_family.MaxVersionsGCRule(2)
    column_families = {COLUMN_FAMILY_ID: max_versions_rule}

    if not table.exists():
        table.create(column_families=column_families)

    bt_row_filter = row_filters.CellsColumnLimitFilter(1)
    bt_row_key = request['receipt_id']
    bt_row = table.read_row(bt_row_key.encode('utf-8'), bt_row_filter)
    return bt_row


def bt_row_bq_row(bt_row):
    values = {}
    for column in COLUMNS:
        column_id = column.encode('utf-8')
        values[column] = bt_row.cells[COLUMN_FAMILY_ID][column_id][0].value.decode(
            'utf-8')
    bq_row = tuple(values.get(column) for column in COLUMNS)
    return bq_row


def write_to_bq(bq_row):
    bq_client = bigquery.Client()
    table_ref = bq_client.dataset(BQ_DATASET).table(BQ_TABLE)
    bq_table = bq_client.get_table(table_ref)

    bq_rows = []
    bq_rows.append(bq_row)
    errors = bq_client.insert_rows(bq_table, bq_rows)
    if not errors and len(errors) > 0:
        print(errors)
        return False
    return True
