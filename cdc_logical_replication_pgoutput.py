import psycopg2
import os
import sys
from psycopg2.extras import LogicalReplicationConnection
from utilities import message_decoder, message_formatter
import clickhouse_connect
import traceback

ENV_VAR_DATABASE_NAME = 'DATABASE_NAME'
ENV_VAR_DATABASE_USERNAME = 'DATABASE_USERNAME'
ENV_VAR_DATABASE_PASSWORD = 'DATABASE_PASSWORD'
ENV_VAR_DATABASE_HOST = 'DATABASE_HOST'
ENV_VAR_DATABASE_PORT = 'DATABASE_PORT'

ENV_VAR_PG_PUBLICATION_NAME = 'PG_PUBLICATION_NAME'
ENV_VAR_PG_REPLICATION_SLOT = 'PG_REPLICATION_SLOT'

ENV_VAR_CLICKHOUSE_HOSTNAME = 'CLICKHOUSE_HOSTNAME'
ENV_VAR_CLICKHOUSE_USERNAME = 'CLICKHOUSE_USERNAME'
ENV_VAR_CLICKHOUSE_PORT = 'CLICKHOUSE_PORT'
ENV_VAR_CLICKHOUSE_PASSWORD = 'CLICKHOUSE_PASSWORD'
ENV_VAR_CLICKHOUSE_DATABASE = 'CLICKHOUSE_DATABASE'

MANDATORY_ENV_VARS = [ENV_VAR_DATABASE_NAME,
                      ENV_VAR_DATABASE_USERNAME,
                      ENV_VAR_DATABASE_PASSWORD,
                      ENV_VAR_DATABASE_HOST,
                      ENV_VAR_DATABASE_PORT,
                      ENV_VAR_PG_PUBLICATION_NAME,
                      ENV_VAR_PG_REPLICATION_SLOT,
                      ENV_VAR_CLICKHOUSE_HOSTNAME,
                      ENV_VAR_CLICKHOUSE_PORT,
                      ENV_VAR_CLICKHOUSE_USERNAME,
                      ENV_VAR_CLICKHOUSE_PASSWORD,
                      ENV_VAR_CLICKHOUSE_DATABASE]

ENV_VAR_DEBUG = 'DEBUG'


def main():
    # Main code of CDC pgoutput

    check_env_vars()

    pg_conn = get_pg_connection()
    cur = pg_conn.cursor()

    pg_publication_name = os.getenv(ENV_VAR_PG_PUBLICATION_NAME)
    pg_replication_slot = os.getenv(ENV_VAR_PG_REPLICATION_SLOT)

    # options for the default decode plugin : pgoutput
    options = {'publication_names': pg_publication_name, 'proto_version': '1'}

    try:
        cur.start_replication(slot_name=pg_replication_slot, decode=False, options=options)
    except psycopg2.ProgrammingError:
        cur.create_replication_slot(pg_replication_slot, output_plugin='pgoutput')
        cur.start_replication(pg_replication_slot, decode=False, options=options)

    ch_conn = get_ch_connection()
    consumer = Consumer(ch_conn)

    def start_stream():
        cur.consume_stream(consumer)

    try:
        start_stream()
    except Exception as e:
        print(e)
        print(traceback.format_exc())
        print('\nStopping Replication ')
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)


def check_env_vars():
    for envvar in MANDATORY_ENV_VARS:
        if os.getenv(envvar) is None:
            print(f"environment variable {envvar} is missing")
            exit(-1)


def get_pg_connection():
    return psycopg2.connect(
        database=os.getenv(ENV_VAR_DATABASE_NAME),
        user=os.getenv(ENV_VAR_DATABASE_USERNAME),
        password=os.getenv(ENV_VAR_DATABASE_PASSWORD),
        host=os.getenv(ENV_VAR_DATABASE_HOST),
        port=os.getenv(ENV_VAR_DATABASE_PORT),
        connection_factory=LogicalReplicationConnection
    )


def get_ch_connection():
    return clickhouse_connect.get_client(
        host=os.getenv(ENV_VAR_CLICKHOUSE_HOSTNAME), port=os.getenv(ENV_VAR_CLICKHOUSE_PORT),
        username=os.getenv(ENV_VAR_CLICKHOUSE_USERNAME), password=os.getenv(ENV_VAR_CLICKHOUSE_PASSWORD),
        database=os.getenv(ENV_VAR_CLICKHOUSE_DATABASE)
    )


class Consumer(object):
    def __init__(self, ch_conn):
        self.debug = os.getenv(ENV_VAR_DEBUG, "false") == "true"
        self.ch_conn = ch_conn

    def __call__(self, msg):
        if self.debug:
            print("\n#################### START OF MESSAGE #########################\n\n")
            print("Original Encoded Message:\n", msg.payload)

        # Original decoded message
        message = message_decoder.decode_message(msg.payload)

        if self.debug:
            print("\nOriginal Decoded Message:\n", message)

        # Decoded message formatted to JSON
        formatted_message = message_formatter.get_message(str(message))

        if self.debug:
            print("\nFormatted Message:\n", formatted_message)
        msg.cursor.send_feedback(flush_lsn=msg.data_start)
        if self.debug:
            print("\n#################### END OF MESSAGE ###########################\n\n")

        if not isinstance(formatted_message, dict):
            return

        operation = formatted_message.get('Operation', "NOP")
        if operation not in ['INSERT', 'UPDATE', 'DELETE']:
            return
        
        # https://clickhouse.com/docs/en/guides/developer/mutations

        if self.debug:
            print(f"{operation} message")
            
        if operation == 'DELETE':
            self.delete_row(formatted_message)
        elif operation == 'INSERT':
            self.insert_row(formatted_message)
        elif operation == 'UPDATE':
            self.delete_row(formatted_message)
            self.insert_row(formatted_message)

    def insert_row(self, formatted_message):
        col_names = formatted_message['col_names']
        row = formatted_message['col_data']
        #data = [row]
        # this doesn't work with dates as string
        #self.ch_conn.insert(formatted_message['Table_name'], data, column_names=col_names)
        table_name = formatted_message['Table_name']
        columns_for_insert = ", ".join(col_names)
        row_for_insert = ", ".join(list(map(lambda el : "'null'" if el is None else f"'{el}'", row)))
        self.ch_conn.command(f"insert into {table_name}({columns_for_insert}) values({row_for_insert})")

    def delete_row(self, formatted_message):
        condition_column = formatted_message['col_names'][0]
        condition_value = formatted_message['col_data'][0]
        table_name = formatted_message['Table_name']
        delete_query = f"delete from {table_name} where {condition_column} = %(condition_value)s"
        self.ch_conn.command(delete_query, parameters={'condition_value': condition_value})

if __name__ == '__main__':
    main()
