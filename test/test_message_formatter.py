import unittest
from utilities import message_formatter

class TestMessageFormatter(unittest.TestCase):

    def setUp(self):
        decoded_message_relation = """
    Operation : RELATION, 
    byte1 : 'R', 
    relation_id : 91003,
    namespace/schema : 'public',
    relation_name : 'test',
    replica_identity_setting : 'd',
    n_columns : 5 ,
    columns : [(1, 'pk', 23, -1), (0, 'a', 25, -1), (0, 'b', 23, -1), (0, 'c', 1114, -1), (0, 'd', 16, -1)]"""
        message_formatter.get_message(decoded_message_relation)

        decoded_message_begin = """
    Operation : BEGIN, 
	byte1 : 'B', 
	final_tx_lsn : 1756792619528, 
	commit_tx_ts : 2024-12-07 15:51:00.280315+00:00, 
	tx_xid : 16726883"""

        message_formatter.get_message(decoded_message_begin)

    def test_get_message(self):
        decoded_message = """
    Operation : INSERT, 
	byte1: 'I', 
	relation_id : 91003, 
	new tuple byte: 'N', 
	new_tuple : ( n_columns : 5, data : [ColumnData(col_data_category='t', col_data_length=2, col_data='10'), ColumnData(col_data_category='t', col_data_length=1, col_data='f'), ColumnData(col_data_category='t', col_data_length=1, col_data='2'), ColumnData(col_data_category='t', col_data_length=26, col_data='2024-12-07 15:51:00.280023'), ColumnData(col_data_category='n', col_data_length=None, col_data=None)])
    """
        formatted_message_expected = "{'Operation': 'INSERT', 'LSN': '1756792619528', 'Transaction_Xid': '16726883', 'Commit_timestamp': '2024-12-07 15:51:00.280315+00:00', 'Schema': 'public', 'Table_name': 'test', 'Relation_id': '91003', 'col_names': ['pk', 'a', 'b', 'c', 'd'], 'col_data': ['10', 'f', '2', '2024-12-07 15:51:00.280023', None]}"
        formatted_message = message_formatter.get_message(decoded_message)
        self.assertEqual(formatted_message_expected, str(formatted_message))

if __name__ == '__main__':
    unittest.main()