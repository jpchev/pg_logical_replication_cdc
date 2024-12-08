import unittest
from utilities import message_decoder

class TestMessageDecoder(unittest.TestCase):

    def test_decode_message(self):
        original_message = b'I\x00\x01c{N\x00\x05t\x00\x00\x00\x0210t\x00\x00\x00\x01ft\x00\x00\x00\x012t\x00\x00\x00\x1a2024-12-07 15:51:00.280023n'
        decoded_message_expected = """\tOperation : INSERT, 
	byte1: 'I', 
	relation_id : 91003, 
	new tuple byte: 'N', 
	new_tuple : ( n_columns : 5, data : [ColumnData(col_data_category='t', col_data_length=2, col_data='10'), ColumnData(col_data_category='t', col_data_length=1, col_data='f'), ColumnData(col_data_category='t', col_data_length=1, col_data='2'), ColumnData(col_data_category='t', col_data_length=26, col_data='2024-12-07 15:51:00.280023'), ColumnData(col_data_category='n', col_data_length=None, col_data=None)])"""
        decoded_message = message_decoder.decode_message(original_message)
        self.assertEqual(decoded_message_expected, str(decoded_message))

if __name__ == '__main__':
    unittest.main()