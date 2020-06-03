import unittest
from log_analyzer import *


class TestCase(unittest.TestCase):

    def test_extract_log(self):
        log = (
            b'1.196.116.32 -  - [29/Jun/2017:03:50:22 +0300] "GET /api/v2/banner/25019354 '
            b'HTTP/1.1" 200 927 "-" "Lynx/2.8.8dev.9 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/2.'
            b'10.5" "-" "1498697422-2190034393-4708-9752759" "dc7161be3" 0.390\n'
        )
        self.assertEqual(
            extract_log(log),
            ['/api/v2/banner/25019354', 0.39]
        )

    def test_mediana(self):
        self.assertEqual(
            mediana([1, 5, 8, 2, 1, 5, 0, 6, 3, 7, 5]),
            5
        )

    def test_agregate_stat(self):
        logs = [
            ['/api/v2', 0.39],
            ['/api/1', 0.133],
            ['/api/v2', 0.199]
        ]
        output_data = (
            {
                '/api/v2': {'count': 2, 'time_sum': 0.589, 'values': [0.39, 0.199]},
                '/api/1': {'count': 1, 'time_sum': 0.133, 'values': [0.133]}
            },
            3,
            0.722
        )
        self.assertEquals(agregate_stat(logs), output_data)

    def test_create_report(self):
        stat = {
            '/api/v2': {'count': 2, 'time_sum': 0.589, 'values': [0.39, 0.199]},
            '/api/1': {'count': 1, 'time_sum': 0.133, 'values': [0.133]}
        }

        output_data = [
            {'url': '/api/v2', 'count': 2, 'count_perc': 66.66666666666666,
             'time_sum': 0.589, 'time_perc': 14.725, 'time_avg': 0.2945,
             'time_max': 0.39, 'time_med': 0.2945},
            {'url': '/api/1', 'count': 1, 'count_perc': 33.33333333333333,
             'time_sum': 0.133, 'time_perc': 3.325, 'time_avg': 0.133,
             'time_max': 0.133, 'time_med': 0.133}
        ]
        self.assertEquals(create_report(stat, 2, 3, 4), output_data)


if __name__ == "__main__":
    unittest.main()
