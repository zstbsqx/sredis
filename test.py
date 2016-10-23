import unittest

from sredis import DataParser, RedisError


class TestParser(unittest.TestCase):

    def setUp(self):
        self.parser = DataParser()

    def parse(self, data):
        self.parser.feed(data)
        print self.parser.__dict__
        self.assertTrue(self.parser.done, 'Incompelete input')
        result = self.parser.result
        self.parser.reset()
        return result

    def assertParseResult(self, teststr, res):
        parse_res = self.parse(teststr)
        self.assertEqual(type(parse_res), type(res))
        if type(res) == dict:
            self.assertDictEqual(parse_res, res)
        elif type(res) == list:
            self.assertSequenceEqual(parse_res, res)
        else:
            self.assertEqual(self.parse(teststr), res)

    def test_simple_string(self):
        teststr = '+OK\r\n'
        self.assertParseResult(teststr, 'OK')

    def test_error(self):
        teststr = '-Error message\r\n'
        try:
            self.parse(teststr)
        except RedisError as e:
            self.assertEqual(e.message, 'Error message')
        else:
            self.assertFalse('No exception raised.')

    def test_int(self):
        teststr = ':1000\r\n'
        self.assertParseResult(teststr, 1000)

    def test_bulk_string(self):
        # normal case
        teststr = '$6\r\nfoobar\r\n'
        self.assertParseResult(teststr, 'foobar')

        # null
        teststr = '$-1\r\n'
        self.assertParseResult(teststr, None)

        # abnormal case
        teststr = '$6\r\nfoo\nbar\r\n'
        with self.assertRaises(SyntaxError):
            self.parse(teststr)

    def test_linear_array(self):
        # basic
        array_str = '*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n'
        self.assertParseResult(array_str, ['foo', 'bar'])

        # combined
        combined_array_str = '*5\r\n$3\r\nfoo\r\n:999\r\n+OK\r\n$6\r\nfoobar\r\n$-1\r\n'
        self.assertParseResult(combined_array_str, ['foo', 999, 'OK', 'foobar', None])

        # empty array
        empty_array_str = '*0\r\n'
        self.assertParseResult(empty_array_str, [])

    def test_syntax_errors(self):
        # error
        error_strs = [
            '*2\r\nfff',
            '+OK1\r\n+OK2\r\n',
            '\not OK',
        ]
        for s in error_strs:
            with self.assertRaises(SyntaxError):
                self.parse(s)

    def test_complicated_array(self):
        teststr = '*4\r\n+test begin\r\n*2\r\n$3\r\nsub\r\n$5\r\narray\r\n$-1\r\n:521\r\n'
        self.assertParseResult(teststr, ['test begin', ['sub', 'array'], None, 521])
