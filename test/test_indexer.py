import os
import sys
from io import BytesIO

try:
    from StringIO import StringIO
except ImportError:  #pragma: no cover
    from io import StringIO

from cdxj_indexer.cdxj_indexer import write_cdx_index, main

import pkg_resources

TEST_DIR = pkg_resources.resource_filename('warcio', os.path.join('..', 'test', 'data'))

from contextlib import contextmanager


# ============================================================================
@contextmanager
def patch_stdout():
    buff = StringIO()
    orig = sys.stdout
    sys.stdout = buff
    yield buff
    sys.stdout = orig


# ============================================================================
class TestIndexing(object):
    def index_file(self, filename, **opts):
        output = StringIO()
        write_cdx_index(output, os.path.join(TEST_DIR, filename), opts)
        return output.getvalue()

    def index_file_cli(self, filename):
        with patch_stdout() as output:
            res = main([os.path.join(TEST_DIR, filename)])

        return output.getvalue()

    def test_warc_cdxj(self):
        res = self.index_file('example.warc.gz')
        exp = """\
com,example)/ 20170306040206 {"url": "http://example.com/", "mime": "text/html", "status": "200", "digest": "G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK", "length": "1228", "offset": "784", "filename": "example.warc.gz"}
com,example)/ 20170306040348 {"url": "http://example.com/", "mime": "warc/revisit", "status": "200", "digest": "G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK", "length": "585", "offset": "2538", "filename": "example.warc.gz"}
"""
        assert res == exp

    def test_warc_cdxj_cli_main(self):
        res = self.index_file_cli('example.warc.gz')
        exp = """\
com,example)/ 20170306040206 {"url": "http://example.com/", "mime": "text/html", "status": "200", "digest": "G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK", "length": "1228", "offset": "784", "filename": "example.warc.gz"}
com,example)/ 20170306040348 {"url": "http://example.com/", "mime": "warc/revisit", "status": "200", "digest": "G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK", "length": "585", "offset": "2538", "filename": "example.warc.gz"}
"""
        assert res == exp

    def test_warc_cdx_11(self):
        res = self.index_file('example.warc.gz', cdx11=True)
        exp = """\
 CDX N b a m s k r M S V g
com,example)/ 20170306040206 http://example.com/ text/html 200 G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK - - 1228 784 example.warc.gz
com,example)/ 20170306040348 http://example.com/ warc/revisit 200 G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK - - 585 2538 example.warc.gz
"""
        assert res == exp

    def test_warc_cdx_9(self):
        res = self.index_file('example.warc.gz', cdx09=True)
        exp = """\
 CDX N b a m s k r V g
com,example)/ 20170306040206 http://example.com/ text/html 200 G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK - 784 example.warc.gz
com,example)/ 20170306040348 http://example.com/ warc/revisit 200 G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK - 2538 example.warc.gz
"""
        assert res == exp

    def test_warc_all_cdxj(self):
        res = self.index_file('example.warc.gz', include_all=True)
        exp = """\
com,example)/ 20170306040206 {"url": "http://example.com/", "mime": "text/html", "status": "200", "digest": "G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK", "length": "1228", "offset": "784", "filename": "example.warc.gz"}
com,example)/ 20170306040206 {"url": "http://example.com/", "mime": "unk", "digest": "3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ", "length": "526", "offset": "2012", "filename": "example.warc.gz"}
com,example)/ 20170306040348 {"url": "http://example.com/", "mime": "warc/revisit", "status": "200", "digest": "G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK", "length": "585", "offset": "2538", "filename": "example.warc.gz"}
com,example)/ 20170306040348 {"url": "http://example.com/", "mime": "unk", "digest": "3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ", "length": "526", "offset": "3123", "filename": "example.warc.gz"}
"""
        assert res == exp

        res = self.index_file('example.warc.gz', include_all=True, post_append=True)
        assert res == exp

    def test_arc_cdxj(self):
        res = self.index_file('example.arc')
        exp = """\
com,example)/ 20140216050221 {"url": "http://example.com/", "mime": "text/html", "status": "200", "digest": "B2LTWWPUOYAH7UIPQ7ZUPQ4VMBSVC36A", "length": "1656", "offset": "151", "filename": "example.arc"}
"""
        assert res == exp

    def test_arc_bad_edgecase(self):
        res = self.index_file('bad.arc', cdx11=True)
        exp = """\
 CDX N b a m s k r M S V g
com,example)/ 20140401000000 http://example.com/ unk - 3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ - - 67 134 bad.arc
com,example)/ 20140102000000 http://example.com/ unk - 3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ - - 59 202 bad.arc
com,example)/ 20140401000000 http://example.com/ unk - 3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ - - 68 262 bad.arc
"""
        assert res == exp

    def test_warc_post_query_append(self):
        res = self.index_file('post-test.warc.gz', post_append=True)
        exp = """\
org,httpbin)/post?foo=bar&test=abc 20140610000859 {"url": "http://httpbin.org/post", "mime": "application/json", "status": "200", "digest": "M532K5WS4GY2H4OVZO6HRPOP47A7KDWU", "length": "720", "offset": "0", "filename": "post-test.warc.gz"}
org,httpbin)/post?a=1&b=[]&c=3 20140610001151 {"url": "http://httpbin.org/post", "mime": "application/json", "status": "200", "digest": "M7YCTM7HS3YKYQTAWQVMQSQZBNEOXGU2", "length": "723", "offset": "1196", "filename": "post-test.warc.gz"}
org,httpbin)/post?data=^&foo=bar 20140610001255 {"url": "http://httpbin.org/post?foo=bar", "mime": "application/json", "status": "200", "digest": "B6E5P6JUZI6UPDTNO4L2BCHMGLTNCUAJ", "length": "723", "offset": "2395", "filename": "post-test.warc.gz"}
"""
        assert res == exp

        res = self.index_file('post-test.warc.gz')
        exp = """\
org,httpbin)/post 20140610000859 {"url": "http://httpbin.org/post", "mime": "application/json", "status": "200", "digest": "M532K5WS4GY2H4OVZO6HRPOP47A7KDWU", "length": "720", "offset": "0", "filename": "post-test.warc.gz"}
org,httpbin)/post 20140610001151 {"url": "http://httpbin.org/post", "mime": "application/json", "status": "200", "digest": "M7YCTM7HS3YKYQTAWQVMQSQZBNEOXGU2", "length": "723", "offset": "1196", "filename": "post-test.warc.gz"}
org,httpbin)/post?foo=bar 20140610001255 {"url": "http://httpbin.org/post?foo=bar", "mime": "application/json", "status": "200", "digest": "B6E5P6JUZI6UPDTNO4L2BCHMGLTNCUAJ", "length": "723", "offset": "2395", "filename": "post-test.warc.gz"}
"""
        assert res == exp

    def test_cdxj_empty(self):
        output = StringIO()

        empty = BytesIO()

        opts = {'filename': 'empty.warc.gz'}

        write_cdx_index(output, empty, opts)

        assert output.getvalue() == ''

    def test_cdxj_middle_empty_records(self):
        empty_gzip_record = b'\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00'

        new_warc = BytesIO()

        with open(os.path.join(TEST_DIR, 'example.warc.gz'), 'rb') as fh:
            new_warc.write(empty_gzip_record)
            new_warc.write(fh.read())
            new_warc.write(empty_gzip_record)
            new_warc.write(empty_gzip_record)
            fh.seek(0)
            new_warc.write(fh.read())

        new_warc.seek(0)

        output = StringIO()
        opts = {'filename': 'empty.warc.gz'}

        write_cdx_index(output, new_warc, opts)

        lines = output.getvalue().rstrip().split('\n')

        assert len(lines) == 4, lines
