import os
import sys
import tempfile
from io import BytesIO

try:
    from StringIO import StringIO
except ImportError:  # pragma: no cover
    from io import StringIO

from cdxj_indexer.main import write_cdx_index, main, CDXJIndexer

import pkg_resources

TEST_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "data")


# ============================================================================
class TestIndexing(object):
    def index_file(self, filename, **opts):
        output = StringIO()
        write_cdx_index(output, os.path.join(TEST_DIR, filename), opts)
        return output.getvalue()

    def index_all(self, filenames, **opts):
        output = StringIO()
        # paths = [os.path.join(TEST_DIR, filename) for filename in os.listdir(TEST_DIR)]
        paths = [os.path.join(TEST_DIR, filename) for filename in filenames]
        write_cdx_index(output, paths, opts)
        return output.getvalue()

    def index_file_cli(self, filename, capsys):
        res = main([os.path.join(TEST_DIR, filename)])

        return capsys.readouterr().out

    def test_warc_cdxj(self):
        res = self.index_file("example.warc.gz")
        exp = """\
com,example)/ 20170306040206 {"url": "http://example.com/", "mime": "text/html", "status": "200", "digest": "G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK", "length": "1242", "offset": "784", "filename": "example.warc.gz"}
com,example)/ 20170306040348 {"url": "http://example.com/", "mime": "warc/revisit", "status": "200", "digest": "G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK", "length": "585", "offset": "2635", "filename": "example.warc.gz"}
"""
        assert res == exp

    def test_warc_cdxj_cli_main(self, capsys):
        res = self.index_file_cli("example.warc.gz", capsys)
        exp = """\
com,example)/ 20170306040206 {"url": "http://example.com/", "mime": "text/html", "status": "200", "digest": "G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK", "length": "1242", "offset": "784", "filename": "example.warc.gz"}
com,example)/ 20170306040348 {"url": "http://example.com/", "mime": "warc/revisit", "status": "200", "digest": "G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK", "length": "585", "offset": "2635", "filename": "example.warc.gz"}
"""
        assert res == exp

    def test_warc_cdxj_sorted(self):
        res = self.index_file("cc.warc.gz", sort=True)
        exp = """\
org,commoncrawl)/ 20170722005011 {"url": "https://commoncrawl.org/", "mime": "text/html", "status": "200", "digest": "RXZILWL37W7MAZTH76FEVIHSF2DZ5HTM", "length": "5357", "offset": "377", "filename": "cc.warc.gz"}
"""
        assert res == exp

    def test_warc_cdxj_dir_root(self):
        res = self.index_file("example.warc.gz", dir_root="./")
        exp = """\
com,example)/ 20170306040206 {"url": "http://example.com/", "mime": "text/html", "status": "200", "digest": "G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK", "length": "1242", "offset": "784", "filename": "test/data/example.warc.gz"}
com,example)/ 20170306040348 {"url": "http://example.com/", "mime": "warc/revisit", "status": "200", "digest": "G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK", "length": "585", "offset": "2635", "filename": "test/data/example.warc.gz"}
"""
        assert res == exp

    def test_warc_cdx_11(self):
        res = self.index_file("example.warc.gz", cdx11=True)
        exp = """\
 CDX N b a m s k r M S V g
com,example)/ 20170306040206 http://example.com/ text/html 200 G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK - - 1242 784 example.warc.gz
com,example)/ 20170306040348 http://example.com/ warc/revisit 200 G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK - - 585 2635 example.warc.gz
"""
        assert res == exp

    def test_warc_cdx_9(self):
        res = self.index_file("example.warc.gz", cdx09=True)
        exp = """\
 CDX N b a m s k r V g
com,example)/ 20170306040206 http://example.com/ text/html 200 G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK - 784 example.warc.gz
com,example)/ 20170306040348 http://example.com/ warc/revisit 200 G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK - 2635 example.warc.gz
"""
        assert res == exp

    def test_warc_cdx_11_avoid_dupe_line(self):
        res = self.index_file("", cdx11=True, sort=True)
        lines = res.split("\n")
        assert lines[0] == " CDX N b a m s k r M S V g"
        assert lines[1] != " CDX N b a m s k r M S V g"

    def test_index_multiple_files(self):
        res = self.index_all(["example.warc.gz", "post-test.warc.gz"])
        assert len(res.strip().split("\n")) == 5

    def test_warc_request_only(self):
        res = self.index_file("example.warc.gz", records="request", fields="method")
        exp = """\
com,example)/ 20170306040206 {"url": "http://example.com/", "digest": "3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ", "length": "609", "offset": "2026", "filename": "example.warc.gz", "method": "GET"}
com,example)/ 20170306040348 {"url": "http://example.com/", "digest": "3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ", "length": "609", "offset": "3220", "filename": "example.warc.gz", "method": "GET"}
"""
        assert res == exp

    def test_warc_all_cdxj(self):
        res = self.index_file("example.warc.gz", records="all")
        exp = """\
- 20170306040353 {"mime": "application/warc-fields", "length": "353", "offset": "0", "filename": "example.warc.gz"}
- 20170306040353 {"mime": "application/warc-fields", "length": "431", "offset": "353", "filename": "example.warc.gz"}
com,example)/ 20170306040206 {"url": "http://example.com/", "mime": "text/html", "status": "200", "digest": "G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK", "length": "1242", "offset": "784", "filename": "example.warc.gz"}
com,example)/ 20170306040206 {"url": "http://example.com/", "digest": "3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ", "length": "609", "offset": "2026", "filename": "example.warc.gz"}
com,example)/ 20170306040348 {"url": "http://example.com/", "mime": "warc/revisit", "status": "200", "digest": "G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK", "length": "585", "offset": "2635", "filename": "example.warc.gz"}
com,example)/ 20170306040348 {"url": "http://example.com/", "digest": "3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ", "length": "609", "offset": "3220", "filename": "example.warc.gz"}
"""
        assert res == exp

        res = self.index_file("example.warc.gz", records="all", post_append=True)
        assert res == exp

    def test_arc_cdxj(self):
        res = self.index_file("example.arc")
        exp = """\
com,example)/ 20140216050221 {"url": "http://example.com/", "mime": "text/html", "status": "200", "digest": "B2LTWWPUOYAH7UIPQ7ZUPQ4VMBSVC36A", "length": "1656", "offset": "151", "filename": "example.arc"}
"""
        assert res == exp

    def test_arc_bad_edgecase(self):
        res = self.index_file("bad.arc", cdx11=True, post_append=True)
        exp = """\
 CDX N b a m s k r M S V g
com,example)/ 20140401000000 http://example.com/ - - 3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ - - 67 134 bad.arc
com,example)/ 20140102000000 http://example.com/ - - 3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ - - 59 202 bad.arc
com,example)/ 20140401000000 http://example.com/ - - 3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ - - 68 262 bad.arc
"""
        assert res == exp

    def test_warc_post_query_append(self):
        res = self.index_file("post-test.warc.gz", post_append=True)
        exp = """\
org,httpbin)/post?__wb_method=post&foo=bar&test=abc 20140610000859 {"url": "http://httpbin.org/post", "mime": "application/json", "status": "200", "digest": "M532K5WS4GY2H4OVZO6HRPOP47A7KDWU", "length": "720", "offset": "0", "filename": "post-test.warc.gz", "requestBody": "foo=bar&test=abc", "method": "POST"}
org,httpbin)/post?__wb_method=post&a=1&b=[]&c=3 20140610001151 {"url": "http://httpbin.org/post", "mime": "application/json", "status": "200", "digest": "M7YCTM7HS3YKYQTAWQVMQSQZBNEOXGU2", "length": "723", "offset": "1196", "filename": "post-test.warc.gz", "requestBody": "A=1&B=[]&C=3", "method": "POST"}
org,httpbin)/post?__wb_method=post&data=^&foo=bar 20140610001255 {"url": "http://httpbin.org/post?foo=bar", "mime": "application/json", "status": "200", "digest": "B6E5P6JUZI6UPDTNO4L2BCHMGLTNCUAJ", "length": "723", "offset": "2395", "filename": "post-test.warc.gz", "requestBody": "data=^", "method": "POST"}
"""
        assert res == exp

        res = self.index_file("post-test.warc.gz")
        exp = """\
org,httpbin)/post 20140610000859 {"url": "http://httpbin.org/post", "mime": "application/json", "status": "200", "digest": "M532K5WS4GY2H4OVZO6HRPOP47A7KDWU", "length": "720", "offset": "0", "filename": "post-test.warc.gz"}
org,httpbin)/post 20140610001151 {"url": "http://httpbin.org/post", "mime": "application/json", "status": "200", "digest": "M7YCTM7HS3YKYQTAWQVMQSQZBNEOXGU2", "length": "723", "offset": "1196", "filename": "post-test.warc.gz"}
org,httpbin)/post?foo=bar 20140610001255 {"url": "http://httpbin.org/post?foo=bar", "mime": "application/json", "status": "200", "digest": "B6E5P6JUZI6UPDTNO4L2BCHMGLTNCUAJ", "length": "723", "offset": "2395", "filename": "post-test.warc.gz"}
"""
        assert res == exp

    def test_warc_post_query_append_multi_and_json(self):
        res = self.index_file("post-test-more.warc", post_append=True)
        exp = """\
org,httpbin)/post?__wb_method=post&another=more^data&test=some+data 20200809195334 {"url": "https://httpbin.org/post", "mime": "application/json", "status": "200", "digest": "7AWVEIPQMCA4KTCNDXWSZ465FITB7LSK", "length": "688", "offset": "0", "filename": "post-test-more.warc", "requestBody": "test=some+data&another=more%5Edata", "method": "POST"}
org,httpbin)/post?__wb_method=post&a=json-data 20200809195334 {"url": "https://httpbin.org/post", "mime": "application/json", "status": "200", "digest": "BYOQWRSQFW3A5SNUBDSASHFLXGL4FNGB", "length": "655", "offset": "1227", "filename": "post-test-more.warc", "requestBody": "a=json-data", "method": "POST"}
org,httpbin)/post?__wb_method=post&__wb_post_data=c29tzwnodw5rlwvuy29kzwrkyxrh 20200810055049 {"url": "https://httpbin.org/post", "mime": "application/json", "status": "200", "digest": "34LEADQD3MOBQ42FCO2WA5TUSEL5QOKP", "length": "628", "offset": "2338", "filename": "post-test-more.warc", "requestBody": "__wb_post_data=c29tZWNodW5rLWVuY29kZWRkYXRh", "method": "POST"}
"""
        assert res == exp

    def test_warc_cdxj_compressed_1(self):
        # specify file directly
        with tempfile.TemporaryFile() as temp_fh:
            res = self.index_file(
                "",
                sort=True,
                post_append=True,
                compress=temp_fh,
                data_out_name="comp.cdxj.gz",
                lines=11,
            )

        exp = """\
!meta 0 {"format": "cdxj-gzip-1.0", "filename": "%s"}
com,example)/ 20140102000000 {"offset": 0, "length": 784}
org,httpbin)/post?__wb_method=post&data=^&foo=bar 20140610001255 {"offset": 784, "length": 321}
"""
        assert res == exp % "comp.cdxj.gz"

        # specify named temp file, extension auto-added
        with tempfile.NamedTemporaryFile() as temp_fh:
            res = self.index_file(
                "", sort=True, post_append=True, compress=temp_fh.name, lines=11
            )
            name = temp_fh.name

        assert res == exp % (name + ".cdxj.gz")

        # specify named temp file, with extension suffix
        with tempfile.NamedTemporaryFile(suffix=".cdxj.gz") as temp2_fh:
            res = self.index_file(
                "", sort=True, post_append=True, compress=temp2_fh.name, lines=11
            )
            name = temp2_fh.name

        assert res == exp % name

    def test_warc_index_add_custom_fields(self):
        res = self.index_file("example.warc.gz", fields="method,referrer,http:date")

        exp = """\
com,example)/ 20170306040206 {"url": "http://example.com/", "mime": "text/html", "status": "200", "digest": "G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK", "length": "1242", "offset": "784", "filename": "example.warc.gz", "method": "GET", "referrer": "https://webrecorder.io/temp-MJFXHZ4S/temp/recording-session/record/http://example.com/", "http:date": "Mon, 06 Mar 2017 04:02:06 GMT"}
com,example)/ 20170306040348 {"url": "http://example.com/", "mime": "warc/revisit", "status": "200", "digest": "G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK", "length": "585", "offset": "2635", "filename": "example.warc.gz", "http:date": "Mon, 06 Mar 2017 04:03:48 GMT"}
"""
        assert res == exp

    def test_warc_index_custom_fields_1(self):
        res = self.index_file(
            "example.warc.gz",
            records="response,request,revisit",
            replace_fields="warc-type",
        )

        exp = """\
com,example)/ 20170306040206 {"warc-type": "response"}
com,example)/ 20170306040206 {"warc-type": "request"}
com,example)/ 20170306040348 {"warc-type": "revisit"}
com,example)/ 20170306040348 {"warc-type": "request"}
"""
        assert res == exp

    def test_warc_index_custom_fields_2(self):
        res = self.index_file(
            "cc.warc.gz", records="all", replace_fields="method,mime,warc-type,date"
        )

        exp = """\
org,commoncrawl)/ 20170722005011 {"method": "GET", "warc-type": "request"}
org,commoncrawl)/ 20170722005011 {"method": "GET", "mime": "text/html", "warc-type": "response"}
org,commoncrawl)/ 20170722005011 {"mime": "application/warc-fields", "warc-type": "metadata"}
"""
        assert res == exp

    def test_cdxj_empty(self):
        output = StringIO()

        empty = BytesIO()

        opts = {"filename": "empty.warc.gz"}

        write_cdx_index(output, empty, opts)

        assert output.getvalue() == ""

    def test_cdxj_middle_empty_records(self):
        empty_gzip_record = b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00"

        new_warc = BytesIO()

        with open(os.path.join(TEST_DIR, "example.warc.gz"), "rb") as fh:
            new_warc.write(empty_gzip_record)
            new_warc.write(fh.read())
            new_warc.write(empty_gzip_record)
            new_warc.write(empty_gzip_record)
            fh.seek(0)
            new_warc.write(fh.read())

        new_warc.seek(0)

        output = StringIO()
        opts = {"filename": "empty.warc.gz"}

        write_cdx_index(output, new_warc, opts)

        lines = output.getvalue().rstrip().split("\n")

        assert len(lines) == 4, lines


class CustomIndexer(CDXJIndexer):
    def process_index_entry(self, it, record, *args):
        type_ = record.rec_headers.get("WARC-Type")
        if type_ == "response" and record.http_headers.get("Content-Type").startswith(
            "text/html"
        ):
            assert record.buffered_stream.read() != b""


def test_custom_indexer():
    output = StringIO()
    indexer = CustomIndexer(
        output=output,
        inputs=[os.path.join(TEST_DIR, "example.warc.gz")],
        fields="referrer",
    )

    assert indexer.collect_records

    indexer.process_all()
