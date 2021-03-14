from __future__ import absolute_import

from warcio.indexer import Indexer
from warcio.timeutils import iso_date_to_timestamp
from warcio.warcwriter import BufferWARCWriter
from warcio.archiveiterator import ArchiveIterator
from warcio.utils import open_or_default

from argparse import ArgumentParser, RawTextHelpFormatter
from cdxj_indexer.postquery import append_method_query

from io import BytesIO
from copy import copy

import json
import surt
import logging
import os
import re
import shutil
import sys
import tempfile
import zlib


# ============================================================================
class CDXJIndexer(Indexer):
    field_names = {
        "warc-target-uri": "url",
        "http:status": "status",
        "warc-payload-digest": "digest",
        "req.http:referer": "referrer",
        "req.http:method": "method",
    }

    inv_field_names = {k: v for v, k in field_names.items()}

    DEFAULT_FIELDS = [
        "warc-target-uri",
        "mime",
        "http:status",
        "warc-payload-digest",
        "length",
        "offset",
        "filename",
    ]

    DEFAULT_RECORDS = ["response", "revisit", "resource", "metadata"]

    ALLOWED_EXT = (".arc", ".arc.gz", ".warc", ".warc.gz")

    RE_SPACE = re.compile(r"[;\s]")

    def __init__(
        self,
        output,
        inputs,
        post_append=False,
        sort=False,
        compress=None,
        lines=300,
        data_out_name=None,
        filename=None,
        fields=None,
        replace_fields=None,
        records=None,
        verify_http=False,
        dir_root=None,
        **kwargs
    ):

        if isinstance(inputs, str) or hasattr(inputs, "read"):
            inputs = [inputs]

        inputs = iter_file_or_dir(inputs)

        fields = self._parse_fields(fields, replace_fields)

        super(CDXJIndexer, self).__init__(
            fields, inputs, output, verify_http=verify_http
        )
        self.writer = None

        self.curr_filename = None
        self.force_filename = filename
        self.post_append = post_append
        self.dir_root = dir_root

        self.num_lines = lines
        self.sort = sort
        self.compress = compress
        self.data_out_name = data_out_name

        self.include_records = records
        if self.include_records == "all":
            self.include_records = None
        elif self.include_records:
            self.include_records = self.include_records.split(",")
        else:
            self.include_records = self.DEFAULT_RECORDS

        self.collect_records = self.post_append or any(
            field.startswith("req.http:") for field in self.fields
        )
        self.record_parse = True

    def _parse_fields(self, fields=None, replace_fields=None):
        add_fields = replace_fields
        if add_fields:
            fields = []
        else:
            add_fields = fields
            fields = copy(self.DEFAULT_FIELDS)

        if add_fields:
            add_fields = add_fields.split(",")
            for field in add_fields:
                fields.append(self.inv_field_names.get(field, field))

        return fields

    def get_field(self, record, name, it, filename):
        if name == "mime":
            if record.rec_type == "revisit":
                return "warc/revisit"
            elif record.rec_type in ("response", "request"):
                name = "http:content-type"
            else:
                name = "content-type"

            value = super(CDXJIndexer, self).get_field(record, name, it, filename)
            if value:
                value = self.RE_SPACE.split(value, 1)[0].strip()

            return value

        if name == "filename":
            return self.curr_filename

        if self.collect_records:
            if name == "offset":
                return str(record.file_offset)
            elif name == "length":
                return str(record.file_length)
            elif name.startswith("req.http:"):
                value = self._get_req_field(name, record)
                if value:
                    return value

        value = super(CDXJIndexer, self).get_field(record, name, it, filename)

        if name == "warc-payload-digest":
            value = self._get_digest(record, name)

        return value

    def _get_req_field(self, name, record):
        if hasattr(record, "req"):
            req = record.req
        elif record.rec_type == "request":
            req = record
        else:
            return None

        if name == "req.http:method":
            return req.http_headers.protocol
        else:
            return req.http_headers.get_header(name[9:])

    def process_all(self):
        data_out = None

        with open_or_default(self.output, "wt", sys.stdout) as fh:
            if self.compress:
                if isinstance(self.compress, str):
                    data_out = open(self.compress, "wb")
                    if os.path.splitext(self.compress)[1] == "":
                        self.compress += ".cdxj.gz"

                    fh = CompressedWriter(
                        fh,
                        data_out=data_out,
                        data_out_name=self.compress,
                        num_lines=self.num_lines,
                    )
                else:
                    fh = CompressedWriter(
                        fh,
                        data_out=self.compress,
                        data_out_name=self.data_out_name,
                        num_lines=self.num_lines,
                    )

            if self.sort:
                fh = SortingWriter(fh)

            self.output = fh

            super().process_all()

            if self.sort or self.compress:
                fh.flush()
                if data_out:
                    data_out.close()

    def _resolve_rel_path(self, filename):
        if not self.dir_root:
            return os.path.basename(filename)

        path = os.path.relpath(filename, self.dir_root)
        if os.path.sep != "/":  # pragma: no cover
            path = path.replace(os.path.sep, "/")
        return path

    def process_one(self, input_, output, filename):
        self.curr_filename = self.force_filename or self._resolve_rel_path(filename)

        it = self._create_record_iter(input_)

        self._write_header(output, filename)

        if self.collect_records:
            wrap_it = self.req_resolving_iter(it)
        else:
            wrap_it = it

        for record in wrap_it:
            if not self.include_records or self.filter_record(record):
                self.process_index_entry(it, record, filename, output)

    def filter_record(self, record):
        if not record.rec_type in self.include_records:
            return False

        if (
            self.include_records == self.DEFAULT_RECORDS
            and record.rec_type in ("resource", "metadata")
            and record.rec_headers.get_header("Content-Type")
            == "application/warc-fields"
        ):
            return False

        return True

    def _get_digest(self, record, name):
        value = record.rec_headers.get(name)
        if not value:
            if not self.writer:
                self.writer = BufferWARCWriter()

            self.writer.ensure_digest(record, block=False, payload=True)
            value = record.rec_headers.get(name)

        if value:
            value = value.split(":")[-1]
        return value

    def _write_line(self, out, index, record, filename):
        url = index.get("url")
        if not url:
            url = record.rec_headers.get("WARC-Target-URI")

        dt = record.rec_headers.get("WARC-Date")

        ts = iso_date_to_timestamp(dt)

        if hasattr(record, "urlkey"):
            urlkey = record.urlkey
        else:
            urlkey = self.get_url_key(url)

        if hasattr(record, "requestBody"):
            index["requestBody"] = record.requestBody
        if hasattr(record, "method"):
            index["method"] = record.method

        self._do_write(urlkey, ts, index, out)

    def _do_write(self, urlkey, ts, index, out):
        out.write(urlkey + " " + ts + " " + json.dumps(index) + "\n")

    def get_url_key(self, url):
        try:
            return surt.surt(url)
        except:  # pragma: no coverage
            return url

    def _concur_req_resp(self, rec_1, rec_2):
        if not rec_1 or not rec_2:
            return None, None

        if rec_1.rec_headers.get_header(
            "WARC-Target-URI"
        ) != rec_2.rec_headers.get_header("WARC-Target-URI"):
            return None, None

        if rec_2.rec_headers.get_header(
            "WARC-Concurrent-To"
        ) != rec_1.rec_headers.get_header("WARC-Record-ID"):
            return None, None

        if rec_1.rec_type == "response" and rec_2.rec_type == "request":
            req = rec_2
            resp = rec_1

        elif rec_1.rec_type == "request" and rec_2.rec_type == "response":
            req = rec_1
            resp = rec_2

        else:
            return None, None

        return req, resp

    def read_content(self, record):
        spool = tempfile.SpooledTemporaryFile()
        shutil.copyfileobj(record.content_stream(), spool)
        spool.seek(0)
        record.buffered_stream = spool
        # record.buffered_stream = BytesIO(record.content_stream().read())

    def req_resolving_iter(self, record_iter):
        prev_record = None

        for record in record_iter:
            # if record.rec_type == "request":
            self.read_content(record)

            record.file_offset = record_iter.get_record_offset()
            record.file_length = record_iter.get_record_length()

            req, resp = self._concur_req_resp(prev_record, record)

            if not req or not resp:
                if prev_record:
                    yield prev_record
                    prev_record.buffered_stream.close()
                prev_record = record
                continue

            self._join_req_resp(req, resp)

            yield prev_record
            prev_record.buffered_stream.close()
            yield record
            record.buffered_stream.close()
            prev_record = None

        if prev_record:
            yield prev_record
            prev_record.buffered_stream.close()

    def _join_req_resp(self, req, resp):
        resp.req = req

        method = req.http_headers.protocol
        if self.post_append and method.upper() in ("POST", "PUT"):
            url = req.rec_headers.get_header("WARC-Target-URI")
            query, append_str = append_method_query(req, resp)
            resp.method = method.upper()
            resp.requestBody = query
            resp.urlkey = self.get_url_key(url + append_str)
            req.urlkey = resp.urlkey


# ============================================================================
class CDXLegacyIndexer(CDXJIndexer):
    def _do_write(self, urlkey, ts, index, out):
        index["urlkey"] = urlkey
        index["timestamp"] = ts

        line = " ".join(index.get(field, "-") for field in self.CDX_FIELDS)
        out.write(line + "\n")

    def _write_header(self, out, filename):
        out.write(self.CDX_HEADER + "\n")


# ============================================================================
class CDX11Indexer(CDXLegacyIndexer):
    CDX_HEADER = " CDX N b a m s k r M S V g"

    CDX_FIELDS = [
        "urlkey",
        "timestamp",
        "url",
        "mime",
        "status",
        "digest",
        "redirect",
        "meta",
        "length",
        "offset",
        "filename",
    ]


# ============================================================================
class CDX09Indexer(CDXLegacyIndexer):
    CDX_HEADER = " CDX N b a m s k r V g"

    CDX_FIELDS = [
        "urlkey",
        "timestamp",
        "url",
        "mime",
        "status",
        "digest",
        "redirect",
        "offset",
        "filename",
    ]


# ============================================================================
class SortingWriter:
    def __init__(self, out):
        self.out = out
        self.sortedlist = []

    def write(self, line):
        self.sortedlist.append(line)

    def flush(self):
        self.sortedlist.sort()
        lastline = None
        for line in self.sortedlist:
            if lastline != line:
                self.out.write(line)
            lastline = line

        self.out.flush()


# ============================================================================
class CompressedWriter:
    def __init__(self, index_out, data_out, num_lines=300, data_out_name=""):
        self.index_out = index_out
        self.data_out = data_out
        self.data_out_name = data_out_name

        self.block = []
        self.offset = 0
        self.prefix = ""
        self.num_lines = num_lines

    def write_header(self):
        meta = json.dumps({"format": "cdxj-gzip-1.0", "filename": self.data_out_name})

        self.index_out.write("!meta 0 {0}\n".format(meta))

    def write(self, line):
        if not len(self.block):
            self.prefix = line.split("{", 1)[0].strip()
            if not self.offset:
                self.write_header()

        self.block.append(line)

        if len(self.block) == self.num_lines:
            self.flush()

    def get_index_json(self, length):
        data = {"offset": self.offset, "length": length}

        return json.dumps(data) + "\n"

    def flush(self):
        comp = zlib.compressobj(wbits=16 + zlib.MAX_WBITS)
        compressed = comp.compress("".join(self.block).encode("utf-8"))
        compressed += comp.flush()

        length = len(compressed)
        line = self.prefix + " " + self.get_index_json(length)
        self.index_out.write(line)
        self.data_out.write(compressed)
        self.offset += length
        self.block = []


# ============================================================================
def main(args=None):
    parser = ArgumentParser(
        description="cdx_indexer", formatter_class=RawTextHelpFormatter
    )

    parser.add_argument("inputs", nargs="+")
    parser.add_argument("-o", "--output")

    group = parser.add_mutually_exclusive_group()
    group.add_argument("-9", "--cdx09", action="store_true")

    group.add_argument("-11", "--cdx11", action="store_true")

    group.add_argument("-f", "--fields")
    group.add_argument("-rf", "--replace-fields")

    parser.add_argument("--records")

    parser.add_argument("--dir-root")

    parser.add_argument("-p", "--post-append", action="store_true")

    parser.add_argument("-s", "--sort", action="store_true")

    parser.add_argument("-c", "--compress")

    parser.add_argument("-l", "--lines", type=int, default=300)

    cmd = parser.parse_args(args=args)

    write_cdx_index(cmd.output, cmd.inputs, vars(cmd))


def write_cdx_index(output, inputs, opts):
    if opts.get("cdx11"):
        cls = CDX11Indexer
    elif opts.get("cdx09"):
        cls = CDX09Indexer
    else:
        cls = CDXJIndexer

    opts.pop("output", "")
    opts.pop("inputs", "")

    indexer = cls(output, inputs, **opts)
    indexer.process_all()
    return indexer


# =================================================================
def iter_file_or_dir(inputs, recursive=True):
    for input_ in inputs:
        if not isinstance(input_, str) or not os.path.isdir(input_):
            yield input_
            continue

        for root, dirs, files in os.walk(input_):
            for filename in files:
                if filename.endswith(CDXJIndexer.ALLOWED_EXT):
                    full_path = os.path.join(root, filename)
                    yield full_path


# ============================================================================
if __name__ == "__main__":  # pragma: no cover
    main()
