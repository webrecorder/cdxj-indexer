from __future__ import absolute_import

from warcio.indexer import Indexer
from warcio.timeutils import iso_date_to_timestamp
from warcio.warcwriter import BufferWARCWriter
from warcio.archiveiterator import ArchiveIterator

from argparse import ArgumentParser, RawTextHelpFormatter
from cdxj_indexer.postquery import append_post_query

from io import BytesIO
from copy import copy

import json
import surt
import logging
import os
import re


# ============================================================================
class CDXJIndexer(Indexer):
    field_names = {'warc-target-uri': 'url',
                   'http:status': 'status',
                   'warc-payload-digest': 'digest',
                   'req.http:referer': 'referrer',
                   'req.http:method': 'method',
                  }

    inv_field_names = {k: v for v, k in field_names.items()}

    DEFAULT_FIELDS = ['warc-target-uri',
                      'mime',
                      'http:status',
                      'warc-payload-digest',
                      'length',
                      'offset',
                      'filename'
                     ]

    DEFAULT_RECORDS = ['response', 'revisit', 'resource', 'metadata']

    def __init__(self, output, inputs, opts=None):
        opts = opts or {}

        fields = self._parse_fields(opts)

        super(CDXJIndexer, self).__init__(fields, inputs, output)
        self.writer = None

        self.curr_filename = None
        self.force_filename = opts.get('filename')
        self.post_append = opts.get('post_append')

        self.write_records = opts.get('records')
        if self.write_records == 'all':
            self.write_records = None
        elif self.write_records:
            self.write_records = self.write_records.split(',')
        else:
            self.write_records = self.DEFAULT_RECORDS

        self.collect_records = self.post_append or any(field.startswith('req.http:') for field in self.fields)
        self.record_parse = True

    def _parse_fields(self, opts):
        add_fields = opts.get('replace_fields')

        if add_fields:
            fields = []
        else:
            add_fields = opts.get('fields')
            fields = copy(self.DEFAULT_FIELDS)

        if add_fields:
            add_fields = add_fields.split(',')
            for field in add_fields:
                fields.append(self.inv_field_names.get(field, field))

        return fields

    def get_field(self, record, name, it, filename):
        if name == 'mime':
            if record.rec_type == 'revisit':
                return 'warc/revisit'
            elif record.rec_type in ('response', 'request'):
                name = 'http:content-type'
            else:
                name = 'content-type'

            value = super(CDXJIndexer, self).get_field(record, name, it, filename)
            if value:
                value = re.split(r'[;\s]', value, 1)[0].strip()

            return value

        if name == 'filename':
            return self.curr_filename

        if self.collect_records:
            if name == 'offset':
                return str(record.file_offset)
            elif name == 'length':
                return str(record.file_length)
            elif name.startswith('req.http:'):
                value = self._get_req_field(name, record)
                if value:
                    return value

        value = super(CDXJIndexer, self).get_field(record, name, it, filename)

        if name == 'warc-payload-digest':
            value = self._get_digest(record, name)

        return value

    def _get_req_field(self, name, record):
        if hasattr(record, 'req'):
            req = record.req
        elif record.rec_type == 'request':
            req = record
        else:
            return None

        if name == 'req.http:method':
            return req.http_headers.protocol
        else:
            return req.http_headers.get_header(name[9:])

    def process_one(self, input_, output, filename):
        self.curr_filename = self.force_filename or os.path.basename(filename)

        it = self._create_record_iter(input_)

        self._write_header(output, filename)

        if self.collect_records:
            wrap_it = self.req_resolving_iter(it)
        else:
            wrap_it = it

        for record in wrap_it:
            if not self.write_records or record.rec_type in self.write_records:
                self.process_index_entry(it, record, filename, output)

    def _get_digest(self, record, name):
        value = record.rec_headers.get(name)
        if not value:
            if not self.writer:
                self.writer = BufferWARCWriter()

            self.writer.ensure_digest(record, block=False, payload=True)
            value = record.rec_headers.get(name)

        if value:
            value = value.split(':')[-1]
        return value

    def _write_line(self, out, index, record, filename):
        url = index.get('url')
        if not url:
            url = record.rec_headers.get('WARC-Target-URI')

        dt = record.rec_headers.get('WARC-Date')

        ts = iso_date_to_timestamp(dt)

        if hasattr(record, 'urlkey'):
            urlkey = record.urlkey
        else:
            urlkey = self.get_url_key(url)

        self._do_write(urlkey, ts, index, out)

    def _do_write(self, urlkey, ts, index, out):
        out.write(urlkey + ' ' + ts + ' ')
        out.write(json.dumps(index) + '\n')

    def get_url_key(self, url):
        try:
            return surt.surt(url)
        except:  #pragma: no coverage
            return url

    def _concur_req_resp(self, rec_1, rec_2):
        if not rec_1 or not rec_2:
            return None, None

        if (rec_1.rec_headers.get_header('WARC-Target-URI') !=
            rec_2.rec_headers.get_header('WARC-Target-URI')):
            return None, None

        if (rec_2.rec_headers.get_header('WARC-Concurrent-To') !=
            rec_1.rec_headers.get_header('WARC-Record-ID')):
            return None, None

        if rec_1.rec_type == 'response' and rec_2.rec_type == 'request':
            req = rec_2
            resp = rec_1

        elif rec_1.rec_type == 'request' and rec_2.rec_type == 'response':
            req = rec_1
            resp = rec_2

        else:
            return None, None

        return req, resp

    def req_resolving_iter(self, record_iter):
        prev_record = None

        for record in record_iter:
            if record.rec_type == 'request':
                record.buffered_stream = BytesIO(record.content_stream().read())

            record.file_offset = record_iter.get_record_offset()
            record.file_length = record_iter.get_record_length()

            req, resp = self._concur_req_resp(prev_record, record)

            if not req or not resp:
                if prev_record:
                    yield prev_record
                prev_record = record
                continue

            self._join_req_resp(req, resp)

            yield prev_record
            yield record
            prev_record = None

        if prev_record:
            yield prev_record

    def _join_req_resp(self, req, resp):
        resp.req = req

        method = req.http_headers.protocol
        if self.post_append and method.upper() in ('POST', 'PUT'):
            post_url = append_post_query(req, resp)
            if post_url:
                resp.urlkey = self.get_url_key(post_url)
                req.urlkey = resp.urlkey


# ============================================================================
class CDXLegacyIndexer(CDXJIndexer):
    def _do_write(self, urlkey, ts, index, out):
        index['urlkey'] = urlkey
        index['timestamp'] = ts

        line = ' '.join(index.get(field, '-') for field in self.CDX_FIELDS)
        out.write(line + '\n')

    def _write_header(self, out, filename):
        out.write(self.CDX_HEADER + '\n')


# ============================================================================
class CDX11Indexer(CDXLegacyIndexer):
    CDX_HEADER = ' CDX N b a m s k r M S V g'

    CDX_FIELDS = ['urlkey', 'timestamp', 'url', 'mime', 'status', 'digest',
                  'redirect', 'meta', 'length', 'offset', 'filename']


# ============================================================================
class CDX09Indexer(CDXLegacyIndexer):
    CDX_HEADER = ' CDX N b a m s k r V g'

    CDX_FIELDS = ['urlkey', 'timestamp', 'url', 'mime', 'status', 'digest',
                  'redirect', 'offset', 'filename']


# ============================================================================
def main(args=None):
    parser = ArgumentParser(description='cdx_indexer',
                            formatter_class=RawTextHelpFormatter)

    parser.add_argument('inputs', nargs='+')
    parser.add_argument('-o', '--output')

    group = parser.add_mutually_exclusive_group()
    group.add_argument('-9', '--cdx09',
                        action='store_true')

    group.add_argument('-11', '--cdx11',
                        action='store_true')

    group.add_argument('-f', '--fields')
    group.add_argument('-rf', '--replace-fields')

    parser.add_argument('-r', '--records')

    parser.add_argument('-p', '--post-append', action='store_true')

    cmd = parser.parse_args(args=args)

    write_cdx_index(cmd.output, cmd.inputs, vars(cmd))


# ============================================================================
def write_cdx_index(output, inputs, opt):
    opt = opt or {}
    if opt.get('cdx11'):
        cls = CDX11Indexer
    elif opt.get('cdx09'):
        cls = CDX09Indexer
    else:
        cls = CDXJIndexer

    if isinstance(inputs, str) or hasattr(inputs, 'read'):
        inputs = [inputs]

    indexer = cls(output, inputs, opt)
    indexer.process_all()


# ============================================================================
if __name__ == "__main__":  #pragma: no cover
    main()


