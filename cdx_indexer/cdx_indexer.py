from warcio.indexer import Indexer
from warcio.timeutils import iso_date_to_timestamp
from warcio.warcwriter import BufferWARCWriter
from warcio.archiveiterator import ArchiveIterator

from argparse import ArgumentParser, RawTextHelpFormatter

import json
import surt
import logging


# ============================================================================
class CDXJIndexer(Indexer):
    field_names = {'warc-target-uri': 'url',
                   'http:content-type': 'mime',
                   'http:status': 'status',
                   'warc-payload-digest': 'digest'
                  }

    DEFAULT_FIELDS = ['warc-target-uri',
                      'http:content-type',
                      'http:status',
                      'warc-payload-digest',
                      'length',
                      'offset',
                      'filename'
                     ]

    WRITE_RECORDS = ['response', 'revisit', 'resource', 'metadata']

    def __init__(self, output, inputs, opts=None):
        opts = opts or {}
        fields = opts.get('fields') or self.DEFAULT_FIELDS

        super(CDXJIndexer, self).__init__(fields, inputs, output)
        self.writer = None

        self.include_all = opts.get('include_all', False)
        self.force_filename = opts.get('filename')

    def get_field(self, record, name, it, filename):
        if name == 'http:content-type':
            if record.rec_type == 'revisit':
                return 'warc/revisit'
            else:
                value = super(CDXJIndexer, self).get_field(record, name, it, filename)
                if value:
                    value = value.split(';')[0].strip()
                else:
                    value = 'unk'

                return value

        if name == 'filename' and self.force_filename:
            return self.force_filename

        value = super(CDXJIndexer, self).get_field(record, name, it, filename)

        if name == 'warc-payload-digest':
            value = self._get_digest(record, name)

        if type(value) == int:
            value = str(value)

        return value

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
        if not self.include_all and record.rec_type not in self.WRITE_RECORDS:
            return

        url = index.get('url')
        if not url:
            logging.debug('No Url, Skipping: ' + str(index))
            return

        dt = record.rec_headers.get('WARC-Date')

        ts = iso_date_to_timestamp(dt)
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

    parser.add_argument('-a', '--include_all', action='store_true')

    group = parser.add_mutually_exclusive_group()
    group.add_argument('-9', '--cdx09',
                        action='store_true')

    group.add_argument('-11', '--cdx11',
                        action='store_true')

    group.add_argument('-j', '--cdxj',
                        action='store_true', default=True)

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


