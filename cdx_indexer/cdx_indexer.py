from warcio.cli import Indexer
from warcio.timeutils import iso_date_to_timestamp
from warcio.warcwriter import BufferWARCWriter

from collections import OrderedDict
from argparse import ArgumentParser, RawTextHelpFormatter

import json
import surt


# ============================================================================
class CDXIndexer(Indexer):
    field_names = {'warc-target-uri': 'url',
                   'http:content-type': 'mime',
                   'http:status': 'status',
                   'warc-payload-digest': 'digest'
                  }

    def __init__(self, *args, **kwargs):
        super(CDXIndexer, self).__init__(*args, **kwargs)
        self.writer = None

    def get_field(self, record, name, it, filename):
        if name == 'http:content-type':
            if record.rec_type == 'revisit':
                return 'warc/revisit'
            else:
                value = super(CDXIndexer, self).get_field(record, name, it, filename)
                if value:
                    value = value.split(';')[0].strip()
                else:
                    value = 'unk'

                return value

        value = super(CDXIndexer, self).get_field(record, name, it, filename)

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
        if record.rec_type not in ('response', 'revisit', 'resource', 'metadata'):
            return

        url = index.get('url')
        if not url:
            return

        dt = record.rec_headers.get('WARC-Date')
        ts = iso_date_to_timestamp(dt)

        try:
            url_key = surt.surt(url)
        except:
            url_key = url

        out.write(url_key + ' ' + ts + ' ')
        out.write(json.dumps(index) + '\n')


# ============================================================================
def main(args=None):
    parser = ArgumentParser(description='cdx_indexer',
                            formatter_class=RawTextHelpFormatter)

    parser.add_argument('inputs', nargs='+')
    parser.add_argument('-o', '--output')

    cmd = parser.parse_args(args=args)

    fields = ['warc-target-uri',
              'http:content-type',
              'http:status',
              'warc-payload-digest',
              'length',
              'offset',
              'filename'
             ]

    indexer = CDXIndexer(fields, cmd.inputs, cmd.output)
    indexer.process_all()


# ============================================================================
if __name__ == "__main__":
    main()
