from warcio.utils import to_native_str

from six.moves.urllib.parse import unquote_plus
from io import BytesIO

import base64
import cgi


# ============================================================================
class PostQueryExtractor(object):
    def __init__(self, method, mime, length, stream,
                       buffered_stream=None,
                       environ=None):
        """
        Extract a url-encoded form POST from stream
        content length, return None
        Attempt to decode application/x-www-form-urlencoded or multipart/*,
        otherwise read whole block and b64encode
        """
        self.post_query = b''

        if method.upper() != 'POST':
            return

        try:
            length = int(length)
        except (ValueError, TypeError):
            return

        if length <= 0:
            return

        post_query = b''

        while length > 0:
            buff = stream.read(length)
            length -= len(buff)

            if not buff:
                break

            post_query += buff

        if buffered_stream:
            buffered_stream.write(post_query)
            buffered_stream.seek(0)

        if not mime:
            mime = ''

        self.post_query = self._make_query(mime)

    def _make_query(self, mime, post_query):
        if mime.startswith('application/x-www-form-urlencoded'):
            post_query = to_native_str(post_query)
            post_query = unquote_plus(post_query)

        elif mime.startswith('multipart/'):
            env = {'REQUEST_METHOD': 'POST',
                   'CONTENT_TYPE': mime,
                   'CONTENT_LENGTH': len(post_query)}

            args = dict(fp=BytesIO(post_query),
                        environ=env,
                        keep_blank_values=True)

            if six.PY3:
                args['encoding'] = 'utf-8'

            data = cgi.FieldStorage(**args)

            values = []
            for item in data.list:
                values.append((item.name, item.value))

            post_query = urlencode(values, True)

        else:
            post_query = base64.b64encode(post_query)
            post_query = to_native_str(post_query)
            post_query = '__wb_post_data=' + post_query

        return post_query

    def append_post_query(self, url):
        if not self.post_query:
            return url

        if '?' not in url:
            url += '?'
        else:
            url += '&'

        url += self.post_query
        return url

