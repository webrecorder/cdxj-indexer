from warcio.utils import to_native_str

from urllib.parse import unquote_plus, urlencode
from io import BytesIO

from cdxj_indexer.amf import amf_parse

import base64
import cgi
import json

MAX_QUERY_LENGTH = 4096

# ============================================================================
def append_method_query_from_req_resp(req, resp):
    len_ = req.http_headers.get_header("Content-Length")
    content_type = req.http_headers.get_header("Content-Type")
    stream = req.buffered_stream
    stream.seek(0)

    url = req.rec_headers.get_header("WARC-Target-URI")
    method = req.http_headers.protocol
    return append_method_query(method, content_type, len_, stream, url)


# ============================================================================
def append_method_query(method, content_type, len_, stream, url):
    #if method == 'GET':
    #    return '', ''

    if method == 'POST' or method == 'PUT':
        query = query_extract(content_type, len_, stream, url)
    else:
        query = ''

    if "?" not in url:
        append_str = "?"
    else:
        append_str = "&"

    append_str += "__wb_method=" + method
    if query:
        append_str += "&" + query

    return query, append_str


# ============================================================================
def query_extract(mime, length, stream, url):
    """
    Extract a url-encoded form POST/PUT from stream
    content length, return None
    Attempt to decode application/x-www-form-urlencoded or multipart/*,
    otherwise read whole block and b64encode
    """
    query_data = b""

    try:
        length = int(length)
    except (ValueError, TypeError):
        if length is None:
            length = 8192
        else:
            return

    while length > 0:
        buff = stream.read(length)

        length -= len(buff)

        if not buff:
            break

        query_data += buff

    if not mime:
        mime = ""

    query = ""

    def handle_binary(query_data):
        query = base64.b64encode(query_data)
        query = to_native_str(query)
        query = "__wb_post_data=" + query
        return query

    if mime.startswith("application/x-www-form-urlencoded"):
        try:
            query = to_native_str(query_data.decode("utf-8"))
            query = unquote_plus(query)
        except UnicodeDecodeError:
            query = handle_binary(query_data)

    elif mime.startswith("multipart/"):
        env = {
            "REQUEST_METHOD": "POST",
            "CONTENT_TYPE": mime,
            "CONTENT_LENGTH": len(query_data),
        }

        args = dict(fp=BytesIO(query_data), environ=env, keep_blank_values=True)

        args["encoding"] = "utf-8"

        try:
            data = cgi.FieldStorage(**args)
        except ValueError:
            # Content-Type multipart/form-data may lack "boundary" info
            query = handle_binary(query_data)
        else:
            values = []
            for item in data.list:
                values.append((item.name, item.value))

            query = urlencode(values, True)

    elif mime.startswith('application/json'):
        try:
            query = json_parse(query_data)
        except Exception as e:
            print(e)
            query = ""

    elif mime.startswith('text/plain'):
        try:
            query = json_parse(query_data)
        except Exception as e:
            query = handle_binary(query_data)

    elif mime.startswith('application/x-amf'):
        query = amf_parse(query_data)
    else:
        query = handle_binary(query_data)

    if query:
        query = query[:MAX_QUERY_LENGTH]

    return query


def json_parse(string):
    data = {}
    dupes = {}

    def get_key(n):
        if n not in data:
            return n

        if n not in dupes:
            dupes[n] = 1

        dupes[n] += 1
        return n + "." + str(dupes[n]) + "_";

    def _parser(dict_var):
        for n, v in dict_var.items():
            if isinstance(v, dict):
                _parser(v)
            else:
                data[get_key(n)] = str(v)

    _parser(json.loads(string))
    return urlencode(data)
