from warcio.utils import to_native_str

from urllib.parse import unquote_plus, urlencode
from io import BytesIO

import base64
import cgi
import json


# ============================================================================
def append_method_query(req, resp):
    len_ = req.http_headers.get_header("Content-Length")
    content_type = req.http_headers.get_header("Content-Type")
    stream = req.buffered_stream
    stream.seek(0)

    url = req.rec_headers.get_header("WARC-Target-URI")

    query = query_extract(content_type, len_, stream, url)

    if "?" not in url:
        append_str = "?"
    else:
        append_str = "&"

    method = req.http_headers.protocol
    append_str += "__wb_method=" + method + "&" + query
    return query, append_str


# ============================================================================
def query_extract(mime, length, stream, url):
    """
    Extract a url-encoded form POST/PUT from stream
    content length, return None
    Attempt to decode application/x-www-form-urlencoded or multipart/*,
    otherwise read whole block and b64encode
    """
    query = b""

    try:
        length = int(length)
    except (ValueError, TypeError):
        length = 8192

    while length > 0:
        buff = stream.read(length)

        length -= len(buff)

        if not buff:
            break

        query += buff

    if not mime:
        mime = ""

    def handle_binary(query):
        query = base64.b64encode(query)
        query = to_native_str(query)
        query = "__wb_post_data=" + query
        return query

    if mime.startswith("application/x-www-form-urlencoded"):
        try:
            query = to_native_str(query.decode("utf-8"))
            query = unquote_plus(query)
        except UnicodeDecodeError:
            query = handle_binary(query)

    elif mime.startswith("multipart/"):
        env = {
            "REQUEST_METHOD": "POST",
            "CONTENT_TYPE": mime,
            "CONTENT_LENGTH": len(query),
        }

        args = dict(fp=BytesIO(query), environ=env, keep_blank_values=True)

        args["encoding"] = "utf-8"

        data = cgi.FieldStorage(**args)

        values = []
        for item in data.list:
            values.append((item.name, item.value))

        query = urlencode(values, True)

    elif mime.startswith("application/json"):
        query = json_parse(query.decode("utf-8"), True)

    elif mime.startswith("text/plain"):
        query = json_parse(query.decode("utf-8"), False)

    else:
        query = handle_binary(query)

    return query


def json_parse(string, warn_on_error=False):
    data = {}

    def _parser(dict_var):
        for n, v in dict_var.items():
            if isinstance(v, dict):
                _parser(v)
            else:
                data[n] = v

    try:
        _parser(json.loads(string))
    except Exception as e:
        if warn_on_error:
            print(e)

    return urlencode(data)
