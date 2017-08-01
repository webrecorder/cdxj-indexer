CDXJ Indexer
~~~~~~~~~~~~

A command-line tool for generating CDXJ (and  CDX) indexes from WARC and ARC files.
The indexer is a new tool redesigned for fast and flexible indexing. (Based on the indexing functionality from `pywb <https://github.com/ikreymer/pywb>`_)

Install with ``pip install cdxj-indexer`` or install locally with ``python setup.py install``


The indexer supports classic CDX index format as well as the more flexible CDXJ. With CDXJ, the indexer supports custom fields and ``request`` record access for WARC files. See the examples below and the command line ``-h`` option for latest features. (This is a work in progress).


Usage examples
~~~~~~~~~~~~~~~~~~~~

Generate CDXJ index:

.. code:: console

    > cdxj-indexer /path/to/archive-file.warc.gz
    com,example)/ 20170730223850 {"url": "http://example.com/", "mime": "text/html", "status": "200", "digest": "G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK", "length": "1219", "offset": "771", "filename": "example-20170730223917.warc.gz"}


CDX Index (11 field):

.. code:: console

    > cdxj-indexer -11 /path/to/archive-file.warc.gz
    CDX N b a m s k r M S V g
    com,example)/ 20170730223850 http://example.com/ text/html 200 G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK - - 1219 771 example-20170730223917.warc.gz


More advanced use cases: add additonal http headers as fields. ``http:`` prefix specifies current record headers, while ``req.http:`` specifies corresponding request record headers. The following adds the Date, Referer headers, and the request method to the index:

.. code:: console

    > cdxj-indexer -f req.http:method,http:date,req.http:referer /path/to/archive-file.warc.gz
    com,example)/ 20170801032435 {"url": "http://example.com/", "mime": "text/html", "status": "200", "digest": "A6DESOVDZ3WLYF57CS5E4RIC4ARPWRK7", "length": "1207", "offset": "834", "filename": "temp-20170801032445.warc.gz", "req.http:method": "GET", "http:date": "Tue, 01 Aug 2017 03:24:35 GMT", "referrer": "https://webrecorder.io/temp-NU34HBNO/temp/recording-session/record/http://example.com/"}
    org,iana)/domains/example 20170801032437 {"url": "http://www.iana.org/domains/example", "mime": "text/html", "status": "302", "digest": "RP3Y66FDBYBZKSFYQ4VJ4RMDA5BPDJX2", "length": "675", "offset": "2652", "filename": "temp-20170801032445.warc.gz", "req.http:method": "GET", "http:date": "Tue, 01 Aug 2017 02:35:05 GMT", "referrer": "http://example.com/"}


The CDXJ Indexer extends the ``Indexer`` functionality in `warcio <https://github.com/webrecorder/warcio>`_ and should be flexible to extend.




