#!/usr/bin/env python3
# Extend http.server to include support for range requests for demo purposes

from datetime import datetime
from http.server import SimpleHTTPRequestHandler, HTTPServer
import argparse, io, os, re, sys
if sys.version_info >= (3, 11): from datetime import UTC
else: import datetime as datetime_fix; UTC=datetime_fix.timezone.utc

STRIP_HTML = True
EXTRA_SEARCH_FN = None
EXTRA_EXAMPLE_FN = None

class RangeHTTPRequestHandler(SimpleHTTPRequestHandler):
    def log_request(self, *args, **kw):
        print(f"{datetime.now(UTC).strftime('%d %H:%M:%S')}: {self.command} '{self.path}' [{self.headers.get('Range', '-')}]")

    def send_head(self):
        if self.path == "/":
            if os.path.isfile("temp_search_page.html"):
                self.path = "/temp_search_page.html"
            elif os.path.isfile("search.html"):
                self.path = "/search.html"
            else:
                self.path = None
                for cur in os.listdir("."):
                    if cur.endswith(".html"):
                        self.path = "/" + cur
                        break
                if self.path is None:
                    print("Unable to find search page!")            
        elif self.path == "/search.html":
            if os.path.isfile("temp_search_page.html"):
                self.path = "/temp_search_page.html"
        elif self.path == "/freq.html":
            if os.path.isfile("temp_word_freq.html"):
                self.path = "/temp_word_freq.html"

        path = self.translate_path(self.path)
        ctype = self.guess_type(path)

        if os.path.isdir(path):
            return SimpleHTTPRequestHandler.send_head(self)

        if not os.path.exists(path):
            return self.send_error(404, self.responses.get(404)[0])

        if path.replace("\\", "/").endswith(".html"):
            with open(path, "rb") as f:
                bits = f.read().decode("utf-8").replace("\r", "")

            bits = bits.replace("<!-- search_title -->", "Search Page Title")
            bits = bits.replace("<!-- search_header -->", "")

            if EXTRA_EXAMPLE_FN is not None and len(EXTRA_EXAMPLE_FN) > 0:
                temp = []
                for cur in EXTRA_EXAMPLE_FN.split(","):
                    with open(cur, "rb") as f:
                        temp.append(f.read().decode("utf-8").replace("\r", ""))
                bits = bits.replace("<!-- example_link -->", "".join(temp))

            if EXTRA_SEARCH_FN is not None and len(EXTRA_SEARCH_FN) > 0:
                with open(EXTRA_SEARCH_FN, "rb") as f:
                    temp = f.read().decode("utf-8").replace("\r", "")
                    bits = bits.replace("<!-- search_terms -->", temp)

            if STRIP_HTML:
                bits = re.sub("/\\*.*?\\*/", "", bits, flags=re.DOTALL)
                bits = bits.split("\n")
                bits = [x.strip() for x in bits]
                bits = "".join(bits)

            bits = re.sub('<img src.*?>', '', bits)
            bits = bits.encode("utf-8")
            f = io.BytesIO(bits)
            size = len(bits)
        else:
            f = open(path, 'rb')
            size = os.fstat(f.fileno())[6]

        start, end = 0, size-1
        if 'Range' in self.headers:
            start, end = self.headers.get('Range').strip().strip('bytes=').split('-')
        if start == "":
            try:
                end = int(end)
            except ValueError as e:
                self.send_error(400, 'invalid range')
            start = size - end
        else:
            try:
                start = int(start)
            except ValueError as e:
                self.send_error(400, 'invalid range')
            if start >= size:
                self.send_error(416, self.responses.get(416)[0])
            if end == "":
                end = size-1
            else:
                try:
                    end = int(end)
                except ValueError as e:
                    self.send_error(400, 'invalid range')

        start = max(start, 0)
        end = min(end, size-1)
        self.range = (start, end)

        l = end - start + 1
        if 'Range' in self.headers:
            self.send_response(206)
        else:
            self.send_response(200)
        self.send_header('Content-type', ctype)
        self.send_header('Accept-Ranges', 'bytes')
        self.send_header('Content-Range', f'bytes {start}-{end}/{size}')
        self.send_header('Content-Length', str(l))
        self.end_headers()

        return f

    def copyfile(self, infile, outfile):
        if not 'Range' in self.headers:
            SimpleHTTPRequestHandler.copyfile(self, infile, outfile)
            return

        start, end = self.range
        infile.seek(start)
        bufsize = 64 * 1024
        left = (end - start) + 1
        while left > 0:
            buf = infile.read(min(left, bufsize))
            if not buf:
                break
            left -= len(buf)
            outfile.write(buf)

def main():
    global STRIP_HTML, EXTRA_SEARCH_FN, EXTRA_EXAMPLE_FN

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, help="Port to run on", default=8000)
    parser.add_argument("--bind", type=str, help="Local IP address to bind to", default="127.0.0.1")
    parser.add_argument("--no_strip", action='store_true', help="Don't strip HTML")
    parser.add_argument("--extra_search", type=str, help="Filename of HTML for search_terms group")
    parser.add_argument("--extra_example", type=str, help="Comma delim filenames of HTML for example_link group")
    args = parser.parse_args()

    STRIP_HTML = not args.no_strip
    EXTRA_SEARCH_FN = args.extra_search
    EXTRA_EXAMPLE_FN = args.extra_example

    class HTTPServerNoReUse(HTTPServer):
        def __init__(self, *args, **kwargs):
            self.allow_reuse_address = 0
            super().__init__(*args, **kwargs)
    server = HTTPServerNoReUse((args.bind, args.port), RangeHTTPRequestHandler)
    print(f"Running server on http://{args.bind}:{args.port}/")
    server.serve_forever()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        exit(0)
