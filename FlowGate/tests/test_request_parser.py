from request_parser import parse_request

def test_parse_basic_get():
    raw = b"GET /hello HTTP/1.1\r\nHost: example.com\r\n\r\n"
    req = parse_request(raw)

    assert req["method"] == "GET"
    assert req["path"] == "/hello"
    assert req["headers"]["Host"] == "example.com"