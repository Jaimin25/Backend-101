BACKENDS = [
    ("localhost", 8001),
    ("localhost", 8002),
    ("localhost", 8003),
]

FAILURE_THRESHOLD = 2
COOLDOWN_SECONDS = 5

TIMEOUTS = {
    "client_read": 2,
    "backend_connect": 2,
    "backend_resposne": 2,
}

LISTEN_HOST = "localhost"
LISTEN_PORT = 8080