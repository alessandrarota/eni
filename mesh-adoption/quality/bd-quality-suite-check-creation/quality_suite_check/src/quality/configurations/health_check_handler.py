import logging
import json
from http.server import BaseHTTPRequestHandler


logging.basicConfig(level=logging.INFO)


def liveness_handler(request, client_address, server):
    logging.info("Liveness check request received")
    response = {
        "status": "Alive"
    }
    response_bytes = json.dumps(response).encode('utf-8')
    request.send_response(200)
    request.send_header('Content-type', 'application/json')
    request.send_header('Content-Length', str(len(response_bytes)))
    request.end_headers()
    request.wfile.write(response_bytes)


def readiness_handler(request, client_address, server):
    logging.info("Readiness check request received")
    response = {
        "status": "Ready"
    }
    response_bytes = json.dumps(response).encode('utf-8')
    request.send_response(200)
    request.send_header('Content-type', 'application/json')
    request.send_header('Content-Length', str(len(response_bytes)))
    request.end_headers()
    request.wfile.write(response_bytes)


class RequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        logging.info(f"Request path: {self.path}")
        if self.path == '/liveness':
            liveness_handler(self, self.client_address, self.server)
        elif self.path == '/readiness':
            readiness_handler(self, self.client_address, self.server)
        else:
            logging.warning(f"Path not found: {self.path}")
            self.send_response(404)
            self.end_headers()
