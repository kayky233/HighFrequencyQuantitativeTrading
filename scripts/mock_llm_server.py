from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        body = json.dumps(
            {
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "action": "BUY",
                                    "quantity": 2,
                                    "limit_price": 149.5,
                                    "confidence": 0.87,
                                    "rationale": "Synthetic LLM smoke response",
                                }
                            )
                        }
                    }
                ]
            }
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


def main() -> None:
    HTTPServer(("127.0.0.1", 18081), Handler).serve_forever()


if __name__ == "__main__":
    main()
