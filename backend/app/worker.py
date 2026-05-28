import json
from datetime import datetime, UTC


def main() -> None:
    payload = {
        "service": "orc-worker",
        "status": "idle",
        "timestamp": datetime.now(UTC).isoformat(),
        "message": "Worker scaffold is running. Implement scheduled jobs next.",
    }
    print(json.dumps(payload))


if __name__ == "__main__":
    main()