import argparse
import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="Generate deterministic order CSV data")
    parser.add_argument("--records", type=int, default=1000)
    parser.add_argument("--output", default="/data/generated-orders.csv")
    parser.add_argument("--prefix", default="load")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.records < 1:
        raise SystemExit("--records must be at least 1")

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)

    with output.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow([
            "orderId", "customerId", "productId", "amount", "status", "eventTime"
        ])
        for index in range(1, args.records + 1):
            event_time = start + timedelta(seconds=index)
            writer.writerow([
                f"{args.prefix}-order-{index:08d}",
                f"customer-{index % 100:03d}",
                f"product-{index % 20:03d}",
                f"{10 + (index % 500)}.99",
                "CREATED",
                event_time.isoformat().replace("+00:00", "Z"),
            ])

    print(f"Generated {args.records} records at {output}")


if __name__ == "__main__":
    main()

