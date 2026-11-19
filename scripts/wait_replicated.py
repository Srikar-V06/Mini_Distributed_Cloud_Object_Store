import argparse, sys, time
import httpx

ap = argparse.ArgumentParser()
ap.add_argument("--base", default="http://localhost:8000")
ap.add_argument("--timeout", type=float, default=180)
ap.add_argument("--require-dip", action="store_true",
                help="only succeed after under_replicated rose above 0 and came back to 0")
args = ap.parse_args()

client = httpx.Client(base_url=args.base, timeout=10)
deadline, dipped, last = time.time() + args.timeout, False, None

while time.time() < deadline:
    try:
        rep = client.get("/api/v1/admin/replication").json()
    except (httpx.HTTPError, ValueError) as e:
        line = f"controller not reachable: {type(e).__name__}"
    else:
        if rep.get("state") != "running":
            line = f"monitor {rep.get('state')}"
        else:
            under, over = rep["under_replicated"], rep["over_replicated"]
            dipped = dipped or under > 0
            line = (f"chunks={rep['total_chunks']} healthy={rep['healthy']} under={under} "
                    f"(waiting={rep['waiting']} degraded={rep['degraded']} "
                    f"unavailable={rep['unavailable']} unrecoverable={rep['unrecoverable']}) "
                    f"over={over} repairs={rep['repairs_done']} trims={rep['trims_done']}")
            if under == 0 and over == 0 and (dipped or not args.require_dip):
                print(time.strftime("%H:%M:%S"), line)
                print("REPLICATED")
                sys.exit(0)
    if line != last:
        print(time.strftime("%H:%M:%S"), line)
        last = line
    time.sleep(2)

print("TIMEOUT")
sys.exit(1)