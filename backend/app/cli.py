import argparse
from getpass import getpass
from uuid import uuid4
from .auth import hash_password, make_token
from .db import SessionLocal
from .models import User
from .models import Factory, Site, Camera, Zone

def main():
    parser = argparse.ArgumentParser(description="IntelliWatch administrative bootstrap tools")
    sub = parser.add_subparsers(dest="command", required=True)
    create = sub.add_parser("create-user", help="Create an operator account")
    create.add_argument("--email", required=True)
    create.add_argument("--role", choices=["viewer", "operator", "admin"], default="admin")
    sub.add_parser("service-token", help="Print a short-lived event-ingest token for AI_SERVICE_TOKEN")
    sub.add_parser("seed-demo", help="Idempotently configure an explicitly labeled IntelliWatch demo facility")
    args = parser.parse_args()
    if args.command == "service-token":
        print(make_token("ai-worker", "ai_service", expires_minutes=525600)); return
    if args.command == "seed-demo":
        with SessionLocal() as db:
            factory = db.get(Factory, "demo-factory")
            if not factory:
                factory = Factory(id="demo-factory", name="IntelliWatch Demonstration Facility", location="Local demo environment", timezone="UTC", configuration={"environment": "DEMO", "source": "seed-demo"})
                db.add(factory)
            site = db.get(Site, "demo-site")
            if not site:
                site = Site(id="demo-site", factory_id=factory.id, name="Demo Assembly Floor", status="demo")
                db.add(site)
            db.flush()
            for camera_id, name in (("demo-camera-01", "Assembly A / North"), ("demo-camera-02", "Assembly A / South")):
                camera = db.get(Camera, camera_id)
                if not camera:
                    db.add(Camera(id=camera_id, site_id=site.id, name=name, resolution="1920x1080", fps=15, status="unconfigured"))
            zone = db.get(Zone, "demo-restricted-zone")
            if not zone:
                db.add(Zone(id="demo-restricted-zone", site_id=site.id, camera_id="demo-camera-01", name="Press safety boundary", polygon=[[500, 250], [1100, 250], [1100, 900], [500, 900]], zone_type="restricted", severity="high", dwell_threshold_seconds=20, enabled=True))
            db.commit()
        print("Demo facility seeded (DEMO labeled); no incidents or measurements were fabricated. Set DEMO_MODE=true on the worker to start the explicitly synthetic simulator.")
        return
    password = getpass("Password (12+ characters): ")
    if len(password) < 12:
        raise SystemExit("Password must contain at least 12 characters")
    if password != getpass("Confirm password: "):
        raise SystemExit("Passwords do not match")
    with SessionLocal() as db:
        if db.query(User).filter_by(email=args.email.lower()).first(): raise SystemExit("User already exists")
        db.add(User(id=str(uuid4()), email=args.email.lower(), password_hash=hash_password(password), role=args.role))
        db.commit()
    print("Account created for " + args.email.lower())

if __name__ == "__main__": main()
