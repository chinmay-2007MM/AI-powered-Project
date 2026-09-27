import argparse
from uuid import uuid4
from .auth import hash_password, make_token
from .db import SessionLocal
from .models import User

def main():
    parser = argparse.ArgumentParser(description="IntelliWatch administrative bootstrap tools")
    sub = parser.add_subparsers(dest="command", required=True)
    create = sub.add_parser("create-user", help="Create an operator account")
    create.add_argument("--email", required=True)
    create.add_argument("--role", choices=["viewer", "operator", "admin"], default="admin")
    sub.add_parser("service-token", help="Print a short-lived event-ingest token for AI_SERVICE_TOKEN")
    args = parser.parse_args()
    if args.command == "service-token":
        print(make_token("ai-worker", "ai_service", expires_minutes=525600)); return
    password = input("Password (12+ characters): ")
    with SessionLocal() as db:
        if db.query(User).filter_by(email=args.email.lower()).first(): raise SystemExit("User already exists")
        db.add(User(id=str(uuid4()), email=args.email.lower(), password_hash=hash_password(password), role=args.role))
        db.commit()
    print("Account created for " + args.email.lower())

if __name__ == "__main__": main()
