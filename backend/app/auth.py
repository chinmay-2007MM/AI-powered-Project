from datetime import datetime, timedelta, timezone
from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi import HTTPException
from .config import settings

passwords = CryptContext(schemes=["argon2"], deprecated="auto")
ALGORITHM = "HS256"
TOKEN_MINUTES = 45

def hash_password(password: str) -> str:
    if len(password) < 12: raise ValueError("Password must be at least 12 characters")
    return passwords.hash(password)

def verify_password(password: str, hashed: str) -> bool:
    return passwords.verify(password, hashed)

def make_token(subject: str, role: str, expires_minutes: int = TOKEN_MINUTES) -> str:
    now = datetime.now(timezone.utc)
    return jwt.encode({"sub": subject, "role": role, "iat": now, "exp": now + timedelta(minutes=expires_minutes)}, settings.jwt_secret, algorithm=ALGORITHM)

def decode_token(token: str) -> dict:
    try: return jwt.decode(token, settings.jwt_secret, algorithms=[ALGORITHM])
    except JWTError as exc: raise HTTPException(status_code=401, detail="Invalid or expired access token") from exc
