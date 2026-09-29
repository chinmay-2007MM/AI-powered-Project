from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_env: str = "development"
    database_url: str = "postgresql+psycopg://intelliwatch:intelliwatch@localhost:5432/intelliwatch"
    redis_url: str = "redis://localhost:6379/0"
    s3_endpoint: str = "http://localhost:9000"
    s3_public_endpoint: str = "http://localhost:9000"
    s3_access_key: str = "intelliwatch"
    s3_secret_key: str = "change-this-in-local-env"
    s3_bucket: str = "evidence"
    jwt_secret: str = "development-only-change-this-secret"
    ai_service_token: str = ""
    event_correlation_window_seconds: int = 30
    event_correlation_max_candidates: int = 20
    cors_origins: str = "http://localhost:5173"
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @model_validator(mode="after")
    def validate_secret(self):
        if self.app_env.lower() == "production" and len(self.jwt_secret) < 32:
            raise ValueError("JWT_SECRET must contain at least 32 characters in production")
        return self


settings = Settings()
