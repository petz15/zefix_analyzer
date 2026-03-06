from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "postgresql://user:password@localhost:5432/zefix_analyzer"

    zefix_api_base_url: str = "https://www.zefix.admin.ch/ZefixREST/api/v1"
    zefix_api_username: str = ""
    zefix_api_password: str = ""

    google_api_key: str = ""
    google_cse_id: str = ""


settings = Settings()
