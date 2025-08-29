from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    CLICKHOUSE_URI: str | None = Field(default=None)
    CLICKHOUSE_HOST: str | None = Field(default=None)
    CLICKHOUSE_PORT: int | None = Field(default=None)
    CLICKHOUSE_DATABASE: str | None = Field(default=None)
    CLICKHOUSE_USER: str | None = Field(default=None)
    CLICKHOUSE_PASSWORD: str | None = Field(default=None)

    @property
    def clickhouse_url(self):
        if self.CLICKHOUSE_URI:
            return self.CLICKHOUSE_URI

        if not any(
            [
                self.CLICKHOUSE_HOST,
                self.CLICKHOUSE_PORT,
                self.CLICKHOUSE_DATABASE,
                self.CLICKHOUSE_USER,
                self.CLICKHOUSE_PASSWORD,
            ],
        ):
            raise ValueError(
                "Either CLICKHOUSE_URI or individual ClickHouse connection parameters must be provided"
            )

        missing_params = []
        if not self.CLICKHOUSE_HOST:
            missing_params.append("CLICKHOUSE_HOST")
        if not self.CLICKHOUSE_PORT:
            missing_params.append("CLICKHOUSE_PORT")
        if not self.CLICKHOUSE_DATABASE:
            missing_params.append("CLICKHOUSE_DATABASE")

        if missing_params:
            raise ValueError(
                f"Missing required ClickHouse parameters: {', '.join(missing_params)}"
            )

        url = "clickhouse://"
        if self.CLICKHOUSE_USER:
            url += self.CLICKHOUSE_USER
            if self.CLICKHOUSE_PASSWORD:
                url += f":{self.CLICKHOUSE_PASSWORD}"
            url += "@"

        url += f"{self.CLICKHOUSE_HOST}:{self.CLICKHOUSE_PORT}/{self.CLICKHOUSE_DATABASE}"
        return url

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )


settings = Settings()  # type: ignore
