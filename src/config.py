from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # 数据库配置
    db_conn_uri: str
    db_schema: str = "public"
    table_prefix: str = "bot_"

    # Worktool 机器人配置
    robot_id: str
    robot_key: str | None = None

    # Dify 配置
    dify_url: str
    dify_token: str
    dify_token_for_memory: str | None = None

    metrics_user: str = "admin"
    metrics_password: str = "admin"

    conversation_expire: int = 60 * 60 * 48
    wake_words: str | None = None
    group_history_limit_count: int = 30
    group_history_limit_chars: int = 500

    owner: str | None = None
    owner_group: str | None = None

    debug: bool = False

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


settings = Settings()