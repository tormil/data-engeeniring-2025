from __future__ import annotations

from datetime import datetime
from pathlib import Path
import os

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from clickhouse_driver import Client

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_TCP_PORT = int(os.getenv("CLICKHOUSE_TCP_PORT", "9000"))
RAW_DATABASE = os.getenv("CLICKHOUSE_DB", "raw_youtube")

DATA_DIR = Path("/opt/airflow/include/data")
CHANNELS_FILE = DATA_DIR / "channels_seed.csv"
VIDEOS_FILE = DATA_DIR / "videos_seed.csv"


def get_client() -> Client:
    return Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_TCP_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )


def ensure_raw_tables(client: Client) -> None:
    client.execute(f"CREATE DATABASE IF NOT EXISTS {RAW_DATABASE}")

    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {RAW_DATABASE}.channels
        (
            ts DateTime DEFAULT now(),
            channelId String,
            title String,
            description String,
            customUrl String,
            subscribers UInt64,
            views UInt64,
            videos UInt64,
            hiddenSubscribers UInt8,
            publishedAt DateTime,
            country String,
            defaultLanguage String,
            keywords String,
            uploadsPlaylistId String,
            topics String,
            loaded_at Date DEFAULT today()
        )
        ENGINE = ReplacingMergeTree(ts)
        PARTITION BY toYYYYMM(loaded_at)
        ORDER BY (channelId, loaded_at)
        TTL loaded_at + INTERVAL 90 DAY
        SETTINGS index_granularity = 8192
        """
    )

    client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {RAW_DATABASE}.videos
        (
            ts DateTime DEFAULT now(),
            videoId String,
            title String,
            publishedAt DateTime,
            tags String,
            categoryId UInt32,
            channelId String,
            duration String,
            definition String,
            caption UInt8,
            licensedContent UInt8,
            regionRestriction String,
            viewCount UInt64,
            likeCount UInt64,
            commentCount UInt64,
            privacyStatus String,
            license String,
            topicCategories String,
            loaded_at Date DEFAULT today()
        )
        ENGINE = ReplacingMergeTree(ts)
        PARTITION BY toYYYYMM(loaded_at)
        ORDER BY (videoId, loaded_at)
        TTL loaded_at + INTERVAL 90 DAY
        SETTINGS index_granularity = 8192
        """
    )


def load_channels_from_csv() -> None:
    if not CHANNELS_FILE.exists():
        raise FileNotFoundError(f"Missing channels seed file at {CHANNELS_FILE}")

    client = get_client()
    ensure_raw_tables(client)

    df = pd.read_csv(CHANNELS_FILE)
    df = df.fillna("")
    df["subscribers"] = pd.to_numeric(df["subscribers"], errors="coerce").fillna(0).astype(int)
    df["views"] = pd.to_numeric(df["views"], errors="coerce").fillna(0).astype(int)
    df["videos"] = pd.to_numeric(df["videos"], errors="coerce").fillna(0).astype(int)
    df["hiddenSubscribers"] = df["hiddenSubscribers"].apply(
        lambda value: str(value).lower() in {"1", "true", "t"}
    ).astype(int)

    ts_value = datetime.now()
    published = pd.to_datetime(df["publishedAt"], errors="coerce")
    published = published.fillna(ts_value)

    rows = [
        (
            ts_value,
            str(row.channelId),
            str(row.title),
            str(row.description),
            str(row.customUrl),
            int(row.subscribers),
            int(row.views),
            int(row.videos),
            int(row.hiddenSubscribers),
            published.iloc[idx].to_pydatetime(),
            str(row.country),
            str(row.defaultLanguage),
            str(row.keywords),
            str(row.uploadsPlaylistId),
            str(row.topics),
        )
        for idx, row in df.iterrows()
    ]

    client.execute(
        f"""
        INSERT INTO {RAW_DATABASE}.channels (
            ts, channelId, title, description, customUrl,
            subscribers, views, videos, hiddenSubscribers,
            publishedAt, country, defaultLanguage, keywords,
            uploadsPlaylistId, topics
        ) VALUES
        """,
        rows,
    )



def load_videos_from_csv() -> None:
    if not VIDEOS_FILE.exists():
        raise FileNotFoundError(f"Missing videos seed file at {VIDEOS_FILE}")

    client = get_client()
    ensure_raw_tables(client)

    df = pd.read_csv(VIDEOS_FILE)
    df = df.fillna("")
    df["categoryId"] = pd.to_numeric(df["categoryId"], errors="coerce").fillna(0).astype(int)
    df["caption"] = df["caption"].apply(lambda value: str(value).lower() in {"1", "true", "t"}).astype(int)
    df["licensedContent"] = df["licensedContent"].apply(
        lambda value: str(value).lower() in {"1", "true", "t"}
    ).astype(int)
    df["viewCount"] = pd.to_numeric(df["viewCount"], errors="coerce").fillna(0).astype(int)
    df["likeCount"] = pd.to_numeric(df["likeCount"], errors="coerce").fillna(0).astype(int)
    df["commentCount"] = pd.to_numeric(df["commentCount"], errors="coerce").fillna(0).astype(int)

    ts_value = datetime.now()
    published = pd.to_datetime(df["publishedAt"], errors="coerce").fillna(ts_value)

    rows = [
        (
            ts_value,
            str(row.videoId),
            str(row.title),
            published.iloc[idx].to_pydatetime(),
            str(row.tags),
            int(row.categoryId),
            str(row.channelId),
            str(row.duration),
            str(row.definition),
            int(row.caption),
            int(row.licensedContent),
            str(row.regionRestriction),
            int(row.viewCount),
            int(row.likeCount),
            int(row.commentCount),
            str(row.privacyStatus),
            str(row.license),
            str(row.topicCategories),
        )
        for idx, row in df.iterrows()
    ]

    client.execute(
        f"""
        INSERT INTO {RAW_DATABASE}.videos (
            ts, videoId, title, publishedAt, tags, categoryId,
            channelId, duration, definition, caption, licensedContent,
            regionRestriction, viewCount, likeCount, commentCount,
            privacyStatus, license, topicCategories
        ) VALUES
        """,
        rows,
    )


with DAG(
    dag_id="bootstrap_clickhouse_from_csv",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["bootstrap", "csv", "clickhouse"],
) as dag:

    seed_channels = PythonOperator(
        task_id="load_seed_channels",
        python_callable=load_channels_from_csv,
    )

    seed_videos = PythonOperator(
        task_id="load_seed_videos",
        python_callable=load_videos_from_csv,
    )

    seed_channels >> seed_videos
