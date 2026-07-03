from pathlib import Path

import pandas as pd

from core.config import settings


festival_df = pd.read_csv(settings.FESTIVALS_PATH)

festival_df["Date"] = pd.to_datetime(
    festival_df["Date"]
)

festival_df["Festival"] = (
    festival_df["Festival"]
    .astype(str)
    .str.strip()
)


def add_calendar_features(df):

    df["day_of_week"] = df["ds"].dt.dayofweek

    df["month"] = df["ds"].dt.month

    df["day_of_year"] = df["ds"].dt.dayofyear

    df["week_of_year"] = (
        df["ds"]
        .dt.isocalendar()
        .week.astype(int)
    )

    df["is_weekend"] = (
        df["day_of_week"] >= 5
    ).astype(int)

    df["quarter"] = df["ds"].dt.quarter

    return df


def add_lag_features(df):

    df["lag_1"] = df["y"].shift(1)

    df["lag_7"] = df["y"].shift(7)

    df["lag_14"] = df["y"].shift(14)

    df["lag_30"] = df["y"].shift(30)

    return df


def add_rolling_features(df):

    df["rolling_7_mean"] = (
        df["y"]
        .shift(1)
        .rolling(7)
        .mean()
    )

    df["rolling_30_mean"] = (
        df["y"]
        .shift(1)
        .rolling(30)
        .mean()
    )

    df["rolling_7_std"] = (
        df["y"]
        .shift(1)
        .rolling(7)
        .std()
    )

    df["ewma_7"] = (
        df["y"]
        .shift(1)
        .ewm(span=7)
        .mean()
    )

    return df


def add_festival_features(
    df,
    known_festivals
):

    for fest in known_festivals:

        col_name = (
            "fest_"
            + fest
            .replace(" ", "_")
            .replace("(", "")
            .replace(")", "")
            .replace("-", "_")
        )

        fest_dates = festival_df[
            festival_df["Festival"] == fest
        ]["Date"]

        df[col_name] = (
            df["ds"]
            .isin(fest_dates)
            .astype(int)
        )

    return df


def build_xgb_features(
    df,
    known_festivals
):

    df = df.copy()

    df["ds"] = pd.to_datetime(df["ds"])

    df = add_calendar_features(df)

    df = add_lag_features(df)

    df = add_rolling_features(df)

    df = add_festival_features(
        df,
        known_festivals
    )

    return df