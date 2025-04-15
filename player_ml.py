import os
import polars as pl
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from csv_upload import get_clickhouse_client


# NUM_SAMPLED_GAMES = len(df["game_id"].unique())
# QUERY_LIMIT = 5000000  # Limit for the number of rows to fetch from ClickHouse

def parse_score_column(score_col: pl.Series) -> pl.DataFrame:
    """Split 'score' string like '102 - 99' into two columns: home_score and away_score"""
    parts = score_col.str.split(" - ", inclusive=False)
    return pl.DataFrame({
        "home_score": parts.list.get(0).cast(pl.Int32, strict=False),
        "away_score": parts.list.get(1).cast(pl.Int32, strict=False),
    })


def load_play_by_play(client) -> pl.DataFrame:
    print("📥 Loading play_by_play data...")
    query = "SELECT * FROM play_by_play"
    result = client.query(query)
    if not result.result_set:
        print("⚠️ No data returned.")
        return pl.DataFrame()
    return pl.DataFrame(dict(zip(result.column_names, zip(*result.result_set))))


def create_features(df: pl.DataFrame) -> pl.DataFrame:
    print("🧪 Creating features...")

    # Count all events per player per quarter
    base_counts = df.group_by(["game_id", "player1_id", "period"]).agg([
        pl.len().alias("total_events")
    ])

    # Count number of each eventmsgtype
    event_type_counts = df.group_by(["game_id", "player1_id", "period", "eventmsgtype"]).agg(
    pl.len().alias("count")
)


    
    event_type_counts = event_type_counts.pivot(
        values="count",
        index=["game_id", "player1_id", "period"],
        on="eventmsgtype",
        aggregate_function="first"
    ).fill_null(0)

    # Rename event_type columns to prefix them
    rename_map = {col: f"event_type_{col}" for col in event_type_counts.columns if isinstance(col, int)}
    event_type_counts = event_type_counts.rename(rename_map)

    # Merge total counts + per-type counts
    event_counts = base_counts.join(event_type_counts, on=["game_id", "player1_id", "period"], how="left").fill_null(0)

    # --- TARGET CONSTRUCTION ---

    print("🔗 Joining with total points...")
    scoring_df = df.filter(
        (pl.col("eventmsgtype") == 1) | (pl.col("eventmsgtype") == 3)
    ).filter(pl.col("score").is_not_null())

    parsed_scores = parse_score_column(scoring_df["score"])
    scoring_df = scoring_df.with_columns([
        parsed_scores["home_score"],
        parsed_scores["away_score"]
    ])

    # Sort and compute previous scores explicitly using groupby + shift
    scoring_df = scoring_df.sort(["game_id", "eventnum"])

    # Compute previous scores by group
    scoring_df = scoring_df.with_columns([
        pl.col("home_score").shift(1).over("game_id").alias("prev_home_score"),
        pl.col("away_score").shift(1).over("game_id").alias("prev_away_score")
    ])

    # Compute score deltas
    scoring_df = scoring_df.with_columns([
        (pl.col("home_score") - pl.col("prev_home_score")).fill_null(0).alias("home_delta"),
        (pl.col("away_score") - pl.col("prev_away_score")).fill_null(0).alias("away_delta"),
    ])

    # Derive points scored by the player
    scoring_df = scoring_df.with_columns([
        pl.when(pl.col("home_delta") > 0).then(pl.col("home_delta"))
        .when(pl.col("away_delta") > 0).then(pl.col("away_delta"))
        .otherwise(0).alias("points_scored")
    ])


    total_points = scoring_df.group_by(["game_id", "player1_id", "period"]).agg(
        pl.col("points_scored").sum().alias("total_points")
    )

    print("📦 Assembling final feature set...")
    features_df = event_counts.join(total_points, on=["game_id", "player1_id", "period"], how="left")
    features_df = features_df.fill_null(0)

    return features_df


def train_model(features_df: pl.DataFrame):
    print("🤖 Training baseline regression model...")
    df = features_df.to_pandas()
    
    df = df.rename(columns={"player1_id": "player_id"})  # rename for clarity
    feature_cols = [col for col in df.columns if col.startswith("event_type_") or col == "total_events"]
    
    X = df[feature_cols]
    y = df["total_points"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)

    print(f"📉 Baseline MAE: {mae:.2f}")


    # NAIVE BASELINE: mean target value
    y_mean = y_train.mean()
    y_naive_pred = [y_mean] * len(y_test)
    naive_mae = mean_absolute_error(y_test, y_naive_pred)

    print(f"📉 Naive baseline MAE (predicting mean): {naive_mae:.2f}")
    return model


def main():
    client = get_clickhouse_client("DATABASE_UNITED")
    df = load_play_by_play(client)

    if df.is_empty():
        return

    print("🔎 Sampling games...")
    # sampled_game_ids = pl.Series(df["game_id"].unique()).sample(n=NUM_SAMPLED_GAMES, with_replacement=False)
    sampled_game_ids = pl.Series(df["game_id"].unique())
    df = df.filter(pl.col("game_id").is_in(sampled_game_ids))

    features = create_features(df)
    model = train_model(features)


if __name__ == "__main__":
    main()
