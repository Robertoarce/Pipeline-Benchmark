# sonarignore:start

import concurrent.futures
import os
import tempfile
import time
from datetime import timedelta
from typing import Callable, Dict, List, Tuple

import pandas as pd
import polars as pl
from pandas import DataFrame


# constants
USER_ID_NAME, TIMESTAMP = "USER_ID", "FIRST_EVENT_TIMESTAMP"


#########################################
# IMPLEMENTATIONS OF get_portal_feature #
#########################################


# --- Implementation 1: Original Pandas (from processing_script.py) ---
def _get_web_events_in_5_minutes_original(  # Renamed to avoid conflict if original is also imported
    user_id: str, timestamp: pd.Timestamp, web_events: pd.DataFrame
) -> pd.DataFrame:
    time_delta = timedelta(minutes=5)
    # 'USER_ID' and 'FIRST_EVENT_TIMESTAMP' are literal column names in web_events_pd
    return web_events[
        (web_events["FIRST_EVENT_TIMESTAMP"] >= timestamp)
        & (web_events["FIRST_EVENT_TIMESTAMP"] <= timestamp + time_delta)
        & (web_events["USER_ID"] == user_id)
    ]


def get_web_feature_original(
    features_email_df: pd.DataFrame, web_events: pd.DataFrame
) -> List[int]:
    print("Executing: Original Pandas (Iterrows, DataFrame Filter)")

    count_web_events = []

    features_email_df_copy = features_email_df.copy()
    web_events_copy = web_events.copy()

    features_email_df_copy[TIMESTAMP] = pd.to_datetime(
        features_email_df_copy[TIMESTAMP])
    web_events_copy["FIRST_EVENT_TIMESTAMP"] = pd.to_datetime(
        web_events_copy["FIRST_EVENT_TIMESTAMP"]
    )

    for _, row in features_email_df_copy.iterrows():
        web_events_in_5_min = _get_web_events_in_5_minutes_original(
            row[USER_ID_NAME], row[TIMESTAMP], web_events_copy
        )
        count_web_events.append(web_events_in_5_min.shape[0])
    return count_web_events


# --- Implementation 2: Optimized Pandas ---
def _apply_pandas_optimized_logic(
    email_row,
    web_lookup: Dict[str, pd.DataFrame],
    user_id_col_name: str,
    timestamp_col_name: str,
) -> int:
    user_id = email_row[user_id_col_name]
    timestamp = email_row[timestamp_col_name]

    if user_id not in web_lookup:
        return 0

    group_timestamps = web_lookup[user_id]["FIRST_EVENT_TIMESTAMP"]
    if group_timestamps.empty:
        return 0

    time_plus_5_min = timestamp + timedelta(minutes=5)
    start_index = group_timestamps.searchsorted(timestamp, side="left")
    end_index = group_timestamps.searchsorted(time_plus_5_min, side="right")
    return end_index - start_index


def get_web_feature_pandas_optimized(
    features_email_df: pd.DataFrame, web_events: pd.DataFrame
) -> List[int]:
    print("Executing: Pandas Optimized (Sort, GroupBy Dict, Apply, SearchSorted)")
    # USER_ID_NAME and TIMESTAMP are global constants
    features_df_copy = features_email_df.copy()
    web_events_copy = web_events.copy()

    features_df_copy[TIMESTAMP] = pd.to_datetime(features_df_copy[TIMESTAMP])
    web_events_copy["FIRST_EVENT_TIMESTAMP"] = pd.to_datetime(
        web_events_copy["FIRST_EVENT_TIMESTAMP"]
    )

    web_events_sorted = web_events_copy.sort_values(
        by=["USER_ID", "FIRST_EVENT_TIMESTAMP"]
    )
    web_lookup = {
        name: group[["FIRST_EVENT_TIMESTAMP"]].reset_index(
            drop=True
        )  # Ensure clean index for searchsorted if needed
        for name, group in web_events_sorted.groupby("USER_ID")
    }

    counts = features_df_copy.apply(
        _apply_pandas_optimized_logic,
        axis=1,
        web_lookup=web_lookup,
        user_id_col_name=USER_ID_NAME,
        timestamp_col_name=TIMESTAMP,
    ).tolist()
    return counts


# --- Implementation 3: Polars Eager Batched (Your working optimized function) ---
def get_web_feature_polars_eager_batched(  # Renamed from get_web_feature_polars_optimized
    features_email_df_pd: pd.DataFrame,
    web_events_pd: pd.DataFrame,
    batch_size: int = 500_000,
) -> List[int]:
    print("Executing: Polars Eager Batched")
    # USER_ID_NAME and TIMESTAMP are global constants
    pl_features_email = pl.from_pandas(
        features_email_df_pd[["EVENT_ID", USER_ID_NAME, TIMESTAMP]]
    )
    pl_web_events = pl.from_pandas(
        web_events_pd[["USER_ID", "FIRST_EVENT_TIMESTAMP"]]
    ).rename(
        {"USER_ID": USER_ID_NAME, "FIRST_EVENT_TIMESTAMP": "web_timestamp_col"}
    )

    num_email_rows = pl_features_email.height
    if num_email_rows == 0:
        return []
    all_counts_list = []
    pl_web_events_sorted = pl_web_events.sort(
        USER_ID_NAME, "web_timestamp_col"
    )

    for i in range(0, num_email_rows, batch_size):
        start_index = i
        actual_batch_size = min(batch_size, num_email_rows - start_index)
        email_batch = pl_features_email.slice(start_index, actual_batch_size)
        email_batch_with_window = email_batch.with_columns(
            (pl.col(TIMESTAMP) + pl.duration(minutes=5)).alias(
                "email_timestamp_plus_5min"
            )
        )
        unique_user_ids_in_batch = email_batch_with_window.select(
            pl.col(USER_ID_NAME).unique()
        )
        relevant_web_events_for_batch = pl_web_events_sorted.join(
            unique_user_ids_in_batch, on=USER_ID_NAME, how="inner"
        )
        joined_df = email_batch_with_window.join(
            relevant_web_events_for_batch, on=USER_ID_NAME, how="left"
        )
        events_in_window = joined_df.filter(
            pl.col("web_timestamp_col").is_between(
                lower_bound=pl.col(TIMESTAMP),
                upper_bound=pl.col("email_timestamp_plus_5min"),
                closed="both",
            )
        )
        batch_counts = events_in_window.group_by("EVENT_ID", maintain_order=True).agg(
            pl.len().alias("NB_WEB_EVENTS_5MIN")
        )
        email_batch_with_counts = email_batch.join(
            batch_counts, on="EVENT_ID", how="left"
        ).with_columns(pl.col("NB_WEB_EVENTS_5MIN").fill_null(0).cast(pl.Int64))
        all_counts_list.extend(
            email_batch_with_counts.get_column(
                "NB_WEB_EVENTS_5MIN").to_list()
        )
    return all_counts_list


# --- Implementation 4: Polars Lazy Batched - --


def get_web_feature_polars_lazy_batched(
    features_email_df_pd: pd.DataFrame,
    web_events_pd: pd.DataFrame,
    batch_size: int = 500_000,
) -> List[int]:
    print("Executing: Polars Lazy Batched")
    # USER_ID_NAME and TIMESTAMP are global constants
    pl_features_email_eager = pl.from_pandas(
        features_email_df_pd[["EVENT_ID", USER_ID_NAME, TIMESTAMP]]
    )
    pl_web_events_eager = pl.from_pandas(
        web_events_pd[["USER_ID", "FIRST_EVENT_TIMESTAMP"]]
    ).rename(
        {"USER_ID": USER_ID_NAME, "FIRST_EVENT_TIMESTAMP": "web_timestamp_col"}
    )

    num_email_rows = pl_features_email_eager.height
    if num_email_rows == 0:
        return []
    all_counts_list = []
    pl_web_events_sorted_lazy = pl_web_events_eager.lazy().sort(
        USER_ID_NAME, "web_timestamp_col"
    )

    for i in range(0, num_email_rows, batch_size):
        start_index = i
        actual_batch_size = min(batch_size, num_email_rows - start_index)
        email_batch_eager = pl_features_email_eager.slice(
            start_index, actual_batch_size)

        email_batch_lazy = email_batch_eager.lazy()
        email_batch_with_window_lazy = email_batch_lazy.with_columns(
            (pl.col(TIMESTAMP) + pl.duration(minutes=5)).alias(
                "email_timestamp_plus_5min"
            )
        )
        unique_user_ids_in_batch_lazy = email_batch_with_window_lazy.select(
            pl.col(USER_ID_NAME).unique()
        )
        relevant_web_events_for_batch_lazy = pl_web_events_sorted_lazy.join(
            unique_user_ids_in_batch_lazy, on=USER_ID_NAME, how="inner"
        )
        joined_df_lazy = email_batch_with_window_lazy.join(
            relevant_web_events_for_batch_lazy, on=USER_ID_NAME, how="left"
        )
        events_in_window_lazy = joined_df_lazy.filter(
            pl.col("web_timestamp_col").is_between(
                lower_bound=pl.col(TIMESTAMP),
                upper_bound=pl.col("email_timestamp_plus_5min"),
                closed="both",
            )
        )
        batch_counts_lazy = events_in_window_lazy.group_by(
            "EVENT_ID", maintain_order=True
        ).agg(pl.len().alias("NB_WEB_EVENTS_5MIN"))
        email_batch_with_counts_lazy = (
            email_batch_eager.lazy()
            .join(batch_counts_lazy, on="EVENT_ID", how="left")
            .with_columns(pl.col("NB_WEB_EVENTS_5MIN").fill_null(0).cast(pl.Int64))
        )
        collected_batch_results = email_batch_with_counts_lazy.collect()
        all_counts_list.extend(
            collected_batch_results.get_column(
                "NB_WEB_EVENTS_5MIN").to_list()
        )
    return all_counts_list


# --- Implementation 5: Polars Streaming ---
def get_web_feature_polars_streaming(
    features_email_df_pd: pd.DataFrame,
    web_events_pd: pd.DataFrame,
    batch_size: int = 500_000,  # Batch size for features_email_df
) -> List[int]:
    print("Executing: Polars Streaming (Scan Parquet for Web Events)")
    # USER_ID_NAME and TIMESTAMP are global constants from your script's context

    # Create a temporary directory and file path for the Parquet file
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_web_file_path = os.path.join(tmpdir, "web_events.parquet")

        # 1. Convert web_events_pd to Polars and save to Parquet
        # This step simulates data being available on disk.
        try:
            pl.from_pandas(
                web_events_pd[["USER_ID", "FIRST_EVENT_TIMESTAMP"]]
            ).write_parquet(temp_web_file_path)
        except Exception as e:
            print(f"Error writing temporary parquet file: {e}")
            raise

        # 2. Scan the Parquet file for web events (creates a LazyFrame)
        web_events_lazy_scanned = pl.scan_parquet(temp_web_file_path)

        # 3. Define the sorted and renamed LazyFrame for web events
        # These operations are defined lazily and will be optimized by Polars.
        web_events_lazy_sorted = web_events_lazy_scanned.rename(
            {
                "USER_ID": USER_ID_NAME,
                "FIRST_EVENT_TIMESTAMP": "web_timestamp_col",
            }
        ).sort(USER_ID_NAME, "web_timestamp_col")

        # --- Process features_email_df in batches against the scanned web_events ---
        # Convert features_email_df to a Polars DataFrame once
        try:
            pl_features_email_eager = pl.from_pandas(
                features_email_df_pd[["EVENT_ID", USER_ID_NAME, TIMESTAMP]]
            )
        except KeyError as e:
            raise KeyError(
                f"Missing one of 'EVENT_ID', '{USER_ID_NAME}', or '{TIMESTAMP}' in features_email_df_pd. Original error: {e}"
            )

        num_email_rows = pl_features_email_eager.height
        if num_email_rows == 0:
            return []

        all_counts_list = []

        for i in range(0, num_email_rows, batch_size):
            start_index = i
            actual_batch_size = min(batch_size, num_email_rows - start_index)
            email_batch_eager = pl_features_email_eager.slice(
                start_index, actual_batch_size
            )  # Current batch is eager

            # Define lazy operations for the current email batch
            email_batch_lazy = email_batch_eager.lazy()
            email_batch_with_window_lazy = email_batch_lazy.with_columns(
                (pl.col(TIMESTAMP) + pl.duration(minutes=5)).alias(
                    "email_timestamp_plus_5min"
                )
            )

            # Optimization: Filter the (potentially large) scanned web_events_lazy_sorted
            # by the User IDs present in the current small email_batch.
            unique_user_ids_in_batch_lazy = email_batch_with_window_lazy.select(
                pl.col(USER_ID_NAME).unique()
            )

            relevant_web_events_for_batch_lazy = web_events_lazy_sorted.join(
                unique_user_ids_in_batch_lazy,  # This is a LazyFrame of unique IDs
                on=USER_ID_NAME,
                how="inner",
            )

            # Join the email batch with the filtered web events
            joined_df_lazy = email_batch_with_window_lazy.join(
                relevant_web_events_for_batch_lazy,  # This is now a filtered LazyFrame
                on=USER_ID_NAME,
                how="left",
            )

            events_in_window_lazy = joined_df_lazy.filter(
                pl.col("web_timestamp_col").is_between(
                    lower_bound=pl.col(TIMESTAMP),
                    upper_bound=pl.col("email_timestamp_plus_5min"),
                    closed="both",
                )
            )

            batch_counts_lazy = events_in_window_lazy.group_by(
                "EVENT_ID", maintain_order=True
            ).agg(pl.len().alias("NB_WEB_EVENTS_5MIN"))

            # Join counts back to the original structure of the email batch (lazy)
            email_batch_with_counts_lazy = (
                email_batch_eager.lazy()
                .join(batch_counts_lazy, on="EVENT_ID", how="left")
                .with_columns(
                    pl.col("NB_WEB_EVENTS_5MIN").fill_null(0).cast(pl.Int64)
                )
            )

            # .collect() executes the lazy plan for this batch
            collected_batch_results = email_batch_with_counts_lazy.collect()
            all_counts_list.extend(
                collected_batch_results.get_column(
                    "NB_WEB_EVENTS_5MIN").to_list()
            )

        return all_counts_list


# --- Implementation 6: Paralelized Pandas ---
def get_web_feature(
    features_email_df: DataFrame, web_events: DataFrame
) -> List[int]:
    """Get the value of the web feature for each email event.
    This value is the number of web events happening in the next
    5 minutes following the email event for the same user ID.

    :param DataFrame features_email_df: DataFrame containing all
    email events
    :param DataFrame web_events: DataFrame containing events
    from web

    :return List[int]: list containing the value of the web
    feature for each email event
    """

    row_indices = features_email_df.index.tolist()

    def process_row(idx, row):
        web_events_in_5_min = _get_web_events_in_5_minutes_original(
            row[USER_ID_NAME], row[TIMESTAMP], web_events
        )
        return idx, web_events_in_5_min.shape[0]

    results_dict = {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Use the actual DataFrame indices
        futures = {
            executor.submit(process_row, idx, row): idx
            for idx, row in features_email_df.iterrows()
        }
        completed = 0
        for future in concurrent.futures.as_completed(futures):
            idx, result = future.result()
            results_dict[idx] = result
            completed += 1
            if completed % 1000 == 0:
                print(f"Processed {completed} rows")

    # Use the stored original indices to reconstruct the results in order
    ordered_results = [results_dict[idx] for idx in row_indices]
    return ordered_results


##############################
# BENCHMARKING MAIN FUNCTION #
##############################


# --- Benchmark Function ---
def benchmark_web_feature_implementations(
    features_email_df_pd: pd.DataFrame,
    web_events_pd: pd.DataFrame,
    num_runs: int = 10,  # Re-Runs per function.
):
    print("=== Web Feature Calculation Performance Benchmark ===\n")
    print(
        f"Benchmarking on features_email_df: {features_email_df_pd.shape} rows")
    print(f"Benchmarking on web_events_df: {web_events_pd.shape} rows")
    print(f"Number of runs per implementation: {num_runs}\n")

    # List of implementations to benchmark

    implementations: List[
        Tuple[str, str, Callable[[pd.DataFrame, pd.DataFrame], List[int]]]
    ] = [
        ("Original Pandas (Iterrows)", "pandas", get_web_feature_original),
        (
            "Pandas Optimized (Apply+SearchSorted)",
            "pandas",
            get_web_feature_pandas_optimized,
        ),
        ("Polars Eager Batched", "polars", get_web_feature_polars_eager_batched),
        ("Polars Lazy Batched", "polars", get_web_feature_polars_lazy_batched),
        (
            "Polars Streaming (Scan Parquet, Parallel by Default)",
            "polars",
            get_web_feature_polars_streaming,
        ),
        ("Pandas Optimized(multi threading)", "pandas", get_web_feature),
    ]

    results_summary = {}
    base_result_sum = None  # For verifying consistency

    for name, lib_type, func in implementations:
        print(f"--- Benchmarking: {name} ({lib_type}) ---")
        total_time = 0
        current_run_results = []
        try:
            for i in range(num_runs):
                start_time = time.perf_counter()
                result = func(
                    features_email_df_pd.copy(), web_events_pd.copy()
                )  # Pass copies (to avoid side effects, such as in-place modifications)
                end_time = time.perf_counter()
                run_time = end_time - start_time
                total_time += run_time
                current_run_results.append(result)
                print(
                    f"Run {i+1}/{num_runs} completed in {run_time:.4f} seconds.")

            avg_time = total_time / num_runs
            results_summary[name] = avg_time
            print(f"Average time for {name}: {avg_time:.4f} seconds.\n")

            # Verification (recommended, is optional)
            if result:  # If not a placeholder that returns empty
                if len(result) != len(features_email_df_pd):
                    print(
                        f"WARNING: Result length mismatch for {name}! Expected {len(features_email_df_pd)}, Got {len(result)}"
                    )

                current_sum = sum(result)
                if base_result_sum is None and name not in [
                    "Polars Window",
                    "Polars Streaming",
                    "Polars Parallel",
                ]:  # Set by first valid run
                    base_result_sum = current_sum
                    print(
                        f"Set base result sum for verification: {base_result_sum}")
                elif (
                    name not in ["Polars Window",
                                 "Polars Streaming", "Polars Parallel"]
                    and current_sum != base_result_sum
                ):
                    print(
                        f"WARNING: Result sum mismatch for {name}! Expected {base_result_sum}, Got {current_sum}"
                    )

        except Exception as e:
            print(f"ERROR benchmarking {name}: {e}")
            results_summary[name] = float("inf")  # Indicate failure
            import traceback

            traceback.print_exc()
            print("\n")
            continue  # Skip to next implementation

    print("\n=== Benchmark Summary ===")
    for name, avg_time in sorted(results_summary.items(), key=lambda item: item[1]):
        if avg_time == float("inf"):
            print(f"{name}: FAILED")
        else:
            print(f"{name}: {avg_time:.4f} seconds")
    print("=========================\n")


if __name__ == "__main__":
    print("CREATING SAMPLES DATA FOR BENCHMARKING")

    # Create Sample Data
    N_email = 10000  # Number of email events
    N_web = 50000  # Number of web events
    user_ids_sample = [f"user_{i%100}" for i in range(N_email)]

    # Creation of synthetic data
    features_email_df_sample = pd.DataFrame(
        {
            "EVENT_ID": range(N_email),
            USER_ID_NAME: user_ids_sample,
            TIMESTAMP: pd.to_datetime("2023-01-01 10:00:00")
            + pd.to_timedelta(list(range(N_email)), unit="s"),
        }
    )

    web_events_df_sample = pd.DataFrame(
        {
            "USER_ID": [f"user_{i%150}" for i in range(N_web)],
            "FIRST_EVENT_TIMESTAMP": pd.to_datetime("2023-01-01 09:58:00")
            + pd.to_timedelta(list(range(N_web)), unit="s"),
            "EVENT_CATEGORY": "pages",
            "TRAFFIC_TYPE": "None",
            "EVENT": "pages",
        }
    )

    print("Sample DataFrames created.")

    benchmark_web_feature_implementations(
        features_email_df_sample, web_events_df_sample, num_runs=10
    )
