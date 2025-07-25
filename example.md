CREATING SAMPLES DATA FOR BENCHMARKING
Sample DataFrames created.
=== Web Feature Calculation Performance Benchmark ===

Benchmarking on features_email_df: (10000, 3) rows
Benchmarking on web_events_df: (50000, 5) rows
Number of runs per implementation: 10

--- Benchmarking: Original Pandas (Iterrows) (pandas) ---
Executing: Original Pandas (Iterrows, DataFrame Filter)
Run 1/10 completed in 10.7597 seconds.
Executing: Original Pandas (Iterrows, DataFrame Filter)
Run 2/10 completed in 10.8204 seconds.
Executing: Original Pandas (Iterrows, DataFrame Filter)
Run 3/10 completed in 11.1164 seconds.
Executing: Original Pandas (Iterrows, DataFrame Filter)
Run 4/10 completed in 11.0629 seconds.
Executing: Original Pandas (Iterrows, DataFrame Filter)
Run 5/10 completed in 10.9932 seconds.
Executing: Original Pandas (Iterrows, DataFrame Filter)
Run 6/10 completed in 11.2906 seconds.
Executing: Original Pandas (Iterrows, DataFrame Filter)
Run 7/10 completed in 11.1248 seconds.
Executing: Original Pandas (Iterrows, DataFrame Filter)
Run 8/10 completed in 10.9857 seconds.
Executing: Original Pandas (Iterrows, DataFrame Filter)
Run 9/10 completed in 11.3055 seconds.
Executing: Original Pandas (Iterrows, DataFrame Filter)
Run 10/10 completed in 11.5295 seconds.
Average time for Original Pandas (Iterrows): 11.0989 seconds.

Set base result sum for verification: 20000
--- Benchmarking: Pandas Optimized (Apply+SearchSorted) (pandas) ---
Executing: Pandas Optimized (Sort, GroupBy Dict, Apply, SearchSorted)
Run 1/10 completed in 0.1347 seconds.
Executing: Pandas Optimized (Sort, GroupBy Dict, Apply, SearchSorted)
Run 2/10 completed in 0.1164 seconds.
Executing: Pandas Optimized (Sort, GroupBy Dict, Apply, SearchSorted)
Run 3/10 completed in 0.1199 seconds.
Executing: Pandas Optimized (Sort, GroupBy Dict, Apply, SearchSorted)
Run 4/10 completed in 0.1267 seconds.
Executing: Pandas Optimized (Sort, GroupBy Dict, Apply, SearchSorted)
Run 5/10 completed in 0.1179 seconds.
Executing: Pandas Optimized (Sort, GroupBy Dict, Apply, SearchSorted)
Run 6/10 completed in 0.1269 seconds.
Executing: Pandas Optimized (Sort, GroupBy Dict, Apply, SearchSorted)
Run 7/10 completed in 0.1193 seconds.
Executing: Pandas Optimized (Sort, GroupBy Dict, Apply, SearchSorted)
Run 8/10 completed in 0.1189 seconds.
Executing: Pandas Optimized (Sort, GroupBy Dict, Apply, SearchSorted)
Run 9/10 completed in 0.1244 seconds.
Executing: Pandas Optimized (Sort, GroupBy Dict, Apply, SearchSorted)
Run 10/10 completed in 0.1175 seconds.
Average time for Pandas Optimized (Apply+SearchSorted): 0.1222 seconds.

--- Benchmarking: Polars Eager Batched (polars) ---
Executing: Polars Eager Batched
Run 1/10 completed in 0.0784 seconds.
Executing: Polars Eager Batched
Run 2/10 completed in 0.0280 seconds.
Executing: Polars Eager Batched
Run 3/10 completed in 0.0278 seconds.
Executing: Polars Eager Batched
Run 4/10 completed in 0.0289 seconds.
Executing: Polars Eager Batched
Run 5/10 completed in 0.0618 seconds.
Executing: Polars Eager Batched
Run 6/10 completed in 0.0372 seconds.
Executing: Polars Eager Batched
Run 7/10 completed in 0.0353 seconds.
Executing: Polars Eager Batched
Run 8/10 completed in 0.0317 seconds.
Executing: Polars Eager Batched
Run 9/10 completed in 0.0318 seconds.
Executing: Polars Eager Batched
Run 10/10 completed in 0.0313 seconds.
Average time for Polars Eager Batched: 0.0392 seconds.

--- Benchmarking: Polars Lazy Batched (polars) ---
Executing: Polars Lazy Batched
Run 1/10 completed in 0.0328 seconds.
Executing: Polars Lazy Batched
Run 2/10 completed in 0.0347 seconds.
Executing: Polars Lazy Batched
Run 3/10 completed in 0.0303 seconds.
Executing: Polars Lazy Batched
Run 4/10 completed in 0.0309 seconds.
Executing: Polars Lazy Batched
Run 5/10 completed in 0.0314 seconds.
Executing: Polars Lazy Batched
Run 6/10 completed in 0.0324 seconds.
Executing: Polars Lazy Batched
Run 7/10 completed in 0.0304 seconds.
Executing: Polars Lazy Batched
Run 8/10 completed in 0.0307 seconds.
Executing: Polars Lazy Batched
Run 9/10 completed in 0.0304 seconds.
Executing: Polars Lazy Batched
Run 10/10 completed in 0.0313 seconds.
Average time for Polars Lazy Batched: 0.0315 seconds.

--- Benchmarking: Polars Streaming (Scan Parquet, Parallel by Default) (polars) ---
Executing: Polars Streaming (Scan Parquet for Web Events)
Run 1/10 completed in 0.0604 seconds.
Executing: Polars Streaming (Scan Parquet for Web Events)
Run 2/10 completed in 0.0376 seconds.
Executing: Polars Streaming (Scan Parquet for Web Events)
Run 3/10 completed in 0.0346 seconds.
Executing: Polars Streaming (Scan Parquet for Web Events)
Run 4/10 completed in 0.0338 seconds.
Executing: Polars Streaming (Scan Parquet for Web Events)
Run 5/10 completed in 0.0339 seconds.
Executing: Polars Streaming (Scan Parquet for Web Events)
Run 6/10 completed in 0.0331 seconds.
Executing: Polars Streaming (Scan Parquet for Web Events)
Run 7/10 completed in 0.0354 seconds.
Executing: Polars Streaming (Scan Parquet for Web Events)
Run 8/10 completed in 0.0348 seconds.
Executing: Polars Streaming (Scan Parquet for Web Events)
Run 9/10 completed in 0.0367 seconds.
Executing: Polars Streaming (Scan Parquet for Web Events)
Run 10/10 completed in 0.0341 seconds.
Average time for Polars Streaming (Scan Parquet, Parallel by Default): 0.0374 seconds.

--- Benchmarking: Pandas Optimized(multi threading) (pandas) ---
Processed 1000 rows
Processed 2000 rows
Processed 3000 rows
Processed 4000 rows
Processed 5000 rows
Processed 6000 rows
Processed 7000 rows
Processed 8000 rows
Processed 9000 rows
Processed 10000 rows
Run 1/10 completed in 12.5344 seconds.
Processed 1000 rows
Processed 2000 rows
Processed 3000 rows
Processed 4000 rows
Processed 5000 rows
Processed 6000 rows
Processed 7000 rows
Processed 8000 rows
Processed 9000 rows
Processed 10000 rows
Run 2/10 completed in 12.3325 seconds.
Processed 1000 rows
Processed 2000 rows
Processed 3000 rows
Processed 4000 rows
Processed 5000 rows
Processed 6000 rows
Processed 7000 rows
Processed 8000 rows
Processed 9000 rows
Processed 10000 rows
Run 3/10 completed in 11.8135 seconds.
Processed 1000 rows
Processed 2000 rows
Processed 3000 rows
Processed 4000 rows
Processed 5000 rows
Processed 6000 rows
Processed 7000 rows
Processed 8000 rows
Processed 9000 rows
Processed 10000 rows
Run 4/10 completed in 12.3489 seconds.
Processed 1000 rows
Processed 2000 rows
Processed 3000 rows
Processed 4000 rows
Processed 5000 rows
Processed 6000 rows
Processed 7000 rows
Processed 8000 rows
Processed 9000 rows
Processed 10000 rows
Run 5/10 completed in 12.0681 seconds.
Processed 1000 rows
Processed 2000 rows
Processed 3000 rows
Processed 4000 rows
Processed 5000 rows
Processed 6000 rows
Processed 7000 rows
Processed 8000 rows
Processed 9000 rows
Processed 10000 rows
Run 6/10 completed in 11.4945 seconds.
Processed 1000 rows
Processed 2000 rows
Processed 3000 rows
Processed 4000 rows
Processed 5000 rows
Processed 6000 rows
Processed 7000 rows
Processed 8000 rows
Processed 9000 rows
Processed 10000 rows
Run 7/10 completed in 11.5197 seconds.
Processed 1000 rows
Processed 2000 rows
Processed 3000 rows
Processed 4000 rows
Processed 5000 rows
Processed 6000 rows
Processed 7000 rows
Processed 8000 rows
Processed 9000 rows
Processed 10000 rows
Run 8/10 completed in 11.4312 seconds.
Processed 1000 rows
Processed 2000 rows
Processed 3000 rows
Processed 4000 rows
Processed 5000 rows
Processed 6000 rows
Processed 7000 rows
Processed 8000 rows
Processed 9000 rows
Processed 10000 rows
Run 9/10 completed in 11.6671 seconds.
Processed 1000 rows
Processed 2000 rows
Processed 3000 rows
Processed 4000 rows
Processed 5000 rows
Processed 6000 rows
Processed 7000 rows
Processed 8000 rows
Processed 9000 rows
Processed 10000 rows
Run 10/10 completed in 11.5293 seconds.
Average time for Pandas Optimized(multi threading): 11.8739 seconds.

=== Benchmark Summary ===
Polars Lazy Batched: 0.0315 seconds
Polars Streaming (Scan Parquet, Parallel by Default): 0.0374 seconds
Polars Eager Batched: 0.0392 seconds
Pandas Optimized (Apply+SearchSorted): 0.1222 seconds
Original Pandas (Iterrows): 11.0989 seconds
Pandas Optimized(multi threading): 11.8739 seconds
=========================
