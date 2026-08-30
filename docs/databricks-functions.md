# Databricks SQL builtin-function coverage

**Generated — do not edit by hand.** Regenerate with:

```sh
cargo build -p oxidant-spark-compat --bin oxidant-parity
./target/debug/oxidant-parity functions --markdown > docs/databricks-functions.md
```

Databricks surface scraped 2026-08-29 from <https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-functions-builtin>.

Oxidant's side is the live function registry — the same union that answers `SHOW FUNCTIONS` (`Engine::registered_function_names`), so this table cannot drift from what the engine actually resolves. A source-level grep would under-count it: DataFusion generates much of its registry through macros and `aliases()`.

## Headline

| | |
|---|---:|
| Documented Databricks functions | 606 |
| In scope for Oxidant | 440 |
| **Registered today** | **325 (73.9%)** |
| Missing | 115 |
| Engine registry size (incl. non-Databricks names) | 458 |

### Out of scope

| Reason | Count |
|---|---:|
| ai-vector | 23 |
| datasketches | 61 |
| file | 6 |
| match-recognize | 6 |
| platform-source | 12 |
| syntax | 41 |
| workspace | 17 |

## By manual category

| Category | Registered | In scope | Missing |
|---|---:|---:|---|
| Analytic window functions | 4 | 4 | — |
| Array functions | 26 | 44 | `aggregate`, `array_insert`, `exists`, `explode`, `explode_outer`, `filter`, `forall`, `get`, `inline`, `inline_outer`, `posexplode`, `posexplode_outer`, `reduce`, `sequence`, `shuffle`, `slice`, `transform`, `zip_with` |
| CSV and Avro functions | 2 | 5 | `from_avro`, `schema_of_csv`, `to_avro` |
| Cast functions and constructors | 24 | 27 | `make_dt_interval`, `make_interval`, `make_ym_interval` |
| Date, timestamp, and interval functions | 62 | 82 | `make_dt_interval`, `make_interval`, `make_ym_interval`, `parse_timestamp`, `session_window`, `time_diff`, `time_from_micros`, `time_from_millis`, `time_from_seconds`, `time_to_micros`, `time_to_millis`, `time_to_seconds`, `time_trunc`, `timediff`, `timestampadd`, `timestampdiff`, `try_parse_timestamp`, `try_to_time`, `window`, `window_time` |
| JSON functions | 5 | 9 | `json_tuple`, `parse_json`, `schema_of_json`, `schema_of_json_agg` |
| Map functions | 8 | 18 | `explode`, `explode_outer`, `map_concat`, `map_filter`, `map_from_arrays`, `map_from_entries`, `map_zip_with`, `str_to_map`, `transform_keys`, `transform_values` |
| Miscellaneous functions | 24 | 38 | `current_user`, `hash`, `input_file_block_length`, `input_file_block_start`, `input_file_name`, `luhn_check`, `monotonically_increasing_id`, `raise_error`, `session_user`, `spark_partition_id`, `stack`, `user`, `window`, `xxhash64` |
| Numeric scalar functions | 125 | 139 | `approx_percentile`, `collect_list`, `collect_set`, `histogram_numeric`, `kurtosis`, `listagg`, `max_by`, `min_by`, `percentile_approx`, `percentile_disc`, `randn`, `schema_of_json_agg`, `schema_of_variant_agg`, `uniform` |
| Operators and predicates | 6 | 7 | `exists` |
| Ranking window functions | 5 | 5 | — |
| String and binary functions | 73 | 91 | `aes_decrypt`, `aes_encrypt`, `base64`, `charindex`, `collate`, `collation`, `format_number`, `locate`, `printf`, `randstr`, `sentences`, `soundex`, `space`, `try_aes_decrypt`, `try_zstd_decompress`, `unbase64`, `zstd_compress`, `zstd_decompress` |
| VARIANT functions | 1 | 10 | `is_variant_null`, `parse_json`, `schema_of_variant_agg`, `to_variant_object`, `try_parse_json`, `try_variant_get`, `variant_explode`, `variant_explode_outer`, `variant_get` |
| XPath and XML functions | 0 | 11 | `from_xml`, `schema_of_xml`, `xpath`, `xpath_boolean`, `xpath_double`, `xpath_float`, `xpath_int`, `xpath_long`, `xpath_number`, `xpath_short`, `xpath_string` |

## Every in-scope function

`origin` is where a registered name comes from: `datafusion` for a DataFusion built-in, `oxidant` for a Spark UDF in `crates/oxidant-loom/src/spark_functions/` or a Spark-name alias from `register_spark_function_aliases`.

| Function | Status | Origin | Category |
|---|---|---|---|
| `abs` | registered | datafusion | Numeric scalar functions; Date, timestamp, and interval functions |
| `acos` | registered | datafusion | Numeric scalar functions |
| `acosh` | registered | datafusion | Numeric scalar functions |
| `add_months` | registered | oxidant | Date, timestamp, and interval functions |
| `aes_decrypt` | **missing** | — | String and binary functions |
| `aes_encrypt` | **missing** | — | String and binary functions |
| `aggregate` | **missing** | — | Array functions |
| `any` | registered | oxidant | Numeric scalar functions |
| `any_value` | registered | oxidant | Numeric scalar functions |
| `approx_count_distinct` | registered | oxidant | Numeric scalar functions |
| `approx_percentile` | **missing** | — | Numeric scalar functions |
| `array` | registered | oxidant | Array functions; Cast functions and constructors |
| `array_agg` | registered | datafusion | Numeric scalar functions |
| `array_append` | registered | datafusion | Array functions |
| `array_compact` | registered | datafusion | Array functions |
| `array_contains` | registered | datafusion | Array functions |
| `array_distinct` | registered | datafusion | Array functions |
| `array_except` | registered | datafusion | Array functions |
| `array_insert` | **missing** | — | Array functions |
| `array_intersect` | registered | datafusion | Array functions |
| `array_join` | registered | datafusion | Array functions |
| `array_max` | registered | datafusion | Array functions |
| `array_min` | registered | datafusion | Array functions |
| `array_position` | registered | datafusion | Array functions |
| `array_prepend` | registered | datafusion | Array functions |
| `array_remove` | registered | datafusion | Array functions |
| `array_repeat` | registered | datafusion | Array functions |
| `array_size` | registered | oxidant | Array functions |
| `array_sort` | registered | datafusion | Array functions |
| `array_union` | registered | datafusion | Array functions |
| `arrays_overlap` | registered | datafusion | Array functions |
| `arrays_zip` | registered | datafusion | Array functions |
| `ascii` | registered | datafusion | String and binary functions |
| `asin` | registered | datafusion | Numeric scalar functions |
| `asinh` | registered | datafusion | Numeric scalar functions |
| `assert_true` | registered | oxidant | Miscellaneous functions |
| `atan` | registered | datafusion | Numeric scalar functions |
| `atan2` | registered | datafusion | Numeric scalar functions |
| `atanh` | registered | datafusion | Numeric scalar functions |
| `avg` | registered | datafusion | Numeric scalar functions |
| `base64` | **missing** | — | String and binary functions |
| `bigint` | registered | oxidant | Numeric scalar functions; Cast functions and constructors |
| `bin` | registered | oxidant | String and binary functions |
| `binary` | registered | oxidant | String and binary functions; Cast functions and constructors |
| `bit_and` | registered | datafusion | Numeric scalar functions |
| `bit_count` | registered | oxidant | Numeric scalar functions |
| `bit_get` | registered | oxidant | Numeric scalar functions |
| `bit_length` | registered | datafusion | String and binary functions |
| `bit_or` | registered | datafusion | Numeric scalar functions |
| `bit_reverse` | registered | oxidant | Numeric scalar functions |
| `bit_xor` | registered | datafusion | Numeric scalar functions |
| `bool_and` | registered | datafusion | Numeric scalar functions |
| `bool_or` | registered | datafusion | Numeric scalar functions |
| `boolean` | registered | oxidant | Cast functions and constructors |
| `bround` | registered | oxidant | Numeric scalar functions |
| `btrim` | registered | datafusion | String and binary functions |
| `cardinality` | registered | datafusion | Array functions; Map functions |
| `cbrt` | registered | datafusion | Numeric scalar functions |
| `ceil` | registered | datafusion | Numeric scalar functions |
| `ceiling` | registered | oxidant | Numeric scalar functions |
| `char` | registered | oxidant | String and binary functions |
| `char_length` | registered | datafusion | String and binary functions |
| `character_length` | registered | datafusion | String and binary functions |
| `charindex` | **missing** | — | String and binary functions |
| `chr` | registered | datafusion | String and binary functions |
| `coalesce` | registered | datafusion | Miscellaneous functions |
| `collate` | **missing** | — | String and binary functions |
| `collation` | **missing** | — | String and binary functions |
| `collect_list` | **missing** | — | Numeric scalar functions |
| `collect_set` | **missing** | — | Numeric scalar functions |
| `concat` | registered | datafusion | String and binary functions; Array functions |
| `concat_ws` | registered | datafusion | String and binary functions |
| `contains` | registered | datafusion | String and binary functions |
| `conv` | registered | oxidant | Numeric scalar functions |
| `convert_timezone` | registered | oxidant | Numeric scalar functions |
| `corr` | registered | datafusion | Numeric scalar functions |
| `cos` | registered | datafusion | Numeric scalar functions |
| `cosh` | registered | datafusion | Numeric scalar functions |
| `cot` | registered | datafusion | Numeric scalar functions |
| `count` | registered | datafusion | Numeric scalar functions |
| `count_if` | registered | oxidant | Numeric scalar functions |
| `covar_pop` | registered | datafusion | Numeric scalar functions |
| `covar_samp` | registered | datafusion | Numeric scalar functions |
| `crc32` | registered | oxidant | String and binary functions |
| `csc` | registered | oxidant | Numeric scalar functions |
| `cume_dist` | registered | datafusion | Analytic window functions |
| `curdate` | registered | oxidant | Date, timestamp, and interval functions |
| `current_catalog` | registered | oxidant | Miscellaneous functions |
| `current_database` | registered | oxidant | Miscellaneous functions |
| `current_date` | registered | datafusion | Date, timestamp, and interval functions |
| `current_schema` | registered | oxidant | Miscellaneous functions |
| `current_time` | registered | datafusion | Date, timestamp, and interval functions |
| `current_timestamp` | registered | datafusion | Date, timestamp, and interval functions |
| `current_timezone` | registered | oxidant | Date, timestamp, and interval functions |
| `current_user` | **missing** | — | Miscellaneous functions |
| `date` | registered | oxidant | Date, timestamp, and interval functions; Cast functions and constructors |
| `date_add` | registered | oxidant | Date, timestamp, and interval functions |
| `date_diff` | registered | oxidant | Date, timestamp, and interval functions |
| `date_format` | registered | datafusion | Date, timestamp, and interval functions |
| `date_from_unix_date` | registered | oxidant | Date, timestamp, and interval functions |
| `date_part` | registered | datafusion | Date, timestamp, and interval functions |
| `date_sub` | registered | oxidant | Date, timestamp, and interval functions |
| `date_trunc` | registered | datafusion | Date, timestamp, and interval functions |
| `dateadd` | registered | oxidant | Date, timestamp, and interval functions |
| `datediff` | registered | oxidant | Date, timestamp, and interval functions |
| `day` | registered | oxidant | Date, timestamp, and interval functions |
| `dayname` | registered | oxidant | Date, timestamp, and interval functions |
| `dayofmonth` | registered | oxidant | Date, timestamp, and interval functions |
| `dayofweek` | registered | oxidant | Date, timestamp, and interval functions |
| `dayofyear` | registered | oxidant | Date, timestamp, and interval functions |
| `decimal` | registered | oxidant | Numeric scalar functions; Cast functions and constructors |
| `decode` | registered | datafusion | Miscellaneous functions; String and binary functions |
| `degrees` | registered | datafusion | Numeric scalar functions |
| `dense_rank` | registered | datafusion | Ranking window functions |
| `double` | registered | oxidant | Numeric scalar functions; Cast functions and constructors |
| `e` | registered | oxidant | Numeric scalar functions |
| `element_at` | registered | datafusion | Array functions; Map functions |
| `elt` | registered | oxidant | Miscellaneous functions |
| `encode` | registered | datafusion | String and binary functions |
| `endswith` | registered | oxidant | String and binary functions |
| `equal_null` | registered | oxidant | Miscellaneous functions |
| `every` | registered | oxidant | Numeric scalar functions |
| `exists` | **missing** | — | Operators and predicates; Array functions |
| `exp` | registered | datafusion | Numeric scalar functions |
| `explode` | **missing** | — | Array functions; Map functions |
| `explode_outer` | **missing** | — | Array functions; Map functions |
| `expm1` | registered | oxidant | Numeric scalar functions |
| `factorial` | registered | datafusion | Numeric scalar functions |
| `filter` | **missing** | — | Array functions |
| `find_in_set` | registered | datafusion | String and binary functions |
| `first` | registered | oxidant | Numeric scalar functions |
| `first_value` | registered | datafusion | Numeric scalar functions |
| `flatten` | registered | datafusion | Array functions |
| `float` | registered | oxidant | Numeric scalar functions; Cast functions and constructors |
| `floor` | registered | datafusion | Numeric scalar functions |
| `forall` | **missing** | — | Array functions |
| `format_number` | **missing** | — | String and binary functions |
| `format_string` | registered | oxidant | String and binary functions |
| `from_avro` | **missing** | — | CSV and Avro functions |
| `from_csv` | registered | oxidant | CSV and Avro functions |
| `from_json` | registered | oxidant | JSON functions |
| `from_unixtime` | registered | datafusion | Date, timestamp, and interval functions |
| `from_utc_timestamp` | registered | oxidant | Date, timestamp, and interval functions |
| `from_xml` | **missing** | — | XPath and XML functions |
| `get` | **missing** | — | Array functions |
| `get_json_object` | registered | oxidant | JSON functions |
| `getbit` | registered | oxidant | Numeric scalar functions |
| `getdate` | registered | oxidant | Date, timestamp, and interval functions |
| `greatest` | registered | datafusion | Miscellaneous functions |
| `grouping` | registered | datafusion | Miscellaneous functions |
| `grouping_id` | registered | oxidant | Miscellaneous functions |
| `hash` | **missing** | — | Miscellaneous functions |
| `hex` | registered | oxidant | String and binary functions |
| `histogram_numeric` | **missing** | — | Numeric scalar functions |
| `hour` | registered | oxidant | Date, timestamp, and interval functions |
| `hypot` | registered | oxidant | Numeric scalar functions |
| `if` | registered | oxidant | Miscellaneous functions |
| `iff` | registered | oxidant | Miscellaneous functions |
| `ifnull` | registered | datafusion | Miscellaneous functions |
| `ilike` | registered | oxidant | Operators and predicates |
| `initcap` | registered | datafusion | String and binary functions |
| `inline` | **missing** | — | Array functions |
| `inline_outer` | **missing** | — | Array functions |
| `input_file_block_length` | **missing** | — | Miscellaneous functions |
| `input_file_block_start` | **missing** | — | Miscellaneous functions |
| `input_file_name` | **missing** | — | Miscellaneous functions |
| `instr` | registered | datafusion | String and binary functions |
| `int` | registered | oxidant | Numeric scalar functions; Cast functions and constructors |
| `is_variant_null` | **missing** | — | VARIANT functions |
| `isnan` | registered | datafusion | Numeric scalar functions |
| `isnotnull` | registered | oxidant | Miscellaneous functions |
| `isnull` | registered | oxidant | Operators and predicates; Miscellaneous functions |
| `json_array_length` | registered | oxidant | JSON functions |
| `json_object_keys` | registered | oxidant | JSON functions |
| `json_tuple` | **missing** | — | JSON functions |
| `kurtosis` | **missing** | — | Numeric scalar functions |
| `lag` | registered | datafusion | Analytic window functions |
| `last` | registered | oxidant | Numeric scalar functions |
| `last_day` | registered | oxidant | Date, timestamp, and interval functions |
| `last_value` | registered | datafusion | Numeric scalar functions |
| `lcase` | registered | oxidant | String and binary functions |
| `lead` | registered | datafusion | Analytic window functions |
| `least` | registered | datafusion | Miscellaneous functions |
| `left` | registered | datafusion | String and binary functions |
| `len` | registered | oxidant | String and binary functions |
| `length` | registered | datafusion | String and binary functions |
| `levenshtein` | registered | datafusion | String and binary functions |
| `like` | registered | oxidant | Operators and predicates; String and binary functions |
| `listagg` | **missing** | — | Numeric scalar functions |
| `ln` | registered | datafusion | Numeric scalar functions |
| `locate` | **missing** | — | String and binary functions |
| `log` | registered | datafusion | Numeric scalar functions |
| `log10` | registered | datafusion | Numeric scalar functions |
| `log1p` | registered | oxidant | Numeric scalar functions |
| `log2` | registered | datafusion | Numeric scalar functions |
| `lower` | registered | datafusion | String and binary functions |
| `lpad` | registered | datafusion | String and binary functions |
| `ltrim` | registered | datafusion | String and binary functions |
| `luhn_check` | **missing** | — | Miscellaneous functions |
| `make_date` | registered | datafusion | Date, timestamp, and interval functions; Cast functions and constructors |
| `make_dt_interval` | **missing** | — | Date, timestamp, and interval functions; Cast functions and constructors |
| `make_interval` | **missing** | — | Date, timestamp, and interval functions; Cast functions and constructors |
| `make_time` | registered | datafusion | Date, timestamp, and interval functions |
| `make_timestamp` | registered | oxidant | Date, timestamp, and interval functions; Cast functions and constructors |
| `make_ym_interval` | **missing** | — | Date, timestamp, and interval functions; Cast functions and constructors |
| `map` | registered | datafusion | Map functions; Cast functions and constructors |
| `map_concat` | **missing** | — | Map functions |
| `map_contains_key` | registered | oxidant | Map functions |
| `map_entries` | registered | datafusion | Map functions |
| `map_filter` | **missing** | — | Map functions |
| `map_from_arrays` | **missing** | — | Map functions |
| `map_from_entries` | **missing** | — | Map functions |
| `map_keys` | registered | datafusion | Map functions |
| `map_values` | registered | datafusion | Map functions |
| `map_zip_with` | **missing** | — | Map functions |
| `mask` | registered | oxidant | String and binary functions |
| `max` | registered | datafusion | Numeric scalar functions |
| `max_by` | **missing** | — | Numeric scalar functions |
| `md5` | registered | datafusion | String and binary functions |
| `mean` | registered | datafusion | Numeric scalar functions |
| `median` | registered | datafusion | Numeric scalar functions |
| `min` | registered | datafusion | Numeric scalar functions |
| `min_by` | **missing** | — | Numeric scalar functions |
| `minute` | registered | oxidant | Date, timestamp, and interval functions |
| `mod` | registered | oxidant | Numeric scalar functions |
| `mode` | registered | oxidant | Numeric scalar functions |
| `monotonically_increasing_id` | **missing** | — | Miscellaneous functions |
| `month` | registered | oxidant | Date, timestamp, and interval functions |
| `months_between` | registered | oxidant | Date, timestamp, and interval functions |
| `named_struct` | registered | datafusion | Cast functions and constructors |
| `nanvl` | registered | datafusion | Numeric scalar functions |
| `negative` | registered | oxidant | Numeric scalar functions |
| `next_day` | registered | oxidant | Date, timestamp, and interval functions |
| `now` | registered | datafusion | Date, timestamp, and interval functions |
| `nth_value` | registered | datafusion | Analytic window functions |
| `ntile` | registered | datafusion | Ranking window functions |
| `nullif` | registered | datafusion | Miscellaneous functions |
| `nullifzero` | registered | oxidant | Numeric scalar functions |
| `nvl` | registered | datafusion | Miscellaneous functions |
| `nvl2` | registered | datafusion | Miscellaneous functions |
| `octet_length` | registered | datafusion | String and binary functions |
| `overlay` | registered | datafusion | String and binary functions |
| `parse_json` | **missing** | — | JSON functions; VARIANT functions |
| `parse_timestamp` | **missing** | — | Date, timestamp, and interval functions |
| `parse_url` | registered | oxidant | String and binary functions |
| `percent_rank` | registered | datafusion | Ranking window functions |
| `percentile` | registered | oxidant | Numeric scalar functions |
| `percentile_approx` | **missing** | — | Numeric scalar functions |
| `percentile_cont` | registered | datafusion | Numeric scalar functions |
| `percentile_disc` | **missing** | — | Numeric scalar functions |
| `pi` | registered | datafusion | Numeric scalar functions |
| `pmod` | registered | oxidant | Numeric scalar functions |
| `posexplode` | **missing** | — | Array functions |
| `posexplode_outer` | **missing** | — | Array functions |
| `position` | registered | datafusion | String and binary functions |
| `positive` | registered | oxidant | Numeric scalar functions |
| `pow` | registered | datafusion | Numeric scalar functions |
| `power` | registered | datafusion | Numeric scalar functions |
| `printf` | **missing** | — | String and binary functions |
| `quarter` | registered | oxidant | Date, timestamp, and interval functions |
| `radians` | registered | datafusion | Numeric scalar functions |
| `raise_error` | **missing** | — | Miscellaneous functions |
| `rand` | registered | datafusion | Numeric scalar functions |
| `randn` | **missing** | — | Numeric scalar functions |
| `random` | registered | datafusion | Numeric scalar functions |
| `randstr` | **missing** | — | String and binary functions |
| `range` | registered | datafusion | Miscellaneous functions |
| `rank` | registered | datafusion | Ranking window functions |
| `reduce` | **missing** | — | Array functions |
| `regexp` | registered | oxidant | Operators and predicates; String and binary functions |
| `regexp_count` | registered | datafusion | String and binary functions |
| `regexp_extract` | registered | oxidant | String and binary functions |
| `regexp_extract_all` | registered | oxidant | String and binary functions |
| `regexp_instr` | registered | datafusion | String and binary functions |
| `regexp_like` | registered | datafusion | Operators and predicates; String and binary functions |
| `regexp_replace` | registered | datafusion | String and binary functions |
| `regexp_substr` | registered | oxidant | String and binary functions |
| `regr_avgx` | registered | datafusion | Numeric scalar functions |
| `regr_avgy` | registered | datafusion | Numeric scalar functions |
| `regr_count` | registered | datafusion | Numeric scalar functions |
| `regr_intercept` | registered | datafusion | Numeric scalar functions |
| `regr_r2` | registered | datafusion | Numeric scalar functions |
| `regr_slope` | registered | datafusion | Numeric scalar functions |
| `regr_sxx` | registered | datafusion | Numeric scalar functions |
| `regr_sxy` | registered | datafusion | Numeric scalar functions |
| `regr_syy` | registered | datafusion | Numeric scalar functions |
| `repeat` | registered | datafusion | String and binary functions |
| `replace` | registered | datafusion | String and binary functions |
| `reverse` | registered | datafusion | String and binary functions; Array functions |
| `right` | registered | datafusion | String and binary functions |
| `rint` | registered | oxidant | Numeric scalar functions |
| `rlike` | registered | oxidant | Operators and predicates; String and binary functions |
| `round` | registered | datafusion | Numeric scalar functions |
| `row_number` | registered | datafusion | Ranking window functions |
| `rpad` | registered | datafusion | String and binary functions |
| `rtrim` | registered | datafusion | String and binary functions |
| `schema_of_csv` | **missing** | — | CSV and Avro functions |
| `schema_of_json` | **missing** | — | JSON functions |
| `schema_of_json_agg` | **missing** | — | Numeric scalar functions; JSON functions |
| `schema_of_variant_agg` | **missing** | — | Numeric scalar functions; VARIANT functions |
| `schema_of_xml` | **missing** | — | XPath and XML functions |
| `sec` | registered | oxidant | Numeric scalar functions |
| `second` | registered | oxidant | Date, timestamp, and interval functions |
| `sentences` | **missing** | — | String and binary functions |
| `sequence` | **missing** | — | Array functions |
| `session_user` | **missing** | — | Miscellaneous functions |
| `session_window` | **missing** | — | Date, timestamp, and interval functions |
| `sha` | registered | oxidant | String and binary functions |
| `sha1` | registered | oxidant | String and binary functions |
| `sha2` | registered | oxidant | String and binary functions |
| `shiftleft` | registered | oxidant | Numeric scalar functions |
| `shiftright` | registered | oxidant | Numeric scalar functions |
| `shiftrightunsigned` | registered | oxidant | Numeric scalar functions |
| `shuffle` | **missing** | — | Array functions |
| `sign` | registered | oxidant | Numeric scalar functions; Date, timestamp, and interval functions |
| `signum` | registered | datafusion | Numeric scalar functions; Date, timestamp, and interval functions |
| `sin` | registered | datafusion | Numeric scalar functions |
| `sinh` | registered | datafusion | Numeric scalar functions |
| `skewness` | registered | oxidant | Numeric scalar functions |
| `slice` | **missing** | — | Array functions |
| `smallint` | registered | oxidant | Numeric scalar functions; Cast functions and constructors |
| `some` | registered | oxidant | Numeric scalar functions |
| `sort_array` | registered | oxidant | Array functions |
| `soundex` | **missing** | — | String and binary functions |
| `space` | **missing** | — | String and binary functions |
| `spark_partition_id` | **missing** | — | Miscellaneous functions |
| `split` | registered | oxidant | String and binary functions |
| `split_part` | registered | datafusion | String and binary functions |
| `sqrt` | registered | datafusion | Numeric scalar functions |
| `stack` | **missing** | — | Miscellaneous functions |
| `startswith` | registered | oxidant | String and binary functions |
| `std` | registered | oxidant | Numeric scalar functions |
| `stddev` | registered | datafusion | Numeric scalar functions |
| `stddev_pop` | registered | datafusion | Numeric scalar functions |
| `stddev_samp` | registered | datafusion | Numeric scalar functions |
| `str_to_map` | **missing** | — | Map functions |
| `string` | registered | oxidant | String and binary functions; Cast functions and constructors |
| `string_agg` | registered | datafusion | Numeric scalar functions |
| `struct` | registered | datafusion | Cast functions and constructors |
| `substr` | registered | datafusion | String and binary functions |
| `substring` | registered | datafusion | String and binary functions |
| `substring_index` | registered | datafusion | String and binary functions |
| `sum` | registered | datafusion | Numeric scalar functions |
| `tan` | registered | datafusion | Numeric scalar functions |
| `tanh` | registered | datafusion | Numeric scalar functions |
| `time_diff` | **missing** | — | Date, timestamp, and interval functions |
| `time_from_micros` | **missing** | — | Date, timestamp, and interval functions |
| `time_from_millis` | **missing** | — | Date, timestamp, and interval functions |
| `time_from_seconds` | **missing** | — | Date, timestamp, and interval functions |
| `time_to_micros` | **missing** | — | Date, timestamp, and interval functions |
| `time_to_millis` | **missing** | — | Date, timestamp, and interval functions |
| `time_to_seconds` | **missing** | — | Date, timestamp, and interval functions |
| `time_trunc` | **missing** | — | Date, timestamp, and interval functions |
| `timediff` | **missing** | — | Date, timestamp, and interval functions |
| `timestamp` | registered | oxidant | Date, timestamp, and interval functions; Cast functions and constructors |
| `timestamp_micros` | registered | oxidant | Date, timestamp, and interval functions |
| `timestamp_millis` | registered | oxidant | Date, timestamp, and interval functions |
| `timestamp_seconds` | registered | oxidant | Date, timestamp, and interval functions |
| `timestampadd` | **missing** | — | Date, timestamp, and interval functions |
| `timestampdiff` | **missing** | — | Date, timestamp, and interval functions |
| `tinyint` | registered | oxidant | Numeric scalar functions; Cast functions and constructors |
| `to_avro` | **missing** | — | CSV and Avro functions |
| `to_binary` | registered | oxidant | String and binary functions |
| `to_char` | registered | datafusion | String and binary functions; Cast functions and constructors |
| `to_csv` | registered | oxidant | CSV and Avro functions |
| `to_date` | registered | datafusion | Date, timestamp, and interval functions; Cast functions and constructors |
| `to_json` | registered | oxidant | JSON functions; VARIANT functions |
| `to_number` | registered | oxidant | Numeric scalar functions; Cast functions and constructors |
| `to_time` | registered | datafusion | Date, timestamp, and interval functions |
| `to_timestamp` | registered | datafusion | Date, timestamp, and interval functions; Cast functions and constructors |
| `to_unix_timestamp` | registered | oxidant | Date, timestamp, and interval functions |
| `to_utc_timestamp` | registered | oxidant | Date, timestamp, and interval functions |
| `to_varchar` | registered | oxidant | String and binary functions; Cast functions and constructors |
| `to_variant_object` | **missing** | — | VARIANT functions |
| `transform` | **missing** | — | Array functions |
| `transform_keys` | **missing** | — | Map functions |
| `transform_values` | **missing** | — | Map functions |
| `translate` | registered | datafusion | String and binary functions |
| `trim` | registered | datafusion | String and binary functions |
| `trunc` | registered | datafusion | Date, timestamp, and interval functions |
| `try_add` | registered | oxidant | Numeric scalar functions; Date, timestamp, and interval functions |
| `try_aes_decrypt` | **missing** | — | String and binary functions |
| `try_avg` | registered | oxidant | Numeric scalar functions |
| `try_divide` | registered | oxidant | Numeric scalar functions; Date, timestamp, and interval functions |
| `try_element_at` | registered | oxidant | Array functions; Map functions |
| `try_mod` | registered | oxidant | Numeric scalar functions |
| `try_multiply` | registered | oxidant | Numeric scalar functions; Date, timestamp, and interval functions |
| `try_parse_json` | **missing** | — | VARIANT functions |
| `try_parse_timestamp` | **missing** | — | Date, timestamp, and interval functions |
| `try_subtract` | registered | oxidant | Numeric scalar functions; Date, timestamp, and interval functions |
| `try_sum` | registered | oxidant | Numeric scalar functions |
| `try_to_binary` | registered | oxidant | String and binary functions |
| `try_to_number` | registered | oxidant | Numeric scalar functions; Cast functions and constructors |
| `try_to_time` | **missing** | — | Date, timestamp, and interval functions |
| `try_to_timestamp` | registered | oxidant | Date, timestamp, and interval functions |
| `try_url_decode` | registered | oxidant | String and binary functions |
| `try_variant_get` | **missing** | — | VARIANT functions |
| `try_zstd_decompress` | **missing** | — | String and binary functions |
| `typeof` | registered | oxidant | Miscellaneous functions |
| `ucase` | registered | oxidant | String and binary functions |
| `unbase64` | **missing** | — | String and binary functions |
| `unhex` | registered | oxidant | String and binary functions |
| `uniform` | **missing** | — | Numeric scalar functions |
| `unix_date` | registered | oxidant | Date, timestamp, and interval functions |
| `unix_micros` | registered | oxidant | Date, timestamp, and interval functions |
| `unix_millis` | registered | oxidant | Date, timestamp, and interval functions |
| `unix_seconds` | registered | oxidant | Date, timestamp, and interval functions |
| `unix_timestamp` | registered | oxidant | Date, timestamp, and interval functions |
| `upper` | registered | datafusion | String and binary functions |
| `url_decode` | registered | oxidant | String and binary functions |
| `url_encode` | registered | oxidant | String and binary functions |
| `user` | **missing** | — | Miscellaneous functions |
| `uuid` | registered | datafusion | Miscellaneous functions |
| `var_pop` | registered | datafusion | Numeric scalar functions |
| `var_samp` | registered | datafusion | Numeric scalar functions |
| `variance` | registered | oxidant | Numeric scalar functions |
| `variant_explode` | **missing** | — | VARIANT functions |
| `variant_explode_outer` | **missing** | — | VARIANT functions |
| `variant_get` | **missing** | — | VARIANT functions |
| `version` | registered | datafusion | Miscellaneous functions |
| `weekday` | registered | oxidant | Date, timestamp, and interval functions |
| `weekofyear` | registered | oxidant | Date, timestamp, and interval functions |
| `width_bucket` | registered | oxidant | Numeric scalar functions |
| `window` | **missing** | — | Date, timestamp, and interval functions; Miscellaneous functions |
| `window_time` | **missing** | — | Date, timestamp, and interval functions |
| `xpath` | **missing** | — | XPath and XML functions |
| `xpath_boolean` | **missing** | — | XPath and XML functions |
| `xpath_double` | **missing** | — | XPath and XML functions |
| `xpath_float` | **missing** | — | XPath and XML functions |
| `xpath_int` | **missing** | — | XPath and XML functions |
| `xpath_long` | **missing** | — | XPath and XML functions |
| `xpath_number` | **missing** | — | XPath and XML functions |
| `xpath_short` | **missing** | — | XPath and XML functions |
| `xpath_string` | **missing** | — | XPath and XML functions |
| `xxhash64` | **missing** | — | Miscellaneous functions |
| `year` | registered | oxidant | Date, timestamp, and interval functions |
| `zeroifnull` | registered | oxidant | Numeric scalar functions |
| `zip_with` | **missing** | — | Array functions |
| `zstd_compress` | **missing** | — | String and binary functions |
| `zstd_decompress` | **missing** | — | String and binary functions |
