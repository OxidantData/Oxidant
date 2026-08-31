-- Databricks SQL language manual: date and timestamp functions.
-- https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-functions-builtin
-- Field extraction, calendar arithmetic, and explicit-zone shifts.
-- Every value is a calendar fact or is pinned by a vendored Spark golden; see README.

SELECT year(DATE'2024-03-05') AS a, month(DATE'2024-03-05') AS b,
       day(DATE'2024-03-05') AS c, dayofmonth(DATE'2024-03-05') AS d,
       quarter(DATE'2024-03-05') AS e;

SELECT dayofweek(DATE'2007-02-03') AS a, weekday(DATE'2007-02-03') AS b,
       dayofweek(DATE'2009-07-30') AS c, weekday(DATE'2009-07-30') AS d;

SELECT dayofyear(DATE'2024-03-05') AS a, dayofyear(DATE'2023-03-05') AS b,
       weekofyear(DATE'2016-01-01') AS c, weekofyear(DATE'2018-01-01') AS d;

SELECT dayname(DATE'2009-07-30') AS a;

SELECT hour(TIMESTAMP'2024-03-05 13:45:59') AS a,
       minute(TIMESTAMP'2024-03-05 13:45:59') AS b,
       second(TIMESTAMP'2024-03-05 13:45:59') AS c,
       hour(DATE'2024-03-05') AS d;

SELECT datediff(DATE'2024-03-05', DATE'2024-03-01') AS a,
       date_diff(DATE'2024-03-01', DATE'2024-03-05') AS b,
       datediff(DATE'2024-03-01', DATE'2024-02-28') AS c,
       datediff(DATE'2023-03-01', DATE'2023-02-28') AS d;

SELECT add_months(DATE'2024-01-31', 1) AS a, add_months(DATE'2023-01-31', 1) AS b,
       last_day(DATE'2024-02-05') AS c, last_day(DATE'2023-02-05') AS d;

SELECT months_between(DATE'2024-03-15', DATE'2024-01-15') AS a,
       months_between(DATE'2024-03-20', DATE'2024-02-15') AS b;

SELECT current_timezone() AS a;

SELECT from_utc_timestamp(TIMESTAMP'2024-01-15 12:00:00', 'America/New_York') AS a,
       from_utc_timestamp(TIMESTAMP'2024-07-15 12:00:00', 'America/New_York') AS b,
       from_utc_timestamp(TIMESTAMP'2024-01-15 12:00:00', 'Asia/Kolkata') AS c;

SELECT to_utc_timestamp(TIMESTAMP'2024-01-15 07:00:00', 'America/New_York') AS a;

SELECT convert_timezone('Europe/Moscow', 'America/Los_Angeles', TIMESTAMP'2022-01-01 00:00:00') AS a;

SELECT unix_micros(TIMESTAMP'2020-12-01 06:30:08.999999') AS a;
