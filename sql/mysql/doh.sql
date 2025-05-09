/* ============================================================
   0. Parameters – adjust if needed
   ============================================================ */
SET @as_of    = CURDATE() - INTERVAL 1 DAY;   -- inventory “as of” date (yesterday)
SET @lookback = 90;                           -- rolling-usage window (days)

/* ============================================================
   1. 90-day average consumption (TEMP TABLES)
   ============================================================ */
/* Item-location level average usage */
DROP TEMPORARY TABLE IF EXISTS tmp_avg_use;
CREATE TEMPORARY TABLE tmp_avg_use AS
SELECT
    mu.item_no,
    mu.location_code,
    SUM(mu.qty_issued) / NULLIF(COUNT(DISTINCT mu.posting_date),0) AS avg_daily_qty,
    ANY_VALUE(mu.std_cost_usd)                            AS std_cost
FROM   material_usage mu
JOIN   item i ON mu.item_no = i.item_no
WHERE  mu.issue_type = 'C'
  AND  mu.posting_date BETWEEN DATE_SUB(@as_of, INTERVAL @lookback DAY) AND @as_of
  AND  i.raw_mat_flag = 'Yes'
GROUP  BY mu.item_no, mu.location_code;

/* Index for performance */
CREATE INDEX ix_tmp_avg ON tmp_avg_use (item_no, location_code);

/* Item-level average usage */
DROP TEMPORARY TABLE IF EXISTS tmp_avg_use_item;
CREATE TEMPORARY TABLE tmp_avg_use_item AS
SELECT
    mu.item_no,
    SUM(mu.qty_issued) / NULLIF(COUNT(DISTINCT mu.posting_date),0) AS avg_daily_qty,
    ANY_VALUE(mu.std_cost_usd)                            AS std_cost
FROM   material_usage mu
JOIN   item i ON mu.item_no = i.item_no
WHERE  mu.issue_type = 'C'
  AND  mu.posting_date BETWEEN DATE_SUB(@as_of, INTERVAL @lookback DAY) AND @as_of
  AND  i.raw_mat_flag = 'Yes'
GROUP  BY mu.item_no;

/* Index for performance */
CREATE INDEX ix_tmp_avg_item ON tmp_avg_use_item (item_no);

/* ============================================================
   2. Detail FACT – Item × Location
   ============================================================ */
DROP TABLE IF EXISTS doh_item_location;
CREATE TABLE doh_item_location ENGINE = InnoDB AS
SELECT
    DATE(@as_of)                   AS doh_date,
    inv.item_no,
    inv.location_code,
    inv.qty                       AS on_hand_qty,
    u.avg_daily_qty,
    inv.qty / NULLIF(u.avg_daily_qty,0)         AS doh_qty,
    inv.extended_price            AS on_hand_value,
    u.avg_daily_qty * u.std_cost  AS daily_spend,
    inv.extended_price
      / NULLIF(u.avg_daily_qty * u.std_cost,0)  AS doh_cost
FROM   v_daily_inventory AS inv
JOIN   item AS i
  ON inv.item_no = i.item_no
LEFT   JOIN tmp_avg_use AS u
  ON u.item_no = inv.item_no
 AND u.location_code = inv.location_code
WHERE  inv.snapshot_date = @as_of
  AND  inv.qty <> 0
  AND  i.raw_mat_flag = 'Yes';

/* Add primary key & helpful index */
ALTER TABLE doh_item_location
    ADD PRIMARY KEY (doh_date, item_no, location_code),
    ADD INDEX ix_loc (doh_date, location_code);

/* ============================================================
   2b. Detail FACT – Item (fixed GROUP BY)
   ============================================================ */
DROP TABLE IF EXISTS doh_item;
CREATE TABLE doh_item ENGINE = InnoDB AS
SELECT
    DATE(@as_of)                   AS doh_date,
    inv.item_no,
    SUM(inv.qty)                  AS on_hand_qty,
    u.avg_daily_qty,
    SUM(inv.qty) / NULLIF(u.avg_daily_qty,0)         AS doh_qty,
    SUM(inv.extended_price)       AS on_hand_value,
    u.avg_daily_qty * u.std_cost  AS daily_spend,
    SUM(inv.extended_price)
      / NULLIF(u.avg_daily_qty * u.std_cost,0)        AS doh_cost
FROM   v_daily_inventory AS inv
JOIN   item AS i
  ON inv.item_no = i.item_no
LEFT   JOIN tmp_avg_use_item AS u
  ON u.item_no = inv.item_no
WHERE  inv.snapshot_date = @as_of
  AND  i.raw_mat_flag = 'Yes'
GROUP  BY inv.item_no, u.avg_daily_qty, u.std_cost
HAVING SUM(inv.qty) <> 0;

/* Add primary key */
ALTER TABLE doh_item
    ADD PRIMARY KEY (doh_date, item_no);

/* ============================================================
   3. Department roll-up – VIEW
   ============================================================ */
CREATE OR REPLACE VIEW v_doh_department AS
SELECT
    d.doh_date,
    m.department,
    SUM(d.on_hand_value)            AS on_hand_value,
    SUM(d.daily_spend)              AS daily_spend,
    SUM(d.on_hand_value)
      / NULLIF(SUM(d.daily_spend),0) AS doh_cost
FROM   doh_item_location AS d
JOIN   material_usage AS m
  ON m.item_no     = d.item_no
 AND m.location_code = d.location_code
 AND m.posting_date  = d.doh_date
GROUP  BY d.doh_date, m.department;

/* ============================================================
   4. Location roll-up – VIEW
   ============================================================ */
CREATE OR REPLACE VIEW v_doh_location AS
SELECT
    doh_date,
    location_code,
    SUM(on_hand_value)            AS on_hand_value,
    SUM(daily_spend)              AS daily_spend,
    SUM(on_hand_value)
      / NULLIF(SUM(daily_spend),0) AS doh_cost
FROM   doh_item_location
GROUP  BY doh_date, location_code;

/* ============================================================
   5. Company-wide headline – VIEW
   ============================================================ */
CREATE OR REPLACE VIEW v_doh_company AS
SELECT
    doh_date,
    SUM(on_hand_value)            AS on_hand_value,
    SUM(daily_spend)              AS daily_spend,
    SUM(on_hand_value)
      / NULLIF(SUM(daily_spend),0) AS doh_cost
FROM   doh_item_location
GROUP  BY doh_date;

/* ============================================================
   6. Clean-up temp tables (optional)
   ============================================================ */
DROP TEMPORARY TABLE IF EXISTS tmp_avg_use;
DROP TEMPORARY TABLE IF EXISTS tmp_avg_use_item;
