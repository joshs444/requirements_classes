/*─────────────────────────────────────────────────────────────
  REBUILD inv_snapshot  – run manually whenever you want it fresh
─────────────────────────────────────────────────────────────*/
START TRANSACTION;

/* 0) parameters you may tweak every run */
SET @sub            = 'US01';            -- subsidiary filter
SET @slice_start    = '2020-01-01';      -- earliest day you want detailed history

/* 1) seed opening balances (everything BEFORE @slice_start) */
DROP TEMPORARY TABLE IF EXISTS tmp_open;
CREATE TEMPORARY TABLE tmp_open ENGINE=InnoDB AS
SELECT
    DATE_SUB(@slice_start, INTERVAL 1 DAY)      AS snapshot_date,
    `Item No_`          AS item_no,
    `Location Code`     AS location_code,
    SUM(Quantity)                               AS daily_qty,
    SUM(`SUM_Root_Cost_Actual_USD`
       +`SUM_Root_Cost_Expected_USD`)           AS daily_root,
    SUM(`SUM_Cost_Amount_Actual_USD`
       +`SUM_Cost_Amount_Expected_USD`)         AS daily_cost
FROM  ledger_all
WHERE TRIM(Subsidiary) = @sub
  AND `Posting Date`  < @slice_start
GROUP BY item_no, location_code;

/* 2) build span rows (seed + @slice_start → today) */
DROP TEMPORARY TABLE IF EXISTS tmp_spans;
CREATE TEMPORARY TABLE tmp_spans ENGINE=InnoDB AS
WITH daily AS (
    /* opening snapshot row */
    SELECT * FROM tmp_open
    UNION ALL
    /* all quantity‑moving ledger rows for the slice */
    SELECT
        CAST(`Posting Date` AS DATE)           AS snapshot_date,
        `Item No_`          AS item_no,
        `Location Code`     AS location_code,
        SUM(Quantity)                               AS daily_qty,
        SUM(`SUM_Root_Cost_Actual_USD`
           +`SUM_Root_Cost_Expected_USD`)           AS daily_root,
        SUM(`SUM_Cost_Amount_Actual_USD`
           +`SUM_Cost_Amount_Expected_USD`)         AS daily_cost
    FROM  ledger_all  USE INDEX(idx_ledger_covering)
    WHERE TRIM(Subsidiary) = @sub
      AND Quantity <> 0
      AND `Posting Date`  >= @slice_start
    GROUP BY snapshot_date, item_no, location_code
),
running AS (
    SELECT
        d.*,
        SUM(d.daily_qty)  OVER (PARTITION BY item_no,location_code
                                ORDER BY snapshot_date)
            AS qty_on_hand,
        SUM(d.daily_root) OVER (PARTITION BY item_no,location_code
                                ORDER BY snapshot_date)
            AS total_root,
        SUM(d.daily_cost) OVER (PARTITION BY item_no,location_code
                                ORDER BY snapshot_date)
            AS total_cost
    FROM daily d
),
changes AS (
    SELECT r.*,
           LAG(qty_on_hand) OVER w AS prev_qty,
           LAG(total_root ) OVER w AS prev_root,
           LAG(total_cost ) OVER w AS prev_cost
    FROM running r
    WINDOW w AS (PARTITION BY item_no,location_code ORDER BY snapshot_date)
),
starts AS (
    /* keep rows where the running balance changed */
    SELECT *
    FROM changes
    WHERE prev_qty IS NULL
       OR prev_qty <> qty_on_hand
       OR prev_root <> total_root
       OR prev_cost <> total_cost
)
SELECT
    item_no,
    location_code,
    snapshot_date                                        AS balance_start,
    DATE_SUB( LEAD(snapshot_date) OVER w, INTERVAL 1 DAY ) AS balance_end,
    qty_on_hand,
    total_root,
    total_cost
FROM starts
WINDOW w AS (PARTITION BY item_no,location_code ORDER BY snapshot_date);

/* 3) replace inv_snapshot with fresh data */
TRUNCATE TABLE inv_snapshot;      -- wipe old spans

INSERT INTO inv_snapshot
  (item_no ,location_code ,balance_start ,balance_end ,
   qty_on_hand ,total_root ,total_cost)
SELECT
   item_no ,location_code ,balance_start ,balance_end ,
   qty_on_hand ,total_root ,total_cost
FROM tmp_spans
ORDER BY item_no, location_code, balance_start;

/* 4) cleanup */
DROP TEMPORARY TABLE IF EXISTS tmp_open;
DROP TEMPORARY TABLE IF EXISTS tmp_spans;

COMMIT;
