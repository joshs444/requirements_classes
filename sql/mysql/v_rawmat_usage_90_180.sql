/* =========================================================================
   View: v_rawmat_usage_90_180
   Purpose : Rolling-window **usage** metrics (qty + ledger $)
             • Raw-material items only
             • 90-day and 180-day look-backs relative to today
   =========================================================================*/
CREATE OR REPLACE VIEW v_rawmat_usage_90_180 AS
WITH usage_last_180 AS (
    /* 1️⃣  Keep only the last 180 days of material-usage rows */
    SELECT
        item_no,
        posting_date,
        qty_issued,
        total_cost_usd          -- full ledger $ for the issue row
    FROM   material_usage
    WHERE  posting_date >= CURDATE() - INTERVAL 180 DAY
)
SELECT
    i.item_no,
    i.description,

    /* ─────────────── 90-day window ─────────────── */
    SUM(CASE                                                -- units issued
            WHEN ul.posting_date >= CURDATE() - INTERVAL 90 DAY
            THEN ul.qty_issued
        END)                                           AS usage_qty_90d,

    SUM(CASE                                                -- $ ledger spend
            WHEN ul.posting_date >= CURDATE() - INTERVAL 90 DAY
            THEN ul.total_cost_usd
        END)                                           AS usage_cost_90d_ledger,

    /* ─────────────── 180-day window ────────────── */
    SUM(ul.qty_issued)                                 AS usage_qty_180d,
    SUM(ul.total_cost_usd)                             AS usage_cost_180d_ledger,

    /* ───────── average daily demand (units) ─────── */
    CAST( SUM(CASE
                WHEN ul.posting_date >= CURDATE() - INTERVAL 90 DAY
                THEN ul.qty_issued
              END) / 90  AS DECIMAL(18,4))             AS usage_qty_day_90d,

    CAST( SUM(ul.qty_issued) / 180 AS DECIMAL(18,4))   AS usage_qty_day_180d

FROM   item            AS i
JOIN   usage_last_180  AS ul  ON ul.item_no = i.item_no
WHERE  i.raw_mat_flag = 'Yes'          -- raw-material items only
GROUP  BY i.item_no , i.description;
