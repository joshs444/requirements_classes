WITH usage_base AS (
  SELECT
    CAST(l.Subsidiary                            AS VARCHAR(50))  AS subsidiary,
    CAST(l.[Entry No_]                           AS INT)          AS entry_no,
    CAST(l.[Item No_]                            AS VARCHAR(50))  AS item_no,
    CAST(l.[Posting Date]                        AS DATE)         AS posting_date,
    CAST(l.[Location Code]                       AS VARCHAR(50))  AS location_code,
    CAST(l.[Order No_]                           AS VARCHAR(50))  AS order_no,
    CAST(l.[Document No_]                        AS VARCHAR(50))  AS document_no,

    /* classify issue type: consumption, sale, or scrap */
    CAST(
      CASE
        WHEN l.[Entry Type] = 5                                            THEN 'C'      -- consumption
        WHEN l.[Entry Type] = 1                                            THEN 'SALE'   -- sale
        WHEN l.[Entry Type] = 3 AND l.[Gen_ Prod_ Posting Group] = 'SCRAP' THEN 'S'      -- scrap
      END
      AS VARCHAR(6)
    ) AS issue_type,

    /* keep your cost/qty calculations the same */
    CAST(ABS(l.Quantity)                                           AS DECIMAL(18,4)) AS qty_issued,
    CAST(ABS(l.SUM_Cost_Amount_Actual_USD + l.SUM_Cost_Amount_Expected_USD)
                                                                  AS DECIMAL(18,4)) AS total_cost_usd,
    CAST(
      CASE WHEN ABS(l.Quantity) > 0
           THEN ABS(l.SUM_Cost_Amount_Actual_USD + l.SUM_Cost_Amount_Expected_USD)
                / ABS(l.Quantity)
      END
      AS DECIMAL(18,4)
    ) AS unit_cost,
    CAST(ABS(l.SUM_Root_Cost_Actual_USD + l.SUM_Root_Cost_Expected_USD)
                                                                  AS DECIMAL(18,4)) AS total_root_cost_usd,
    CAST(
      CASE WHEN ABS(l.Quantity) > 0
           THEN ABS(l.SUM_Root_Cost_Actual_USD + l.SUM_Root_Cost_Expected_USD)
                / ABS(l.Quantity)
      END
      AS DECIMAL(18,4)
    ) AS unit_cost_root,

    CAST(l.[Global Dimension 1 Code]           AS VARCHAR(50))  AS department
  FROM dbo.item_ledger_entry_all_v AS l
  WHERE
    l.Subsidiary = 'US010'
    AND l.Quantity   <> 0
    AND (
         l.[Entry Type] = 5                                       -- consumption
      OR l.[Entry Type] = 1                                       -- sale
      OR (l.[Entry Type] = 3 AND l.[Gen_ Prod_ Posting Group] = 'SCRAP')
    )
)
SELECT *
FROM usage_base
ORDER BY posting_date, item_no, department, entry_no;
