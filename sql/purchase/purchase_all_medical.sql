/*************************************************************************************************
  Procurement Analytics – FULL STAND-ALONE SCRIPT   (v8.3-MC-noMFG, May 2025)
  • Table set: IPG Medical Corporation (subsidiary code = US020)
  • Removes Manufacturer Code / Manufacturer Part No_ (not present in MC tables)
  • Moves Order Date to the first column of the final SELECT
  • Adds item_index  = Subsidiary + Item No_
  • 1-year (365 d) & 2-year (730 d) price windows
  • Item + vendor adaptive baselines in an OUTER APPLY block
  • Single-source flag, high-volume PO & spend flags, SLA metrics
**************************************************************************************************/

/* ============================================================ 0. Parameters */
DECLARE @w1y       INT   = 365;      -- 1-year look-back (days)
DECLARE @w2y       INT   = 730;      -- 2-year look-back (days)
DECLARE @hv_po     INT   = 12;       -- high-volume PO threshold (count)
DECLARE @hv_spend  MONEY = 100000;   -- high-volume spend threshold ($)
DECLARE @hv_window INT   = 365;      -- window (days) for hv flags
DECLARE @on_time_days INT = 4;       -- on-time delivery threshold (business days late)
SET NOCOUNT ON;

/* ============================================================ 1. #LineData  */
IF OBJECT_ID('tempdb..#LineData') IS NOT NULL DROP TABLE #LineData;

SELECT
    src.[Status],
    src.[Document Type],
    src.[Document No_],
    src.[Line No_],
    src.[Shortcut Dimension 1 Code],
    src.[Buy-from Vendor No_],
    src.[Type],
    src.[No_],
    src.[Location Code],
    src.[Expected Receipt Date],
    src.[Promised Receipt Date],
    src.[Planned Receipt Date],
    src.[Description],
    src.[Currency Code],
    src.qty_factor                              AS [Qty_ per Unit of Measure],
    src.[Quantity]  * src.qty_factor            AS [Quantity],
    src.[Outstanding Quantity] * src.qty_factor AS [Outstanding Quantity],
    src.[Unit Cost (LCY)] / src.qty_factor      AS [Unit Cost],
    src.[Requested Receipt Date],
    ([Quantity] - [Outstanding Quantity])
        * ([Unit Cost (LCY)] / src.qty_factor)  AS [Total],
    ([Quantity] - [Outstanding Quantity])
        * src.qty_factor                        AS [Quantity Delivered],
    'US020'                                     AS [Subsidiary]
INTO #LineData
FROM (
    /* ---------- HISTORY lines ---------- */
    SELECT
        'HISTORY'                                AS [Status],
        l.[Document Type],
        l.[Document No_],
        l.[Line No_],
        l.[Shortcut Dimension 1 Code],
        l.[Buy-from Vendor No_],
        CASE l.[Type] WHEN 1 THEN 'GL'
                      WHEN 2 THEN 'Item'
                      WHEN 4 THEN 'FA' END       AS [Type],
        l.[No_],
        l.[Location Code],
        l.[Expected Receipt Date],
        l.[Promised Receipt Date],
        l.[Planned Receipt Date],
        l.[Description],
        COALESCE(NULLIF(l.[Currency Code],''),'USD') AS [Currency Code],
        COALESCE(NULLIF(l.[Qty_ per Unit of Measure],0),1)      AS qty_factor,
        l.[Quantity],
        l.[Outstanding Quantity],
        l.[Unit Cost (LCY)],
        l.[Requested Receipt Date]
    FROM [dbo].[IPG Medical Corporation$Purchase History Line] l
    WHERE l.[Order Date] > '2019-01-01'
      AND l.[Quantity] > 0
      AND l.[Unit Cost (LCY)] > 0
      AND l.[Document Type] = 1
      AND l.[Type] IN (1,2,4)
      AND l.[Quantity] - l.[Outstanding Quantity] <> 0

    UNION ALL

    /* ---------- OPEN lines -------------- */
    SELECT
        'OPEN',
        l.[Document Type],
        l.[Document No_],
        l.[Line No_],
        l.[Shortcut Dimension 1 Code],
        l.[Buy-from Vendor No_],
        CASE l.[Type] WHEN 1 THEN 'GL'
                      WHEN 2 THEN 'Item'
                      WHEN 4 THEN 'FA' END,
        l.[No_],
        l.[Location Code],
        l.[Expected Receipt Date],
        l.[Promised Receipt Date],
        l.[Planned Receipt Date],
        l.[Description],
        COALESCE(NULLIF(l.[Currency Code],''),'USD') AS [Currency Code],
        COALESCE(NULLIF(l.[Qty_ per Unit of Measure],0),1),
        l.[Quantity],
        l.[Outstanding Quantity],
        l.[Unit Cost (LCY)],
        l.[Requested Receipt Date]
    FROM [dbo].[IPG Medical Corporation$Purchase Line] l
    WHERE l.[Order Date] > '2019-01-01'
      AND l.[Quantity] > 0
      AND l.[Unit Cost (LCY)] > 0
      AND l.[Document Type] = 1
      AND l.[Type] IN (1,2,4)
) src;

CREATE CLUSTERED INDEX IX_LineData_DocLineItem
        ON #LineData ([Document No_], [Line No_], [No_]);

/* ============================================================ 2. #HeaderData */
IF OBJECT_ID('tempdb..#HeaderData') IS NOT NULL DROP TABLE #HeaderData;

WITH HeaderCTE AS (
    SELECT
        [Document Type],[No_],[Order Date],[Posting Date],
        [Order Confirmation Date],[Purchaser Code],
        ROW_NUMBER() OVER (PARTITION BY [No_] ORDER BY [Order Date] DESC, [Posting Date] DESC) AS rn
    FROM (
        SELECT DISTINCT [Document Type],[No_],[Order Date],[Posting Date],
               [Order Confirmation Date],[Purchaser Code]
        FROM [dbo].[IPG Medical Corporation$Purchase History Header]
        WHERE [Order Date] > '2018-12-31'
          AND [Document Type] = 1
          AND [Buy-from Vendor No_] <> ''

        UNION

        SELECT DISTINCT [Document Type],[No_],[Order Date],[Posting Date],
               [Order Confirmation Date],[Purchaser Code]
        FROM [dbo].[IPG Medical Corporation$Purchase Header]
        WHERE [Order Date] > '2018-12-31'
          AND [Document Type] = 1
          AND [Buy-from Vendor No_] <> ''
    ) h
)
SELECT
    [Document Type],
    [No_]                    AS [Doc_No_],
    [Order Date],
    [Posting Date],
    [Order Confirmation Date],
    [Purchaser Code],
    'US020'                  AS [Subsidiary]
INTO #HeaderData
FROM HeaderCTE
WHERE rn = 1;

CREATE UNIQUE CLUSTERED INDEX IX_HeaderData_Doc
        ON #HeaderData ([Doc_No_]);

/* ============================================================ 3. #Receipts  */
IF OBJECT_ID('tempdb..#Receipts') IS NOT NULL DROP TABLE #Receipts;

SELECT
    [Line No_]  AS line_no,
    [Order No_] AS order_no,
    [No_]       AS item_no,
    MIN([Posting Date]) AS posting_date
INTO #Receipts
FROM [dbo].[IPG Medical Corporation$Purch_ Rcpt_ Line]
WHERE [Quantity] > 0
  AND [Posting Date] > '2017-12-31'
GROUP BY [Line No_], [Order No_], [No_];

CREATE UNIQUE CLUSTERED INDEX IX_Receipts_OrderLineItem
        ON #Receipts (order_no, line_no, item_no);

/* ============================================================ 4. Final SELECT */
SELECT
    /* ---------- Order date FIRST --------------------------- */
    h.[Order Date]                     AS order_date,

    /* ---------- IDs & dimensions --------------------------- */
    l.[Status]                         AS status,
    l.[Document Type]                  AS document_type,
    l.[Document No_]                   AS document_no,
    l.[Line No_]                       AS line_no,
    l.[Buy-from Vendor No_]            AS buy_from_vendor_no,
    v.[Name]                           AS vendor_name,
    CASE WHEN v.[Country_Region Code]='HK' THEN 'CN'
         ELSE v.[Country_Region Code] END                      AS vendor_country,
    v.[Vendor Posting Group]           AS vendor_posting_group,
    l.[Type]                           AS type,
    l.[No_]                            AS item_no,
    l.[Shortcut Dimension 1 Code]      AS cost_center,
    l.[Location Code]                  AS location_code,
    l.[Currency Code]                  AS currency_code,

    /* ---------- Dates, quantity, cost ---------------------- */
    l.[Expected Receipt Date]          AS expected_receipt_date,
    l.[Promised Receipt Date]          AS promised_receipt_date,
    r.posting_date                     AS posting_date,
    l.[Qty_ per Unit of Measure]       AS qty_per_unit_of_measure,
    l.[Quantity]                       AS quantity,
    l.[Outstanding Quantity]           AS outstanding_quantity,
    l.[Unit Cost]                      AS unit_cost,

    /* ---------- UOM sanity flag ---------------------------- */
    CASE
        WHEN l.[Qty_ per Unit of Measure] IS NULL               THEN 'NULL'
        WHEN l.[Qty_ per Unit of Measure] <= 0                  THEN 'BAD'
        WHEN l.[Qty_ per Unit of Measure] BETWEEN 0.01 AND 100  THEN 'OK'
        ELSE 'CHECK'
    END                                                         AS uom_sanity_flag,

    /* ---------- Raw baselines (1-year / 2-year) ------------ */
    ISNULL(b1y.avg_price, l.[Unit Cost])                        AS avg_price_1y,
    ISNULL(b2y.avg_price, l.[Unit Cost])                        AS avg_price_2y,
    ISNULL(b1y_v.avg_price_vendor, l.[Unit Cost])               AS avg_price_1y_vendor,
    ISNULL(b2y_v.avg_price_vendor, l.[Unit Cost])               AS avg_price_2y_vendor,

    /* ---------- Adaptive baselines ------------------------- */
    bl.baseline_unit_cost,
    bl.baseline_unit_cost_vendor,

    /* ---------- Variance & savings KPIs -------------------- */
    (l.[Unit Cost] - bl.baseline_unit_cost)
        / NULLIF(bl.baseline_unit_cost, l.[Unit Cost])          AS price_var_pct,
    CASE
        WHEN l.[Unit Cost] < bl.baseline_unit_cost
             THEN (bl.baseline_unit_cost - l.[Unit Cost]) * l.[Quantity]
             ELSE 0
    END                                                         AS savings_value,

    (l.[Unit Cost] - bl.baseline_unit_cost_vendor)
        / NULLIF(bl.baseline_unit_cost_vendor, l.[Unit Cost])   AS price_var_pct_vendor,
    CASE
        WHEN l.[Unit Cost] < bl.baseline_unit_cost_vendor
             THEN (bl.baseline_unit_cost_vendor - l.[Unit Cost]) * l.[Quantity]
             ELSE 0
    END                                                         AS savings_value_vendor,

    /* ---------- Sourcing / risk flags ---------------------- */
    CASE WHEN ss.vendor_cnt = 1 THEN 'Yes' ELSE 'No' END         AS single_source_flag,
    CASE WHEN hv.po_cnt    >= @hv_po    THEN 'Yes' ELSE 'No' END AS high_volume_po_flag,
    CASE WHEN hv.spend_amt >= @hv_spend THEN 'Yes' ELSE 'No' END AS high_volume_spend_flag,

    /* ---------- Delivery metrics --------------------------- */
    DATEDIFF(day, l.[Promised Receipt Date], r.posting_date)     AS days_late_early,
    (
         DATEDIFF(day, l.[Promised Receipt Date], r.posting_date)
       - (DATEDIFF(week, l.[Promised Receipt Date], r.posting_date) * 2)
       - CASE WHEN DATENAME(weekday, l.[Promised Receipt Date]) IN ('Saturday','Sunday') THEN 1 ELSE 0 END
       - CASE WHEN DATENAME(weekday, r.posting_date)            IN ('Saturday','Sunday') THEN 1 ELSE 0 END
    )                                                           AS bus_days_late,
    CASE
        WHEN r.posting_date IS NULL THEN NULL
        WHEN (
             DATEDIFF(day, l.[Promised Receipt Date], r.posting_date)
           - (DATEDIFF(week, l.[Promised Receipt Date], r.posting_date) * 2)
           - CASE WHEN DATENAME(weekday, l.[Promised Receipt Date]) IN ('Saturday','Sunday') THEN 1 ELSE 0 END
           - CASE WHEN DATENAME(weekday, r.posting_date)            IN ('Saturday','Sunday') THEN 1 ELSE 0 END
        ) > @on_time_days THEN 0 ELSE 1
    END                                                          AS supplier_on_time_flag,
    CASE
        WHEN r.posting_date IS NULL THEN NULL
        WHEN (
             DATEDIFF(day, l.[Expected Receipt Date], r.posting_date)
           - (DATEDIFF(week, l.[Expected Receipt Date], r.posting_date) * 2)
           - CASE WHEN DATENAME(weekday, l.[Expected Receipt Date]) IN ('Saturday','Sunday') THEN 1 ELSE 0 END
           - CASE WHEN DATENAME(weekday, r.posting_date)            IN ('Saturday','Sunday') THEN 1 ELSE 0 END
        ) > @on_time_days THEN 0 ELSE 1
    END                                                          AS buyer_on_time_flag,

    /* ---------- Purchase-history intelligence -------------- */
    ISNULL(last.last_unit_cost, l.[Unit Cost])                   AS last_unit_cost,
    CASE WHEN l.[Type]='Item' AND last.last_unit_cost IS NULL
         THEN 'Yes' ELSE 'No' END                                AS first_purchase,
    CASE
        WHEN l.[Type]<>'Item' OR last.last_unit_cost IS NULL THEN 'No'
        WHEN last.last_vendor_country <>
             (CASE WHEN v.[Country_Region Code]='HK' THEN 'CN'
                   ELSE v.[Country_Region Code] END)
             THEN 'Yes' ELSE 'No'
    END                                                          AS country_change,
    CASE
        WHEN l.[Type]<>'Item' OR last.last_unit_cost IS NULL THEN 'No'
        WHEN last.last_vendor_country='CN'                       THEN 'Yes' ELSE 'No'
    END                                                          AS china_change,

    /* ---------- Misc original columns ---------------------- */
    l.[Description],
    l.[Requested Receipt Date],
    l.[Total],
    l.[Planned Receipt Date],
    l.[Quantity Delivered],
    DATEDIFF(day, h.[Order Date], l.[Promised Receipt Date])     AS promised_lead_time_days,
    DATEDIFF(day, h.[Order Date], r.posting_date)                AS actual_lead_time_days,
    h.[Order Confirmation Date]        AS order_confirmation_date,
    h.[Purchaser Code]                 AS purchaser_code,
    l.[Subsidiary]                     AS subsidiary,

    /* ---------- item_index --------------------------------- */
    l.[Subsidiary] + l.[No_]           AS item_index,
    l.[Subsidiary] + l.[Buy-from Vendor No_] AS vendor_index
FROM #LineData  AS l
JOIN #HeaderData AS h
      ON l.[Document No_] = h.[Doc_No_]

/* ---------- Baseline & KPI helper OUTER APPLY blocks ------- */
OUTER APPLY (
    SELECT SUM(ld.quantity * ld.[Unit Cost]) /
           NULLIF(SUM(ld.quantity),0) AS avg_price
    FROM   #LineData  ld
    JOIN   #HeaderData hd ON ld.[Document No_] = hd.[Doc_No_]
    WHERE  ld.[Type]='Item'
      AND  ld.[No_]  = l.[No_]
      AND  hd.[Order Date] BETWEEN DATEADD(day,-@w1y,h.[Order Date]) AND h.[Order Date]-1
) AS b1y

OUTER APPLY (
    SELECT SUM(ld.quantity * ld.[Unit Cost]) /
           NULLIF(SUM(ld.quantity),0) AS avg_price
    FROM   #LineData  ld
    JOIN   #HeaderData hd ON ld.[Document No_] = hd.[Doc_No_]
    WHERE  ld.[Type]='Item'
      AND  ld.[No_]  = l.[No_]
      AND  hd.[Order Date] BETWEEN DATEADD(day,-@w2y,h.[Order Date]) AND h.[Order Date]-1
) AS b2y

OUTER APPLY (
    SELECT SUM(ld.quantity * ld.[Unit Cost]) /
           NULLIF(SUM(ld.quantity),0) AS avg_price_vendor
    FROM   #LineData  ld
    JOIN   #HeaderData hd ON ld.[Document No_] = hd.[Doc_No_]
    WHERE  ld.[Type]='Item'
      AND  ld.[No_]  = l.[No_]
      AND  ld.[Buy-from Vendor No_] = l.[Buy-from Vendor No_]
      AND  hd.[Order Date] BETWEEN DATEADD(day,-@w1y,h.[Order Date]) AND h.[Order Date]-1
) AS b1y_v

OUTER APPLY (
    SELECT SUM(ld.quantity * ld.[Unit Cost]) /
           NULLIF(SUM(ld.quantity),0) AS avg_price_vendor
    FROM   #LineData  ld
    JOIN   #HeaderData hd ON ld.[Document No_] = hd.[Doc_No_]
    WHERE  ld.[Type]='Item'
      AND  ld.[No_]  = l.[No_]
      AND  ld.[Buy-from Vendor No_] = l.[Buy-from Vendor No_]
      AND  hd.[Order Date] BETWEEN DATEADD(day,-@w2y,h.[Order Date]) AND h.[Order Date]-1
) AS b2y_v

OUTER APPLY (
    SELECT TOP 1
           ld.[Unit Cost] AS last_unit_cost,
           CASE WHEN v_prev.[Country_Region Code]='HK' THEN 'CN'
                ELSE v_prev.[Country_Region Code] END AS last_vendor_country
    FROM   #LineData  ld
    JOIN   #HeaderData hd ON ld.[Document No_] = hd.[Doc_No_]
    LEFT JOIN [dbo].[IPG Medical Corporation$Vendor] v_prev
           ON ld.[Buy-from Vendor No_] = v_prev.[No_]
    WHERE  ld.[Type]='Item'
      AND  ld.[No_]  = l.[No_]
      AND  hd.[Order Date] < h.[Order Date]
    ORDER BY hd.[Order Date] DESC,
             ld.[Document No_] DESC,
             ld.[Line No_] DESC
) AS last

OUTER APPLY (
    SELECT TOP 1 ld.[Unit Cost] AS last_unit_cost_vendor
    FROM   #LineData  ld
    JOIN   #HeaderData hd ON ld.[Document No_] = hd.[Doc_No_]
    WHERE  ld.[Type]='Item'
      AND  ld.[No_]  = l.[No_]
      AND  ld.[Buy-from Vendor No_] = l.[Buy-from Vendor No_]
      AND  hd.[Order Date] < h.[Order Date]
    ORDER BY hd.[Order Date] DESC,
             ld.[Document No_] DESC,
             ld.[Line No_] DESC
) AS last_v

OUTER APPLY (
    SELECT COUNT(DISTINCT ld.[Buy-from Vendor No_]) AS vendor_cnt
    FROM   #LineData ld
    JOIN   #HeaderData hd2 ON ld.[Document No_] = hd2.[Doc_No_]
    WHERE  ld.[Type]='Item'
      AND  ld.[No_] = l.[No_]
      AND  hd2.[Order Date] <= h.[Order Date]
) AS ss

OUTER APPLY (
    SELECT COUNT(DISTINCT hd3.[Doc_No_])                AS po_cnt,
           SUM(ld3.quantity * ld3.[Unit Cost])          AS spend_amt
    FROM   #LineData ld3
    JOIN   #HeaderData hd3 ON ld3.[Document No_] = hd3.[Doc_No_]
    WHERE  ld3.[Type]='Item'
      AND  ld3.[No_] = l.[No_]
      AND  hd3.[Order Date] BETWEEN DATEADD(day,-@hv_window,h.[Order Date]) AND h.[Order Date]-1
) AS hv

OUTER APPLY (
    SELECT
        CASE
            WHEN b1y.avg_price       IS NOT NULL THEN b1y.avg_price
            WHEN b2y.avg_price       IS NOT NULL THEN b2y.avg_price
            WHEN last.last_unit_cost IS NOT NULL THEN last.last_unit_cost
            ELSE l.[Unit Cost]
        END AS baseline_unit_cost,

        CASE
            WHEN b1y_v.avg_price_vendor      IS NOT NULL THEN b1y_v.avg_price_vendor
            WHEN b2y_v.avg_price_vendor      IS NOT NULL THEN b2y_v.avg_price_vendor
            WHEN last_v.last_unit_cost_vendor IS NOT NULL THEN last_v.last_unit_cost_vendor
            ELSE
                CASE
                    WHEN b1y.avg_price       IS NOT NULL THEN b1y.avg_price
                    WHEN b2y.avg_price       IS NOT NULL THEN b2y.avg_price
                    WHEN last.last_unit_cost IS NOT NULL THEN last.last_unit_cost
                    ELSE l.[Unit Cost]
                END
        END AS baseline_unit_cost_vendor
) AS bl

LEFT JOIN #Receipts AS r
       ON l.[Document No_] = r.order_no
      AND l.[Line No_]     = r.line_no
      AND l.[No_]          = r.item_no
LEFT JOIN [dbo].[IPG Medical Corporation$Vendor] AS v
       ON l.[Buy-from Vendor No_] = v.[No_];
