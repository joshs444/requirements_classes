/*************************************************************************************************
  Procurement Analytics –  DE010  (IPG Laser GmbH)
  v8.3-DE-5  · adds item_index and fixes collation + OpenLines alias
  • identical business logic to US010; no Manufacturer fields, no Order-Confirmation-Date
*************************************************************************************************/

/* ============================================================ 0. Parameters */
DECLARE
    @w1y       INT   = 365,      -- 1-year look-back
    @w2y       INT   = 730,      -- 2-year look-back
    @hv_po     INT   = 12,       -- high-volume PO threshold (count)
    @hv_spend  MONEY = 100000,   -- high-volume spend threshold ($)
    @hv_window INT   = 365;      -- window (days) for hv flags
SET NOCOUNT ON;

/* ============================================================ 1. #LineData  (history + open) */
IF OBJECT_ID('tempdb..#LineData') IS NOT NULL DROP TABLE #LineData;

;WITH
LatestArchive AS (   -- newest version of each archived line
    SELECT *
    FROM (
        SELECT  l.*,
                ROW_NUMBER() OVER (PARTITION BY l.[Document No_],l.[Line No_]
                                   ORDER BY l.[Version No_] DESC) AS rn
        FROM    [dbo].[IPG Laser GmbH$Purchase Line Archive] l
        WHERE   l.[Order Date]      > '2019-01-01'
          AND   l.[Document Type]   = 1
          AND   l.[Type]           IN (1,2,4)
          AND   l.[Quantity]        > 0
          AND   l.[Unit Cost (LCY)] > 0
    ) a
    WHERE rn = 1
),
HistoryLines AS (
    SELECT
        'HISTORY'                                         AS [Status],
        l.[Document Type],  l.[Document No_], l.[Line No_],
        l.[Shortcut Dimension 1 Code], l.[Buy-from Vendor No_],
        CASE l.[Type] WHEN 1 THEN 'GL'
                      WHEN 2 THEN 'Item'
                      WHEN 4 THEN 'FA' END                AS [Type],
        l.[No_], l.[Location Code],
        l.[Expected Receipt Date], l.[Promised Receipt Date],
        l.[Planned Receipt Date],  l.[Description],
        COALESCE(NULLIF(l.[Qty_ per Unit of Measure],0),1) AS qty_factor,
        l.[Quantity]             AS OrigQuantity,
        l.[Outstanding Quantity] AS OrigOutstandingQty,
        l.[Unit Cost (LCY)]      AS OrigUnitCost,
        l.[Requested Receipt Date]
    FROM LatestArchive l
    WHERE l.[Quantity] - l.[Outstanding Quantity] <> 0
),
OpenLines AS (        -- ← alias added on first column
    SELECT
        'OPEN'                                           AS [Status],
        pl.[Document Type], pl.[Document No_], pl.[Line No_],
        pl.[Shortcut Dimension 1 Code], pl.[Buy-from Vendor No_],
        CASE pl.[Type] WHEN 1 THEN 'GL'
                       WHEN 2 THEN 'Item'
                       WHEN 4 THEN 'FA' END               AS [Type],
        pl.[No_], pl.[Location Code],
        pl.[Expected Receipt Date], pl.[Promised Receipt Date],
        pl.[Planned Receipt Date],  pl.[Description],
        COALESCE(NULLIF(pl.[Qty_ per Unit of Measure],0),1) AS qty_factor,
        pl.[Quantity]             AS OrigQuantity,
        pl.[Outstanding Quantity] AS OrigOutstandingQty,
        pl.[Unit Cost (LCY)]      AS OrigUnitCost,
        pl.[Requested Receipt Date]
    FROM [dbo].[IPG Laser GmbH$Purchase Line] pl
    WHERE pl.[Order Date] > '2019-01-01'
      AND pl.[Document Type] = 1
      AND pl.[Type] IN (1,2,4)
      AND pl.[Quantity] > 0
      AND pl.[Unit Cost (LCY)] > 0
)
SELECT
    s.[Status], s.[Document Type], s.[Document No_], s.[Line No_],
    s.[Shortcut Dimension 1 Code], s.[Buy-from Vendor No_], s.[Type], s.[No_],
    s.[Location Code], s.[Expected Receipt Date], s.[Promised Receipt Date],
    s.[Planned Receipt Date], s.[Description],
    s.qty_factor                               AS [Qty_ per Unit of Measure],
    s.OrigQuantity      * s.qty_factor         AS [Quantity],
    s.OrigOutstandingQty* s.qty_factor         AS [Outstanding Quantity],
    s.OrigUnitCost      / s.qty_factor         AS [Unit Cost],
    s.[Requested Receipt Date],
    (s.OrigQuantity - s.OrigOutstandingQty)
        * (s.OrigUnitCost / s.qty_factor)      AS [Total],
    (s.OrigQuantity - s.OrigOutstandingQty)
        * s.qty_factor                         AS [Quantity Delivered],
    'DE010'                                    AS [Subsidiary]
INTO #LineData
FROM (SELECT * FROM HistoryLines UNION ALL SELECT * FROM OpenLines) s;

CREATE CLUSTERED INDEX IX_LineData_DocLineItem
    ON #LineData([Document No_], [Line No_], [No_]);

/* ============================================================ 2. #HeaderData (deduplicated) */
IF OBJECT_ID('tempdb..#HeaderData') IS NOT NULL DROP TABLE #HeaderData;

;WITH AllHeaders AS (
    SELECT  h.[Document Type], h.[No_], h.[Order Date], h.[Posting Date],
            h.[Purchaser Code], h.[Buy-from Vendor No_]
    FROM    [dbo].[IPG Laser GmbH$Purchase Header] h
    WHERE   h.[Order Date] > '2018-12-31'
      AND   h.[Document Type] = 1
      AND   h.[Buy-from Vendor No_] <> ''
    UNION ALL
    SELECT  ah.[Document Type], ah.[No_], ah.[Order Date], ah.[Posting Date],
            ah.[Purchaser Code], ah.[Buy-from Vendor No_]
    FROM    [dbo].[IPG Laser GmbH$Purchase Header Archive] ah
    WHERE   ah.[Order Date] > '2018-12-31'
      AND   ah.[Document Type] = 1
      AND   ah.[Buy-from Vendor No_] <> ''
),
LatestHeader AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY [No_] ORDER BY [Posting Date] DESC) AS rn
    FROM   AllHeaders
)
SELECT
    h.[Document Type],
    h.[No_]                      AS [Doc_No_],
    h.[Order Date],
    h.[Posting Date],
    v.[Strategic Purchaser Code] AS [Assigned User ID],
    h.[Purchaser Code],
    'DE010'                      AS [Subsidiary]
INTO #HeaderData
FROM   LatestHeader h
LEFT   JOIN [dbo].[IPG Laser GmbH$Vendor] v
       ON h.[Buy-from Vendor No_] = v.[No_]
WHERE  h.rn = 1;

CREATE UNIQUE CLUSTERED INDEX IX_HeaderData_Doc
    ON #HeaderData([Doc_No_]);

/* ============================================================ 3. #Receipts */
IF OBJECT_ID('tempdb..#Receipts') IS NOT NULL DROP TABLE #Receipts;

SELECT
    [Line No_]  AS line_no,
    [Order No_] AS order_no,
    [No_]       AS item_no,
    MIN([Posting Date]) AS posting_date
INTO #Receipts
FROM [dbo].[IPG Laser GmbH$Purch_ Rcpt_ Line]
WHERE [Quantity] > 0
  AND [Posting Date] > '2017-12-31'
GROUP BY [Line No_], [Order No_], [No_];

CREATE UNIQUE CLUSTERED INDEX IX_Receipts_OrderLineItem
    ON #Receipts(order_no, line_no, item_no);

/* ============================================================ 4. Final SELECT – KPIs, baselines, flags */
SELECT
    h.[Order Date]                           AS order_date,
    l.[Status], l.[Document Type], l.[Document No_], l.[Line No_],
    l.[Buy-from Vendor No_], v.[Name]        AS vendor_name,
    CASE WHEN v.[Country_Region Code]='HK' THEN 'CN' ELSE v.[Country_Region Code] END AS vendor_country,
    v.[Vendor Posting Group],
    l.[Type], l.[No_]                        AS item_no,
    l.[Shortcut Dimension 1 Code]            AS cost_center,
    l.[Location Code],
    l.[Expected Receipt Date], l.[Promised Receipt Date], r.posting_date,
    l.[Qty_ per Unit of Measure], l.[Quantity], l.[Outstanding Quantity], l.[Unit Cost],

    CASE WHEN l.[Qty_ per Unit of Measure] IS NULL               THEN 'NULL'
         WHEN l.[Qty_ per Unit of Measure] <= 0                  THEN 'BAD'
         WHEN l.[Qty_ per Unit of Measure] BETWEEN 0.01 AND 100  THEN 'OK'
         ELSE 'CHECK' END                   AS uom_sanity_flag,

    ISNULL(b1y.avg_price, l.[Unit Cost])            AS avg_price_1y,
    ISNULL(b2y.avg_price, l.[Unit Cost])            AS avg_price_2y,
    ISNULL(b1y_v.avg_price_vendor, l.[Unit Cost])   AS avg_price_1y_vendor,
    ISNULL(b2y_v.avg_price_vendor, l.[Unit Cost])   AS avg_price_2y_vendor,

    bl.baseline_unit_cost, bl.baseline_unit_cost_vendor,

    (l.[Unit Cost] - bl.baseline_unit_cost)
         / NULLIF(bl.baseline_unit_cost, l.[Unit Cost])          AS price_var_pct,
    CASE WHEN l.[Unit Cost] < bl.baseline_unit_cost
         THEN (bl.baseline_unit_cost - l.[Unit Cost]) * l.[Quantity] ELSE 0 END AS savings_value,

    (l.[Unit Cost] - bl.baseline_unit_cost_vendor)
         / NULLIF(bl.baseline_unit_cost_vendor, l.[Unit Cost])   AS price_var_pct_vendor,
    CASE WHEN l.[Unit Cost] < bl.baseline_unit_cost_vendor
         THEN (bl.baseline_unit_cost_vendor - l.[Unit Cost]) * l.[Quantity] ELSE 0 END AS savings_value_vendor,

    CASE WHEN ss.vendor_cnt = 1 THEN 'Yes' ELSE 'No' END          AS single_source_flag,
    CASE WHEN hv.po_cnt    >= @hv_po    THEN 'Yes' ELSE 'No' END  AS high_volume_po_flag,
    CASE WHEN hv.spend_amt >= @hv_spend THEN 'Yes' ELSE 'No' END  AS high_volume_spend_flag,

    DATEDIFF(day, l.[Promised Receipt Date], r.posting_date)      AS days_late_early,
    (
         DATEDIFF(day, l.[Promised Receipt Date], r.posting_date)
       - DATEDIFF(week,l.[Promised Receipt Date], r.posting_date)*2
       - CASE WHEN DATENAME(weekday,l.[Promised Receipt Date]) IN ('Saturday','Sunday') THEN 1 ELSE 0 END
       - CASE WHEN DATENAME(weekday,r.posting_date)            IN ('Saturday','Sunday') THEN 1 ELSE 0 END
    )                                                             AS bus_days_late,
    CASE WHEN r.posting_date IS NULL THEN NULL
         WHEN (
             DATEDIFF(day, l.[Promised Receipt Date], r.posting_date)
           - DATEDIFF(week,l.[Promised Receipt Date], r.posting_date)*2
           - CASE WHEN DATENAME(weekday,l.[Promised Receipt Date]) IN ('Saturday','Sunday') THEN 1 ELSE 0 END
           - CASE WHEN DATENAME(weekday,r.posting_date)            IN ('Saturday','Sunday') THEN 1 ELSE 0 END
         ) > 3 THEN 0 ELSE 1 END                                  AS on_time_flag,

    ISNULL(last.last_unit_cost, l.[Unit Cost])                    AS last_unit_cost,
    CASE WHEN l.[Type]='Item' AND last.last_unit_cost IS NULL THEN 'Yes' ELSE 'No' END AS first_purchase,
    CASE WHEN l.[Type]<>'Item' OR last.last_unit_cost IS NULL THEN 'No'
         WHEN last.last_vendor_country <>
              CASE WHEN v.[Country_Region Code]='HK' THEN 'CN' ELSE v.[Country_Region Code] END
              THEN 'Yes' ELSE 'No' END                            AS country_change,
    CASE WHEN l.[Type]<>'Item' OR last.last_unit_cost IS NULL THEN 'No'
         WHEN last.last_vendor_country='CN' THEN 'Yes' ELSE 'No' END AS china_change,

    l.[Description], l.[Requested Receipt Date], l.[Total],
    l.[Planned Receipt Date], l.[Quantity Delivered],
    DATEDIFF(day, h.[Order Date], l.[Promised Receipt Date])      AS promised_lead_time_days,
    DATEDIFF(day, h.[Order Date], r.posting_date)                 AS actual_lead_time_days,
    h.[Assigned User ID]                                          AS assigned_user_id,
    h.[Purchaser Code]                                            AS purchaser_code,
    l.[Subsidiary]                                                AS subsidiary,

    /* composite key with explicit collation on both operands */
    CONCAT(
        l.[Subsidiary] COLLATE Latin1_General_100_CI_AS,
        l.[No_]        COLLATE Latin1_General_100_CI_AS
    )                                                             AS item_index,
    /* composite vendor key with explicit collation */
    CONCAT(
        l.[Subsidiary] COLLATE Latin1_General_100_CI_AS,
        l.[Buy-from Vendor No_] COLLATE Latin1_General_100_CI_AS
    )                                                             AS vendor_index

FROM #LineData l
JOIN #HeaderData h
      ON l.[Document No_] = h.[Doc_No_]

/* ---------- 1-year baseline (item) ---------- */
OUTER APPLY (
    SELECT SUM(ld.Quantity*ld.[Unit Cost]) /
           NULLIF(SUM(ld.Quantity),0) AS avg_price
    FROM   #LineData ld
    JOIN   #HeaderData hd
           ON ld.[Document No_] = hd.[Doc_No_]
    WHERE  ld.[Type]='Item'
      AND  ld.[No_] = l.[No_]
      AND  hd.[Order Date] BETWEEN DATEADD(day,-@w1y,h.[Order Date])
                               AND     h.[Order Date]-1
) b1y
/* ---------- 2-year baseline (item) ---------- */
OUTER APPLY (
    SELECT SUM(ld.Quantity*ld.[Unit Cost]) /
           NULLIF(SUM(ld.Quantity),0) AS avg_price
    FROM   #LineData ld
    JOIN   #HeaderData hd
           ON ld.[Document No_] = hd.[Doc_No_]
    WHERE  ld.[Type]='Item'
      AND  ld.[No_] = l.[No_]
      AND  hd.[Order Date] BETWEEN DATEADD(day,-@w2y,h.[Order Date])
                               AND     h.[Order Date]-1
) b2y
/* ---------- 1-year baseline (item+vendor) ---------- */
OUTER APPLY (
    SELECT SUM(ld.Quantity*ld.[Unit Cost]) /
           NULLIF(SUM(ld.Quantity),0) AS avg_price_vendor
    FROM   #LineData ld
    JOIN   #HeaderData hd
           ON ld.[Document No_] = hd.[Doc_No_]
    WHERE  ld.[Type]='Item'
      AND  ld.[No_] = l.[No_]
      AND  ld.[Buy-from Vendor No_] = l.[Buy-from Vendor No_]
      AND  hd.[Order Date] BETWEEN DATEADD(day,-@w1y,h.[Order Date])
                               AND     h.[Order Date]-1
) b1y_v
/* ---------- 2-year baseline (item+vendor) ---------- */
OUTER APPLY (
    SELECT SUM(ld.Quantity*ld.[Unit Cost]) /
           NULLIF(SUM(ld.Quantity),0) AS avg_price_vendor
    FROM   #LineData ld
    JOIN   #HeaderData hd
           ON ld.[Document No_] = hd.[Doc_No_]
    WHERE  ld.[Type]='Item'
      AND  ld.[No_] = l.[No_]
      AND  ld.[Buy-from Vendor No_] = l.[Buy-from Vendor No_]
      AND  hd.[Order Date] BETWEEN DATEADD(day,-@w2y,h.[Order Date])
                               AND     h.[Order Date]-1
) b2y_v
/* ---------- most-recent prior purchase (any vendor) ---------- */
OUTER APPLY (
    SELECT TOP 1
           ld.[Unit Cost] AS last_unit_cost,
           CASE WHEN v_prev.[Country_Region Code]='HK' THEN 'CN'
                ELSE v_prev.[Country_Region Code] END AS last_vendor_country
    FROM   #LineData ld
    JOIN   #HeaderData hd
           ON ld.[Document No_] = hd.[Doc_No_]
    LEFT   JOIN [dbo].[IPG Laser GmbH$Vendor] v_prev
           ON ld.[Buy-from Vendor No_] = v_prev.[No_]
    WHERE  ld.[Type]='Item'
      AND  ld.[No_] = l.[No_]
      AND  hd.[Order Date] < h.[Order Date]
    ORDER BY hd.[Order Date] DESC,
             ld.[Document No_] DESC,
             ld.[Line No_] DESC
) last
/* ---------- most-recent prior purchase (same vendor) ---------- */
OUTER APPLY (
    SELECT TOP 1 ld.[Unit Cost] AS last_unit_cost_vendor
    FROM   #LineData ld
    JOIN   #HeaderData hd
           ON ld.[Document No_] = hd.[Doc_No_]
    WHERE  ld.[Type]='Item'
      AND  ld.[No_] = l.[No_]
      AND  ld.[Buy-from Vendor No_] = l.[Buy-from Vendor No_]
      AND  hd.[Order Date] < h.[Order Date]
    ORDER BY hd.[Order Date] DESC,
             ld.[Document No_] DESC,
             ld.[Line No_] DESC
) last_v
/* ---------- single-source count ---------- */
OUTER APPLY (
    SELECT COUNT(DISTINCT ld.[Buy-from Vendor No_]) AS vendor_cnt
    FROM   #LineData ld
    JOIN   #HeaderData hd2
           ON ld.[Document No_] = hd2.[Doc_No_]
    WHERE  ld.[Type]='Item'
      AND  ld.[No_] = l.[No_]
      AND  hd2.[Order Date] <= h.[Order Date]
) ss
/* ---------- high-volume PO & spend ---------- */
OUTER APPLY (
    SELECT COUNT(DISTINCT hd3.[Doc_No_])      AS po_cnt,
           SUM(ld3.Quantity*ld3.[Unit Cost])  AS spend_amt
    FROM   #LineData ld3
    JOIN   #HeaderData hd3
           ON ld3.[Document No_] = hd3.[Doc_No_]
    WHERE  ld3.[Type]='Item'
      AND  ld3.[No_] = l.[No_]
      AND  hd3.[Order Date] BETWEEN DATEADD(day,-@hv_window,h.[Order Date])
                                AND     h.[Order Date]-1
) hv
/* ---------- adaptive baselines ---------- */
OUTER APPLY (
    SELECT
        CASE
            WHEN b1y.avg_price       IS NOT NULL THEN b1y.avg_price
            WHEN b2y.avg_price       IS NOT NULL THEN b2y.avg_price
            WHEN last.last_unit_cost IS NOT NULL THEN last.last_unit_cost
            ELSE l.[Unit Cost]
        END                                             AS baseline_unit_cost,
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
        END                                             AS baseline_unit_cost_vendor
) bl

LEFT JOIN #Receipts r
       ON l.[Document No_] = r.order_no
      AND l.[Line No_]     = r.line_no
      AND l.[No_]          = r.item_no

LEFT JOIN [dbo].[IPG Laser GmbH$Vendor] v
       ON l.[Buy-from Vendor No_] = v.[No_];
