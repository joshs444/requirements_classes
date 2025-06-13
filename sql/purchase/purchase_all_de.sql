/*************************************************************************************************
  Procurement Analytics – DE010 (IPG Laser GmbH)  
  v8.3-DE-10  (May 2025)   · column aliases normalised to lower_snake_case
*************************************************************************************************/
SET NOCOUNT ON;

/* ============================================================ 0. Parameters */
DECLARE
    @w1y       INT   = 365 ,
    @w2y       INT   = 730 ,
    @hv_po     INT   = 12  ,
    @hv_spend  MONEY = 100000,
    @hv_window INT   = 365 ;

/* ============================================================ 1.  #LineData  (history + open) */
IF OBJECT_ID('tempdb..#LineData') IS NOT NULL DROP TABLE #LineData;

;WITH
LatestArchive AS (
    SELECT *
    FROM (
        SELECT  l.*,
                ROW_NUMBER() OVER (PARTITION BY l.[Document No_], l.[Line No_]
                                   ORDER BY l.[Version No_] DESC) AS rn
        FROM    [dbo].[IPG Laser GmbH$Purchase Line Archive] l
        WHERE   l.[Document Type] = 1
          AND   l.[Type]        IN (1,2,4,5)
          AND   l.[Quantity]      > 0
          AND   l.[Order Date]    > '2019-12-31'
    ) x WHERE rn = 1
),
HistoryLines AS (
    SELECT
        'HISTORY'                                         AS status,
        l.[Document Type]                                 AS document_type,
        l.[Document No_]                                  AS document_no,
        l.[Line No_]                                      AS line_no,
        l.[Shortcut Dimension 1 Code]                     AS cost_center,
        l.[Buy-from Vendor No_]                           AS buy_from_vendor_no,
        l.[Type]                                          AS type_numeric,
        l.[No_]                                           AS item_no,
        l.[Location Code]                                 AS location_code,
        l.[Requested Receipt Date]                        AS expected_receipt_date,
        l.[Promised Receipt Date]                         AS promised_receipt_date,
        l.[Planned Receipt Date]                          AS planned_receipt_date,
        l.[Description]                                   AS description,
        COALESCE(NULLIF(l.[Currency Code],''),'EUR')      AS currency_code,
        COALESCE(NULLIF(l.[Qty_ per Unit of Measure],0),1) AS qty_factor,
        l.[Quantity]               AS orig_quantity,
        l.[Outstanding Quantity]   AS orig_outstanding_qty,
        l.[Unit Cost (LCY)]        AS orig_unit_cost,
        l.[Requested Receipt Date] AS requested_receipt_date
    FROM LatestArchive l
),
OpenLines AS (
    SELECT
        'OPEN'                                            AS status,
        pl.[Document Type]                                AS document_type,
        pl.[Document No_]                                 AS document_no,
        pl.[Line No_]                                     AS line_no,
        pl.[Shortcut Dimension 1 Code]                    AS cost_center,
        pl.[Buy-from Vendor No_]                          AS buy_from_vendor_no,
        pl.[Type]                                         AS type_numeric,
        pl.[No_]                                          AS item_no,
        pl.[Location Code]                                AS location_code,
        pl.[Expected Receipt Date]                        AS expected_receipt_date,
        pl.[Promised Receipt Date]                        AS promised_receipt_date,
        pl.[Planned Receipt Date]                         AS planned_receipt_date,
        pl.[Description]                                  AS description,
        COALESCE(NULLIF(pl.[Currency Code],''),'EUR')      AS currency_code,
        COALESCE(NULLIF(pl.[Qty_ per Unit of Measure],0),1) AS qty_factor,
        pl.[Quantity]               AS orig_quantity,
        pl.[Outstanding Quantity]   AS orig_outstanding_qty,
        pl.[Unit Cost (LCY)]        AS orig_unit_cost,
        pl.[Requested Receipt Date] AS requested_receipt_date
    FROM [dbo].[IPG Laser GmbH$Purchase Line] pl
    WHERE pl.[Document Type] = 1
      AND pl.[Type]        IN (1,2,4,5)
      AND pl.[Quantity]      > 0
)
SELECT
    s.status,
    s.document_type,
    s.document_no,
    s.line_no,
    s.cost_center,
    s.buy_from_vendor_no,
    s.type_numeric,
    s.item_no,
    s.location_code,
    s.expected_receipt_date,
    s.promised_receipt_date,
    s.planned_receipt_date,
    s.description,
    s.currency_code,
    s.qty_factor                              AS qty_per_unit_of_measure,
    s.orig_quantity      * s.qty_factor       AS quantity,
    s.orig_outstanding_qty* s.qty_factor      AS outstanding_quantity,
    s.orig_unit_cost      / s.qty_factor      AS unit_cost,
    s.requested_receipt_date,
    CAST(NULL AS nvarchar(50))                AS manufacturer_part_no,
    CAST(NULL AS nvarchar(50))                AS manufacturer_code,
    (s.orig_quantity - s.orig_outstanding_qty)
        * (s.orig_unit_cost / s.qty_factor)   AS total,
    (s.orig_quantity - s.orig_outstanding_qty)
        * s.qty_factor                        AS quantity_delivered,
    'DE010'                                   AS subsidiary
INTO #LineData
FROM (SELECT * FROM HistoryLines UNION ALL SELECT * FROM OpenLines) s;

CREATE CLUSTERED INDEX IX_LineData ON #LineData(document_no,line_no,item_no);

/* ============================================================ 2.  #HeaderData  */
IF OBJECT_ID('tempdb..#HeaderData') IS NOT NULL DROP TABLE #HeaderData;

;WITH
AllHeaders AS (
    SELECT  h.[Document Type], h.[No_], h.[Order Date], h.[Posting Date],
            h.[Purchaser Code], h.[Buy-from Vendor No_]
    FROM    [dbo].[IPG Laser GmbH$Purchase Header] h
    WHERE   h.[Document Type] = 1
      AND   h.[Buy-from Vendor No_] <> ''
    UNION ALL
    SELECT  ah.[Document Type], ah.[No_], ah.[Order Date], ah.[Posting Date],
            ah.[Purchaser Code], ah.[Buy-from Vendor No_]
    FROM    [dbo].[IPG Laser GmbH$Purchase Header Archive] ah
    WHERE   ah.[Document Type] = 1
      AND   ah.[Buy-from Vendor No_] <> ''
),
LatestHeader AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY [No_] ORDER BY [Posting Date] DESC) AS rn
    FROM   AllHeaders
)
SELECT
    h.[Document Type]         AS document_type,
    h.[No_]                   AS doc_no,
    h.[Order Date]            AS order_date,
    h.[Posting Date]          AS posting_date,
    v.[Strategic Purchaser Code] AS assigned_user_id,
    CAST(NULL AS date)        AS order_confirmation_date,
    h.[Purchaser Code]        AS purchaser_code,
    'DE010'                   AS subsidiary
INTO #HeaderData
FROM LatestHeader h
LEFT JOIN [dbo].[IPG Laser GmbH$Vendor] v
       ON h.[Buy-from Vendor No_] = v.[No_]
WHERE h.rn = 1;

CREATE UNIQUE CLUSTERED INDEX IX_HeaderData ON #HeaderData(doc_no);

/* ============================================================ 3.  #Receipts */
IF OBJECT_ID('tempdb..#Receipts') IS NOT NULL DROP TABLE #Receipts;

SELECT
    [Line No_]  AS line_no,
    [Order No_] AS order_no,
    [No_]       AS item_no,
    MIN([Posting Date]) AS posting_date
INTO #Receipts
FROM [dbo].[IPG Laser GmbH$Purch_ Rcpt_ Line]
WHERE [Quantity] > 0
GROUP BY [Line No_], [Order No_], [No_];

CREATE UNIQUE CLUSTERED INDEX IX_Receipts ON #Receipts(order_no,line_no,item_no);

/* ============================================================ 4.  Final SELECT */
SELECT
    /* ---------- Order date FIRST --------------------------- */
    h.order_date,

    /* ---------- IDs & dimensions --------------------------- */
    l.status,
    l.document_type,
    l.document_no,
    l.line_no,
    l.buy_from_vendor_no,
    v.[Name]                       AS vendor_name,
    CASE WHEN v.[Country_Region Code]='HK' THEN 'CN'
         ELSE v.[Country_Region Code] END   AS vendor_country,
    v.[Vendor Posting Group]        AS vendor_posting_group,

    /* map type codes */
    CASE l.type_numeric
         WHEN 1 THEN 'GL'
         WHEN 2 THEN 'Item'
         WHEN 4 THEN 'FA'
         ELSE CAST(l.type_numeric AS varchar(3))
    END                             AS type,

    l.item_no,
    l.cost_center,
    l.location_code,
    l.currency_code,

    /* ---------- Dates, quantity, cost ---------------------- */
    l.expected_receipt_date,
    l.promised_receipt_date,
    r.posting_date,
    l.qty_per_unit_of_measure,
    l.quantity,
    l.outstanding_quantity,
    l.unit_cost,

    /* ---------- UOM sanity flag ---------------------------- */
    CASE
        WHEN l.qty_per_unit_of_measure IS NULL              THEN 'null'
        WHEN l.qty_per_unit_of_measure <= 0                 THEN 'bad'
        WHEN l.qty_per_unit_of_measure BETWEEN 0.01 AND 100 THEN 'ok'
        ELSE 'check'
    END                             AS uom_sanity_flag,

    /* ---------- Raw baselines ------------------------------ */
    ISNULL(b1y.avg_price,      l.unit_cost)   AS avg_price_1y,
    ISNULL(b2y.avg_price,      l.unit_cost)   AS avg_price_2y,
    ISNULL(b1y_v.avg_price_vendor,l.unit_cost)AS avg_price_1y_vendor,
    ISNULL(b2y_v.avg_price_vendor,l.unit_cost)AS avg_price_2y_vendor,

    /* ---------- Adaptive baselines ------------------------- */
    bl.baseline_unit_cost,
    bl.baseline_unit_cost_vendor,

    /* ---------- Variance & savings ------------------------- */
    (l.unit_cost - bl.baseline_unit_cost)
        / NULLIF(bl.baseline_unit_cost,0)     AS price_var_pct,
    CASE
        WHEN bl.baseline_unit_cost = 0
             OR l.unit_cost >= bl.baseline_unit_cost
        THEN 0
        ELSE (bl.baseline_unit_cost - l.unit_cost) * l.quantity
    END                                         AS savings_value,

    (l.unit_cost - bl.baseline_unit_cost_vendor)
        / NULLIF(bl.baseline_unit_cost_vendor,0)AS price_var_pct_vendor,
    CASE
        WHEN bl.baseline_unit_cost_vendor = 0
             OR l.unit_cost >= bl.baseline_unit_cost_vendor
        THEN 0
        ELSE (bl.baseline_unit_cost_vendor - l.unit_cost) * l.quantity
    END                                         AS savings_value_vendor,

    /* ---------- Risk flags --------------------------------- */
    CASE WHEN ss.vendor_cnt = 1 THEN 'yes' ELSE 'no' END    AS single_source_flag,
    CASE WHEN hv.po_cnt    >= @hv_po    THEN 'yes' ELSE 'no' END AS high_volume_po_flag,
    CASE WHEN hv.spend_amt >= @hv_spend THEN 'yes' ELSE 'no' END AS high_volume_spend_flag,

    /* ---------- Delivery metrics --------------------------- */
    DATEDIFF(day,l.promised_receipt_date,r.posting_date)      AS days_late_early,
    (
         DATEDIFF(day,l.promised_receipt_date,r.posting_date)
       - DATEDIFF(week,l.promised_receipt_date,r.posting_date)*2
       - CASE WHEN DATENAME(weekday,l.promised_receipt_date) IN ('Saturday','Sunday') THEN 1 ELSE 0 END
       - CASE WHEN DATENAME(weekday,r.posting_date)          IN ('Saturday','Sunday') THEN 1 ELSE 0 END
    )                                                        AS bus_days_late,
    CASE
        WHEN r.posting_date IS NULL THEN NULL
        WHEN (
             DATEDIFF(day,l.promised_receipt_date,r.posting_date)
           - DATEDIFF(week,l.promised_receipt_date,r.posting_date)*2
           - CASE WHEN DATENAME(weekday,l.promised_receipt_date) IN ('Saturday','Sunday') THEN 1 ELSE 0 END
           - CASE WHEN DATENAME(weekday,r.posting_date)          IN ('Saturday','Sunday') THEN 1 ELSE 0 END
         ) > 3 THEN 0 ELSE 1
    END                                         AS on_time_flag,

    /* ---------- Purchase-history intelligence -------------- */
    ISNULL(last.last_unit_cost,l.unit_cost)     AS last_unit_cost,
    CASE WHEN l.type_numeric=2 AND last.last_unit_cost IS NULL
         THEN 'yes' ELSE 'no' END               AS first_purchase,
    CASE
         WHEN l.type_numeric<>2 OR last.last_unit_cost IS NULL THEN 'no'
         WHEN last.last_vendor_country <>
              CASE WHEN v.[Country_Region Code]='HK' THEN 'CN'
                   ELSE v.[Country_Region Code] END
              THEN 'yes' ELSE 'no'
    END                                         AS country_change,
    CASE
         WHEN l.type_numeric<>2 OR last.last_unit_cost IS NULL THEN 'no'
         WHEN last.last_vendor_country='CN'                      THEN 'yes' ELSE 'no'
    END                                         AS china_change,

    /* ---------- Misc original columns ---------------------- */
    l.description,
    l.requested_receipt_date,
    l.manufacturer_part_no,
    l.manufacturer_code,
    l.total,
    l.planned_receipt_date,
    l.quantity_delivered,
    DATEDIFF(day,h.order_date,l.promised_receipt_date)        AS promised_lead_time_days,
    DATEDIFF(day,h.order_date,r.posting_date)                 AS actual_lead_time_days,
    h.assigned_user_id,
    h.order_confirmation_date,
    h.purchaser_code,
    l.subsidiary,

    /* ---------- composite indexes --------------------------- */
    CONCAT(l.subsidiary COLLATE Latin1_General_100_CI_AS,
           l.item_no   COLLATE Latin1_General_100_CI_AS)      AS item_index,
    CONCAT(l.subsidiary COLLATE Latin1_General_100_CI_AS,
           l.buy_from_vendor_no COLLATE Latin1_General_100_CI_AS) AS vendor_index

FROM   #LineData  l
JOIN   #HeaderData h ON l.document_no = h.doc_no

/* ---------- Baseline / KPI helper blocks ------------------- */
OUTER APPLY ( /* 1-y item */           SELECT SUM(ld.quantity*ld.unit_cost)/
                                            NULLIF(SUM(ld.quantity),0) AS avg_price
                                       FROM #LineData ld
                                       JOIN #HeaderData hd ON ld.document_no = hd.doc_no
                                       WHERE ld.type_numeric=2 AND ld.item_no=l.item_no
                                         AND hd.order_date BETWEEN DATEADD(day,-@w1y,h.order_date)
                                                               AND     h.order_date-1 ) b1y
OUTER APPLY ( /* 2-y item */           SELECT SUM(ld.quantity*ld.unit_cost)/
                                            NULLIF(SUM(ld.quantity),0) AS avg_price
                                       FROM #LineData ld
                                       JOIN #HeaderData hd ON ld.document_no = hd.doc_no
                                       WHERE ld.type_numeric=2 AND ld.item_no=l.item_no
                                         AND hd.order_date BETWEEN DATEADD(day,-@w2y,h.order_date)
                                                               AND     h.order_date-1 ) b2y
OUTER APPLY ( /* 1-y item+vendor */    SELECT SUM(ld.quantity*ld.unit_cost)/
                                            NULLIF(SUM(ld.quantity),0) AS avg_price_vendor
                                       FROM #LineData ld
                                       JOIN #HeaderData hd ON ld.document_no = hd.doc_no
                                       WHERE ld.type_numeric=2 AND ld.item_no=l.item_no
                                         AND ld.buy_from_vendor_no=l.buy_from_vendor_no
                                         AND hd.order_date BETWEEN DATEADD(day,-@w1y,h.order_date)
                                                               AND     h.order_date-1 ) b1y_v
OUTER APPLY ( /* 2-y item+vendor */    SELECT SUM(ld.quantity*ld.unit_cost)/
                                            NULLIF(SUM(ld.quantity),0) AS avg_price_vendor
                                       FROM #LineData ld
                                       JOIN #HeaderData hd ON ld.document_no = hd.doc_no
                                       WHERE ld.type_numeric=2 AND ld.item_no=l.item_no
                                         AND ld.buy_from_vendor_no=l.buy_from_vendor_no
                                         AND hd.order_date BETWEEN DATEADD(day,-@w2y,h.order_date)
                                                               AND     h.order_date-1 ) b2y_v
OUTER APPLY ( /* last purchase – any vendor */ SELECT TOP 1
                                                ld.unit_cost                            AS last_unit_cost,
                                                CASE WHEN v_prev.[Country_Region Code]='HK' THEN 'CN'
                                                     ELSE v_prev.[Country_Region Code] END AS last_vendor_country
                                               FROM #LineData ld
                                               JOIN #HeaderData hd ON ld.document_no = hd.doc_no
                                               LEFT JOIN [dbo].[IPG Laser GmbH$Vendor] v_prev
                                                      ON ld.buy_from_vendor_no = v_prev.[No_]
                                               WHERE ld.type_numeric=2 AND ld.item_no=l.item_no
                                                 AND hd.order_date < h.order_date
                                               ORDER BY hd.order_date DESC,
                                                        ld.document_no DESC,
                                                        ld.line_no DESC ) last
OUTER APPLY ( /* last purchase – same vendor */ SELECT TOP 1
                                                 ld.unit_cost AS last_unit_cost_vendor
                                               FROM #LineData ld
                                               JOIN #HeaderData hd ON ld.document_no = hd.doc_no
                                               WHERE ld.type_numeric=2 AND ld.item_no=l.item_no
                                                 AND ld.buy_from_vendor_no = l.buy_from_vendor_no
                                                 AND hd.order_date < h.order_date
                                               ORDER BY hd.order_date DESC,
                                                        ld.document_no DESC,
                                                        ld.line_no DESC ) last_v
OUTER APPLY ( /* vendor count */              SELECT COUNT(DISTINCT ld.buy_from_vendor_no) AS vendor_cnt
                                               FROM #LineData ld
                                               JOIN #HeaderData hd2 ON ld.document_no = hd2.doc_no
                                               WHERE ld.type_numeric=2 AND ld.item_no=l.item_no
                                                 AND hd2.order_date <= h.order_date ) ss
OUTER APPLY ( /* rolling-year PO/spend */      SELECT COUNT(DISTINCT hd3.doc_no)  AS po_cnt,
                                                      SUM(ld3.quantity*ld3.unit_cost) AS spend_amt
                                               FROM #LineData ld3
                                               JOIN #HeaderData hd3 ON ld3.document_no = hd3.doc_no
                                               WHERE ld3.type_numeric=2 AND ld3.item_no=l.item_no
                                                 AND hd3.order_date BETWEEN DATEADD(day,-@hv_window,h.order_date)
                                                                       AND     h.order_date-1 ) hv
OUTER APPLY ( /* adaptive baselines */         SELECT
                                                   /* item baseline */
                                                   CASE WHEN b1y.avg_price IS NOT NULL THEN b1y.avg_price
                                                        WHEN b2y.avg_price IS NOT NULL THEN b2y.avg_price
                                                        WHEN last.last_unit_cost IS NOT NULL THEN last.last_unit_cost
                                                        ELSE l.unit_cost END                       AS baseline_unit_cost,
                                                   /* vendor baseline */
                                                   CASE WHEN b1y_v.avg_price_vendor IS NOT NULL THEN b1y_v.avg_price_vendor
                                                        WHEN b2y_v.avg_price_vendor IS NOT NULL THEN b2y_v.avg_price_vendor
                                                        WHEN last_v.last_unit_cost_vendor IS NOT NULL THEN last_v.last_unit_cost_vendor
                                                        ELSE
                                                            CASE WHEN b1y.avg_price IS NOT NULL THEN b1y.avg_price
                                                                 WHEN b2y.avg_price IS NOT NULL THEN b2y.avg_price
                                                                 WHEN last.last_unit_cost IS NOT NULL THEN last.last_unit_cost
                                                                 ELSE l.unit_cost END
                                                   END                                             AS baseline_unit_cost_vendor ) bl
LEFT JOIN #Receipts r
       ON r.order_no = l.document_no
      AND r.line_no  = l.line_no
      AND r.item_no  = l.item_no
LEFT JOIN [dbo].[IPG Laser GmbH$Vendor] v
       ON l.buy_from_vendor_no = v.[No_];
