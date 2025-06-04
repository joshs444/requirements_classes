/*************************************************************************************************
  Vendor‑Item Snapshot – DE010  (v6‑fixed · May 2025)
  • order_date from line tables  • lean indexes  • spend metrics
  • STDEV < 0.01 shown as 0      • duplicate‑safe clustered IX_Fact
*************************************************************************************************/
SET NOCOUNT ON;

/* ────────── parameters ---------------------------------------------------- */
DECLARE
    @today      date = CAST(GETDATE() AS date),
    @start_date date = '2019‑01‑01',
    @w1y        int  = 365,
    @w2y        int  = 730,
    @inactive_d int  = 180;

/* ────────── 1.  Purchase‑Line UNION → #Line ------------------------------ */
IF OBJECT_ID('tempdb..#Line') IS NOT NULL DROP TABLE #Line;

WITH base AS (
    /* ---------- HISTORY (archived, posted POs) --------------------------- */
    SELECT
        'HIST'                          AS src,
        pl.[Buy-from Vendor No_]        AS vendor_no,
        pl.[No_]                        AS item_no,
        pl.[Document No_]               AS document_no,
        pl.[Line No_]                   AS line_no,
        pl.[Quantity]                   AS raw_qty,
        pl.[Unit Cost (LCY)]            AS raw_cost,
        pl.[Qty_ per Unit of Measure]   AS qty_per_uom,
        pl.[Promised Receipt Date]      AS promised_receipt_date,
        pl.[Order Date]                 AS order_date          --  pulled directly
    FROM [dbo].[IPG Laser GmbH$Purchase Line Archive] pl
    WHERE pl.[Document Type]=1 AND pl.[Type]=2
      AND pl.[Quantity]            > 0
      AND pl.[Unit Cost (LCY)]     > 0
      AND pl.[Order Date]         >= @start_date

    UNION ALL

    /* ---------- OPEN (live POs) ----------------------------------------- */
    SELECT
        'OPEN',
        pl.[Buy-from Vendor No_],
        pl.[No_],
        pl.[Document No_],
        pl.[Line No_],
        pl.[Quantity],
        pl.[Unit Cost (LCY)],
        pl.[Qty_ per Unit of Measure],
        pl.[Promised Receipt Date],
        pl.[Order Date]
    FROM [dbo].[IPG Laser GmbH$Purchase Line] pl
    WHERE pl.[Document Type]=1 AND pl.[Type]=2
      AND pl.[Quantity]            > 0
      AND pl.[Unit Cost (LCY)]     > 0
      AND pl.[Order Date]         >= @start_date
)
SELECT
    'DE010'                                                             AS subsidiary,
    b.vendor_no,
    b.item_no,
    b.document_no,
    b.line_no,
    b.raw_qty  * COALESCE(NULLIF(b.qty_per_uom,0),1)                    AS quantity,
    b.raw_cost / COALESCE(NULLIF(b.qty_per_uom,0),1)                    AS unit_cost,
    b.promised_receipt_date,
    b.order_date,
    CASE WHEN b.src='OPEN' THEN 1 ELSE 0 END                            AS is_open,
    CASE WHEN b.raw_qty<=0 OR b.raw_cost<=0 THEN 'BAD' ELSE 'OK' END    AS uom_flag
INTO #Line
FROM base b;

CREATE CLUSTERED INDEX IX_Line ON #Line (vendor_no,item_no,order_date DESC);
CREATE NONCLUSTERED INDEX IX_Line_lookup ON #Line (document_no,line_no,item_no);

/* ────────── 2.  Earliest receipt per PO‑line  → #Rcpt -------------------- */
IF OBJECT_ID('tempdb..#Rcpt') IS NOT NULL DROP TABLE #Rcpt;

SELECT
    [Order No_] AS document_no,
    [Line No_]  AS line_no,
    [No_]       AS item_no,
    MIN([Posting Date]) AS posting_date
INTO #Rcpt
FROM [dbo].[IPG Laser GmbH$Purch_ Rcpt_ Line]
WHERE [Quantity] > 0
GROUP BY [Order No_],[Line No_],[No_];

CREATE UNIQUE CLUSTERED INDEX IX_Rcpt ON #Rcpt(document_no,line_no,item_no);

/* ────────── 3.  Add vendor & receipt info → #Fact ------------------------ */
IF OBJECT_ID('tempdb..#Fact') IS NOT NULL DROP TABLE #Fact;

SELECT
    l.*,
    v.[Name]                                           AS vendor_name,
    CASE WHEN v.[Country_Region Code]='HK' THEN 'CN'
         ELSE v.[Country_Region Code] END              AS vendor_country,
    r.posting_date,
    DATEDIFF(day,l.order_date,r.posting_date)          AS lead_time_days,
    CASE
        WHEN r.posting_date IS NULL THEN NULL
        WHEN (
               DATEDIFF(day,l.promised_receipt_date,r.posting_date)
             - DATEDIFF(week,l.promised_receipt_date,r.posting_date)*2
             - CASE WHEN DATENAME(weekday,l.promised_receipt_date) IN ('Saturday','Sunday') THEN 1 ELSE 0 END
             - CASE WHEN DATENAME(weekday,r.posting_date)          IN ('Saturday','Sunday') THEN 1 ELSE 0 END
           ) > 3 THEN 0 ELSE 1 END                     AS on_time_flag
INTO #Fact
FROM #Line l
LEFT JOIN #Rcpt r  ON r.document_no=l.document_no AND r.line_no=l.line_no AND r.item_no=l.item_no
LEFT JOIN [dbo].[IPG Laser GmbH$Vendor] v ON l.vendor_no=v.[No_];

-- *** duplicate‑safe clustered index ***
CREATE CLUSTERED INDEX IX_Fact
ON #Fact(vendor_no,item_no,order_date DESC, document_no DESC, line_no DESC);

/* ────────── 4.  Aggregate to vendor‑item level --------------------------- */
;WITH Ranked AS (
    SELECT f.*,
           ROW_NUMBER() OVER (PARTITION BY vendor_no,item_no
                              ORDER BY order_date DESC, document_no DESC, line_no DESC) AS rn
    FROM #Fact f
),
Agg AS (
    SELECT
        f.subsidiary,
        f.vendor_no,
        f.vendor_name,
        f.vendor_country,
        f.item_no,

        /* latest PO */
        MAX(CASE WHEN rn=1 THEN f.unit_cost END)             AS last_unit_cost,
        MAX(CASE WHEN rn=1 THEN f.quantity  END)             AS last_quantity,
        MAX(CASE WHEN rn=1 THEN f.order_date END)            AS last_order_date,
        MAX(CASE WHEN rn=1 THEN f.document_no END)           AS last_po_no,
        MAX(CASE WHEN rn=1 THEN f.uom_flag END)              AS uom_sanity_flag_latest,
        MAX(CASE WHEN rn=1 THEN f.quantity*f.unit_cost END)  AS last_spend,

        /* rolling 1‑y */
        AVG (CASE WHEN f.order_date>=DATEADD(day,-@w1y,@today) THEN f.unit_cost END)      AS avg_price_1y_raw,
        MIN (CASE WHEN f.order_date>=DATEADD(day,-@w1y,@today) THEN f.unit_cost END)      AS min_price_1y,
        MAX (CASE WHEN f.order_date>=DATEADD(day,-@w1y,@today) THEN f.unit_cost END)      AS max_price_1y,
        STDEV(CASE WHEN f.order_date>=DATEADD(day,-@w1y,@today) THEN f.unit_cost END)     AS stdev_1y_raw,
        SUM (CASE WHEN f.order_date>=DATEADD(day,-@w1y,@today) THEN f.quantity END)       AS qty_1y,
        COUNT(DISTINCT CASE WHEN f.order_date>=DATEADD(day,-@w1y,@today) THEN f.document_no END) AS po_cnt_1y,
        SUM (CASE WHEN f.order_date>=DATEADD(day,-@w1y,@today) THEN f.quantity*f.unit_cost END)  AS spend_1y,
        AVG (CASE WHEN f.order_date>=DATEADD(day,-@w1y,@today) THEN CAST(f.on_time_flag AS float) END) AS on_time_pct_1y,
        AVG (CASE WHEN f.order_date>=DATEADD(day,-@w1y,@today) THEN f.lead_time_days END)           AS avg_lead_time_days_1y,

        /* rolling 2‑y */
        AVG (CASE WHEN f.order_date>=DATEADD(day,-@w2y,@today) THEN f.unit_cost END)      AS avg_price_2y_raw,
        STDEV(CASE WHEN f.order_date>=DATEADD(day,-@w2y,@today) THEN f.unit_cost END)     AS stdev_2y_raw,
        SUM (CASE WHEN f.order_date>=DATEADD(day,-@w2y,@today) THEN f.quantity END)       AS qty_2y,
        COUNT(DISTINCT CASE WHEN f.order_date>=DATEADD(day,-@w2y,@today) THEN f.document_no END) AS po_cnt_2y,
        SUM (CASE WHEN f.order_date>=DATEADD(day,-@w2y,@today) THEN f.quantity*f.unit_cost END)  AS spend_2y,
        AVG (CASE WHEN f.order_date>=DATEADD(day,-@w2y,@today) THEN CAST(f.on_time_flag AS float) END) AS on_time_pct_2y,
        AVG (CASE WHEN f.order_date>=DATEADD(day,-@w2y,@today) THEN f.lead_time_days END)          AS avg_lead_time_days_2y,

        /* full history */
        SUM(f.quantity*f.unit_cost)                                AS spend_all,
        SUM(CASE WHEN f.is_open=1 THEN f.quantity END)             AS open_outstanding_qty,
        MIN(f.order_date)                                          AS first_po_date
    FROM Ranked f
    GROUP BY f.subsidiary,f.vendor_no,f.vendor_name,f.vendor_country,f.item_no
)

/* ────────── 5.  Final select -------------------------------------------- */
SELECT
    a.subsidiary,
    a.vendor_no,
    a.vendor_name,
    a.vendor_country,
    a.item_no,

    /* latest PO */
    a.last_unit_cost,
    a.last_quantity,
    a.last_order_date,
    a.last_po_no,
    a.last_spend,
    a.uom_sanity_flag_latest,

    /* 1‑y window */
    CASE WHEN a.avg_price_1y_raw IS NULL THEN NULL ELSE
         CASE WHEN ABS(a.avg_price_1y_raw)<0.0000001 THEN 0 ELSE a.avg_price_1y_raw END END AS avg_price_1y,
    a.min_price_1y,
    a.max_price_1y,
    CASE WHEN a.stdev_1y_raw<0.01 THEN 0 ELSE a.stdev_1y_raw END AS price_volatility_1y,
    a.qty_1y, a.po_cnt_1y, a.spend_1y, a.on_time_pct_1y, a.avg_lead_time_days_1y,

    /* 2‑y window */
    CASE WHEN a.avg_price_2y_raw IS NULL THEN NULL ELSE
         CASE WHEN ABS(a.avg_price_2y_raw)<0.0000001 THEN 0 ELSE a.avg_price_2y_raw END END AS avg_price_2y,
    CASE WHEN a.stdev_2y_raw<0.01 THEN 0 ELSE a.stdev_2y_raw END AS price_volatility_2y,
    a.qty_2y, a.po_cnt_2y, a.spend_2y, a.on_time_pct_2y, a.avg_lead_time_days_2y,

    /* exposure & history */
    a.open_outstanding_qty,
    a.first_po_date,
    a.spend_all,

    /* derived KPIs */
    a.qty_1y*1.0/NULLIF(SUM(a.qty_1y) OVER (PARTITION BY a.item_no),0) AS vendor_share_qty_1y,
    a.qty_2y*1.0/NULLIF(SUM(a.qty_2y) OVER (PARTITION BY a.item_no),0) AS vendor_share_qty_2y,
    (a.last_unit_cost -
        COALESCE(a.avg_price_1y_raw,a.last_unit_cost))
      /NULLIF(COALESCE(a.avg_price_1y_raw,a.last_unit_cost),0)          AS price_delta_vs_1y,
    CASE WHEN a.last_order_date<DATEADD(day,-@inactive_d,@today)
         THEN 'Yes' ELSE 'No' END                                       AS inactive_flag
FROM   Agg a
ORDER BY a.vendor_no, a.item_no;
