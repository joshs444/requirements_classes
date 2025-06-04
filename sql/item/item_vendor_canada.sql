/*************************************************************************************************
  Vendor‑Item Snapshot – CA010  (v1 · May 2025)
  • Subsidiary: IPG Canada  (code = CA010)
  • order_date pulled directly from line tables (history rows via header once)
  • Lean TempDB indexes, duplicate‑safe #Fact index
  • Spend metrics (last / 1‑y / 2‑y / all), STDEV < 0.01 ⇒ 0
*************************************************************************************************/
SET NOCOUNT ON;

/* ──────────────────────────  parameters  ─────────────────────────── */
DECLARE
    @today      date = CAST(GETDATE() AS date),
    @start_date date = '2019‑01‑01',
    @w1y        int  = 365,         -- 1‑year window
    @w2y        int  = 730,         -- 2‑year window
    @inactive_d int  = 180;         -- inactivity flag (days)

/* ===========================================================
   1.  Purchase‑line UNION  →  #Line
   =========================================================== */
IF OBJECT_ID('tempdb..#Line') IS NOT NULL DROP TABLE #Line;

WITH hist AS (   /* posted‑history lines */
    SELECT
        'HIST'                              AS src,
        phl.[Buy-from Vendor No_]           AS vendor_no,
        phl.[No_]                           AS item_no,
        phl.[Document No_]                  AS document_no,
        phl.[Line No_]                      AS line_no,
        phl.[Quantity]                      AS raw_qty,
        phl.[Unit Cost (LCY)]               AS raw_cost,
        phl.[Qty_ per Unit of Measure]      AS qty_per_uom,
        phl.[Promised Receipt Date]         AS promised_receipt_date,
        hhh.[Order Date]                    AS order_date
    FROM   [dbo].[IPG Canada$Purchase History Line]   phl
    JOIN   [dbo].[IPG Canada$Purchase History Header] hhh
           ON phl.[Document No_] = hhh.[No_]
    WHERE  phl.[Document Type] = 1
      AND  phl.[Type]          = 2
      AND  phl.[Quantity]            > 0
      AND  phl.[Unit Cost (LCY)]     > 0
      AND  hhh.[Order Date]         >= @start_date
),
open_lines AS (  /* open / released lines */
    SELECT
        'OPEN'                             AS src,
        pl.[Buy-from Vendor No_]           AS vendor_no,
        pl.[No_]                           AS item_no,
        pl.[Document No_]                  AS document_no,
        pl.[Line No_]                      AS line_no,
        pl.[Quantity]                      AS raw_qty,
        pl.[Unit Cost (LCY)]               AS raw_cost,
        pl.[Qty_ per Unit of Measure]      AS qty_per_uom,
        pl.[Promised Receipt Date]         AS promised_receipt_date,
        pl.[Order Date]                    AS order_date
    FROM   [dbo].[IPG Canada$Purchase Line] pl
    WHERE  pl.[Document Type] = 1
      AND  pl.[Type]          = 2
      AND  pl.[Quantity]            > 0
      AND  pl.[Unit Cost (LCY)]     > 0
      AND  pl.[Order Date]         >= @start_date
),
base AS (SELECT * FROM hist UNION ALL SELECT * FROM open_lines)
SELECT
    'CA010'                                                             AS subsidiary,
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

CREATE CLUSTERED INDEX IX_Line          ON #Line (vendor_no,item_no,order_date DESC);
CREATE NONCLUSTERED INDEX IX_Line_lookup ON #Line (document_no,line_no,item_no);

/* ===========================================================
   2.  Earliest receipt per PO‑line  →  #Rcpt
   =========================================================== */
IF OBJECT_ID('tempdb..#Rcpt') IS NOT NULL DROP TABLE #Rcpt;

SELECT
    [Order No_] AS document_no,
    [Line No_]  AS line_no,
    [No_]       AS item_no,
    MIN([Posting Date]) AS posting_date
INTO #Rcpt
FROM [dbo].[IPG Canada$Purch_ Rcpt_ Line]
WHERE [Quantity] > 0
  AND [Posting Date] >= '2018‑01‑01'
GROUP BY [Order No_],[Line No_],[No_];

CREATE UNIQUE CLUSTERED INDEX IX_Rcpt ON #Rcpt(document_no,line_no,item_no);

/* ===========================================================
   3.  Join vendor & receipts  →  #Fact
   =========================================================== */
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
            - CASE WHEN DATENAME(weekday,r.posting_date)            IN ('Saturday','Sunday') THEN 1 ELSE 0 END
          ) > 3 THEN 0 ELSE 1 END                     AS on_time_flag
INTO #Fact
FROM #Line l
LEFT JOIN #Rcpt r  ON r.document_no=l.document_no AND r.line_no=l.line_no AND r.item_no=l.item_no
LEFT JOIN [dbo].[IPG Canada$Vendor] v ON l.vendor_no=v.[No_];

CREATE CLUSTERED INDEX IX_Fact
        ON #Fact(vendor_no,item_no,order_date DESC,document_no DESC,line_no DESC);

/* ===========================================================
   4.  Aggregate to vendor‑item
   =========================================================== */
;WITH Ranked AS (
    SELECT f.*,
           ROW_NUMBER() OVER (PARTITION BY vendor_no,item_no
                              ORDER BY order_date DESC,document_no DESC,line_no DESC) AS rn
    FROM #Fact f
),
Agg AS (
    SELECT
        f.subsidiary,
        f.vendor_no,
        f.vendor_name,
        f.vendor_country,
        f.item_no,

        /* latest */
        MAX(CASE WHEN rn=1 THEN f.unit_cost END)            AS last_unit_cost,
        MAX(CASE WHEN rn=1 THEN f.quantity  END)            AS last_quantity,
        MAX(CASE WHEN rn=1 THEN f.order_date END)           AS last_order_date,
        MAX(CASE WHEN rn=1 THEN f.document_no END)          AS last_po_no,
        MAX(CASE WHEN rn=1 THEN f.uom_flag END)             AS uom_sanity_flag_latest,
        MAX(CASE WHEN rn=1 THEN f.quantity*f.unit_cost END) AS last_spend,

        /* 1‑year */
        AVG (CASE WHEN f.order_date>=DATEADD(day,-@w1y,@today) THEN f.unit_cost END)     AS avg_price_1y_raw,
        MIN (CASE WHEN f.order_date>=DATEADD(day,-@w1y,@today) THEN f.unit_cost END)     AS min_price_1y,
        MAX (CASE WHEN f.order_date>=DATEADD(day,-@w1y,@today) THEN f.unit_cost END)     AS max_price_1y,
        STDEV(CASE WHEN f.order_date>=DATEADD(day,-@w1y,@today) THEN f.unit_cost END)    AS stdev_1y_raw,
        SUM (CASE WHEN f.order_date>=DATEADD(day,-@w1y,@today) THEN f.quantity END)      AS qty_1y,
        COUNT(DISTINCT CASE WHEN f.order_date>=DATEADD(day,-@w1y,@today) THEN f.document_no END) AS po_cnt_1y,
        SUM (CASE WHEN f.order_date>=DATEADD(day,-@w1y,@today) THEN f.quantity*f.unit_cost END)  AS spend_1y,
        AVG (CASE WHEN f.order_date>=DATEADD(day,-@w1y,@today) THEN CAST(f.on_time_flag AS float) END) AS on_time_pct_1y,
        AVG (CASE WHEN f.order_date>=DATEADD(day,-@w1y,@today) THEN f.lead_time_days END)          AS avg_lead_time_days_1y,

        /* 2‑year */
        AVG (CASE WHEN f.order_date>=DATEADD(day,-@w2y,@today) THEN f.unit_cost END)     AS avg_price_2y_raw,
        STDEV(CASE WHEN f.order_date>=DATEADD(day,-@w2y,@today) THEN f.unit_cost END)    AS stdev_2y_raw,
        SUM (CASE WHEN f.order_date>=DATEADD(day,-@w2y,@today) THEN f.quantity END)      AS qty_2y,
        COUNT(DISTINCT CASE WHEN f.order_date>=DATEADD(day,-@w2y,@today) THEN f.document_no END) AS po_cnt_2y,
        SUM (CASE WHEN f.order_date>=DATEADD(day,-@w2y,@today) THEN f.quantity*f.unit_cost END)  AS spend_2y,
        AVG (CASE WHEN f.order_date>=DATEADD(day,-@w2y,@today) THEN CAST(f.on_time_flag AS float) END) AS on_time_pct_2y,
        AVG (CASE WHEN f.order_date>=DATEADD(day,-@w2y,@today) THEN f.lead_time_days END)          AS avg_lead_time_days_2y,

        /* history */
        SUM(f.quantity*f.unit_cost)                                AS spend_all,
        SUM(CASE WHEN f.is_open=1 THEN f.quantity END)             AS open_outstanding_qty,
        MIN(f.order_date)                                          AS first_po_date
    FROM Ranked f
    GROUP BY f.subsidiary,f.vendor_no,f.vendor_name,f.vendor_country,f.item_no
)

/* ===========================================================
   5.  Final select
   =========================================================== */
SELECT
    a.subsidiary,
    a.vendor_no,
    a.vendor_name,
    a.vendor_country,
    a.item_no,

    /* latest */
    a.last_unit_cost,
    a.last_quantity,
    a.last_order_date,
    a.last_po_no,
    a.last_spend,
    a.uom_sanity_flag_latest,

    /* 1‑year */
    CASE WHEN ABS(a.avg_price_1y_raw)<0.0000001 THEN 0 ELSE a.avg_price_1y_raw END AS avg_price_1y,
    a.min_price_1y,
    a.max_price_1y,
    CASE WHEN a.stdev_1y_raw<0.01 THEN 0 ELSE a.stdev_1y_raw END                    AS price_volatility_1y,
    a.qty_1y, a.po_cnt_1y, a.spend_1y, a.on_time_pct_1y, a.avg_lead_time_days_1y,

    /* 2‑year */
    CASE WHEN ABS(a.avg_price_2y_raw)<0.0000001 THEN 0 ELSE a.avg_price_2y_raw END AS avg_price_2y,
    CASE WHEN a.stdev_2y_raw<0.01 THEN 0 ELSE a.stdev_2y_raw END                    AS price_volatility_2y,
    a.qty_2y, a.po_cnt_2y, a.spend_2y, a.on_time_pct_2y, a.avg_lead_time_days_2y,

    /* exposure & history */
    a.open_outstanding_qty,
    a.first_po_date,
    a.spend_all,

    /* derived shares / flags */
    a.qty_1y*1.0/NULLIF(SUM(a.qty_1y) OVER (PARTITION BY a.item_no),0) AS vendor_share_qty_1y,
    a.qty_2y*1.0/NULLIF(SUM(a.qty_2y) OVER (PARTITION BY a.item_no),0) AS vendor_share_qty_2y,
    (a.last_unit_cost - COALESCE(a.avg_price_1y_raw,a.last_unit_cost))
        / NULLIF(COALESCE(a.avg_price_1y_raw,a.last_unit_cost),0)       AS price_delta_vs_1y,
    CASE WHEN a.last_order_date<DATEADD(day,-@inactive_d,@today)
         THEN 'Yes' ELSE 'No' END                                       AS inactive_flag
FROM   Agg a
ORDER  BY a.vendor_no, a.item_no;
