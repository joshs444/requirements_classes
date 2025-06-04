/* ========================================================================
   OPEN‑PURCHASES  –  Snapshot + Current (US010 · US020 · CA010)
   Flags returned:
       • Overdue_Status
       • Unconfirmed_Status
       • Overdue On Last Report
       • Unconfirmed On Last Report
       • DateLabel            (latest snapshot = 0, next older = 1, …)
       • Promise Status       (new: 'No Promise' or NULL)
   ======================================================================== */
SET DATEFIRST 7;   -- 1 = Sunday, 7 = Saturday  (weekday math)

/* ------------------------------------------------------------------------
   1.  SNAPSHOT TABLES
   ------------------------------------------------------------------------ */
WITH snapshot_union AS (
    /* ---------- US010 snapshot ---------- */
    SELECT
        s.Subsidiary,
        s.[Document Type],
        s.[Document No_],
        s.[Line No_],
        s.[Buy-from Vendor No_],
        s.[Type],
        s.[No_],
        s.[Description],
        s.[Shortcut Dimension 1 Code],
        s.[Requested Receipt Date],
        s.[Promised Receipt Date],
        s.[Expected Receipt Date],
        s.[Order Date],
        s.[Order Confirmation Date],
        s.[Quantity],
        s.[Outstanding Quantity],
        s.[Unit Cost (LCY)],
        s.[Package Tracking No_],
        s.[Assigned User ID],
        s.[Purchaser Code],
        'Snapshot'                    AS [Data Type],
        CAST(s.lst_updt_dtm AS date)  AS lst_updt_dtm
    FROM dbo.stg_open_purchases_us_t AS s

    UNION ALL
    /* ---------- CA010 snapshot ---------- */
    SELECT
        s.Subsidiary,
        s.[Document Type], s.[Document No_], s.[Line No_],
        s.[Buy-from Vendor No_], s.[Type], s.[No_], s.[Description],
        s.[Shortcut Dimension 1 Code],
        s.[Requested Receipt Date], s.[Promised Receipt Date], s.[Expected Receipt Date],
        s.[Order Date], s.[Order Confirmation Date],
        s.[Quantity], s.[Outstanding Quantity], s.[Unit Cost (LCY)],
        NULL                           AS [Package Tracking No_],
        s.[Assigned User ID], s.[Purchaser Code],
        'Snapshot', CAST(s.lst_updt_dtm AS date)
    FROM dbo.stg_open_purchases_canada_t AS s

    UNION ALL
    /* ---------- US020 snapshot ---------- */
    SELECT
        s.Subsidiary,
        s.[Document Type], s.[Document No_], s.[Line No_],
        s.[Buy-from Vendor No_], s.[Type], s.[No_], s.[Description],
        s.[Shortcut Dimension 1 Code],
        s.[Requested Receipt Date], s.[Promised Receipt Date], s.[Expected Receipt Date],
        s.[Order Date], s.[Order Confirmation Date],
        s.[Quantity], s.[Outstanding Quantity], s.[Unit Cost (LCY)],
        NULL,
        s.[Assigned User ID], s.[Purchaser Code],
        'Snapshot', CAST(s.lst_updt_dtm AS date)
    FROM dbo.stg_open_purchases_med_t AS s
),

/* ------------------------------------------------------------------------
   2.  CURRENT‑DAY LINES
   ------------------------------------------------------------------------ */
current_union AS (
    /* ---------- US010 current ---------- */
    SELECT
        'US010'                        AS Subsidiary,
        pl.[Document Type],
        pl.[Document No_],
        pl.[Line No_],
        pl.[Buy-from Vendor No_],
        pl.[Type],
        pl.[No_],
        pl.[Description],
        pl.[Shortcut Dimension 1 Code],
        pl.[Requested Receipt Date],
        pl.[Promised Receipt Date],
        pl.[Expected Receipt Date],
        ph.[Order Date],
        ph.[Order Confirmation Date],
        pl.[Quantity],
        pl.[Outstanding Quantity],
        pl.[Unit Cost (LCY)],
        pl.[Package Tracking No_],
        ph.[Assigned User ID],
        ph.[Purchaser Code],
        'Current'                      AS [Data Type],
        CAST(GETDATE() AS date)        AS lst_updt_dtm
    FROM   [dbo].[IPG Photonics Corporation$Purchase Line]   AS pl
    JOIN   [dbo].[IPG Photonics Corporation$Purchase Header] AS ph
           ON ph.[No_] = pl.[Document No_]
          AND ph.[Document Type] = pl.[Document Type]
    WHERE  pl.[Order Date]      > '2019-01-01'
      AND  pl.[Quantity]        > 0
      AND  pl.[Unit Cost (LCY)] > 0
      AND  pl.[Document Type]   = 1
      AND  pl.[Type] IN (1,2,4)

    UNION ALL
    /* ---------- US020 current ---------- */
    SELECT
        'US020',
        pl.[Document Type], pl.[Document No_], pl.[Line No_],
        pl.[Buy-from Vendor No_], pl.[Type], pl.[No_], pl.[Description],
        pl.[Shortcut Dimension 1 Code],
        pl.[Requested Receipt Date], pl.[Promised Receipt Date], pl.[Expected Receipt Date],
        ph.[Order Date], ph.[Order Confirmation Date],
        pl.[Quantity], pl.[Outstanding Quantity], pl.[Unit Cost (LCY)],
        NULL,
        ph.[Assigned User ID], ph.[Purchaser Code],
        'Current', CAST(GETDATE() AS date)
    FROM   [dbo].[IPG Medical Corporation$Purchase Line]     AS pl
    JOIN   [dbo].[IPG Medical Corporation$Purchase Header]   AS ph
           ON ph.[No_] = pl.[Document No_]
          AND ph.[Document Type] = pl.[Document Type]
    WHERE  pl.[Order Date]      > '2019-01-01'
      AND  pl.[Quantity]        > 0
      AND  pl.[Unit Cost (LCY)] > 0
      AND  pl.[Document Type]   = 1
      AND  pl.[Type] IN (1,2,4)

    UNION ALL
    /* ---------- CA010 current ---------- */
    SELECT
        'CA010',
        pl.[Document Type], pl.[Document No_], pl.[Line No_],
        pl.[Buy-from Vendor No_], pl.[Type], pl.[No_], pl.[Description],
        pl.[Shortcut Dimension 1 Code],
        pl.[Requested Receipt Date], pl.[Promised Receipt Date], pl.[Expected Receipt Date],
        ph.[Order Date], ph.[Order Confirmation Date],
        pl.[Quantity], pl.[Outstanding Quantity], pl.[Unit Cost (LCY)],
        NULL,
        ph.[Assigned User ID], ph.[Purchaser Code],
        'Current', CAST(GETDATE() AS date)
    FROM   [dbo].[IPG Canada$Purchase Line]                 AS pl
    JOIN   [dbo].[IPG Canada$Purchase Header]               AS ph
           ON ph.[No_] = pl.[Document No_]
          AND ph.[Document Type] = pl.[Document Type]
    WHERE  pl.[Order Date]      > '2019-01-01'
      AND  pl.[Quantity]        > 0
      AND  pl.[Unit Cost (LCY)] > 0
      AND  pl.[Document Type]   = 1
      AND  pl.[Type] IN (1,2,4)
),

/* ------------------------------------------------------------------------
   3.  UNION ALL SNAPSHOT + CURRENT
   ------------------------------------------------------------------------ */
all_rows AS (
    SELECT * FROM snapshot_union
    UNION ALL
    SELECT * FROM current_union
),

/* ------------------------------------------------------------------------
   4.  EMPLOYEE LOOKUP
   ------------------------------------------------------------------------ */
employees AS (
    SELECT DISTINCT * FROM (VALUES
        ('CCURTIS','Christine Curtis'),
        ('DFAZZUOLI','David Fazzouli'),
        ('DGRIMALA','Dennis Grimala'),
        ('IPG-DOMAIN\BLOCKE','Bob Locke'),
        ('IPG-DOMAIN\EAKOULENOK','Elena Akoulenok'),
        ('IPG-DOMAIN\HLIMA','Helena Soares'),
        ('IPG-DOMAIN\LMORRISON','Lindsay Morrison'),
        ('IPG-DOMAIN\NSTEEL','Nick Steel'),
        ('IPG-DOMAIN\SHERNANDEZ','Silvio Hernandez'),
        ('JFRAIN','Jack Frain'),
        ('K_SEEK','Kim Seek'),
        ('KAMCGRATH','Kathleen McGrath'),
        ('S_BERARD','Susan Berard'),
        ('S_LIER','Susan Lier'),
        ('SRENNER','Steven Renner'),
        ('T_EDDY','Tammy Eddy'),
        ('IPG-DOMAIN\JMAGNUSON','Jim Magnuson'),
        ('IPG-DOMAIN\JMULLIGAN','Jennifer Mulligan'),
        ('IPG-DOMAIN\KENGEN','Kimberly Engen'),
        ('IPG-DOMAIN\MFRIEDLEY','Mike Friedley'),
        ('IPG-DOMAIN\PBARNES','Phillip Barnes'),
        ('IPG-DOMAIN\APERLOV','Annie Perlov'),
        ('IPG-DOMAIN\CBATALLAS','Charlotte Batallas'),
        ('IPG-DOMAIN\SRENNER','Steven Renner'),
        ('IPG-DOMAIN\KAMCGRATH','Kathleen McGrath'),
        ('IPG-DOMAIN\MMARSH','Matthew Marsh'),
        ('IPG-DOMAIN\JDESOUSA','Jacquie De Sousa'),
        ('IPG-DOMAIN\RKHEN','Ronen Chen'),
        ('IPG-DOMAIN\EALEXEEVA','Eugen Alexeev'),
        ('IPG-DOMAIN\AAHRENKIEL','Andrew Ahrenkiel'),
        ('IPG-DOMAIN\KOLSON','Kristin Olson'),
        ('IPG-DOMAIN\HSOARES','Helena Soares')
    ) AS x(nav_name, full_name)
),

/* ------------------------------------------------------------------------
   5.  BASE FLAG (Overdue / Unconfirmed)
   ------------------------------------------------------------------------ */
flags AS (
    SELECT
        a.*,

        /* business‑day difference Expected → snapshot (minus 1) */
        ((DATEDIFF(day, a.[Expected Receipt Date], a.lst_updt_dtm) + 1)
          - (DATEDIFF(week, a.[Expected Receipt Date], a.lst_updt_dtm) * 2)
          - CASE WHEN DATEPART(weekday, a.[Expected Receipt Date]) = 7 THEN 1 ELSE 0 END
          - CASE WHEN DATEPART(weekday, a.lst_updt_dtm) = 1 THEN 1 ELSE 0 END
        ) - 1 AS days_overdue,

        /* business‑day difference OrderDate → snapshot (minus 1) */
        ((DATEDIFF(day, a.[Order Date], a.lst_updt_dtm) + 1)
          - (DATEDIFF(week, a.[Order Date], a.lst_updt_dtm) * 2)
          - CASE WHEN DATEPART(weekday, a.[Order Date]) = 7 THEN 1 ELSE 0 END
          - CASE WHEN DATEPART(weekday, a.lst_updt_dtm) = 1 THEN 1 ELSE 0 END
        ) - 1 AS days_since_order
    FROM all_rows AS a
),

flags_with_status AS (
    SELECT
        f.*,

        CASE
            WHEN f.[Outstanding Quantity] > 0
             AND (
                    (f.[Type] = 1  AND f.days_overdue   > 7)
                 OR (f.[Type] <> 1 AND f.days_overdue   > 3)
                 )
            THEN 'Overdue'
        END AS Overdue_Status,

        CASE
            WHEN f.[Order Confirmation Date] = '1753-01-01'
             AND f.[Outstanding Quantity] > 0
             AND f.days_since_order > 3
            THEN 'Unconfirmed'
        END AS Unconfirmed_Status
    FROM flags AS f
),

/* ------------------------------------------------------------------------
   6.  PREVIOUS SNAPSHOT’S STATUS (LAG)
   ------------------------------------------------------------------------ */
lags AS (
    SELECT
        fs.*,

        LAG(fs.Overdue_Status) OVER (
            PARTITION BY fs.[Document No_], fs.[Line No_], fs.[No_]
            ORDER BY     fs.lst_updt_dtm
        ) AS prev_overdue_status,

        LAG(fs.Unconfirmed_Status) OVER (
            PARTITION BY fs.[Document No_], fs.[Line No_], fs.[No_]
            ORDER BY     fs.lst_updt_dtm
        ) AS prev_unconfirmed_status
    FROM flags_with_status AS fs
),

/* ------------------------------------------------------------------------
   7.  DENSE RANK FOR DateLabel  (Snapshots only)
   ------------------------------------------------------------------------ */
ranked AS (
    SELECT
        l.*,
        CASE
            WHEN l.[Data Type] = 'Snapshot'
            THEN DENSE_RANK() OVER (ORDER BY l.lst_updt_dtm DESC) - 1
        END AS DateLabel
    FROM lags AS l
)

/* ------------------------------------------------------------------------
   8.  FINAL SELECT
   ------------------------------------------------------------------------ */
SELECT
    r.Subsidiary,
    r.[Document Type],
    r.[Document No_],
    r.[Line No_],
    r.[Buy-from Vendor No_],
    v.[Vendor Name],
    r.[Type],
    r.[No_],
    r.[Description],
    r.[Shortcut Dimension 1 Code],
    r.[Requested Receipt Date],
    r.[Promised Receipt Date],
    r.[Expected Receipt Date],
    r.[Order Date],
    r.[Order Confirmation Date],
    r.[Quantity],
    r.[Outstanding Quantity],
    r.[Unit Cost (LCY)],
    r.[Package Tracking No_],
    r.[Assigned User ID],
    e.full_name                     AS [Employee Name],
    r.[Purchaser Code],
    r.[Data Type],
    r.lst_updt_dtm,

    r.DateLabel,

    r.Overdue_Status,
    r.Unconfirmed_Status,

    CASE WHEN r.prev_overdue_status     = 'Overdue'     THEN 'On Last Report' END AS [Overdue On Last Report],
    CASE WHEN r.prev_unconfirmed_status = 'Unconfirmed' THEN 'On Last Report' END AS [Unconfirmed On Last Report],
    CASE 
        WHEN r.[Promised Receipt Date] = '1753-01-01' 
        AND r.[Outstanding Quantity] > 0 
        AND r.[Order Confirmation Date] <> '1753-01-01' 
        AND r.[Order Confirmation Date] < DATEADD(day, -4, r.lst_updt_dtm) 
        AND r.[Order Date] > '2023-09-27'
        THEN 'No Promise'
        ELSE NULL
    END AS [Promise Status]

FROM       ranked            AS r
LEFT JOIN  dbo.vendor_all_v  AS v
       ON  v.Subsidiary  = r.Subsidiary
      AND v.[Vendor No] = r.[Buy-from Vendor No_]
LEFT JOIN  employees         AS e
       ON  e.nav_name  = r.[Assigned User ID]
WHERE      r.[No_] NOT IN ('200130','501080');