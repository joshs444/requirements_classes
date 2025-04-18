SET NOCOUNT ON;

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
    src.qty_factor                      AS [Qty_ per Unit of Measure],
    src.[Quantity]  * src.qty_factor    AS [Quantity],
    src.[Outstanding Quantity] * src.qty_factor AS [Outstanding Quantity],
    src.[Unit Cost (LCY)] / src.qty_factor      AS [Unit Cost],
    src.[Requested Receipt Date],
    ([Quantity] - [Outstanding Quantity]) * ([Unit Cost (LCY)] / src.qty_factor) AS [Total],
    ([Quantity] - [Outstanding Quantity]) * src.qty_factor AS [Quantity Delivered],
    'US010'                             AS [Subsidiary]
INTO #LineData
FROM (
    SELECT
        'HISTORY' AS [Status],
        l.[Document Type],
        l.[Document No_],
        l.[Line No_],
        l.[Shortcut Dimension 1 Code],
        l.[Buy-from Vendor No_],
        CASE l.[Type]
            WHEN 1 THEN 'GL'
            WHEN 2 THEN 'Item'
            WHEN 4 THEN 'FA'
        END                             AS [Type],
        l.[No_],
        l.[Location Code],
        l.[Expected Receipt Date],
        l.[Promised Receipt Date],
        l.[Planned Receipt Date],
        l.[Description],
        COALESCE(NULLIF(l.[Qty_ per Unit of Measure],0),1) AS qty_factor,
        l.[Quantity],
        l.[Outstanding Quantity],
        l.[Unit Cost (LCY)],
        l.[Requested Receipt Date]
    FROM [dbo].[IPG Photonics Corporation$Purchase History Line] l
    WHERE l.[Order Date] > '2019-01-01'
      AND l.[Quantity] > 0
      AND l.[Unit Cost (LCY)] > 0
      AND l.[Document Type] = 1
      AND l.[Type] IN (1,2,4)
      AND l.[Quantity] - l.[Outstanding Quantity] <> 0

    UNION ALL

    SELECT
        'OPEN',
        l.[Document Type],
        l.[Document No_],
        l.[Line No_],
        l.[Shortcut Dimension 1 Code],
        l.[Buy-from Vendor No_],
        CASE l.[Type]
            WHEN 1 THEN 'GL'
            WHEN 2 THEN 'Item'
            WHEN 4 THEN 'FA'
        END,
        l.[No_],
        l.[Location Code],
        l.[Expected Receipt Date],
        l.[Promised Receipt Date],
        l.[Planned Receipt Date],
        l.[Description],
        COALESCE(NULLIF(l.[Qty_ per Unit of Measure],0),1),
        l.[Quantity],
        l.[Outstanding Quantity],
        l.[Unit Cost (LCY)],
        l.[Requested Receipt Date]
    FROM [dbo].[IPG Photonics Corporation$Purchase Line] l
    WHERE l.[Order Date] > '2019-01-01'
      AND l.[Quantity] > 0
      AND l.[Unit Cost (LCY)] > 0
      AND l.[Document Type] = 1
      AND l.[Type] IN (1,2,4)
) src;

CREATE CLUSTERED INDEX IX_LineData_DocLineItem
    ON #LineData ([Document No_], [Line No_], [No_]);

IF OBJECT_ID('tempdb..#HeaderData') IS NOT NULL DROP TABLE #HeaderData;

SELECT
    h.[Document Type],
    h.[No_]                         AS [Doc_No_],
    h.[Order Date],
    h.[Posting Date],
    h.[Assigned User ID],
    h.[Order Confirmation Date],
    h.[Purchaser Code],
    'US010'                         AS [Subsidiary]
INTO #HeaderData
FROM (
    SELECT [Document Type],[No_],[Order Date],[Posting Date],
           [Assigned User ID],[Order Confirmation Date],[Purchaser Code]
    FROM [dbo].[IPG Photonics Corporation$Purchase History Header]
    WHERE [Order Date] > '2018-12-31'
      AND [Document Type] = 1
      AND [Buy-from Vendor No_] <> ''
    UNION ALL
    SELECT [Document Type],[No_],[Order Date],[Posting Date],
           [Assigned User ID],[Order Confirmation Date],[Purchaser Code]
    FROM [dbo].[IPG Photonics Corporation$Purchase Header]
    WHERE [Order Date] > '2018-12-31'
      AND [Document Type] = 1
      AND [Buy-from Vendor No_] <> ''
) h;

CREATE UNIQUE CLUSTERED INDEX IX_HeaderData_Doc
        ON #HeaderData ([Doc_No_]);

IF OBJECT_ID('tempdb..#Receipts') IS NOT NULL DROP TABLE #Receipts;

SELECT
    [Line No_]  AS line_no,
    [Order No_] AS order_no,
    [No_]       AS item_no,
    MIN([Posting Date]) AS posting_date
INTO #Receipts
FROM [dbo].[IPG Photonics Corporation$Purch_ Rcpt_ Line]
WHERE [Quantity] > 0
  AND [Posting Date] > '2017-12-31'
GROUP BY [Line No_], [Order No_], [No_];

CREATE UNIQUE CLUSTERED INDEX IX_Receipts_OrderLineItem
        ON #Receipts (order_no, line_no, item_no);

SELECT
    l.[Status]                         AS status,
    l.[Document Type]                  AS document_type,
    l.[Document No_]                   AS document_no,
    l.[Line No_]                       AS line_no,
    l.[Buy-from Vendor No_]            AS buy_from_vendor_no,
    v.[Name]                           AS vendor_name,
    CASE WHEN v.[Country_Region Code] = 'HK' THEN 'CN'
         ELSE v.[Country_Region Code] END AS vendor_country,
    l.[Type]                           AS type,
    l.[No_]                            AS item_no,
    l.[Shortcut Dimension 1 Code]      AS cost_center,
    l.[Location Code]                  AS location_code,
    l.[Expected Receipt Date]          AS expected_receipt_date,
    l.[Promised Receipt Date]          AS promised_receipt_date,
    l.[Description]                    AS description,
    l.[Qty_ per Unit of Measure]       AS qty_per_unit_of_measure,
    l.[Quantity]                       AS quantity,
    l.[Outstanding Quantity]           AS outstanding_quantity,
    r.posting_date                     AS posting_date,
    l.[Unit Cost]                      AS unit_cost,
    l.[Requested Receipt Date]         AS requested_receipt_date,
    l.[Total]                          AS total,
    l.[Planned Receipt Date]           AS planned_receipt_date,
    l.[Quantity Delivered]             AS quantity_delivered,
    h.[Order Date]                     AS order_date,
    DATEDIFF(day, h.[Order Date], l.[Promised Receipt Date]) AS promised_lead_time_days,
    DATEDIFF(day, h.[Order Date], r.posting_date)           AS actual_lead_time_days,
    h.[Assigned User ID]               AS assigned_user_id,
    h.[Order Confirmation Date]        AS order_confirmation_date,
    h.[Purchaser Code]                 AS purchaser_code,
    l.[Subsidiary]                     AS subsidiary
FROM #LineData  l
JOIN #HeaderData h
  ON l.[Document No_] = h.[Doc_No_]
LEFT JOIN #Receipts r
  ON l.[Document No_] = r.order_no
 AND l.[Line No_]     = r.line_no
 AND l.[No_]          = r.item_no
LEFT JOIN [dbo].[IPG Photonics Corporation$Vendor] v
  ON l.[Buy-from Vendor No_] = v.[No_];