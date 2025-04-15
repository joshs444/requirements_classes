WITH LineData AS (
    SELECT
        'HISTORY' AS [Status],
        [Document Type],
        [Document No_],
        [Line No_],
        [Shortcut Dimension 1 Code],
        [Buy-from Vendor No_],
        CASE [Type]
            WHEN 1 THEN 'GL'
            WHEN 2 THEN 'Item'
            WHEN 4 THEN 'FA'
        END AS [Type],
        [No_],
        [Location Code],
        [Expected Receipt Date],
        [Promised Receipt Date],
        [Planned Receipt Date],
        [Description],
        CASE WHEN [Qty_ per Unit of Measure] = 0 THEN 1 ELSE [Qty_ per Unit of Measure] END AS [Qty_ per Unit of Measure],
        [Quantity] * CASE WHEN [Qty_ per Unit of Measure] = 0 THEN 1 ELSE [Qty_ per Unit of Measure] END AS [Quantity],
        [Outstanding Quantity] * CASE WHEN [Qty_ per Unit of Measure] = 0 THEN 1 ELSE [Qty_ per Unit of Measure] END AS [Outstanding Quantity],
        [Unit Cost (LCY)] / CASE WHEN [Qty_ per Unit of Measure] = 0 THEN 1 ELSE [Qty_ per Unit of Measure] END AS [Unit Cost],
        [Requested Receipt Date],
        ([Quantity] - [Outstanding Quantity]) * ([Unit Cost (LCY)] / CASE WHEN [Qty_ per Unit of Measure] = 0 THEN 1 ELSE [Qty_ per Unit of Measure] END) AS [Total],
        ([Quantity] - [Outstanding Quantity]) * CASE WHEN [Qty_ per Unit of Measure] = 0 THEN 1 ELSE [Qty_ per Unit of Measure] END AS [Quantity Delivered],
        'US010' AS [Subsidiary]
    FROM
        [dbo].[IPG Photonics Corporation$Purchase History Line]
    WHERE
        [Order Date] > '2019-01-01'
        AND [Quantity] > 0
        AND [Unit Cost (LCY)] > 0
        AND [Document Type] = 1
        AND [Type] IN (1, 2, 4)
        AND [Quantity] - [Outstanding Quantity] <> 0
    UNION ALL
    SELECT
        'OPEN' AS [Status],
        [Document Type],
        [Document No_],
        [Line No_],
        [Shortcut Dimension 1 Code],
        [Buy-from Vendor No_],
        CASE [Type]
            WHEN 1 THEN 'GL'
            WHEN 2 THEN 'Item'
            WHEN 4 THEN 'FA'
        END AS [Type],
        [No_],
        [Location Code],
        [Expected Receipt Date],
        [Promised Receipt Date],
        [Planned Receipt Date],
        [Description],
        CASE WHEN [Qty_ per Unit of Measure] = 0 THEN 1 ELSE [Qty_ per Unit of Measure] END,
        [Quantity] * CASE WHEN [Qty_ per Unit of Measure] = 0 THEN 1 ELSE [Qty_ per Unit of Measure] END,
        [Outstanding Quantity] * CASE WHEN [Qty_ per Unit of Measure] = 0 THEN 1 ELSE [Qty_ per Unit of Measure] END,
        [Unit Cost (LCY)] / CASE WHEN [Qty_ per Unit of Measure] = 0 THEN 1 ELSE [Qty_ per Unit of Measure] END,
        [Requested Receipt Date],
        [Quantity] * ([Unit Cost (LCY)] / CASE WHEN [Qty_ per Unit of Measure] = 0 THEN 1 ELSE [Qty_ per Unit of Measure] END) AS [Total],
        ([Quantity] - [Outstanding Quantity]) * CASE WHEN [Qty_ per Unit of Measure] = 0 THEN 1 ELSE [Qty_ per Unit of Measure] END,
        'US010' AS [Subsidiary]
    FROM
        [dbo].[IPG Photonics Corporation$Purchase Line]
    WHERE
        [Order Date] > '2019-01-01'
        AND [Quantity] > 0
        AND [Unit Cost (LCY)] > 0
        AND [Document Type] = 1
        AND [Type] IN (1, 2, 4)
),
HeaderData AS (
    SELECT
        [Document Type],
        [No_],
        [Order Date],
        [Posting Date],
        [Assigned User ID],
        [Order Confirmation Date],
        [Purchaser Code],
        'US010' AS [Subsidiary]
    FROM
        [dbo].[IPG Photonics Corporation$Purchase History Header]
    WHERE
        [Order Date] > '2018-12-31'
        AND [Document Type] = 1
        AND [Buy-from Vendor No_] <> ''
    UNION ALL
    SELECT
        [Document Type],
        [No_],
        [Order Date],
        [Posting Date],
        [Assigned User ID],
        [Order Confirmation Date],
        [Purchaser Code],
        'US010' AS [Subsidiary]
    FROM
        [dbo].[IPG Photonics Corporation$Purchase Header]
    WHERE
        [Order Date] > '2018-12-31'
        AND [Document Type] = 1
        AND [Buy-from Vendor No_] <> ''
),
ReceiptTable AS (
    SELECT 
        [Line No_] AS line_no,
        [Order No_] AS order_no,
        [No_] AS item_no,
        [Posting Date] AS posting_date
    FROM (
        SELECT 
            [Line No_],
            [Order No_],
            [No_],
            [Posting Date],
            ROW_NUMBER() OVER (PARTITION BY [Line No_], [Order No_], [No_] ORDER BY [Posting Date]) AS RowNumber
        FROM [dbo].[IPG Photonics Corporation$Purch_ Rcpt_ Line]
        WHERE [Quantity] > 0 AND [Posting Date] > '2017-12-31'
    ) AS InnerQuery
    WHERE RowNumber = 1
)
SELECT
    LineData.[Status] AS status,
    LineData.[Document Type] AS document_type,
    LineData.[Document No_] AS document_no,
    LineData.[Line No_] AS line_no,
    LineData.[Buy-from Vendor No_] AS buy_from_vendor_no,
    Vendor.[Name] AS vendor_name,
    CASE WHEN Vendor.[Country_Region Code] = 'HK' THEN 'CN' ELSE Vendor.[Country_Region Code] END AS vendor_country,
    LineData.[Type] AS type,
    LineData.[No_] AS item_no,
    LineData.[Shortcut Dimension 1 Code] AS cost_center,
    LineData.[Location Code] AS location_code,
    LineData.[Expected Receipt Date] AS expected_receipt_date,
    LineData.[Promised Receipt Date] AS promised_receipt_date,
    LineData.[Description] AS description,
    LineData.[Qty_ per Unit of Measure] AS qty_per_unit_of_measure,
    LineData.[Quantity] AS quantity,
    LineData.[Outstanding Quantity] AS outstanding_quantity,
    ReceiptTable.posting_date,
    LineData.[Unit Cost] AS unit_cost,
    LineData.[Requested Receipt Date] AS requested_receipt_date,
    LineData.[Total] AS total,
    LineData.[Planned Receipt Date] AS planned_receipt_date,
    LineData.[Quantity Delivered] AS quantity_delivered,
    HeaderData.[Order Date] AS order_date,
    HeaderData.[Assigned User ID] AS assigned_user_id,
    HeaderData.[Order Confirmation Date] AS order_confirmation_date,
    HeaderData.[Purchaser Code] AS purchaser_code,
    LineData.[Subsidiary] AS subsidiary
FROM
    LineData
JOIN HeaderData
    ON LineData.[Document No_] = HeaderData.[No_]
LEFT JOIN ReceiptTable
    ON LineData.[Document No_] = ReceiptTable.order_no 
    AND LineData.[Line No_] = ReceiptTable.line_no 
    AND LineData.[No_] = ReceiptTable.item_no
LEFT JOIN [dbo].[IPG Photonics Corporation$Vendor] AS Vendor
    ON LineData.[Buy-from Vendor No_] = Vendor.[No_]
ORDER BY
    document_no, line_no;