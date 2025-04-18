WITH ReceiptTable AS (
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
            ROW_NUMBER() OVER (
                PARTITION BY [Line No_], [Order No_], [No_] 
                ORDER BY [Posting Date]
            ) AS RowNumber
        FROM [dbo].[IPG Photonics Corporation$Purch_ Rcpt_ Line]
        WHERE [Quantity] > 0
            AND [Posting Date] >= '2018-01-01'  -- Changed from YEAR filter to direct date comparison
    ) AS InnerQuery
    WHERE RowNumber = 1
)
SELECT
    h.[Document No_] AS document_no,
    h.[Line No_] AS line_no,
    h.[Buy-from Vendor No_] AS buy_from_vendor_no,
    h.[No_] AS item_no,
    h.[Shortcut Dimension 1 Code] AS cost_center,
    h.[Location Code] AS location_code,
    h.[Quantity] AS quantity,
    h.[Outstanding Quantity] AS outstanding_quantity,
    h.[Unit Cost (LCY)] AS unit_cost,
    h.[Quantity] * h.[Unit Cost (LCY)] AS total,
    h.[Expected Receipt Date] AS expected_receipt_date,
    h.[Promised Receipt Date] AS promised_receipt_date,
    h.[Order Date] AS order_date,
    r.posting_date AS receipt_posting_date,
    'HISTORY' AS status
FROM [dbo].[IPG Photonics Corporation$Purchase History Line] h
LEFT JOIN ReceiptTable r
    ON h.[Document No_] = r.order_no 
    AND h.[Line No_] = r.line_no 
    AND h.[No_] = r.item_no
WHERE h.[Document Type] = 1
    AND h.[Type] = 2
    AND h.[Quantity] > 0
    AND h.[Unit Cost (LCY)] > 0
    AND h.[Order Date] > '2019-01-01';