SELECT
    [Document No_] AS document_no,
    [Line No_] AS line_no,
    [Buy-from Vendor No_] AS buy_from_vendor_no,
    [No_] AS item_no,
    [Shortcut Dimension 1 Code] AS cost_center,
    [Location Code] AS location_code,
    [Quantity] AS quantity,
    [Outstanding Quantity] AS outstanding_quantity,
    [Unit Cost (LCY)] AS unit_cost,
    [Quantity] * [Unit Cost (LCY)] AS total,
    [Expected Receipt Date] AS expected_receipt_date,
    [Promised Receipt Date] AS promised_receipt_date,
    [Order Date] AS order_date,
    'OPEN' AS status
FROM
    [dbo].[IPG Photonics Corporation$Purchase Line] WITH (NOLOCK)
WHERE
    [Document Type] = 1
    AND [Type] = 2
    AND [Outstanding Quantity] > 0
    AND [Quantity] > 0
    AND [Unit Cost (LCY)] > 0
    AND [Order Date] > '2019-01-01'
ORDER BY
    document_no, line_no
OPTION (RECOMPILE, MAXDOP 1);