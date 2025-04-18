SELECT
    [No_] AS item_no,
    [Sell-to Customer Name] AS customer_name,
    [Document No_] AS document_no,
    CAST([Planned Delivery Date] AS date) AS date,
    SUM([Outstanding Quantity]) AS qty
FROM dbo.stg_sales_header_booking_us_t
WHERE
    [Type] = 2
    AND [Outstanding Quantity] > 0
    AND [Planned Delivery Date] > DATEADD(MONTH, -6, GETDATE())
GROUP BY
    [No_],
    [Sell-to Customer Name],
    [Document No_],
    CAST([Planned Delivery Date] AS date)
ORDER BY item_no, document_no, date;