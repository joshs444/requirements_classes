WITH
  /* ──────────────────────────────────────────────────────────────────────
     1. Base CTEs: only include lines where Type = 2 (“Item”)
  ──────────────────────────────────────────────────────────────────────*/
  LineData AS (
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
      [No_]                   AS item_no,
      [Location Code],
      [Expected Receipt Date],
      [Promised Receipt Date],
      [Planned Receipt Date],
      [Description],
      CASE WHEN [Qty_ per Unit of Measure]=0 THEN 1 ELSE [Qty_ per Unit of Measure] END AS qty_per_uom,
      [Quantity]*CASE WHEN [Qty_ per Unit of Measure]=0 THEN 1 ELSE [Qty_ per Unit of Measure] END AS quantity,
      [Outstanding Quantity]*CASE WHEN [Qty_ per Unit of Measure]=0 THEN 1 ELSE [Qty_ per Unit of Measure] END AS outstanding_quantity,
      [Unit Cost (LCY)]/CASE WHEN [Qty_ per Unit of Measure]=0 THEN 1 ELSE [Qty_ per Unit of Measure] END AS unit_cost,
      [Requested Receipt Date],
      ([Quantity]-[Outstanding Quantity]) * ([Unit Cost (LCY)]/CASE WHEN [Qty_ per Unit of Measure]=0 THEN 1 ELSE [Qty_ per Unit of Measure] END) AS total_cost,
      ([Quantity]-[Outstanding Quantity]) * CASE WHEN [Qty_ per Unit of Measure]=0 THEN 1 ELSE [Qty_ per Unit of Measure] END AS quantity_delivered,
      'US010' AS subsidiary
    FROM [dbo].[IPG Photonics Corporation$Purchase History Line]
    WHERE
      [Order Date] >= DATEADD(year, -5, GETDATE())
      AND [Quantity] > 0
      AND [Unit Cost (LCY)] > 0
      AND [Document Type] = 1
      AND [Type] = 2                -- only “Item”
      AND [Quantity] <> [Outstanding Quantity]

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
      [No_]                   AS item_no,
      [Location Code],
      [Expected Receipt Date],
      [Promised Receipt Date],
      [Planned Receipt Date],
      [Description],
      CASE WHEN [Qty_ per Unit of Measure]=0 THEN 1 ELSE [Qty_ per Unit of Measure] END AS qty_per_uom,
      [Quantity]*CASE WHEN [Qty_ per Unit of Measure]=0 THEN 1 ELSE [Qty_ per Unit of Measure] END AS quantity,
      [Outstanding Quantity]*CASE WHEN [Qty_ per Unit of Measure]=0 THEN 1 ELSE [Qty_ per Unit of Measure] END AS outstanding_quantity,
      [Unit Cost (LCY)]/CASE WHEN [Qty_ per Unit of Measure]=0 THEN 1 ELSE [Qty_ per Unit of Measure] END AS unit_cost,
      [Requested Receipt Date],
      [Quantity]*([Unit Cost (LCY)]/CASE WHEN [Qty_ per Unit of Measure]=0 THEN 1 ELSE [Qty_ per Unit of Measure] END) AS total_cost,
      ([Quantity]-[Outstanding Quantity])*CASE WHEN [Qty_ per Unit of Measure]=0 THEN 1 ELSE [Qty_ per Unit of Measure] END AS quantity_delivered,
      'US010' AS subsidiary
    FROM [dbo].[IPG Photonics Corporation$Purchase Line]
    WHERE
      [Order Date] >= DATEADD(year, -5, GETDATE())
      AND [Quantity] > 0
      AND [Unit Cost (LCY)] > 0
      AND [Document Type] = 1
      AND [Type] = 2                -- only “Item”
  ),

  HeaderData AS (
    SELECT
      [Document Type],
      [No_]           AS document_no,
      [Order Date],
      [Posting Date],
      [Assigned User ID],
      [Order Confirmation Date],
      [Purchaser Code],
      'US010'         AS subsidiary
    FROM [dbo].[IPG Photonics Corporation$Purchase History Header]
    WHERE
      [Order Date] >= DATEADD(year, -5, GETDATE())
      AND [Document Type] = 1
      AND [Buy-from Vendor No_] <> ''

    UNION ALL

    SELECT
      [Document Type],
      [No_]           AS document_no,
      [Order Date],
      [Posting Date],
      [Assigned User ID],
      [Order Confirmation Date],
      [Purchaser Code],
      'US010'         AS subsidiary
    FROM [dbo].[IPG Photonics Corporation$Purchase Header]
    WHERE
      [Order Date] >= DATEADD(year, -5, GETDATE())
      AND [Document Type] = 1
      AND [Buy-from Vendor No_] <> ''
  ),

  ReceiptTable AS (
    SELECT
      [Order No_]   AS order_no,
      [Line No_]    AS line_no,
      [No_]         AS item_no,
      [Posting Date]
    FROM (
      SELECT
        [Order No_],
        [Line No_],
        [No_],
        [Posting Date],
        ROW_NUMBER() OVER (PARTITION BY [Order No_],[Line No_],[No_]
                           ORDER BY [Posting Date]) AS rn
      FROM [dbo].[IPG Photonics Corporation$Purch_ Rcpt_ Line]
      WHERE
        [Quantity] > 0
        AND [Posting Date] >= DATEADD(year, -5, GETDATE())
    ) AS sub
    WHERE rn = 1
  ),

  /* ──────────────────────────────────────────────────────────────────────
     2. LeadTimeFact: promised vs actual + per-group median, with additional columns
  ──────────────────────────────────────────────────────────────────────*/
  LeadTimeFact AS (
    SELECT
      L.[Buy-from Vendor No_]                                      AS vendor_no,
      V.[Name]                                                     AS vendor_name,
      CASE WHEN V.[Country_Region Code]='HK' THEN 'CN'
           ELSE V.[Country_Region Code] END                        AS vendor_country,
      L.item_no                                                    AS item_no,
      L.[Description]                                              AS item_desc,
      DATEDIFF(DAY, H.[Order Date], L.[Promised Receipt Date])     AS promised_lead_days,
      DATEDIFF(DAY, H.[Order Date], R.[Posting Date])              AS actual_lead_days,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DATEDIFF(DAY,H.[Order Date],R.[Posting Date]))
        OVER (
          PARTITION BY L.[Buy-from Vendor No_], L.item_no
        )                                                          AS median_actual_days,
      L.quantity                                                   AS quantity,
      H.[Order Date]                                               AS order_date,
      L.[Document No_]                                             AS document_no
    FROM LineData L
    JOIN HeaderData H
      ON L.[Document No_] = H.document_no
    LEFT JOIN ReceiptTable R
      ON L.[Document No_] = R.order_no
      AND L.[Line No_] = R.line_no
      AND L.item_no = R.item_no
    LEFT JOIN [dbo].[IPG Photonics Corporation$Vendor] V
      ON L.[Buy-from Vendor No_] = V.[No_]
    WHERE
      R.[Posting Date] IS NOT NULL
      AND DATEDIFF(DAY, H.[Order Date], R.[Posting Date]) >= 0
  ),

  /* ──────────────────────────────────────────────────────────────────────
     3. VendorCountPerItem: number of unique vendors per item
  ──────────────────────────────────────────────────────────────────────*/
  VendorCountPerItem AS (
    SELECT
      item_no,
      COUNT(DISTINCT vendor_no) AS vendor_count
    FROM LeadTimeFact
    GROUP BY item_no
  ),

  /* ──────────────────────────────────────────────────────────────────────
     4. ItemVendorLeadTime: final KPIs per Item×Vendor with new metrics
  ──────────────────────────────────────────────────────────────────────*/
  ItemVendorLeadTime AS (
    SELECT
      vendor_no,
      vendor_name,
      vendor_country,
      item_no,
      item_desc,
      COUNT(*)                                                  AS po_lines,
      AVG(CAST(promised_lead_days AS decimal(10,2)))            AS avg_promised_days,
      AVG(CAST(actual_lead_days AS decimal(10,2)))              AS avg_actual_days,
      MAX(median_actual_days)                                   AS median_actual_days,
      STDEV(actual_lead_days)                                   AS sd_actual_days,
      MIN(actual_lead_days)                                     AS min_actual_days,
      MAX(actual_lead_days)                                     AS max_actual_days,
      100.0 * SUM(CASE WHEN actual_lead_days <= promised_lead_days THEN 1 ELSE 0 END)
            / COUNT(*)                                          AS pct_on_time,
      AVG(quantity)                                             AS avg_order_quantity,
      COUNT(DISTINCT CASE WHEN order_date >= DATEADD(year, -1, GETDATE()) THEN document_no END) AS orders_past_year,
      SUM(CASE WHEN order_date >= DATEADD(year, -1, GETDATE()) THEN quantity ELSE 0 END) AS total_quantity_past_year,
      AVG(CASE WHEN YEAR(order_date) = YEAR(GETDATE()) THEN actual_lead_days END) AS avg_actual_days_current_year,
      AVG(CASE WHEN YEAR(order_date) = YEAR(GETDATE()) - 1 THEN actual_lead_days END) AS avg_actual_days_previous_year
    FROM LeadTimeFact
    GROUP BY vendor_no, vendor_name, vendor_country, item_no, item_desc
  )

/* ──────────────────────────────────────────────────────────────────────
   5. Final select with all metrics including vendor count
──────────────────────────────────────────────────────────────────────*/
SELECT
  IV.*,
  VC.vendor_count
FROM ItemVendorLeadTime IV
LEFT JOIN VendorCountPerItem VC
  ON IV.item_no = VC.item_no
ORDER BY IV.vendor_no, IV.item_no;