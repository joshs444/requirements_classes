/* ─────────────────────────────────────────────────────────────
   Item master snapshot + purchasing intelligence
   (adds last_vendor_name • last_purchase_qty • last_unit_cost
          last_vendor_country • last_order_date • last_mfg_part_no
          item_index)                               ←  NEW COLUMNS
   ────────────────────────────────────────────────────────── */
SET NOCOUNT ON;

/* ============================================================
   1.  Items
   ============================================================ */
IF OBJECT_ID('tempdb..#Items') IS NOT NULL DROP TABLE #Items;

SELECT
    ROW_NUMBER() OVER (ORDER BY i.[No_], i.[Revision No_])          AS row_index,
    i.[No_]                                                         AS item_no,
    i.[Description],
    i.[Inventory Posting Group]                                     AS inventory_posting_group,
    i.[Unit Cost],
    i.[Lead Time Calculation],
    i.[Global Dimension 1 Code],
    i.[Replenishment System],
    i.[Revision No_],
    i.[Item Source],
    i.[Common Item No_],
    i.[HTS],
    i.[Item Category Code]
INTO #Items
FROM [dbo].[IPG Photonics Corporation$Item] i
WHERE i.[No_] <> '';

CREATE CLUSTERED INDEX IX_Items_No ON #Items (item_no);

/* ============================================================
   2.  9-month usage – output & purchases
   ============================================================ */
IF OBJECT_ID('tempdb..#Ledger9m') IS NOT NULL DROP TABLE #Ledger9m;

SELECT
    le.[Item No_]                                                   AS item_no,
    SUM(CASE WHEN le.[Entry Type] = 6 THEN le.[Quantity] END)       AS last_9m_output_qty,
    SUM(CASE WHEN le.[Entry Type] = 0 THEN le.[Quantity] END)       AS last_9m_purchase_qty
INTO #Ledger9m
FROM [dbo].[IPG Photonics Corporation$Item Ledger Entry] le
WHERE le.[Posting Date] >= DATEADD(MONTH, -9, CAST(GETDATE() AS date))
GROUP BY le.[Item No_];

CREATE CLUSTERED INDEX IX_Ledger9m ON #Ledger9m (item_no);

/* ============================================================
   3.  Open-PO position
   ============================================================ */
IF OBJECT_ID('tempdb..#OpenPO') IS NOT NULL DROP TABLE #OpenPO;

SELECT
    pl.[No_]                                                        AS item_no,
    SUM(pl.[Outstanding Quantity])                                  AS open_purchase_qty
INTO #OpenPO
FROM [dbo].[IPG Photonics Corporation$Purchase Line] pl
WHERE pl.[Document Type]      = 1        -- Standard PO
  AND pl.[Type]               = 2        -- Item
  AND pl.[Outstanding Quantity] > 0
  AND pl.[Quantity]           > 0
  AND pl.[Unit Cost (LCY)]    > 0
  AND pl.[Order Date]         >= '2019-01-01'
GROUP BY pl.[No_];

CREATE CLUSTERED INDEX IX_OpenPO ON #OpenPO (item_no);

/* ============================================================
   4.  PO-line fact set – only fields needed for “last purchase”
   ============================================================ */
IF OBJECT_ID('tempdb..#POLines') IS NOT NULL DROP TABLE #POLines;

SELECT
    l.[No_]                                                        AS item_no,
    v.[Name]                                                       AS vendor_name,
    /* country with HK normalised to CN */
    CASE WHEN v.[Country_Region Code] = 'HK'
         THEN 'CN' ELSE v.[Country_Region Code] END                AS vendor_country,
    h.[Order Date]                                                 AS order_date,
    l.[Document No_]                                               AS document_no,
    l.[Manufacturer Part No_]                                      AS mfg_part_no,
    SUM(l.[Quantity] * COALESCE(NULLIF(l.[Qty_ per Unit of Measure],0),1)) AS quantity,
    MAX(l.[Unit Cost (LCY)] / COALESCE(NULLIF(l.[Qty_ per Unit of Measure],0),1)) AS unit_cost
INTO #POLines
FROM (
    /* ─ Purchase history lines ─ */
    SELECT phl.[No_], phl.[Quantity], phl.[Unit Cost (LCY)],
           phl.[Qty_ per Unit of Measure], phl.[Buy-from Vendor No_],
           phl.[Document No_], phl.[Manufacturer Part No_]
    FROM [dbo].[IPG Photonics Corporation$Purchase History Line] phl
    WHERE phl.[Document Type] = 1 AND phl.[Type] = 2
      AND phl.[Quantity] > 0 AND phl.[Unit Cost (LCY)] > 0

    UNION ALL

    /* ─ Open / released PO lines ─ */
    SELECT pl.[No_], pl.[Quantity], pl.[Unit Cost (LCY)],
           pl.[Qty_ per Unit of Measure], pl.[Buy-from Vendor No_],
           pl.[Document No_], pl.[Manufacturer Part No_]
    FROM [dbo].[IPG Photonics Corporation$Purchase Line] pl
    WHERE pl.[Document Type] = 1 AND pl.[Type] = 2
      AND pl.[Quantity] > 0 AND pl.[Unit Cost (LCY)] > 0
) l
JOIN (
    /* ─ Headers supply Order Date ─ */
    SELECT [No_] AS document_no, [Order Date]
    FROM [dbo].[IPG Photonics Corporation$Purchase History Header]
    WHERE [Document Type] = 1

    UNION ALL

    SELECT [No_], [Order Date]
    FROM [dbo].[IPG Photonics Corporation$Purchase Header]
    WHERE [Document Type] = 1
) h
    ON l.[Document No_] = h.document_no
LEFT JOIN [dbo].[IPG Photonics Corporation$Vendor] v
    ON l.[Buy-from Vendor No_] = v.[No_]
GROUP BY l.[No_], v.[Name],
         CASE WHEN v.[Country_Region Code] = 'HK' THEN 'CN' ELSE v.[Country_Region Code] END,
         h.[Order Date], l.[Document No_], l.[Manufacturer Part No_];

CREATE CLUSTERED INDEX IX_POLines
    ON #POLines (item_no, order_date DESC, document_no DESC);

/* ============================================================
   5.  Most-recent PO per item
   ============================================================ */
IF OBJECT_ID('tempdb..#LastPurchase') IS NOT NULL DROP TABLE #LastPurchase;

WITH ranked AS (
    SELECT p.*,
           ROW_NUMBER() OVER (PARTITION BY p.item_no
                              ORDER BY p.order_date DESC, p.document_no DESC) AS rn
    FROM #POLines p
)
SELECT
    item_no,
    vendor_name              AS last_vendor_name,
    vendor_country           AS last_vendor_country,
    quantity                 AS last_purchase_qty,
    unit_cost                AS last_unit_cost,
    order_date               AS last_order_date,
    mfg_part_no              AS last_mfg_part_no
INTO #LastPurchase
FROM ranked
WHERE rn = 1;

CREATE CLUSTERED INDEX IX_LastPurchase ON #LastPurchase (item_no);

/* ============================================================
   6.  Assemble the item view
   ============================================================ */
WITH base AS (
    SELECT
        it.row_index,
        it.item_no,
        it.[Description],
        it.inventory_posting_group,
        it.[Unit Cost]                           AS unit_cost,
        it.[Lead Time Calculation]               AS lead_time_calculation,
        it.[Global Dimension 1 Code]             AS global_dimension_1_code,
        CASE it.[Replenishment System]
             WHEN 0 THEN 'Purchase'
             WHEN 1 THEN 'Output'
             WHEN 2 THEN 'Assembly'
             ELSE 'Unknown'
        END                                      AS replenishment_system,
        it.[Revision No_]                        AS revision_no,
        CASE CAST(it.[Item Source] AS varchar(10))
             WHEN '0' THEN ''
             WHEN '3' THEN 'Made In-House'
             WHEN '1' THEN 'Third Party Purchase'
             WHEN '2' THEN 'Interco Purchase'
             ELSE CAST(it.[Item Source] AS varchar(10))
        END                                      AS item_source,
        it.[Common Item No_]                     AS common_item_no,
        it.[HTS]                                 AS hts,
        it.[Item Category Code]                  AS item_category_code,
        ic.[Parent Category]                     AS parent_category_code,
        ic.[Description]                         AS item_category_description,
        COALESCE(ls.last_9m_output_qty, 0)       AS last_9m_output_qty,
        COALESCE(ls.last_9m_purchase_qty, 0)     AS last_9m_purchase_qty,
        COALESCE(op.open_purchase_qty, 0)        AS open_purchase_qty,
        /* replenishment decision */
        CASE
            WHEN COALESCE(op.open_purchase_qty, 0) > 0 THEN 'Buy'
            ELSE
                CASE
                    WHEN it.[Item Source] = 3 THEN 'Make'
                    WHEN it.[Item Source] IN (1,2) THEN 'Buy'
                    WHEN it.[Replenishment System] IN (1,2) THEN 'Make'
                    WHEN it.[Replenishment System] = 0 THEN 'Buy'
                    ELSE 'Unknown'
                END
                + CASE
                    WHEN COALESCE(ls.last_9m_output_qty, 0) > COALESCE(ls.last_9m_purchase_qty, 0)
                         AND it.[Item Source] NOT IN (3)
                         AND it.[Replenishment System] = 0 THEN '→Make'
                    WHEN COALESCE(ls.last_9m_purchase_qty, 0) > COALESCE(ls.last_9m_output_qty, 0)
                         AND (it.[Item Source] = 3 OR it.[Replenishment System] IN (1,2)) THEN '→Buy'
                    ELSE ''
                  END
        END                                      AS make_buy
    FROM #Items it
    LEFT JOIN [dbo].[IPG Photonics Corporation$Item Category] ic
        ON it.[Item Category Code] = ic.[Code]
    LEFT JOIN #Ledger9m ls
        ON it.item_no = ls.item_no
    LEFT JOIN #OpenPO op
        ON it.item_no = op.item_no
)
SELECT
    base.*,
    /* ─────────── NEW COLUMNS ─────────── */
    lp.last_vendor_name,
    lp.last_vendor_country,
    COALESCE(lp.last_purchase_qty, 0)         AS last_purchase_qty,
    lp.last_unit_cost                         AS last_unit_cost,
    lp.last_order_date                        AS last_order_date,
    lp.last_mfg_part_no                       AS last_mfg_part_no,
    /* raw-material flag */
    CASE
         WHEN base.inventory_posting_group = 'FIN GOODS'                    THEN 'No'
         WHEN base.make_buy = 'Buy'
              AND base.last_9m_output_qty = 0                               THEN 'Yes'
         ELSE 'No'
    END                                         AS raw_mat_flag,
    /* item index (subsidiary + item_no) */
    CONCAT('US010', base.item_no)               AS item_index
FROM base
LEFT JOIN #LastPurchase lp
    ON base.item_no = lp.item_no
ORDER BY base.row_index;