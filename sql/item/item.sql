SET NOCOUNT ON;

IF OBJECT_ID('tempdb..#Items') IS NOT NULL DROP TABLE #Items;

SELECT
    ROW_NUMBER() OVER (ORDER BY i.[No_], i.[Revision No_]) AS row_index,
    i.[No_],
    i.[Description],
    i.[Inventory Posting Group],
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
FROM [dbo].[IPG Photonics Corporation$Item] AS i
WHERE i.[No_] <> '';

CREATE CLUSTERED INDEX IX_Items_No ON #Items ([No_]);

IF OBJECT_ID('tempdb..#Ledger9m') IS NOT NULL DROP TABLE #Ledger9m;

SELECT
    le.[Item No_],
    SUM(CASE WHEN le.[Entry Type] = 6 THEN le.[Quantity] ELSE 0 END) AS last_9m_output_qty,
    SUM(CASE WHEN le.[Entry Type] = 0 THEN le.[Quantity] ELSE 0 END) AS last_9m_purchase_qty
INTO #Ledger9m
FROM [dbo].[IPG Photonics Corporation$Item Ledger Entry] AS le
WHERE le.[Posting Date] >= DATEADD(MONTH, -9, CAST(GETDATE() AS date))
GROUP BY le.[Item No_];

CREATE CLUSTERED INDEX IX_Ledger9m_Item ON #Ledger9m ([Item No_]);

IF OBJECT_ID('tempdb..#OpenPO') IS NOT NULL DROP TABLE #OpenPO;

SELECT
    pl.[No_],
    SUM(pl.[Outstanding Quantity]) AS open_purchase_qty
INTO #OpenPO
FROM [dbo].[IPG Photonics Corporation$Purchase Line] AS pl
WHERE pl.[Document Type] = 1
  AND pl.[Type] = 2
  AND pl.[Outstanding Quantity] > 0
  AND pl.[Quantity] > 0
  AND pl.[Unit Cost (LCY)] > 0
  AND pl.[Order Date] >= '2019-01-01'
GROUP BY pl.[No_];

CREATE CLUSTERED INDEX IX_OpenPO_No ON #OpenPO ([No_]);

WITH base AS (
SELECT
    it.row_index,
    it.[No_] AS item_no,
    it.[Description] AS description,
    it.[Inventory Posting Group] AS inventory_posting_group,
    it.[Unit Cost] AS unit_cost,
    it.[Lead Time Calculation] AS lead_time_calculation,
    it.[Global Dimension 1 Code] AS global_dimension_1_code,
    CASE it.[Replenishment System]
         WHEN 0 THEN 'Purchase'
         WHEN 1 THEN 'Output'
         WHEN 2 THEN 'Assembly'
         ELSE 'Unknown'
    END AS replenishment_system,
    it.[Revision No_] AS revision_no,
    CASE CAST(it.[Item Source] AS varchar(10))
         WHEN '0' THEN ''
         WHEN '3' THEN 'Made In-House'
         WHEN '1' THEN 'Third Party Purchase'
         WHEN '2' THEN 'Interco Purchase'
         ELSE CAST(it.[Item Source] AS varchar(10))
    END AS item_source,
    it.[Common Item No_] AS common_item_no,
    it.[HTS] AS hts,
    it.[Item Category Code] AS item_category_code,
    ic.[Parent Category] AS parent_category_code,
    ic.[Description] AS item_category_description,
    COALESCE(ls.last_9m_output_qty, 0) AS last_9m_output_qty,
    COALESCE(ls.last_9m_purchase_qty, 0) AS last_9m_purchase_qty,
    COALESCE(op.open_purchase_qty, 0) AS open_purchase_qty,
    COALESCE(ls.last_9m_purchase_qty, 0) AS purchase,
    COALESCE(ls.last_9m_output_qty, 0) AS output,
    COALESCE(op.open_purchase_qty, 0) AS [open],
    CASE
        WHEN COALESCE(op.open_purchase_qty, 0) > 0 THEN 'Purchase'
        ELSE
             CASE
                 WHEN it.[Item Source] = 3 THEN 'Output'
                 WHEN it.[Item Source] IN (1, 2) THEN 'Purchase'
                 WHEN it.[Replenishment System] IN (1, 2) THEN 'Output'
                 WHEN it.[Replenishment System] = 0 THEN 'Purchase'
                 ELSE 'Unknown'
             END
             + CASE
                 WHEN COALESCE(ls.last_9m_output_qty, 0) > COALESCE(ls.last_9m_purchase_qty, 0)
                      AND it.[Item Source] NOT IN (3)
                      AND it.[Replenishment System] = 0 THEN '→Output'
                 WHEN COALESCE(ls.last_9m_purchase_qty, 0) > COALESCE(ls.last_9m_output_qty, 0)
                      AND (it.[Item Source] = 3 OR it.[Replenishment System] IN (1, 2)) THEN '→Purchase'
                 ELSE ''
               END
    END AS purchase_output
FROM #Items it
LEFT JOIN [dbo].[IPG Photonics Corporation$Item Category] ic
       ON it.[Item Category Code] = ic.[Code]
LEFT JOIN #Ledger9m ls
       ON it.[No_] = ls.[Item No_]
LEFT JOIN #OpenPO op
       ON it.[No_] = op.[No_]
)
SELECT
    base.*,
    CASE
        WHEN base.inventory_posting_group = 'FIN GOODS' THEN 'No'
        WHEN base.purchase_output = 'Purchase'
             AND base.last_9m_output_qty = 0 THEN 'Yes'
        ELSE 'No'
    END AS raw_mat_flag
FROM base
ORDER BY base.row_index;
