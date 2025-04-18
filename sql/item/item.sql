WITH ItemCTE AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY [No_], [Revision No_]) AS [RowIndex],
        [No_],
        [Description],
        [Inventory Posting Group],
        [Unit Cost],
        [Lead Time Calculation],
        [Global Dimension 1 Code],
        [Replenishment System],
        [Revision No_],
        [Item Source],
        [Common Item No_],
        [HTS],
        [Item Category Code]
    FROM 
        [dbo].[IPG Photonics Corporation$Item]
    WHERE 
        [No_] <> ''
),
LedgerSummary AS (
    SELECT 
        [Item No_],
        SUM(CASE WHEN [Entry Type] = 6 THEN [Quantity] ELSE 0 END) AS last_9m_output_qty,
        SUM(CASE WHEN [Entry Type] = 0 THEN [Quantity] ELSE 0 END) AS last_9m_purchase_qty
    FROM 
        [dbo].[IPG Photonics Corporation$Item Ledger Entry]
    WHERE 
        [Posting Date] >= DATEADD(MONTH, -9, GETDATE())
    GROUP BY 
        [Item No_]
),
PurchaseSummary AS (
    SELECT 
        [No_],
        SUM([Outstanding Quantity]) AS open_purchase_qty
    FROM 
        [dbo].[IPG Photonics Corporation$Purchase Line]
    WHERE 
        [Document Type] = 1
        AND [Type] = 2
        AND [Outstanding Quantity] > 0
        AND [Quantity] > 0
        AND [Unit Cost (LCY)] > 0
        AND [Order Date] > '2019-01-01'
    GROUP BY 
        [No_]
)
SELECT
    i.[RowIndex] AS row_index,
    i.[No_] AS item_no,
    i.[Description] AS description,
    i.[Inventory Posting Group] AS inventory_posting_group,
    i.[Unit Cost] AS unit_cost,
    i.[Lead Time Calculation] AS lead_time_calculation,
    i.[Global Dimension 1 Code] AS global_dimension_1_code,
    CASE
        WHEN i.[Replenishment System] = 0 THEN 'Purchase'
        WHEN i.[Replenishment System] = 1 THEN 'Output'
        WHEN i.[Replenishment System] = 2 THEN 'Assembly'
        ELSE 'Unknown'
    END AS replenishment_system,
    i.[Revision No_] AS revision_no,
    CASE CAST(i.[Item Source] AS varchar(10))
        WHEN '0' THEN ''
        WHEN '3' THEN 'Made In-House'
        WHEN '1' THEN 'Third Party Purchase'
        WHEN '2' THEN 'Interco Purchase'
        ELSE CAST(i.[Item Source] AS varchar(10))
    END AS item_source,
    i.[Common Item No_] AS common_item_no,
    i.[HTS] AS hts,
    i.[Item Category Code] AS item_category_code,
    ic.[Parent Category] AS parent_category_code,
    ic.[Description] AS item_category_description,
    COALESCE(ls.last_9m_output_qty, 0) AS last_9m_output_qty,
    COALESCE(ls.last_9m_purchase_qty, 0) AS last_9m_purchase_qty,
    COALESCE(ps.open_purchase_qty, 0) AS open_purchase_qty,
    COALESCE(ls.last_9m_purchase_qty, 0) AS purchase,
    COALESCE(ls.last_9m_output_qty, 0) AS output,
    COALESCE(ps.open_purchase_qty, 0) AS [open],
    CASE
        WHEN COALESCE(ps.open_purchase_qty, 0) > 0 THEN 'Purchase'
        WHEN (
            CASE
                WHEN i.[Item Source] = 3 THEN 'Output'
                WHEN i.[Item Source] IN (1, 2) THEN 'Purchase'
                ELSE CASE
                    WHEN i.[Replenishment System] IN (1, 2) THEN 'Output'
                    WHEN i.[Replenishment System] = 0 THEN 'Purchase'
                    ELSE 'Unknown'
                END
            END
        ) = 'Purchase' AND COALESCE(ls.last_9m_output_qty, 0) > COALESCE(ls.last_9m_purchase_qty, 0) THEN 'Output'
        WHEN (
            CASE
                WHEN i.[Item Source] = 3 THEN 'Output'
                WHEN i.[Item Source] IN (1, 2) THEN 'Purchase'
                ELSE CASE
                    WHEN i.[Replenishment System] IN (1, 2) THEN 'Output'
                    WHEN i.[Replenishment System] = 0 THEN 'Purchase'
                    ELSE 'Unknown'
                END
            END
        ) = 'Output' AND COALESCE(ls.last_9m_purchase_qty, 0) > COALESCE(ls.last_9m_output_qty, 0) THEN 'Purchase'
        ELSE 
            CASE
                WHEN i.[Item Source] = 3 THEN 'Output'
                WHEN i.[Item Source] IN (1, 2) THEN 'Purchase'
                ELSE CASE
                    WHEN i.[Replenishment System] IN (1, 2) THEN 'Output'
                    WHEN i.[Replenishment System] = 0 THEN 'Purchase'
                    ELSE 'Unknown'
                END
            END
    END AS purchase_output
FROM 
    ItemCTE i
LEFT JOIN 
    [dbo].[IPG Photonics Corporation$Item Category] ic ON i.[Item Category Code] = ic.[Code]
LEFT JOIN 
    LedgerSummary ls ON i.[No_] = ls.[Item No_]
LEFT JOIN 
    PurchaseSummary ps ON i.[No_] = ps.[No_]
ORDER BY 
    i.[RowIndex];