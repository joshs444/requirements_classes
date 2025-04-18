WITH SourceData AS (
    SELECT
        [Posting Date],
        [SUM_Cost_Amount_Actual],
        [SUM_Cost_Amount_Expected],
        [SUM_Root_Cost_Actual],
        [SUM_Root_Cost_Expected],
        [Quantity],
        [Subsidiary],
        [Item No_],
        [Entry Type],
        [Location Code],
        [Global Dimension 1 Code],
        CAST([Posting Date] AS DATE) AS [Posting_Date]
    FROM dbo.item_ledger_entry_all_v WITH (NOLOCK)
),
FilteredRows AS (
    SELECT
        [Posting_Date],
        [SUM_Cost_Amount_Actual],
        [SUM_Cost_Amount_Expected],
        [SUM_Root_Cost_Actual],
        [SUM_Root_Cost_Expected],
        [Quantity],
        [Subsidiary],
        [Item No_],
        [Entry Type],
        [Location Code],
        [Global Dimension 1 Code],
        [SUM_Cost_Amount_Actual] + [SUM_Cost_Amount_Expected] AS [SUM_Inventory_Cost_LC],
        [SUM_Root_Cost_Actual] + [SUM_Root_Cost_Expected] AS [SUM_Root_Cost_LC]
    FROM SourceData
    WHERE [Quantity] != 0
),
CustomRootCost AS (
    SELECT
        [Posting_Date],
        [SUM_Inventory_Cost_LC],
        [SUM_Root_Cost_LC],
        [Quantity],
        [Subsidiary],
        [Item No_],
        [Location Code],
        CASE
            WHEN [SUM_Inventory_Cost_LC] = 0 THEN 1
            ELSE ABS((COALESCE([SUM_Inventory_Cost_LC], 0) - COALESCE([SUM_Root_Cost_LC], 0)) / NULLIF([SUM_Inventory_Cost_LC], 0))
        END AS [RelativeDifference],
        CASE
            WHEN [SUM_Root_Cost_LC] IS NULL 
                OR [SUM_Root_Cost_LC] = 0
                OR ABS((COALESCE([SUM_Inventory_Cost_LC], 0) - COALESCE([SUM_Root_Cost_LC], 0)) / NULLIF([SUM_Inventory_Cost_LC], 0)) >= 1
            THEN COALESCE([SUM_Inventory_Cost_LC], 0)
            ELSE COALESCE([SUM_Root_Cost_LC], 0)
        END AS [SUM_Custom_Root_Cost],
        EOMONTH([Posting_Date]) AS [End_of_Month]
    FROM FilteredRows
    WHERE [Subsidiary] = 'US010'
      AND [Location Code] NOT LIKE '%MRB%'
),
AggregatedData AS (
    SELECT
        [Subsidiary],
        [Item No_],
        [Location Code],
        SUM([SUM_Inventory_Cost_LC]) AS [SUM_Inventory_Cost_LC],
        SUM([Quantity]) AS [Quantity]
    FROM CustomRootCost
    GROUP BY
        [Subsidiary],
        [Item No_],
        [Location Code]
    HAVING SUM([Quantity]) != 0
)
SELECT
    [Subsidiary] AS subsidiary,
    [Item No_] AS item_no,
    [Location Code] AS location_code,
    [SUM_Inventory_Cost_LC] AS inventory_cost_lc,
    [Quantity] AS quantity
FROM AggregatedData
ORDER BY item_no
OPTION (RECOMPILE);