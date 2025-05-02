SELECT
    [Subsidiary],
    [Entry No_],
    [Item No_],
    [Posting Date],
    [Entry Type],
    [Document No_],
    [Location Code],
    [Quantity],
    [Global Dimension 1 Code],
    [Order No_],
    [SUM_Cost_Amount_Actual_USD],
    [SUM_Cost_Amount_Expected_USD],
    [SUM_Root_Cost_Actual_USD],
    [SUM_Root_Cost_Expected_USD]
FROM dbo.item_ledger_entry_all_v
WHERE
    [Subsidiary] = 'US010'
    AND [Quantity] <> 0; 