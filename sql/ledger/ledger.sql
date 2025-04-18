SELECT
    vile.[Entry No_] AS entry_no,
    vile.[Item No_] AS item_no,
    vile.[Posting Date] AS posting_date,
    vile.[Entry Type] AS entry_type,
    vile.[Document No_] AS document_no,
    vile.[Location Code] AS location_code,
    vile.[Quantity] AS quantity,
    vile.[Global Dimension 1 Code] AS cost_center
FROM
    [dbo].[IPG Photonics Corporation$Item Ledger Entry] vile WITH (NOLOCK)
WHERE
    vile.[Posting Date] >= '2022-01-01'
OPTION (RECOMPILE);