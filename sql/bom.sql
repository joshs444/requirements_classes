SELECT
    p.[Production BOM No_] AS production_bom_no,
    p.[No_] AS component_no,
    SUM(p.[Quantity per]) AS total
FROM
    [dbo].[IPG Photonics Corporation$Production BOM Line] p
INNER JOIN
    [dbo].[IPG Photonics Corporation$Item] i
    ON p.[Production BOM No_] = i.[No_]
    AND p.[Version Code] = i.[Revision No_]
WHERE
    p.[Quantity per] != 0
    AND p.[No_] IS NOT NULL
GROUP BY
    p.[Production BOM No_],
    p.[No_]
OPTION (RECOMPILE);