/*───────────────────────────────────────────────────────────────
  Filtered BOM roll‑up  +  last country purchased
  – keeps only Production BOMs
  – adds parent / component descriptions
  – adds last _vendor country_ for each component
  ─────────────────────────────────────────────────────────────*/

/*==============================================================
  1.  BOM lines we care about (all Production BOMs)
  ==============================================================*/
WITH bom AS (
    SELECT  p.[Production BOM No_]              AS production_bom_no,
            p.[No_]                             AS component_no,
            SUM(p.[Quantity per])               AS total_qty
    FROM    [dbo].[IPG Photonics Corporation$Production BOM Line] p
    WHERE   p.[Quantity per] <> 0
      AND   p.[No_]                IS NOT NULL
      AND   p.[Production BOM No_] <> p.[No_]
    GROUP BY p.[Production BOM No_], p.[No_]
),

/*==============================================================
  2.  Most‑recent purchase country for every component
      (history + open PO lines, HK → CN normalisation)
  ==============================================================*/
polines AS (   /* raw PO lines with order date & country */
    SELECT  l.[No_]  AS item_no,
            CASE WHEN v.[Country_Region Code] = 'HK'
                 THEN 'CN' ELSE v.[Country_Region Code] END AS vendor_country,
            h.[Order Date]  AS order_date,
            l.[Document No_],
            ROW_NUMBER() OVER (PARTITION BY l.[No_]
                               ORDER BY h.[Order Date] DESC,
                                        l.[Document No_] DESC)        AS rn
    FROM  ( /* history lines */
            SELECT phl.[No_], phl.[Buy-from Vendor No_], phl.[Document No_]
            FROM   [dbo].[IPG Photonics Corporation$Purchase History Line] phl
            WHERE  phl.[Document Type] = 1
              AND  phl.[Type]         = 2        -- item
              AND  phl.[Quantity]     > 0
              AND  phl.[Unit Cost (LCY)] > 0
            UNION ALL
            /* open / released PO lines */
            SELECT pl.[No_],  pl.[Buy-from Vendor No_], pl.[Document No_]
            FROM   [dbo].[IPG Photonics Corporation$Purchase Line] pl
            WHERE  pl.[Document Type] = 1
              AND  pl.[Type]         = 2
              AND  pl.[Quantity]     > 0
              AND  pl.[Unit Cost (LCY)] > 0
          ) AS l
    JOIN  ( /* headers → order date */
            SELECT [No_] AS document_no, [Order Date]
            FROM   [dbo].[IPG Photonics Corporation$Purchase History Header]
            WHERE  [Document Type] = 1
            UNION ALL
            SELECT [No_], [Order Date]
            FROM   [dbo].[IPG Photonics Corporation$Purchase Header]
            WHERE  [Document Type] = 1
          ) AS h
          ON l.[Document No_] = h.document_no
    LEFT JOIN [dbo].[IPG Photonics Corporation$Vendor] v
          ON l.[Buy-from Vendor No_] = v.[No_]
),
last_country AS (   /* keep the latest row per item */
    SELECT item_no,
           vendor_country AS last_vendor_country
    FROM   polines
    WHERE  rn = 1
)

/*==============================================================
  3.  Final view  –  parent & component descriptions + country
  ==============================================================*/
SELECT  b.production_bom_no,
        ip.[Description]                 AS production_bom_description,
        b.component_no,
        ic.[Description]                 AS component_description,
        b.total_qty                      AS component_qty_per,
        lc.last_vendor_country
FROM    bom b
LEFT JOIN [dbo].[IPG Photonics Corporation$Item]  ip
       ON b.production_bom_no = ip.[No_]          -- parent description
LEFT JOIN [dbo].[IPG Photonics Corporation$Item]  ic
       ON b.component_no      = ic.[No_]          -- component description
LEFT JOIN last_country lc
       ON b.component_no      = lc.item_no        -- last purchase country
ORDER BY b.production_bom_no, b.component_no;
