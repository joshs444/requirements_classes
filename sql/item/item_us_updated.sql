/* ──────────────────────────────────────────────────────────────────────────
   ITEM MASTER SNAPSHOT  –  Performance-tuned + alias-fix
   (single ILE scan · OPEN-ORDER-AWARE ratios · OUTER APPLY last purchase)
   ────────────────────────────────────────────────────────────────────────── */
SET NOCOUNT ON;

/* ==========================================================
   0  Date helpers
   ========================================================== */
DECLARE @Today     date = CAST(GETDATE() AS date);
DECLARE @Start3m   date = DATEADD(MONTH ,-3 ,@Today);
DECLARE @Start6m   date = DATEADD(MONTH ,-6 ,@Today);
DECLARE @Start9m   date = DATEADD(MONTH ,-9 ,@Today);
DECLARE @Start12m  date = DATEADD(MONTH ,-12,@Today);

DECLARE @Days3m  int = DATEDIFF(DAY,@Start3m ,@Today);
DECLARE @Days6m  int = DATEDIFF(DAY,@Start6m ,@Today);

/* ==========================================================
   1  Items
   ========================================================== */
IF OBJECT_ID('tempdb..#Items') IS NOT NULL DROP TABLE #Items;

SELECT ROW_NUMBER() OVER (ORDER BY i.[No_],i.[Revision No_])  AS row_index,
       i.[No_]                                               AS item_no,
       i.[Description],
       i.[Inventory Posting Group]                           AS inventory_posting_group,
       i.[Unit Cost],
       i.[Lead Time Calculation],
       i.[Global Dimension 1 Code],
       i.[Replenishment System],
       i.[Revision No_],
       i.[Item Source],
       i.[Common Item No_],
       i.[HTS],
       i.[Item Category Code]
INTO   #Items
FROM   dbo.[IPG Photonics Corporation$Item] i
WHERE  i.[No_]<> '';

CREATE CLUSTERED INDEX IX_Items ON #Items(item_no);

/* ==========================================================
   2  Ledger history – single scan
   ========================================================== */
IF OBJECT_ID('tempdb..#Ledger') IS NOT NULL DROP TABLE #Ledger;

SELECT le.[Item No_]                                         AS item_no,
       /* 3-, 9-, 12-month windows */
       SUM(CASE WHEN le.[Entry Type]=6 AND le.[Posting Date]>=@Start3m  THEN le.[Quantity] END) AS last_3m_output_qty,
       SUM(CASE WHEN le.[Entry Type]=0 AND le.[Posting Date]>=@Start3m  THEN le.[Quantity] END) AS last_3m_purchase_qty,
       SUM(CASE WHEN le.[Entry Type]=6 AND le.[Posting Date]>=@Start9m  THEN le.[Quantity] END) AS last_9m_output_qty,
       SUM(CASE WHEN le.[Entry Type]=0 AND le.[Posting Date]>=@Start9m  THEN le.[Quantity] END) AS last_9m_purchase_qty,
       SUM(CASE WHEN le.[Entry Type]=6 AND le.[Posting Date]>=@Start12m THEN le.[Quantity] END) AS last_12m_output_qty,
       SUM(CASE WHEN le.[Entry Type]=0 AND le.[Posting Date]>=@Start12m THEN le.[Quantity] END) AS last_12m_purchase_qty,
       /* inventory snapshot */
       SUM(le.[Quantity])                                    AS inventory_qty
INTO   #Ledger
FROM   dbo.[IPG Photonics Corporation$Item Ledger Entry] le
GROUP  BY le.[Item No_];

CREATE CLUSTERED INDEX IX_Ledger ON #Ledger(item_no);

/* ==========================================================
   3  Open purchase orders
   ========================================================== */
IF OBJECT_ID('tempdb..#OpenPO') IS NOT NULL DROP TABLE #OpenPO;

SELECT pl.[No_]                        AS item_no,
       SUM(pl.[Outstanding Quantity])  AS open_purchase_qty
INTO   #OpenPO
FROM   dbo.[IPG Photonics Corporation$Purchase Line] pl
WHERE  pl.[Document Type]=1                 -- Standard PO
  AND  pl.[Type]=2                          -- Item
  AND  pl.[Outstanding Quantity]>0
  AND  pl.[Unit Cost (LCY)]>0
GROUP BY pl.[No_];

CREATE NONCLUSTERED INDEX IX_OpenPO ON #OpenPO(item_no);

/* ==========================================================
   4  Open production – parent & component
   ========================================================== */
IF OBJECT_ID('tempdb..#OpenProdParent') IS NOT NULL DROP TABLE #OpenProdParent;
IF OBJECT_ID('tempdb..#OpenProdComp')   IS NOT NULL DROP TABLE #OpenProdComp;

/* parent items (dedup per Prod-Order) */
SELECT pol.[Item No_]                                         AS item_no,
       SUM(pol.[Remaining Quantity] *
           COALESCE(NULLIF(pol.[Qty_ per Unit of Measure],0),1)) AS open_prod_parent_qty
INTO   #OpenProdParent
FROM (
     SELECT DISTINCT
            [Prod_ Order No_],
            [Item No_],
            [Remaining Quantity],
            [Qty_ per Unit of Measure]
     FROM dbo.[IPG Photonics Corporation$Prod_ Order Line]
     WHERE [Status]=3
) pol
GROUP BY pol.[Item No_];

CREATE NONCLUSTERED INDEX IX_OpenProdParent ON #OpenProdParent(item_no);

/* component items (no dedup) */
SELECT poc.[Item No_]                                         AS item_no,
       SUM(poc.[Remaining Quantity] *
           COALESCE(NULLIF(poc.[Qty_ per Unit of Measure],0),1)) AS open_prod_component_qty
INTO   #OpenProdComp
FROM   dbo.[IPG Photonics Corporation$Prod_ Order Component] poc
WHERE  poc.[Status]=3
GROUP BY poc.[Item No_];

CREATE NONCLUSTERED INDEX IX_OpenProdComp ON #OpenProdComp(item_no);

/* ==========================================================
   5  Usage – 3 / 6 months
   ========================================================== */
IF OBJECT_ID('tempdb..#Usage') IS NOT NULL DROP TABLE #Usage;

SELECT CAST(l.[Item No_] AS varchar(50))                                    AS item_no,
       SUM(CASE WHEN l.[Posting Date]>=@Start3m THEN ABS(l.Quantity) END)   AS last_3m_usage_qty,
       SUM(ABS(l.Quantity))                                                 AS last_6m_usage_qty
INTO   #Usage
FROM   dbo.item_ledger_entry_all_v l
WHERE  l.Subsidiary='US010'
  AND  l.Quantity<>0
  AND  l.[Entry Type] IN (1,3,5)  -- sale, scrap, consumption
  AND  l.[Posting Date]>=@Start6m
GROUP BY l.[Item No_];

CREATE NONCLUSTERED INDEX IX_Usage ON #Usage(item_no);

/* ==========================================================
   6  Last-purchase facts  –  unified CTE + OUTER APPLY
   ========================================================== */
WITH po_unioned AS (
    /* ─ Purchase-history lines ─ */
    SELECT phl.[No_]                                     AS item_no,
           phh.[Order Date],
           phl.[Document No_],
           /* normalise qty & cost to EACH */
           phl.[Quantity] *
           COALESCE(NULLIF(phl.[Qty_ per Unit of Measure],0),1)       AS quantity,
           phl.[Unit Cost (LCY)] /
           COALESCE(NULLIF(phl.[Qty_ per Unit of Measure],0),1)       AS unit_cost,
           phl.[Manufacturer Part No_]                                AS mfg_part_no,
           phl.[Buy-from Vendor No_]                                  AS vendor_no
    FROM   dbo.[IPG Photonics Corporation$Purchase History Line]   phl
    JOIN   dbo.[IPG Photonics Corporation$Purchase History Header] phh
           ON phl.[Document No_] = phh.[No_]
    WHERE  phl.[Document Type]=1 AND phl.[Type]=2 AND phl.[Quantity]>0

    UNION ALL

    /* ─ Open / released PO lines ─ */
    SELECT pl.[No_]                                     AS item_no,
           ph.[Order Date],
           pl.[Document No_],
           pl.[Quantity] *
           COALESCE(NULLIF(pl.[Qty_ per Unit of Measure],0),1)        AS quantity,
           pl.[Unit Cost (LCY)] /
           COALESCE(NULLIF(pl.[Qty_ per Unit of Measure],0),1)        AS unit_cost,
           pl.[Manufacturer Part No_]                                 AS mfg_part_no,
           pl.[Buy-from Vendor No_]                                   AS vendor_no
    FROM   dbo.[IPG Photonics Corporation$Purchase Line] pl
    JOIN   dbo.[IPG Photonics Corporation$Purchase Header] ph
           ON pl.[Document No_] = ph.[No_]
    WHERE  pl.[Document Type]=1 AND pl.[Type]=2 AND pl.[Quantity]>0
)

/* ==========================================================
   7  Assemble final recordset
   ========================================================== */
SELECT it.row_index,
       it.item_no,
       it.[Description],
       it.inventory_posting_group,
       it.[Unit Cost]                               AS unit_cost,
       it.[Lead Time Calculation]                   AS lead_time_calculation,
       it.[Global Dimension 1 Code]                 AS global_dimension_1_code,
       CASE it.[Replenishment System]
            WHEN 0 THEN 'Purchase'
            WHEN 1 THEN 'Output'
            WHEN 2 THEN 'Assembly'
            ELSE 'Unknown'
       END                                          AS replenishment_system,
       it.[Revision No_]                            AS revision_no,
       CASE CAST(it.[Item Source] AS varchar(10))
            WHEN '0' THEN ''
            WHEN '3' THEN 'Made In-House'
            WHEN '1' THEN 'Third Party Purchase'
            WHEN '2' THEN 'Interco Purchase'
            ELSE CAST(it.[Item Source] AS varchar(10))
       END                                          AS item_source,
       it.[Common Item No_]                         AS common_item_no,
       it.[HTS]                                     AS hts,
       it.[Item Category Code]                      AS item_category_code,

       /* Ledger-based history & inventory */
       led.last_3m_output_qty,
       led.last_3m_purchase_qty,
       led.last_9m_output_qty,
       led.last_9m_purchase_qty,
       led.last_12m_output_qty,
       led.last_12m_purchase_qty,
       led.inventory_qty,

       /* Usage & open orders */
       u.last_3m_usage_qty,
       u.last_6m_usage_qty,
       opo.open_purchase_qty,
       opp.open_prod_parent_qty,
       opc.open_prod_component_qty,

       /* ─── OUTER APPLY: latest PO ─── */
       lp.last_vendor_name,
       lp.last_vendor_country,
       lp.last_purchase_qty,
       lp.last_unit_cost,
       lp.last_order_date,
       lp.last_mfg_part_no,

       /* Ratios incl. open orders */
       CASE WHEN (led.last_3m_purchase_qty + COALESCE(opo.open_purchase_qty,0))=0
            THEN NULL
            ELSE ROUND(
                 CAST(led.last_3m_output_qty + COALESCE(opp.open_prod_parent_qty,0) AS decimal(18,4))
                 / NULLIF(led.last_3m_purchase_qty + COALESCE(opo.open_purchase_qty,0),0)
               ,4)
       END                                         AS make_buy_ratio_3m,

       CASE WHEN (led.last_12m_purchase_qty + COALESCE(opo.open_purchase_qty,0))=0
            THEN NULL
            ELSE ROUND(
                 CAST(led.last_12m_output_qty + COALESCE(opp.open_prod_parent_qty,0) AS decimal(18,4))
                 / NULLIF(led.last_12m_purchase_qty + COALESCE(opo.open_purchase_qty,0),0)
               ,4)
       END                                         AS make_buy_ratio_12m,

       /* DOH (precise) */
       CASE WHEN u.last_3m_usage_qty=0
            THEN NULL
            ELSE ROUND( led.inventory_qty / (u.last_3m_usage_qty / @Days3m),1)
       END                                         AS doh_3m,
       CASE WHEN u.last_6m_usage_qty=0
            THEN NULL
            ELSE ROUND( led.inventory_qty / (u.last_6m_usage_qty / @Days6m),1)
       END                                         AS doh_6m,

       /* Final make/buy decision – original logic */
       COALESCE(
           CASE
               WHEN COALESCE(opo.open_purchase_qty,0) > 0
                AND COALESCE(opp.open_prod_parent_qty,0)=0  THEN 'Buy'
               WHEN COALESCE(opo.open_purchase_qty,0) = 0
                AND COALESCE(opp.open_prod_parent_qty,0) > 0 THEN 'Make'
               WHEN COALESCE(opo.open_purchase_qty,0) > COALESCE(opp.open_prod_parent_qty,0) THEN 'Buy'
               WHEN COALESCE(opp.open_prod_parent_qty,0) > COALESCE(opo.open_purchase_qty,0) THEN 'Make'
           END,
           CASE
               WHEN it.[Item Source]=3 OR it.[Replenishment System] IN (1,2) THEN
                    CASE
                        WHEN (led.last_3m_output_qty+led.last_3m_purchase_qty)>0
                         AND led.last_3m_purchase_qty>led.last_3m_output_qty THEN 'Buy'
                        WHEN (led.last_3m_output_qty+led.last_3m_purchase_qty)>0 THEN 'Make'
                        WHEN (led.last_12m_output_qty+led.last_12m_purchase_qty)>0
                         AND led.last_12m_purchase_qty>led.last_12m_output_qty THEN 'Buy'
                        WHEN (led.last_12m_output_qty+led.last_12m_purchase_qty)>0 THEN 'Make'
                        ELSE 'Make'
                    END
               ELSE
                    CASE
                        WHEN (led.last_3m_output_qty+led.last_3m_purchase_qty)>0
                         AND led.last_3m_output_qty>led.last_3m_purchase_qty THEN 'Make'
                        WHEN (led.last_3m_output_qty+led.last_3m_purchase_qty)>0 THEN 'Buy'
                        WHEN (led.last_12m_output_qty+led.last_12m_purchase_qty)>0
                         AND led.last_12m_output_qty>led.last_12m_purchase_qty THEN 'Make'
                        WHEN (led.last_12m_output_qty+led.last_12m_purchase_qty)>0 THEN 'Buy'
                        ELSE 'Buy'
                    END
           END)                                    AS make_buy,

       CONCAT('US010',it.item_no)                  AS item_index
FROM   #Items it
LEFT   JOIN #Ledger         led ON it.item_no = led.item_no
LEFT   JOIN #Usage          u   ON it.item_no = u.item_no
LEFT   JOIN #OpenPO         opo ON it.item_no = opo.item_no
LEFT   JOIN #OpenProdParent opp ON it.item_no = opp.item_no
LEFT   JOIN #OpenProdComp   opc ON it.item_no = opc.item_no

/* OUTER APPLY to fetch latest PO row */
OUTER APPLY (
     SELECT TOP (1)
            v.[Name]                                            AS last_vendor_name,
            CASE WHEN v.[Country_Region Code]='HK'
                 THEN 'CN' ELSE v.[Country_Region Code] END     AS last_vendor_country,
            pu.quantity                                         AS last_purchase_qty,
            pu.unit_cost                                        AS last_unit_cost,
            pu.[Order Date]                                     AS last_order_date,
            pu.mfg_part_no                                      AS last_mfg_part_no
     FROM   po_unioned pu
     LEFT   JOIN dbo.[IPG Photonics Corporation$Vendor] v
            ON pu.vendor_no = v.[No_]
     WHERE  pu.item_no = it.item_no
     ORDER BY pu.[Order Date] DESC, pu.[Document No_] DESC
) lp
ORDER BY it.row_index;
