SELECT 
    [Line No_], 
    [Description], 
    [Buy-from Vendor No_], 
    [Type], 
    [No_], 
    [Location Code], 
    [Unit of Measure], 
    [Quantity], 
    [Unit Cost (LCY)], 
    [Shortcut Dimension 1 Code], 
    [Order No_], 
    [Order Line No_], 
    [Posting Date]
FROM 
    [dbo].[IPG Photonics Corporation$Purch_ Rcpt_ Line]
WHERE 
    [Quantity] <> 0
    AND [Posting Date] > '2020-12-31 00:00:00' 