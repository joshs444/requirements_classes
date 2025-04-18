SELECT
    rl.REQID,
    rl.REQType,
    rl.PartNum,
    rl.PartRev,
    rl.CostCenter,
    rl.Description,
    rl.Units,
    rl.OrderQty,
    rl.ShipToLocation,
    rl.RequestDelivery,
    rl.UnitPrice,
    rl.LastDirectCost,
    rl.SubmitDate,
    rl.ExtPrice,
    rl.REQLineID,
    eh.VendorID,
    eh.ContactID,
    eh.Shipping,
    eh.ShipTo,
    eh.PurchaseType,
    eh.SubmitUser,
    eh.PaymentTerms,
    rid.REQDATE,
    rid.Status,
    rid.NAVID,
    rid.Department,
    rs.StatusDesc
FROM dbo.PR_REQLines rl
LEFT JOIN dbo.PREntryHeader eh ON rl.REQID = eh.REQID
LEFT JOIN dbo.PR_REQID rid ON rl.REQID = rid.REQID
LEFT JOIN dbo.PR_REQStatus rs ON rid.Status = rs.StatusID
WHERE rid.REQDATE > '2023-06-01'
  AND (rs.StatusDesc = 'Hold' OR rs.StatusDesc = 'Pending Approval')
  AND rl.REQType = 'Item'
ORDER BY rid.REQDATE DESC; 