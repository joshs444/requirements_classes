CREATE 
    ALGORITHM = UNDEFINED 
    DEFINER = `root`@`localhost` 
    SQL SECURITY DEFINER
VIEW `v_daily_inventory` AS
    SELECT 
        `d`.`snapshot_date` AS `snapshot_date`,
        `s`.`item_no` AS `item_no`,
        `s`.`location_code` AS `location_code`,
        COALESCE(`s`.`qty_on_hand`, 0) AS `qty`,
        COALESCE(`s`.`total_cost`, 0) AS `extended_price`,
        COALESCE(`s`.`total_root`, 0) AS `root_cost`
    FROM
        (`dim_date` `d`
        JOIN `inv_snapshot` `s` ON ((`d`.`snapshot_date` BETWEEN `s`.`balance_start` AND COALESCE(`s`.`balance_end`, `d`.`snapshot_date`))))