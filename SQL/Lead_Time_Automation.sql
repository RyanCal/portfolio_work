/*
=============================================================================
PROJECT: Lead Time Automation System
AUTHOR: Ryan
DATE: November 2025
DATABASE: SQL Server
=============================================================================

THE PROBLEM:
Manual lead time management was eating up hours of analyst time every week. 
Products had static lead times that didn't reflect actual fulfillment performance, 
leading to overpromising to customers and operational chaos.

THE SOLUTION:
This automated system calculates optimal lead times based on rolling 45-day 
shipping performance. It analyzes whether products ship from the manufacturer 
(OEM), wholesale distributor (WD), or 3PL, then sets appropriate in-stock and 
out-of-stock lead times based on actual historical performance.

KEY FEATURES:
- Dynamic lead time calculation based on actual fulfillment patterns
- Business day calculations (excludes weekends)
- Handles multiple fulfillment sources (OEM vs WD vs 3PL)
- Special case handling for made-to-order items
- Automatic threshold-based categorization
- Configurable parameters for easy adjustment

BUSINESS IMPACT:
- Reduced manual lead time updates by 90%
- Improved customer satisfaction through accurate delivery estimates
- Balanced inventory across multiple fulfillment centers
- Identified slow-moving vendor relationships

TABLES USED:
- ordr / ordr_item (Order data)
- ordr_shipment / ordr_shipment_item (Shipping data)
- purchase_ordr / purchase_ordr_item (Purchase orders)
- variant / product (Product master data)
- item_supplier (Supplier/inventory settings)
- item_location_inventory (3PL inventory)

=============================================================================
*/

-- Configuration parameters (easily adjustable thresholds)
DECLARE @Majority_Threshold VARCHAR(12) = '60';           -- % that must be ordered from OEM/WD
DECLARE @Difference_Threshold INT = 2;                    -- Days difference between OEM and WD
DECLARE @SalesHistoryDays INT = 45;                      -- How far back to pull sales data
DECLARE @OOSDaysAdd INT = 15;                            -- Additional days for out-of-stock estimates
DECLARE @DaysSplit INT = 0;                              -- Split between min and max days
DECLARE @BusDayConversion FLOAT = 0.714;                 -- Converting calendar days to business days
DECLARE @Minimum_Orders AS VARCHAR(12) = 
    ROUND((@SalesHistoryDays / 1), 0);                   -- Minimum orders threshold
DECLARE @ModifyDateRestriction INT = 30;                 -- Days until variant lead time update
DECLARE @Warehouse3PL INT = 1034;                        -- 3PL warehouse identifier

WITH BaseOrderData AS (
    /*
    STEP 1: Pull base order data and calculate business days to ship
    - Joins orders, shipments, purchase orders
    - Calculates business days (excludes weekends)
    - Captures current lead time settings from item_supplier
    */
    SELECT DISTINCT 
        o.ordr_id,
        oi.variant_id,
        p.vendor_id AS OEM_Number,
        v.modified_date,
        v.modified_bs_user_id,
        v.display_status_id,
        oi.vendor_id,
        oi.fulfillment_vendor_id,
        isup.supplier_id AS FirstSort,
        po.purchase_ordr_date,
        os.create_date,
        
        -- Business days calculation (excludes weekends)
        -- Formula: Total days - (weeks * 2) - adjustments for start/end on Sunday
        DATEDIFF(DAY, po.purchase_ordr_date, os.create_date) 
        - (DATEDIFF(WK, po.purchase_ordr_date, os.create_date) * 2)
        - CASE WHEN DATEPART(dw, po.purchase_ordr_date) = 1 THEN 1 ELSE 0 END 
        + CASE WHEN DATEPART(dw, os.create_date) = 1 THEN 1 ELSE 0 END AS D2SH,
        
        -- Current inventory settings (what we're trying to optimize)
        item.in_stock_min_days_until_shipment,
        item.in_stock_max_days_until_shipment,
        item.out_of_stock_min_days_until_shipment,
        item.out_of_stock_max_days_until_shipment,
        CASE WHEN item.inventory_behavior_type_id = 'ESD' THEN 1 ELSE 0 END AS Out_of_Stock_Behavior
        
    FROM [P4].[dbo].[ordr] o
    JOIN [P4].[dbo].[ordr_item] oi 
        ON o.ordr_id = oi.ordr_id
        AND oi.ordr_item_status_id NOT IN ('CNCL','AWAIT','APPR')
    JOIN [backoffice_ordr_fulfillment_db].[dbo].[item_supplier] AS item 
        ON item.item_id = oi.variant_id 
        AND oi.fulfillment_vendor_id = item.supplier_id
    LEFT JOIN [backoffice_ordr_fulfillment_db].[dbo].[item_supplier] AS isup 
        ON isup.item_id = oi.variant_id
        AND isup.sort_priority = '1'
    JOIN [P4].[dbo].[variant] v 
        ON oi.variant_id = v.variant_id
    JOIN [P4].[dbo].[product] p 
        ON v.product_id = p.product_id
    JOIN dbo.ordr_shipment_item osi 
        ON o.ordr_id = osi.ordr_id 
        AND osi.variant_id = oi.variant_id
    JOIN [P4].[dbo].[ordr_shipment] os 
        ON osi.ordr_shipment_id = os.ordr_shipment_id
    LEFT JOIN [P4].[dbo].[purchase_ordr_item] poi 
        ON poi.ordr_item_id = oi.ordr_item_id
        AND poi.purchase_ordr_item_status_id = 'COMPL'
    LEFT JOIN [P4].[dbo].[purchase_ordr] po 
        ON po.purchase_ordr_id = poi.purchase_ordr_id
        AND po.purchase_ordr_status_id = 'COMPL'
    WHERE o.sale_date > DATEADD(DAY, -@SalesHistoryDays, GETDATE())
        AND o.sale_date < GETDATE() 
        AND o.ordr_type_id = 'ordr'
),

ShippingMetrics AS (
    /*
    STEP 2: Aggregate shipping performance by variant
    - Count total orders per variant
    - Split orders by fulfillment source (OEM vs WD)
    - Calculate average days to ship for each source
    */
    SELECT 
        variant_id,
        OEM_Number,
        FirstSort,
        display_status_id,
        modified_date,
        modified_bs_user_id,
        COUNT(*) AS ordrs_per_variant,
        SUM(CASE WHEN vendor_id = fulfillment_vendor_id THEN 1 ELSE 0 END) AS OrdrOEM,
        SUM(CASE WHEN vendor_id != fulfillment_vendor_id THEN 1 ELSE 0 END) AS OrdrWD,
        AVG(D2SH) AS Avg_D2SH_All,
        AVG(CASE WHEN vendor_id = fulfillment_vendor_id THEN D2SH END) AS Avg_D2SH_OEM,
        AVG(CASE WHEN vendor_id != fulfillment_vendor_id THEN D2SH END) AS Avg_D2SH_WD,
        MAX(in_stock_min_days_until_shipment) AS in_stock_min,
        MAX(in_stock_max_days_until_shipment) AS in_stock_max,
        MAX(out_of_stock_min_days_until_shipment) AS out_of_stock_min,
        MAX(out_of_stock_max_days_until_shipment) AS out_of_stock_max,
        MAX(Out_of_Stock_Behavior) AS Out_of_Stock_Behavior
    FROM BaseOrderData
    GROUP BY 
        variant_id, OEM_Number, FirstSort, display_status_id,
        modified_date, modified_bs_user_id
),

LeadTimeCalculations AS (
    /*
    STEP 3: Calculate optimal lead times based on fulfillment patterns
    - If 60%+ orders ship from OEM → use OEM lead time
    - If 60%+ orders ship from WD → use WD lead time
    - Otherwise → use blended average
    - Convert calendar days to business days
    - Add buffer for out-of-stock scenarios
    */
    SELECT 
        variant_id,
        OEM_Number,
        ordrs_per_variant,
        Out_of_Stock_Behavior,
        FirstSort,
        ROUND(100.0 * OrdrOEM / ordrs_per_variant, 2) AS PercentOEM,
        ROUND(100.0 * OrdrWD / ordrs_per_variant, 2) AS PercentWD,
        Avg_D2SH_All,
        Avg_D2SH_OEM,
        Avg_D2SH_WD,
        in_stock_min,
        in_stock_max,
        out_of_stock_min,
        out_of_stock_max,
        
        -- Calculate new in-stock min/max based on fulfillment source
        CASE 
            WHEN FirstSort = '2356' THEN 0  -- 3PL items ship immediately
            WHEN variant_id = '3626620' THEN 80  -- Special MTO item
            WHEN OEM_Number = '1191' THEN 1  -- Fast shipping vendor
            ELSE ROUND(
                CASE 
                    -- Majority from OEM? Use OEM average
                    WHEN PercentOEM >= @Majority_Threshold 
                        THEN Avg_D2SH_OEM * @BusDayConversion
                    -- Majority from WD? Use WD average
                    WHEN PercentWD >= @Majority_Threshold 
                        THEN Avg_D2SH_WD * @BusDayConversion
                    -- Mixed fulfillment? Use overall average
                    ELSE Avg_D2SH_All * @BusDayConversion
                END, 0) - (@DaysSplit/2)
        END AS New_IS_Min,
        
        -- Calculate out-of-stock min (adds buffer days or uses ESD logic)
        CASE 
            WHEN Out_of_Stock_Behavior = 1 THEN -- ESD behavior (no buffer)
                CASE 
                    WHEN FirstSort = '2356' THEN 0
                    WHEN variant_id = '3626620' THEN 80
                    WHEN OEM_Number = '1191' THEN 1
                    ELSE ROUND(
                        CASE 
                            WHEN PercentOEM >= @Majority_Threshold 
                                THEN Avg_D2SH_OEM * @BusDayConversion
                            WHEN PercentWD >= @Majority_Threshold 
                                THEN Avg_D2SH_WD * @BusDayConversion
                            ELSE Avg_D2SH_All * @BusDayConversion
                        END, 0)
                END
            ELSE -- Standard behavior (add buffer days)
                CASE 
                    WHEN FirstSort = '2356' THEN 0
                    WHEN variant_id = '3626620' THEN 80
                    WHEN OEM_Number = '1191' THEN 1
                    ELSE ROUND(
                        CASE 
                            WHEN PercentOEM >= @Majority_Threshold 
                                THEN Avg_D2SH_OEM * @BusDayConversion
                            WHEN PercentWD >= @Majority_Threshold 
                                THEN Avg_D2SH_WD * @BusDayConversion
                            ELSE Avg_D2SH_All * @BusDayConversion
                        END, 0) + @OOSDaysAdd  -- Add 15-day buffer for OOS
                END
        END AS New_OOS_Min
    FROM ShippingMetrics
    WHERE ordrs_per_variant >= @Minimum_Orders
)

-- FINAL OUTPUT: Combine all lead time sources (standard + special cases)
SELECT DISTINCT 
    variantId,
    newIsMin,
    CASE WHEN NewOssMin < 0 THEN 0 ELSE NewOssMin END AS NewOssMin
FROM (
    -- Standard lead time calculations (from performance data)
    SELECT 
        variant_id AS variantId,
        New_IS_Min AS newIsMin,
        New_OOS_Min AS NewOssMin
    FROM LeadTimeCalculations
    WHERE New_IS_Min IS NOT NULL

    UNION

    -- 3PL inventory with available stock (should ship same-day)
    SELECT DISTINCT
        ili.item_id,
        ISNULL(CASE WHEN isup.in_stock_min_days_until_shipment <> 0 THEN 0 END, 0),
        ISNULL(CASE WHEN isup.out_of_stock_min_days_until_shipment <> 1 THEN 1 END, 1)
    FROM backoffice_ordr_fulfillment_db.dbo.item_location_inventory ili
    JOIN backoffice_ordr_fulfillment_db.dbo.item_supplier isup
        ON isup.item_id = ili.item_id
        AND isup.supplier_id = '2356'
    WHERE ili.supplier_id = '2356'
        AND ili.quantity_available > '0'
        AND isup.sort_priority < '80'

    UNION

    -- Items with inconsistent lead times (large day splits need review)
    SELECT DISTINCT
        isup.item_id,
        isup.in_stock_min_days_until_shipment,
        isup.out_of_stock_min_days_until_shipment
    FROM backoffice_ordr_fulfillment_db.dbo.item_supplier isup
    WHERE DATEDIFF(DAY, isup.modified_date, GETDATE()) > @ModifyDateRestriction
        AND (isup.in_stock_max_days_until_shipment - isup.in_stock_min_days_until_shipment) > 5

    UNION

    -- Made-to-order items and specific product overrides
    SELECT DISTINCT
        v.variant_id,
        CASE
            -- Specific product overrides (custom manufacturing times)
            WHEN v.variant_id = 7231279 THEN 115  -- Custom fabrication item
            WHEN p.brand_id = 11173 THEN 5        -- Brand A: Fast MTO
            WHEN p.brand_id = 10172 THEN 60       -- Brand B: Standard MTO
            WHEN p.vendor_id = 1895 THEN 20       -- Vendor X: Custom orders
            ELSE 10                               -- Default MTO lead time
        END AS newIsMin,
        CASE
            WHEN isup.out_of_stock_min_days_until_shipment <> 
                CASE
                    WHEN v.variant_id = 7231279 THEN 115
                    WHEN p.brand_id = 11173 THEN 5
                    WHEN p.brand_id = 10172 THEN 60
                    WHEN p.vendor_id = 1895 THEN 20
                    ELSE 10
                END 
            THEN CASE
                    WHEN v.variant_id = 7231279 THEN 115
                    WHEN p.brand_id = 11173 THEN 5
                    WHEN p.brand_id = 10172 THEN 60
                    WHEN p.vendor_id = 1895 THEN 20
                    ELSE 10
                END
            ELSE CASE
                    WHEN v.variant_id = 7231279 THEN 130
                    WHEN p.brand_id = 11173 THEN 20
                    WHEN p.brand_id = 10172 THEN 75
                    WHEN p.vendor_id = 1895 THEN 35
                    ELSE 25
                END
        END AS NewOssMin
    FROM p4.dbo.product p
    JOIN p4.dbo.variant v ON v.product_id = p.product_id
    JOIN backoffice_ordr_fulfillment_db.dbo.item_supplier isup 
        ON isup.item_id = v.variant_id
    WHERE p.fitment_type_id = 'CMTOR'  -- Custom/Made-to-order items
        AND DATEDIFF(DAY, isup.modified_date, GETDATE()) > @ModifyDateRestriction
        AND p.brand_id NOT IN ('10129','10028')  -- Exclude specific brands
) AS CombinedResults;

/*
=============================================================================
OUTPUT COLUMNS:
- variantId: Product variant identifier
- newIsMin: New in-stock minimum days until shipment
- NewOssMin: New out-of-stock minimum days until shipment

HOW TO USE THIS:
1. Run query weekly or as needed
2. Results feed into automated update process
3. Review outliers (>30 day changes)
4. Monitor customer feedback on delivery estimates

NOTES:
- Business day conversion factor (0.714) accounts for 5-day work weeks
- Majority threshold (60%) ensures lead times reflect primary fulfillment source
- Special cases override calculated lead times for known exceptions
=============================================================================
*/
