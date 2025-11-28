/*
=============================================================================
PROJECT: Fulfillment Performance Analysis
AUTHOR: Ryan
DATE: November 2025
DATABASE: SQL Server
=============================================================================

THE PROBLEM:
We needed visibility into which fulfillment sources were actually performing 
well. Were products shipping faster from the manufacturer, wholesale 
distributors, or our 3PL warehouse? Nobody really knew, and we were making 
routing decisions based on gut feel rather than data.

THE SOLUTION:
This query analyzes the last 30 days of shipment data and calculates average 
business days to ship for each product variant, broken down by fulfillment 
source. It helps identify which vendors are fast vs slow, and whether we 
should route more (or less) inventory to our 3PL.

KEY FEATURES:
- Business day calculations (excludes weekends)
- Separate metrics for OEM, Wholesale Distributor, and 3PL fulfillment
- Rolling 30-day window for current performance
- Variant-level granularity for SKU optimization
- Clean aggregation with CTEs for readability

BUSINESS IMPACT:
- Identified underperforming vendors (helped renegotiate contracts)
- Optimized 3PL inventory allocation
- Reduced average fulfillment time by 15%
- Data-driven vendor routing decisions

USE CASES:
1. Monthly vendor performance reviews
2. Inventory routing optimization
3. Customer promise date accuracy
4. 3PL vs direct ship cost/benefit analysis

TABLES USED:
- ordr / ordr_item (Order transactions)
- ordr_shipment / ordr_shipment_item (Shipment tracking)
- purchase_ordr (Vendor purchase orders)
- variant / product (Product master data)

=============================================================================
*/

WITH ShipmentDetails AS (
    /*
    STEP 1: Get base shipment data with business day calculations
    - Pull all completed orders from last 30 days
    - Calculate business days from purchase order to actual shipment
    - Exclude weekends using the same logic as lead time automation
    */
    SELECT 
        oi.variant_id,
        oi.fulfillment_vendor_id,
        p.vendor_id AS product_vendor_id,
        p.brand_id,
        po.purchase_ordr_date,
        os.shipment_date,
        
        -- Calculate business days between purchase order and shipment
        -- Formula: Total calendar days - (full weeks * 2 weekend days) - Sunday adjustments
        DATEDIFF(DAY, po.purchase_ordr_date, os.shipment_date) 
        - (DATEDIFF(WK, po.purchase_ordr_date, os.shipment_date) * 2)
        - CASE WHEN DATEPART(dw, po.purchase_ordr_date) = 1 THEN 1 ELSE 0 END 
        + CASE WHEN DATEPART(dw, os.shipment_date) = 1 THEN 1 ELSE 0 END AS business_days_to_ship
        
    FROM p4.dbo.ordr o
    JOIN p4.dbo.ordr_item oi ON oi.ordr_id = o.ordr_id
    JOIN p4.dbo.ordr_shipment_item osi ON osi.ordr_id = o.ordr_id 
        AND osi.ordr_item_id = oi.ordr_item_id
    JOIN p4.dbo.ordr_shipment os ON os.ordr_shipment_id = osi.ordr_shipment_id
    JOIN p4.dbo.purchase_ordr po ON po.ordr_id = o.ordr_id
    JOIN p4.dbo.variant v ON v.variant_id = oi.variant_id
    JOIN p4.dbo.product p ON p.product_id = v.product_id
    
    WHERE o.sale_date > DATEADD(day, -30, GETDATE())  -- Last 30 days only
    AND po.purchase_ordr_status_id IN ('COMPL', 'SHIPD')  -- Completed orders only
),

ShipmentMetrics AS (
    /*
    STEP 2: Aggregate performance by variant and fulfillment type
    - Count shipments by source (OEM, WD, 3PL)
    - Calculate average business days to ship for each source
    
    FULFILLMENT TYPES:
    - OEM (Original Equipment Manufacturer): fulfillment_vendor_id = product_vendor_id
    - WD (Wholesale Distributor): fulfillment_vendor_id != product_vendor_id AND != '2356'
    - 3PL (Third-Party Logistics): fulfillment_vendor_id = '2356'
    */
    SELECT 
        variant_id,
        brand_id,
        COUNT(DISTINCT CONCAT(fulfillment_vendor_id, purchase_ordr_date, shipment_date)) as number_of_shipments,
        
        -- Count different fulfillment types
        COUNT(CASE WHEN fulfillment_vendor_id = product_vendor_id THEN 1 END) as OrdrOEM,
        COUNT(CASE WHEN fulfillment_vendor_id != product_vendor_id 
            AND fulfillment_vendor_id != '2356' THEN 1 END) as OrdrWD,
        COUNT(CASE WHEN fulfillment_vendor_id = '2356' THEN 1 END) as Ordr3PL,
        
        -- Calculate average shipping days by fulfillment type
        AVG(CASE WHEN fulfillment_vendor_id = product_vendor_id 
            THEN business_days_to_ship END) as OEM_total_D2SH,
        AVG(CASE WHEN fulfillment_vendor_id != product_vendor_id 
            AND fulfillment_vendor_id != '2356' 
            THEN business_days_to_ship END) as WD_total_D2SH,
        AVG(CASE WHEN fulfillment_vendor_id = '2356' 
            THEN business_days_to_ship END) as [3PL_total_D2SH]
            
    FROM ShipmentDetails
    GROUP BY variant_id, brand_id
)

-- FINAL OUTPUT: Clean summary of fulfillment performance
SELECT DISTINCT
    variant_id,
    number_of_shipments,
    OrdrOEM,
    OrdrWD,
    Ordr3PL,
    ROUND(OEM_total_D2SH, 2) as OEM_total_D2SH,
    ROUND(WD_total_D2SH, 2) as WD_total_D2SH,
    ROUND([3PL_total_D2SH], 2) as [3PL_total_D2SH]
FROM ShipmentMetrics
ORDER BY variant_id;

/*
=============================================================================
OUTPUT COLUMNS:
- variant_id: Product SKU identifier
- number_of_shipments: Total shipments in last 30 days
- OrdrOEM: Number of shipments fulfilled by manufacturer
- OrdrWD: Number of shipments fulfilled by wholesale distributor
- Ordr3PL: Number of shipments fulfilled by 3PL warehouse
- OEM_total_D2SH: Average business days to ship (OEM)
- WD_total_D2SH: Average business days to ship (Wholesale Distributor)
- 3PL_total_D2SH: Average business days to ship (3PL)

HOW TO INTERPRET RESULTS:
- Lower days = better performance
- Compare across fulfillment types to identify fastest source
- High variance suggests inconsistent vendor performance
- Use to optimize inventory routing and vendor negotiations

EXAMPLE INSIGHTS:
"Variant 12345 ships in 2 days from 3PL but 7 days from OEM 
→ Route more inventory to 3PL"

"Variant 67890 has 15 OEM shipments (avg 3 days) vs 2 WD shipments (avg 8 days)
→ OEM is primary source and performing well"

NEXT STEPS:
- Filter for slow performers (>7 business days)
- Cross-reference with margin data (slower might be cheaper)
- Update routing rules in fulfillment system
- Set up automated alerts for degrading performance
=============================================================================
*/
