select 
  * 
from 
  AdventureWorksDW2022.dbo.FactInternetSales fs -- core fact table
  left join (
    select 
      dc.CustomerKey, 
      dc.FirstName, 
      dc.LastName, 
      dc.FirstName + ' ' + dc.LastName as 'FullName', 
      CASE dc.gender WHEN 'M' THEN 'Male' WHEN 'F' THEN 'Female' END AS Gender, 
      dc.datefirstpurchase, 
      g.City 
    from 
      AdventureWorksDW2022.dbo.DimCustomer dc 
      left join AdventureWorksDW2022.dbo.DimGeography g on dc.GeographyKey = g.GeographyKey
  ) dc -- dim customer table
  on fs.CustomerKey = dc.CustomerKey 
  left join (
    select 
      dp.ProductKey, 
      dp.ProductSubcategoryKey, 
      dp.ProductAlternateKey, 
      c.EnglishProductCategoryName, 
      sc.EnglishProductSubcategoryName, 
      dp.EnglishProductName, 
      dp.EnglishDescription, 
      dp.ProductLine 
    from 
      AdventureWorksDW2022.dbo.DimProduct dp 
      left join AdventureWorksDW2022.dbo.DimProductSubcategory sc on dp.ProductSubcategoryKey = sc.ProductSubcategoryKey 
      left join AdventureWorksDW2022.dbo.DimProductCategory c on sc.ProductCategoryKey = c.ProductCategoryKey
  ) dp --dim product table
  on fs.ProductKey = dp.ProductKey 
  left join (
    select 
      dd.DateKey, 
      dd.EnglishDayNameOfWeek, 
      dd.EnglishMonthName, 
      Left(dd.EnglishMonthName, 3) AS MonthShort, 
      dd.MonthNumberOfYear, 
      dd.CalendarQuarter, 
      dd.CalendarYear, 
      Concat(
        dd.MonthNumberOfYear, ' - ', dd.CalendarYear
      ) as 'Month_Year' 
    from 
      AdventureWorksDW2022.dbo.DimDate dd
  ) dd on fs.OrderDateKey = dd.DateKey 
where 
  fs.OrderDate >= '2022-01-01' 
  and fs.OrderDate <= '2024-12-31'