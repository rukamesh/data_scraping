# data_scraping

1. Method GrabStocks() -> scrap data sector wise from  MoneyControl and Put it in DB
 
2. It checks company already inserted in DB inside GrabStocks() to avoid duplication entry

3. In file 3rd_4th_Market_Cap.txt  query for 3rd and 4th highest market cap companies sector wise

4. method get_P_ratio_E_in_interval() generate a csv having columns interval and company_list 
    inerval: started from 11 and end with max of P/E ratio 
    company_list : companies having P/E ratio in corresponding interval
    
5. method third_4th_highest_market_cap_sectorwise() generate a csv having columns 'Company', 'Market_Cap' and 'Sector' for 3rd and 4th highest market cap companies sector wise

 Note: get_P_ratio_E_in_interval and third_4th_highest_market_cap_sectorwise can be done using pandas only but i have pandas for only export csv.