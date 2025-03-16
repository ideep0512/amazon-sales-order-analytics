-- Internal Stage - Query The CSV Data File Format
LIST @SALES_DWH.SOURCE.MY_INTERNAL_STG/sales/source=IN;
SELECT 
    t.$1::TEXT AS order_id, 
    t.$2::TEXT AS customer_name, 
    t.$3::TEXT AS mobile_key,
    t.$4::NUMBER AS order_quantity, 
    t.$5::NUMBER AS unit_price, 
    t.$6::NUMBER AS order_value,  
    t.$7::TEXT AS promotion_code , 
    t.$8::NUMBER(10,2)  AS final_order_amount,
    t.$9::NUMBER(10,2) AS tax_amount,
    t.$10::DATE AS order_dt,
    t.$11::TEXT AS payment_status,
    t.$12::TEXT AS shipping_status,
    t.$13::TEXT AS payment_method,
    t.$14::TEXT AS payment_provider,
    t.$15::TEXT AS mobile,
    t.$16::TEXT AS shipping_address,
    metadata$filename AS stg_file_name,
    CAST(metadata$file_row_number AS NUMBER(38,0)) AS stg_row_number,
    metadata$file_last_modified AS stg_last_modified
FROM 
    @sales_dwh.source.my_internal_stg/sales/source=IN/format=csv/
    (file_format => 'sales_dwh.common.my_csv_format') AS t;  

SELECT 
    metadata$file_row_number, 
    metadata$filename, 
    metadata$file_last_modified 
FROM 
    @sales_dwh.source.my_internal_stg/sales/source=IN/format=csv/
    (file_format => 'sales_dwh.common.my_csv_format');

LIST @SALES_DWH.SOURCE.MY_INTERNAL_STG;
SHOW FILE FORMATS IN sales_dwh.common;

-- Internal Stage - Query The Parquet Data File Format
select 
  $1:"Order ID"::text as orde_id,
  $1:"Customer Name"::text as customer_name,
  $1:"Mobile Model"::text as mobile_key,
  to_number($1:"Quantity") as quantity,
  to_number($1:"Price per Unit") as unit_price,
  to_decimal($1:"Total Price") as total_price,
  $1:"Promotion Code"::text as promotion_code,
  $1:"Order Amount"::number(10,2) as order_amount,
  to_decimal($1:"Tax") as tax,
  $1:"Order Date"::date as order_dt,
  $1:"Payment Status"::text as payment_status,
  $1:"Shipping Status"::text as shipping_status,
  $1:"Payment Method"::text as payment_method,
  $1:"Payment Provider"::text as payment_provider,
  $1:"Phone"::text as phone,
  $1:"Delivery Address"::text as shipping_address
from 
     @sales_dwh.source.my_internal_stg/sales/source=US/format=parquet/
     (file_format => 'sales_dwh.common.my_parquet_format');


-- Internal Stage - Query The JSON Data File Format
select                                                       
    $1:"Order ID"::text as orde_id,                   
    $1:"Customer Name"::text as customer_name,          
    $1:"Mobile Model"::text as mobile_key,              
    to_number($1:"Quantity") as quantity,               
    to_number($1:"Price per Unit") as unit_price,       
    to_decimal($1:"Total Price") as total_price,        
    $1:"Promotion Code"::text as promotion_code,        
    $1:"Order Amount"::number(10,2) as order_amount,    
    to_decimal($1:"Tax") as tax,                        
    $1:"Order Date"::date as order_dt,                  
    $1:"Payment Status"::text as payment_status,        
    $1:"Shipping Status"::text as shipping_status,      
    $1:"Payment Method"::text as payment_method,        
    $1:"Payment Provider"::text as payment_provider,    
    $1:"Phone"::text as phone,                          
    $1:"Delivery Address"::text as shipping_address
from                                                
@sales_dwh.source.my_internal_stg/sales/source=FR/format=json/
(file_format => sales_dwh.common.my_json_format);





