create view vw_revenues as 

SELECT p.title as producto, date(c.fecha) as fecha, sum(c.quantity*p.price) as revenue
from (
SELECT date + interval '3 hour' as fecha, process_date, x.*
FROM carts
   , json_to_recordset(replace(lower(products),E'\'',E'\"')::json) x  
        (  productId numeric
         , quantity numeric
        )
     ) c
 left join 
 (select id, title, price, process_date 
  from products) p on p.id=c.productId and p.process_date=c.process_date
 group by p.title, date(c.fecha)
 order by date(c.fecha) desc