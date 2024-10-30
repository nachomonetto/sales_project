Este proyecto tiene como objetivo desarrollar el código necesario para establecer el ambiente de desarrollo a través de Docker, y además, para resolver las ingestas de la api https://fakestoreapi.com/ a través de Airflow.
Las ingestas que se llevan a cabo en el proyecto son de Products y Carts.
Los datos ingestados cuentan con los siguientes estadíos:
  1) Capa Raw: Primer capa del datalake, donde los datos ingestados son guardados tal cual los presenta la api (json)
  2) Capa Master: Segunda capa del datalake, donde los datos ya son formateados a csv.
  3) Tablas en base de datos: Después de la capa master, los datos son guardados en ciertas tablas de la BD Postgresql.
  4) Vistas en base de datos: Por último, los datos son presentados al usuario final a través de vistas, en las cuales se pueden reflejar distintas métricas calculadas en base a los datos ingestados.

En cuando al diseño del pipeline, se optó por un solo archivo .py, en el cual se refleja una Dag Factory, es decir, un algoritmo que permite crear en tiempos de compilación uno o más Dags de Airflow con la existencia de un solo archivo .py (sales_dag_factory.py).
El objetivo de esta Dag Factory es crear un Dag por cada entidad que se quiera ingestar. Por ejemplo, para nuestro caso, se buscan ingestar las entidades Products y Carts.
Para lograr esto, se crean un archivo .json para cada entidad que se quiera ingestar. Dentro de este json se definen los siguientes parámetros:
     entity: nombre de la entidad a ingestar.
     endpoint: endpoint de la entidad a ingestar.
     ingestion_mode: modo de ingesta como se quiere ingestar la entidad. Puede ser "daily_filter" donde se busca ingestar los datos a partir de un filtro de fecha. O puede ser snapshot, donde se trae la foto completa para esa entidad.
     fields: campos que se quieren traer de la entidad.
La Dag factory lee cada uno de esos json, para crear un dag por cada entidad. Cada dag contiene las mismas tasks, pero aplicado para la correspondiente entidad.
De esta manera, si en el futuro se quiere cargar otra entidad, solamente el desarrollador tiene que cargar el archivo json correspondiente para la nueva entidad a ingestar, estableciendo los parámetros mencionados.
Así se logra tener una capa de abstracción más, haciendo más robusto el pipeline.
Las tasks que se presentan en cada Dag son:
ext_entity: Extrae los datos de la api y los guarda en la capa raw.
val_entity_raw: Valida que se cumplan ciertas reglas de calidad para los datos en raw. Por simplificación, por el momento se introdujo una sola regla de calidad, la cual valida que el json recogido desde la api tenga realmente datos.
trf_entity: Transforma los datos del json en raw, a un archivo csv en master.
val_entity_master: Valida que se cumplan ciertas reglas de calidad para los datos en master. Por simplificación, por el momento se introdujo una sola regla de calidad, la cual valida que no existan datos nulos.
load_entity: Carga los datos del csv en master, a una BD Postgresql.
Consideraciones importantes:
-Las capas raw y master del datalake, optimamente se podrían haber implementado en un almacenamiento en la nube como ser el servicio de S3 de AWS. Para fines practicos, el datalake se implementó dentro del mismo repositorio.
-Para resolver el calculo de la métrica pedida en el enunciado, se utilizó una vista creada en la BD. Optimamente, se podría haber usado DBT como herramienta que permite hacer un mejor mantenimiento de los ETLs, mejor entendimiento, y favorecer la trazabilidad.

