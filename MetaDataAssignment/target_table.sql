use shubham;


create table shubham.metastore(
id int not null auto_increment primary key,
tableName varchar(1000),
columnName varchar(1000),
columnType varchar(1000),
columnSize varchar(1000),
ordinal varchar(1000),
tablecatalog varchar(1000),
data varchar(1000)
);


select * from shubham.metastore;



