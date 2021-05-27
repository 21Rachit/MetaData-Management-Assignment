use testnew;

CREATE TABLE `emp99` (
  `id` int not null primary key auto_increment,
  `item_name` varchar(45) ,
  `item_image` varchar(45) ,
  `item_category` varchar(45) ,
  `item_quantity` varchar(45) ,
  `item_unit` varchar(45) ,
  `item_unit_price` varchar(45),
  `item_status` varchar(45),
  `user_id` int
);

CREATE TABLE `user99` (
  `id` int not null primary key auto_increment,
  `salutation` varchar(45) ,
  `first_name` varchar(45) ,
  `last_name` varchar(45) ,
  `gender` varchar(45) ,
  `mobile` varchar(45) ,
  `email` varchar(45),
  `address` varchar(45),
  `role` varchar(45),
  `status` varchar(45) 
);


CREATE TABLE `student99` (
  `id` int not null primary key auto_increment,
  `first_name` varchar(45) ,
  `last_name` varchar(45) ,
  `email` varchar(45)
);



select * from emp99;
select * from user99;
select * from student99;

