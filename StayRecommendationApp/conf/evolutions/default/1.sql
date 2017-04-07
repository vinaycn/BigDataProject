#--- !Ups
create table `user` (
  `id` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `name` TEXT NOT NULL,
  `email` TEXT NOT NULL,
  `age` BIGINT NOT NULL,
  `password` TEXT NOT NULL
)


#--- !Downs
drop table "user"