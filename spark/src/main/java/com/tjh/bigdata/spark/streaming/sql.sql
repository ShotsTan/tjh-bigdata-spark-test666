CREATE TABLE `offstes` (
                           `groupId` varchar(255) NOT NULL,
                           `topic` varchar(255) NOT NULL,
                           `partitionId` int(11) NOT NULL,
                           `offset` bigint(20) DEFAULT NULL,
                           PRIMARY KEY (`groupId`,`topic`,`partitionId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `wordcount` (
                             `word` varchar(255) NOT NULL,
                             `count` bigint(20) DEFAULT NULL,
                             PRIMARY KEY (`word`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ;
insert into wordcount values('a',4)
on duplicate key update count = count+values(count)