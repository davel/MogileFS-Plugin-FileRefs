CREATE TABLE `fileref` (
  `dkey` varchar(255) DEFAULT NULL,
  `ref   varchar(255) DEFAULT NULL,
  UNIQUE KEY `i_unique` (`dkey`,`ref`),
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
