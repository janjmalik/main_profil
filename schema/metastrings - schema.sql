-- MySQL dump 10.13  Distrib 8.0.18, for Win64 (x86_64)
--
-- Host: localhost    Database: metastrings
-- ------------------------------------------------------
-- Server version	8.0.18

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `bvalues`
--

DROP TABLE IF EXISTS `bvalues`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `bvalues` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `isNumeric` bit(1) NOT NULL,
  `numberValue` double NOT NULL,
  `stringValue` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_uniq_prefix_number` (`stringValue`,`numberValue`,`isNumeric`),
  KEY `idx_bvalues_number` (`numberValue`,`isNumeric`,`id`),
  KEY `idx_bvalues_prefix` (`stringValue`,`isNumeric`,`id`)
) ENGINE=InnoDB AUTO_INCREMENT=601159 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `itemnamevalues`
--

DROP TABLE IF EXISTS `itemnamevalues`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `itemnamevalues` (
  `itemid` bigint(20) NOT NULL,
  `nameid` int(11) NOT NULL,
  `valueid` bigint(20) NOT NULL,
  PRIMARY KEY (`itemid`,`nameid`),
  KEY `fk_inv_names_idx` (`nameid`),
  KEY `fk_inv_values_idx` (`valueid`),
  CONSTRAINT `fk_inv_items` FOREIGN KEY (`itemid`) REFERENCES `items` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `fk_inv_names` FOREIGN KEY (`nameid`) REFERENCES `names` (`id`),
  CONSTRAINT `fk_inv_values` FOREIGN KEY (`valueid`) REFERENCES `bvalues` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `items`
--

DROP TABLE IF EXISTS `items`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `items` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `tableid` int(11) NOT NULL,
  `valueid` bigint(20) NOT NULL,
  `created` timestamp NOT NULL,
  `lastmodified` timestamp NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_items_valueid_typenameid` (`valueid`,`tableid`),
  KEY `fk_item_typeid_idx` (`tableid`),
  KEY `idx_items_created` (`created`),
  KEY `idx_items_lastmodified` (`lastmodified`),
  CONSTRAINT `fk_item_tables` FOREIGN KEY (`tableid`) REFERENCES `tables` (`id`),
  CONSTRAINT `fk_item_values` FOREIGN KEY (`valueid`) REFERENCES `bvalues` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=738951 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Temporary view structure for view `itemvalues`
--

DROP TABLE IF EXISTS `itemvalues`;
/*!50001 DROP VIEW IF EXISTS `itemvalues`*/;
SET @saved_cs_client     = @@character_set_client;
/*!50503 SET character_set_client = utf8mb4 */;
/*!50001 CREATE VIEW `itemvalues` AS SELECT 
 1 AS `itemid`,
 1 AS `nameid`,
 1 AS `valueid`,
 1 AS `isNumeric`,
 1 AS `numberValue`,
 1 AS `stringValue`*/;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `likes`
--

DROP TABLE IF EXISTS `likes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `likes` (
  `beatid` bigint(20) NOT NULL,
  `userid` bigint(20) NOT NULL,
  PRIMARY KEY (`beatid`,`userid`),
  KEY `idx_like_users` (`userid`),
  CONSTRAINT `fk_likes_items` FOREIGN KEY (`beatid`) REFERENCES `items` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `longstrings`
--

DROP TABLE IF EXISTS `longstrings`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `longstrings` (
  `itemid` bigint(20) NOT NULL,
  `name` varchar(255) NOT NULL,
  `longstring` text NOT NULL,
  PRIMARY KEY (`itemid`,`name`),
  CONSTRAINT `fk_longstrings_items` FOREIGN KEY (`itemid`) REFERENCES `items` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `names`
--

DROP TABLE IF EXISTS `names`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `names` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `tableid` int(11) NOT NULL,
  `name` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `isNumeric` bit(1) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name_types` (`name`,`tableid`),
  KEY `fk_name_table_id_idx` (`tableid`),
  CONSTRAINT `fk_name_table_id` FOREIGN KEY (`tableid`) REFERENCES `tables` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE=InnoDB AUTO_INCREMENT=5030 DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `playlikes`
--

DROP TABLE IF EXISTS `playlikes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `playlikes` (
  `beatid` bigint(20) NOT NULL,
  `plays` bigint(20) NOT NULL DEFAULT '0',
  `likes` bigint(20) NOT NULL DEFAULT '0',
  PRIMARY KEY (`beatid`),
  CONSTRAINT `fk_playlikes_items` FOREIGN KEY (`beatid`) REFERENCES `items` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `plays`
--

DROP TABLE IF EXISTS `plays`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `plays` (
  `beatid` bigint(20) NOT NULL,
  `userid` bigint(20) NOT NULL,
  PRIMARY KEY (`beatid`,`userid`),
  KEY `idx_plays_uesrs` (`userid`) /*!80000 INVISIBLE */,
  CONSTRAINT `fx_plays_items` FOREIGN KEY (`beatid`) REFERENCES `items` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `tables`
--

DROP TABLE IF EXISTS `tables`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `tables` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  `isNumeric` bit(1) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name_UNIQUE` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=1861 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Final view structure for view `itemvalues`
--

/*!50001 DROP VIEW IF EXISTS `itemvalues`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_unicode_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50001 VIEW `itemvalues` AS select `inv`.`itemid` AS `itemid`,`inv`.`nameid` AS `nameid`,`v`.`id` AS `valueid`,`v`.`isNumeric` AS `isNumeric`,`v`.`numberValue` AS `numberValue`,`v`.`stringValue` AS `stringValue` from (`itemnamevalues` `inv` join `bvalues` `v` on((`v`.`id` = `inv`.`valueid`))) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2021-04-18 14:00:19
