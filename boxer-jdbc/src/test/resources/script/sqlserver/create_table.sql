/*
 Navicat Premium Data Transfer

 Source Server         : sqlserver
 Source Server Type    : SQL Server
 Source Server Version : 11002100
 Source Host           : localhost:1433
 Source Catalog        : test
 Source Schema         : dbo

 Target Server Type    : SQL Server
 Target Server Version : 11002100
 File Encoding         : 65001

 Date: 01/06/2022 14:28:35
*/


-- ----------------------------
-- Table structure for stu
-- ----------------------------
IF EXISTS (SELECT * FROM sys.all_objects WHERE object_id = OBJECT_ID(N'[dbo].[stu]') AND type IN ('U'))
	DROP TABLE [dbo].[stu]
GO

CREATE TABLE [dbo].[stu] (
  [id] bigint  NOT NULL,
  [name] nvarchar(50) COLLATE Chinese_PRC_CI_AS  NULL,
  [age] int  NULL,
  [height] float(53)  NULL,
  [birth_date] date  NULL,
  [update_time] datetime  NULL
)
GO

ALTER TABLE [dbo].[stu] SET (LOCK_ESCALATION = TABLE)
GO


-- ----------------------------
-- Records of stu
-- ----------------------------
BEGIN TRANSACTION
GO

INSERT INTO [dbo].[stu] ([id], [name], [age], [height], [birth_date], [update_time]) VALUES (N'1', N'ss', N'17', N'172.3', N'2000-11-11', N'2022-06-01 12:12:12.000')
GO

INSERT INTO [dbo].[stu] ([id], [name], [age], [height], [birth_date], [update_time]) VALUES (N'2', N'nn', N'23', N'175.5', N'2000-06-01', N'2022-06-01 12:12:12.000')
GO

INSERT INTO [dbo].[stu] ([id], [name], [age], [height], [birth_date], [update_time]) VALUES (N'3', N'dd', N'22', N'183.5', N'2000-06-01', N'2022-06-01 12:12:12.000')
GO

COMMIT
GO


-- ----------------------------
-- Primary Key structure for table stu
-- ----------------------------
ALTER TABLE [dbo].[stu] ADD CONSTRAINT [PK__stu__3213E83F0D449F20] PRIMARY KEY CLUSTERED ([id])
WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)  
ON [PRIMARY]
GO

