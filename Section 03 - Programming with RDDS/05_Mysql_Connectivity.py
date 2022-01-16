"""
CREATE DATABASE DEMO01;

USE DEMO01

CREATE TABLE DEMO_TABLE(
    ID INT
  , NAME VARCHAR(20)
  , AGE INT
);

INSERT INTO DEMO_TABLE(ID, NAME, AGE) VALUES (1, "ABHAY", 25), (2, "BUNTY", 25), (3, "ABC", 30);

SELECT * FROM DEMO_TABLE;
"""
import mysql

dataframe_mysql = sqlContext.read.format("jdbc").options(url="jdbc:mysql//3306/demo01", driver="com.mysql.jdbc.river",
                                                         dbtable="demo_table", user="root", password="root123").load()
