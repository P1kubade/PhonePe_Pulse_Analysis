
CREATE DATABASE  phonepe_pulse;
USE phonepe_pulse;


CREATE TABLE aggregated_transaction (
    State VARCHAR(255),
    Year INT,
    Quarter INT,
    Transaction_Type VARCHAR(255),
    Transaction_Count BIGINT,
    Transaction_Amount DOUBLE
);


CREATE TABLE aggregated_user (
    State VARCHAR(255),
    Year INT,
    Quarter INT,
    Brand VARCHAR(255),
    User_Count BIGINT,
    Percentage DOUBLE
);


CREATE TABLE aggregated_insurance (
    State VARCHAR(255),
    Year INT,
    Quarter INT,
    Transaction_Type VARCHAR(255),
    Transaction_Count BIGINT,
    Transaction_Amount DOUBLE
);


CREATE TABLE map_transaction (
    State VARCHAR(255),
    Year INT,
    Quarter INT,
    District VARCHAR(255),
    Transaction_Count BIGINT,
    Transaction_Amount DOUBLE
);


CREATE TABLE map_user (
    State VARCHAR(255),
    Year INT,
    Quarter INT,
    District VARCHAR(255),
    Registered_Users BIGINT,
    App_Opens BIGINT
);


CREATE TABLE map_insurance (
    State VARCHAR(255),
    Year INT,
    Quarter INT,
    District VARCHAR(255),
    Insurance_Count BIGINT,
    Insurance_Amount DOUBLE
);


CREATE TABLE top_transaction (
    State VARCHAR(255),
    Year INT,
    Quarter INT,
    Entity_Name VARCHAR(255),
    Metric_Type VARCHAR(50), 
    Transaction_Count BIGINT,
    Transaction_Amount DOUBLE
);


CREATE TABLE top_user (
    State VARCHAR(255),
    Year INT,
    Quarter INT,
    Entity_Name VARCHAR(255),
    Metric_Type VARCHAR(50),
    Registered_Users BIGINT
);


CREATE TABLE top_insurance (
    State VARCHAR(255),
    Year INT,
    Quarter INT,
    Entity_Name VARCHAR(255),
    Metric_Type VARCHAR(50),
    Insurance_Count BIGINT,
    Insurance_Amount DOUBLE
);