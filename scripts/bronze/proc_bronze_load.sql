/*
================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
================================================================

Script Purpose:
This stored procedure loads data into the 'bronze' schema from external CSV files.
It performs the following actions:
- Truncates the bronze tables before loading data.
Uses the BULK INSERT command to load data from csv Files to bronze tables.

Parameters:
None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC bronze.load_bronze;

*/

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN 
    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    SET @batch_start_time = GETDATE(); 

    BEGIN TRY
        PRINT '============================================================';
        PRINT ' BRONZE LAYER LOAD - STARTED ';
        PRINT ' Batch Start Time : ' + CAST(@batch_start_time AS NVARCHAR);
        PRINT '============================================================';

        PRINT '';
        PRINT '------------------------------------------------------------';
        PRINT ' SECTION : CRM SOURCE TABLES ';
        PRINT '------------------------------------------------------------';

        -- CRM CUSTOMER INFO
        SET @start_time = GETDATE();
        PRINT ' [CRM] Step 1 : Truncating table [bronze.crm_cust_info]';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT ' [CRM] Step 2 : Loading data into [bronze.crm_cust_info]';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\velpu\Downloads\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT ' [CRM] Load Completed | Duration : ' 
              + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) 
              + ' seconds';

        PRINT '------------------------------------------------------------';

        -- CRM PRODUCT INFO
        SET @start_time = GETDATE();
        PRINT ' [CRM] Step 3 : Truncating table [bronze.crm_prd_info]';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT ' [CRM] Step 4 : Loading data into [bronze.crm_prd_info]';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\velpu\Downloads\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT ' [CRM] Load Completed | Duration : ' 
              + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) 
              + ' seconds';

        PRINT '------------------------------------------------------------';

        -- CRM SALES DETAILS
        SET @start_time = GETDATE();
        PRINT ' [CRM] Step 5 : Truncating table [bronze.crm_sales_details]';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT ' [CRM] Step 6 : Loading data into [bronze.crm_sales_details]';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\velpu\Downloads\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT ' [CRM] Load Completed | Duration : ' 
              + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) 
              + ' seconds';

        PRINT '';
        PRINT '------------------------------------------------------------';
        PRINT ' SECTION : ERP SOURCE TABLES ';
        PRINT '------------------------------------------------------------';

        -- ERP CUSTOMER
        SET @start_time = GETDATE();
        PRINT ' [ERP] Step 7 : Truncating table [bronze.erp_cust_az12]';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT ' [ERP] Step 8 : Loading data into [bronze.erp_cust_az12]';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\velpu\Downloads\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT ' [ERP] Load Completed | Duration : ' 
              + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) 
              + ' seconds';

        PRINT '------------------------------------------------------------';

        -- ERP LOCATION
        SET @start_time = GETDATE();
        PRINT ' [ERP] Step 9 : Truncating table [bronze.erp_loc_a101]';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT ' [ERP] Step 10 : Loading data into [bronze.erp_loc_a101]';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\velpu\Downloads\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT ' [ERP] Load Completed | Duration : ' 
              + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) 
              + ' seconds';

        PRINT '------------------------------------------------------------';

        -- ERP CATEGORY
        SET @start_time = GETDATE();
        PRINT ' [ERP] Step 11 : Truncating table [bronze.erp_px_cat_g1v2]';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT ' [ERP] Step 12 : Loading data into [bronze.erp_px_cat_g1v2]';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\velpu\Downloads\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT ' [ERP] Load Completed | Duration : ' 
              + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) 
              + ' seconds';

    END TRY 
    BEGIN CATCH
        PRINT '============================================================';
        PRINT ' BRONZE LAYER LOAD - FAILED ';
        PRINT ' Error Number  : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT ' Error Message : ' + ERROR_MESSAGE();
        PRINT '============================================================';
    END CATCH;

    SET @batch_end_time = GETDATE();
    PRINT '';
    PRINT '============================================================';
    PRINT ' BRONZE LAYER LOAD - COMPLETED ';
    PRINT ' Batch End Time   : ' + CAST(@batch_end_time AS NVARCHAR);
    PRINT ' Total Duration   : ' 
          + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) 
          + ' seconds';
    PRINT '============================================================';
END;
