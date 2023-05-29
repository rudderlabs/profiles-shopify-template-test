
	
		BEGIN

		DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;

























  
  
  
  
  

			
    
        
            
    
        
    
        
            
                
                CREATE OR REPLACE VIEW MATERIAL_RSPAGES_AE075D7C_1 AS 
SELECT
    *
FROM
    RUDDERSTACK_TEST_DB.DATA_APPS_SIMULATED_SHOPIFY.PAGES

WHERE
    
        ((
         timestamp <= '2023-05-29T06:34:49.347741Z'
        )
         OR timestamp IS NULL )
    

;
                
            
        
    
        
    

        
    

			/* Macros */
/* Declare and Define Macro for user defined functions */










/* endMacros */

/* Define macros */
    BEGIN

        
    
    CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_SESSION_3F3_1 AS (
        SELECT DISTINCT session_main_id FROM Material_shopify_session_id_stitcher_a47a3fc3_1
    );

        /* Create input tables */
        
            
            
                
    
    
        CREATE TEMPORARY TABLE inputVarTable_rsPages_ae0_1 AS (
            SELECT
                
        left(sha1(random()::text),32)
 AS input_row_id,
                COALESCE(NULL, Material_shopify_session_id_stitcher_a47a3fc3_1_4.session_main_id) AS session_main_id,
                Material_rsPages_ae075d7c_1.*
            FROM
                Material_rsPages_ae075d7c_1
            
                        LEFT JOIN Material_shopify_session_id_stitcher_a47a3fc3_1 AS Material_shopify_session_id_stitcher_a47a3fc3_1_4
                    
                    
                        ON concat(coalesce(anonymous_id, 'null'), coalesce(to_char(context_session_id), 'null')) = Material_shopify_session_id_stitcher_a47a3fc3_1_4.other_id
                        AND 'session_id' = Material_shopify_session_id_stitcher_a47a3fc3_1_4.other_id_type
                    
        );
    
    

            
            
        
            
            
            
        

        /* Create entityvars */
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_SESSION_FEATURES_ACA4E614_1_INTERNAL_EV_SESSION_END_TIME AS (
            
                SELECT
                    session_main_id,
                    ANY_VALUE (session_end_time) AS session_end_time
                FROM (
            SELECT
                session_main_id,
                max(timestamp)
	
	 AS session_end_time
            FROM entityVarTable_session_3f3_1
            
            RIGHT JOIN inputVarTable_rsPages_ae0_1 USING (session_main_id)
            
            WHERE session_main_id IS NOT NULL
            
            
            GROUP BY session_main_id
            
        )
                GROUP BY session_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_session_3f3_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_session_3f3_1
                      LEFT JOIN MATERIAL_SHOPIFY_SESSION_FEATURES_ACA4E614_1_INTERNAL_EV_SESSION_END_TIME USING (session_main_id)
              );
              DROP TABLE entityVarTable_session_3f3_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_SESSION_3F3_1 AS (SELECT * FROM entityVarTable_session_3f3_1New);
              DROP TABLE entityVarTable_session_3f3_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_SESSION_FEATURES_ACA4E614_1_INTERNAL_EV_SESSION_END_TIME;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_SESSION_FEATURES_ACA4E614_1_INTERNAL_EV_SESSION_START_TIME AS (
            
                SELECT
                    session_main_id,
                    ANY_VALUE (session_start_time) AS session_start_time
                FROM (
            SELECT
                session_main_id,
                min(timestamp)
	
	 AS session_start_time
            FROM entityVarTable_session_3f3_1
            
            RIGHT JOIN inputVarTable_rsPages_ae0_1 USING (session_main_id)
            
            WHERE session_main_id IS NOT NULL
            
            
            GROUP BY session_main_id
            
        )
                GROUP BY session_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_session_3f3_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_session_3f3_1
                      LEFT JOIN MATERIAL_SHOPIFY_SESSION_FEATURES_ACA4E614_1_INTERNAL_EV_SESSION_START_TIME USING (session_main_id)
              );
              DROP TABLE entityVarTable_session_3f3_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_SESSION_3F3_1 AS (SELECT * FROM entityVarTable_session_3f3_1New);
              DROP TABLE entityVarTable_session_3f3_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_SESSION_FEATURES_ACA4E614_1_INTERNAL_EV_SESSION_START_TIME;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_SESSION_FEATURES_ACA4E614_1_INTERNAL_EV_SESSION_LENGTH AS (
            
                SELECT * FROM (
            SELECT
                session_main_id,
                TIMESTAMPDIFF(SECOND, session_start_time, session_end_time)
	
	 AS session_length
            FROM entityVarTable_session_3f3_1
            
            WHERE session_main_id IS NOT NULL
            
            
        )
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_session_3f3_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_session_3f3_1
                      LEFT JOIN MATERIAL_SHOPIFY_SESSION_FEATURES_ACA4E614_1_INTERNAL_EV_SESSION_LENGTH USING (session_main_id)
              );
              DROP TABLE entityVarTable_session_3f3_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_SESSION_3F3_1 AS (SELECT * FROM entityVarTable_session_3f3_1New);
              DROP TABLE entityVarTable_session_3f3_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_SESSION_FEATURES_ACA4E614_1_INTERNAL_EV_SESSION_LENGTH;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_SESSION_FEATURES_ACA4E614_1_INTERNAL_EV_USER_ID AS (
            
                SELECT
                    session_main_id,
                    ANY_VALUE (user_id) AS user_id
                FROM (
            SELECT
                session_main_id,
                max(user_id)
	
	 AS user_id
            FROM entityVarTable_session_3f3_1
            
            RIGHT JOIN inputVarTable_rsPages_ae0_1 USING (session_main_id)
            
            WHERE session_main_id IS NOT NULL
            
            AND user_id is not null
            
            
            GROUP BY session_main_id
            
        )
                GROUP BY session_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_session_3f3_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_session_3f3_1
                      LEFT JOIN MATERIAL_SHOPIFY_SESSION_FEATURES_ACA4E614_1_INTERNAL_EV_USER_ID USING (session_main_id)
              );
              DROP TABLE entityVarTable_session_3f3_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_SESSION_3F3_1 AS (SELECT * FROM entityVarTable_session_3f3_1New);
              DROP TABLE entityVarTable_session_3f3_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_SESSION_FEATURES_ACA4E614_1_INTERNAL_EV_USER_ID;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_SESSION_FEATURES_ACA4E614_1_INTERNAL_EV_ANONYMOUS_ID AS (
            
                SELECT
                    session_main_id,
                    ANY_VALUE (anonymous_id) AS anonymous_id
                FROM (
            SELECT
                session_main_id,
                max(anonymous_id)
	
	 AS anonymous_id
            FROM entityVarTable_session_3f3_1
            
            RIGHT JOIN inputVarTable_rsPages_ae0_1 USING (session_main_id)
            
            WHERE session_main_id IS NOT NULL
            
            AND anonymous_id is not null
            
            
            GROUP BY session_main_id
            
        )
                GROUP BY session_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_session_3f3_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_session_3f3_1
                      LEFT JOIN MATERIAL_SHOPIFY_SESSION_FEATURES_ACA4E614_1_INTERNAL_EV_ANONYMOUS_ID USING (session_main_id)
              );
              DROP TABLE entityVarTable_session_3f3_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_SESSION_3F3_1 AS (SELECT * FROM entityVarTable_session_3f3_1New);
              DROP TABLE entityVarTable_session_3f3_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_SESSION_FEATURES_ACA4E614_1_INTERNAL_EV_ANONYMOUS_ID;

        /* Handle default setting */
        

    

            
            
        

        /* Create output table */
        
        CREATE OR REPLACE TABLE MATERIAL_SHOPIFY_SESSION_FEATURES_ACA4E614_1 AS 
            SELECT session_main_id
                
                , '2023-05-29T06:34:49.347741Z' AS valid_at
                
            
                , session_end_time
            
                , session_start_time
            
                , session_length
            
                , user_id
            
                , anonymous_id
            
            FROM entityVarTable_session_3f3_1
        ;

        

        /* Drop temp tables */
        DROP TABLE entityVarTable_session_3f3_1;
        
        
            
            DROP TABLE inputVarTable_rsPages_ae0_1;
            
        
            
        
        
    
	END;

			
    
        
            
    
        
    
        
            
                DROP VIEW IF EXISTS MATERIAL_RSPAGES_AE075D7C_1;
            
        
    
        
    

        
    
 
	
	END;
	