
	
		BEGIN

		DROP VIEW IF EXISTS DUMMY_NON_EXISTENT_VIEW;

























  
  
  
  
  

			
    
        
            
    
        
    
        
    
        
            
                
                CREATE OR REPLACE VIEW MATERIAL_RSORDERCREATED_C9E04568_1 AS 
SELECT
    *
FROM
    RUDDERSTACK_TEST_DB.DATA_APPS_SIMULATED_SHOPIFY.ORDER_CREATED

WHERE
    
        ((
         timestamp <= '2023-05-29T06:34:49.347741Z'
        )
         OR timestamp IS NULL )
    

;
                
            
        
    
        
            
                
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
                
            
        
    
        
    
        
    
        
    
        
    
        
    
        
            
                
                CREATE OR REPLACE VIEW MATERIAL_RSCARTUPDATE_E50A60EA_1 AS 
SELECT
    *
FROM
    RUDDERSTACK_TEST_DB.DATA_APPS_SIMULATED_SHOPIFY.CART_UPDATE

WHERE
    
        ((
         timestamp <= '2023-05-29T06:34:49.347741Z'
        )
         OR timestamp IS NULL )
    

;
                
            
        
    
        
            
                
                CREATE OR REPLACE VIEW MATERIAL_RSORDERCANCELLED_4A77009B_1 AS 
SELECT
    *
FROM
    RUDDERSTACK_TEST_DB.DATA_APPS_SIMULATED_SHOPIFY.ORDER_CANCELLED

WHERE
    
        ((
         timestamp <= '2023-05-29T06:34:49.347741Z'
        )
         OR timestamp IS NULL )
    

;
                
            
        
    
        
            
                
                CREATE OR REPLACE VIEW MATERIAL_RSCARTCREATE_5B46D5D1_1 AS 
SELECT
    *
FROM
    RUDDERSTACK_TEST_DB.DATA_APPS_SIMULATED_SHOPIFY.CART_CREATE

WHERE
    
        ((
         timestamp <= '2023-05-29T06:34:49.347741Z'
        )
         OR timestamp IS NULL )
    

;
                
            
        
    
        
            
                
                CREATE OR REPLACE VIEW MATERIAL_RSTRACKS_59AABFAA_1 AS 
SELECT
    *
FROM
    RUDDERSTACK_TEST_DB.DATA_APPS_SIMULATED_SHOPIFY.TRACKS

WHERE
    
        ((
         timestamp <= '2023-05-29T06:34:49.347741Z'
        )
         OR timestamp IS NULL )
    

;
                
            
        
    
        
            
                
                CREATE OR REPLACE VIEW MATERIAL_RSIDENTIFIES_62307D86_1 AS 
SELECT
    *
FROM
    RUDDERSTACK_TEST_DB.DATA_APPS_SIMULATED_SHOPIFY.IDENTIFIES

WHERE
    
        ((
         timestamp <= '2023-05-29T06:34:49.347741Z'
        )
         OR timestamp IS NULL )
    

;
                
            
        
    

        
    

			/* Set template variables */




    







/* Macros */
























/* endMacros */



BEGIN

    
        
        
        
        
    
        
            DECLARE 
	is_not_converged_var  DEFAULT True;
	iter_var INTEGER DEFAULT 0;
	max_iter_var INTEGER DEFAULT 30;
	
            BEGIN 
    BEGIN

    
    CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW 
        (
            id1 varchar,
            id1_type varchar,
            id2 varchar,
            id2_type varchar,
            valid_at timestamp)
    ;

    
        
            
    
        
            
                
                    
                        INSERT INTO MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW (id1, id1_type, id2, id2_type, valid_at)
                            WITH temporary_IDedges as (
                            SELECT
                                user_id AS id1_temp,
                                'user_id' AS id1_type,
                                user_id AS id2_temp,
                                'user_id' AS id2_type,
                                MIN(timestamp) AS valid_at
                            FROM MATERIAL_RSIDENTIFIES_62307D86_1
                            
                            GROUP BY id1_temp, id2_temp)
                            SELECT id1_temp, id1_type, id2_temp, id2_type, valid_at FROM temporary_IDedges
                            WHERE
                                id1_temp IS NOT NULL
                                AND id1_type IS NOT NULL
                                AND id2_temp IS NOT NULL
                                AND id2_type IS NOT NULL
                        ;
                    
                
            
                
                    
                        INSERT INTO MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW (id1, id1_type, id2, id2_type, valid_at)
                            WITH temporary_IDedges as (
                            SELECT
                                user_id AS id1_temp,
                                'user_id' AS id1_type,
                                anonymous_id AS id2_temp,
                                'anonymous_id' AS id2_type,
                                MIN(timestamp) AS valid_at
                            FROM MATERIAL_RSIDENTIFIES_62307D86_1
                            
                            GROUP BY id1_temp, id2_temp)
                            SELECT id1_temp, id1_type, id2_temp, id2_type, valid_at FROM temporary_IDedges
                            WHERE
                                id1_temp IS NOT NULL
                                AND id1_type IS NOT NULL
                                AND id2_temp IS NOT NULL
                                AND id2_type IS NOT NULL
                        ;
                    
                
            
                
                    
                        INSERT INTO MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW (id1, id1_type, id2, id2_type, valid_at)
                            WITH temporary_IDedges as (
                            SELECT
                                user_id AS id1_temp,
                                'user_id' AS id1_type,
                                lower(email) AS id2_temp,
                                'email' AS id2_type,
                                MIN(timestamp) AS valid_at
                            FROM MATERIAL_RSIDENTIFIES_62307D86_1
                            
                            GROUP BY id1_temp, id2_temp)
                            SELECT id1_temp, id1_type, id2_temp, id2_type, valid_at FROM temporary_IDedges
                            WHERE
                                id1_temp IS NOT NULL
                                AND id1_type IS NOT NULL
                                AND id2_temp IS NOT NULL
                                AND id2_type IS NOT NULL
                        ;
                    
                
            
        
            
                
                    
                
            
                
                    
                        INSERT INTO MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW (id1, id1_type, id2, id2_type, valid_at)
                            WITH temporary_IDedges as (
                            SELECT
                                anonymous_id AS id1_temp,
                                'anonymous_id' AS id1_type,
                                anonymous_id AS id2_temp,
                                'anonymous_id' AS id2_type,
                                MIN(timestamp) AS valid_at
                            FROM MATERIAL_RSIDENTIFIES_62307D86_1
                            
                            GROUP BY id1_temp, id2_temp)
                            SELECT id1_temp, id1_type, id2_temp, id2_type, valid_at FROM temporary_IDedges
                            WHERE
                                id1_temp IS NOT NULL
                                AND id1_type IS NOT NULL
                                AND id2_temp IS NOT NULL
                                AND id2_type IS NOT NULL
                        ;
                    
                
            
                
                    
                        INSERT INTO MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW (id1, id1_type, id2, id2_type, valid_at)
                            WITH temporary_IDedges as (
                            SELECT
                                anonymous_id AS id1_temp,
                                'anonymous_id' AS id1_type,
                                lower(email) AS id2_temp,
                                'email' AS id2_type,
                                MIN(timestamp) AS valid_at
                            FROM MATERIAL_RSIDENTIFIES_62307D86_1
                            
                            GROUP BY id1_temp, id2_temp)
                            SELECT id1_temp, id1_type, id2_temp, id2_type, valid_at FROM temporary_IDedges
                            WHERE
                                id1_temp IS NOT NULL
                                AND id1_type IS NOT NULL
                                AND id2_temp IS NOT NULL
                                AND id2_type IS NOT NULL
                        ;
                    
                
            
        
            
                
                    
                
            
                
                    
                
            
                
                    
                        INSERT INTO MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW (id1, id1_type, id2, id2_type, valid_at)
                            WITH temporary_IDedges as (
                            SELECT
                                lower(email) AS id1_temp,
                                'email' AS id1_type,
                                lower(email) AS id2_temp,
                                'email' AS id2_type,
                                MIN(timestamp) AS valid_at
                            FROM MATERIAL_RSIDENTIFIES_62307D86_1
                            
                            GROUP BY id1_temp, id2_temp)
                            SELECT id1_temp, id1_type, id2_temp, id2_type, valid_at FROM temporary_IDedges
                            WHERE
                                id1_temp IS NOT NULL
                                AND id1_type IS NOT NULL
                                AND id2_temp IS NOT NULL
                                AND id2_type IS NOT NULL
                        ;
                    
                
            
        
    

        
    
        
            
    
        
            
                
                    
                        INSERT INTO MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW (id1, id1_type, id2, id2_type, valid_at)
                            WITH temporary_IDedges as (
                            SELECT
                                user_id AS id1_temp,
                                'user_id' AS id1_type,
                                user_id AS id2_temp,
                                'user_id' AS id2_type,
                                MIN(timestamp) AS valid_at
                            FROM MATERIAL_RSTRACKS_59AABFAA_1
                            
                            GROUP BY id1_temp, id2_temp)
                            SELECT id1_temp, id1_type, id2_temp, id2_type, valid_at FROM temporary_IDedges
                            WHERE
                                id1_temp IS NOT NULL
                                AND id1_type IS NOT NULL
                                AND id2_temp IS NOT NULL
                                AND id2_type IS NOT NULL
                        ;
                    
                
            
                
                    
                        INSERT INTO MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW (id1, id1_type, id2, id2_type, valid_at)
                            WITH temporary_IDedges as (
                            SELECT
                                user_id AS id1_temp,
                                'user_id' AS id1_type,
                                anonymous_id AS id2_temp,
                                'anonymous_id' AS id2_type,
                                MIN(timestamp) AS valid_at
                            FROM MATERIAL_RSTRACKS_59AABFAA_1
                            
                            GROUP BY id1_temp, id2_temp)
                            SELECT id1_temp, id1_type, id2_temp, id2_type, valid_at FROM temporary_IDedges
                            WHERE
                                id1_temp IS NOT NULL
                                AND id1_type IS NOT NULL
                                AND id2_temp IS NOT NULL
                                AND id2_type IS NOT NULL
                        ;
                    
                
            
        
            
                
                    
                
            
                
                    
                        INSERT INTO MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW (id1, id1_type, id2, id2_type, valid_at)
                            WITH temporary_IDedges as (
                            SELECT
                                anonymous_id AS id1_temp,
                                'anonymous_id' AS id1_type,
                                anonymous_id AS id2_temp,
                                'anonymous_id' AS id2_type,
                                MIN(timestamp) AS valid_at
                            FROM MATERIAL_RSTRACKS_59AABFAA_1
                            
                            GROUP BY id1_temp, id2_temp)
                            SELECT id1_temp, id1_type, id2_temp, id2_type, valid_at FROM temporary_IDedges
                            WHERE
                                id1_temp IS NOT NULL
                                AND id1_type IS NOT NULL
                                AND id2_temp IS NOT NULL
                                AND id2_type IS NOT NULL
                        ;
                    
                
            
        
    

        
    
        
            
    
        
            
                
                    
                        INSERT INTO MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW (id1, id1_type, id2, id2_type, valid_at)
                            WITH temporary_IDedges as (
                            SELECT
                                user_id AS id1_temp,
                                'user_id' AS id1_type,
                                user_id AS id2_temp,
                                'user_id' AS id2_type,
                                MIN(timestamp) AS valid_at
                            FROM MATERIAL_RSPAGES_AE075D7C_1
                            
                            GROUP BY id1_temp, id2_temp)
                            SELECT id1_temp, id1_type, id2_temp, id2_type, valid_at FROM temporary_IDedges
                            WHERE
                                id1_temp IS NOT NULL
                                AND id1_type IS NOT NULL
                                AND id2_temp IS NOT NULL
                                AND id2_type IS NOT NULL
                        ;
                    
                
            
                
                    
                        INSERT INTO MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW (id1, id1_type, id2, id2_type, valid_at)
                            WITH temporary_IDedges as (
                            SELECT
                                user_id AS id1_temp,
                                'user_id' AS id1_type,
                                anonymous_id AS id2_temp,
                                'anonymous_id' AS id2_type,
                                MIN(timestamp) AS valid_at
                            FROM MATERIAL_RSPAGES_AE075D7C_1
                            
                            GROUP BY id1_temp, id2_temp)
                            SELECT id1_temp, id1_type, id2_temp, id2_type, valid_at FROM temporary_IDedges
                            WHERE
                                id1_temp IS NOT NULL
                                AND id1_type IS NOT NULL
                                AND id2_temp IS NOT NULL
                                AND id2_type IS NOT NULL
                        ;
                    
                
            
                
                    
                        INSERT INTO MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW (id1, id1_type, id2, id2_type, valid_at)
                            WITH temporary_IDedges as (
                            SELECT
                                user_id AS id1_temp,
                                'user_id' AS id1_type,
                                concat(coalesce(anonymous_id, 'null'), coalesce(to_char(context_session_id), 'null')) AS id2_temp,
                                'session_id' AS id2_type,
                                MIN(timestamp) AS valid_at
                            FROM MATERIAL_RSPAGES_AE075D7C_1
                            
                            GROUP BY id1_temp, id2_temp)
                            SELECT id1_temp, id1_type, id2_temp, id2_type, valid_at FROM temporary_IDedges
                            WHERE
                                id1_temp IS NOT NULL
                                AND id1_type IS NOT NULL
                                AND id2_temp IS NOT NULL
                                AND id2_type IS NOT NULL
                        ;
                    
                
            
                
            
        
            
                
                    
                
            
                
                    
                        INSERT INTO MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW (id1, id1_type, id2, id2_type, valid_at)
                            WITH temporary_IDedges as (
                            SELECT
                                anonymous_id AS id1_temp,
                                'anonymous_id' AS id1_type,
                                anonymous_id AS id2_temp,
                                'anonymous_id' AS id2_type,
                                MIN(timestamp) AS valid_at
                            FROM MATERIAL_RSPAGES_AE075D7C_1
                            
                            GROUP BY id1_temp, id2_temp)
                            SELECT id1_temp, id1_type, id2_temp, id2_type, valid_at FROM temporary_IDedges
                            WHERE
                                id1_temp IS NOT NULL
                                AND id1_type IS NOT NULL
                                AND id2_temp IS NOT NULL
                                AND id2_type IS NOT NULL
                        ;
                    
                
            
                
                    
                        INSERT INTO MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW (id1, id1_type, id2, id2_type, valid_at)
                            WITH temporary_IDedges as (
                            SELECT
                                anonymous_id AS id1_temp,
                                'anonymous_id' AS id1_type,
                                concat(coalesce(anonymous_id, 'null'), coalesce(to_char(context_session_id), 'null')) AS id2_temp,
                                'session_id' AS id2_type,
                                MIN(timestamp) AS valid_at
                            FROM MATERIAL_RSPAGES_AE075D7C_1
                            
                            GROUP BY id1_temp, id2_temp)
                            SELECT id1_temp, id1_type, id2_temp, id2_type, valid_at FROM temporary_IDedges
                            WHERE
                                id1_temp IS NOT NULL
                                AND id1_type IS NOT NULL
                                AND id2_temp IS NOT NULL
                                AND id2_type IS NOT NULL
                        ;
                    
                
            
                
            
        
            
                
                    
                
            
                
                    
                
            
                
                    
                        INSERT INTO MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW (id1, id1_type, id2, id2_type, valid_at)
                            WITH temporary_IDedges as (
                            SELECT
                                concat(coalesce(anonymous_id, 'null'), coalesce(to_char(context_session_id), 'null')) AS id1_temp,
                                'session_id' AS id1_type,
                                concat(coalesce(anonymous_id, 'null'), coalesce(to_char(context_session_id), 'null')) AS id2_temp,
                                'session_id' AS id2_type,
                                MIN(timestamp) AS valid_at
                            FROM MATERIAL_RSPAGES_AE075D7C_1
                            
                            GROUP BY id1_temp, id2_temp)
                            SELECT id1_temp, id1_type, id2_temp, id2_type, valid_at FROM temporary_IDedges
                            WHERE
                                id1_temp IS NOT NULL
                                AND id1_type IS NOT NULL
                                AND id2_temp IS NOT NULL
                                AND id2_type IS NOT NULL
                        ;
                    
                
            
                
            
        
            
                
            
                
            
                
            
                
            
        
    

        
    
        
            
    
        
            
                
                    
                        INSERT INTO MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW (id1, id1_type, id2, id2_type, valid_at)
                            WITH temporary_IDedges as (
                            SELECT
                                user_id AS id1_temp,
                                'user_id' AS id1_type,
                                user_id AS id2_temp,
                                'user_id' AS id2_type,
                                MIN(timestamp) AS valid_at
                            FROM MATERIAL_RSORDERCREATED_C9E04568_1
                            
                            GROUP BY id1_temp, id2_temp)
                            SELECT id1_temp, id1_type, id2_temp, id2_type, valid_at FROM temporary_IDedges
                            WHERE
                                id1_temp IS NOT NULL
                                AND id1_type IS NOT NULL
                                AND id2_temp IS NOT NULL
                                AND id2_type IS NOT NULL
                        ;
                    
                
            
                
                    
                        INSERT INTO MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW (id1, id1_type, id2, id2_type, valid_at)
                            WITH temporary_IDedges as (
                            SELECT
                                user_id AS id1_temp,
                                'user_id' AS id1_type,
                                anonymous_id AS id2_temp,
                                'anonymous_id' AS id2_type,
                                MIN(timestamp) AS valid_at
                            FROM MATERIAL_RSORDERCREATED_C9E04568_1
                            
                            GROUP BY id1_temp, id2_temp)
                            SELECT id1_temp, id1_type, id2_temp, id2_type, valid_at FROM temporary_IDedges
                            WHERE
                                id1_temp IS NOT NULL
                                AND id1_type IS NOT NULL
                                AND id2_temp IS NOT NULL
                                AND id2_type IS NOT NULL
                        ;
                    
                
            
        
            
                
                    
                
            
                
                    
                        INSERT INTO MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW (id1, id1_type, id2, id2_type, valid_at)
                            WITH temporary_IDedges as (
                            SELECT
                                anonymous_id AS id1_temp,
                                'anonymous_id' AS id1_type,
                                anonymous_id AS id2_temp,
                                'anonymous_id' AS id2_type,
                                MIN(timestamp) AS valid_at
                            FROM MATERIAL_RSORDERCREATED_C9E04568_1
                            
                            GROUP BY id1_temp, id2_temp)
                            SELECT id1_temp, id1_type, id2_temp, id2_type, valid_at FROM temporary_IDedges
                            WHERE
                                id1_temp IS NOT NULL
                                AND id1_type IS NOT NULL
                                AND id2_temp IS NOT NULL
                                AND id2_type IS NOT NULL
                        ;
                    
                
            
        
    

        
    
        
            
    
        
            
                
                    
                        INSERT INTO MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW (id1, id1_type, id2, id2_type, valid_at)
                            WITH temporary_IDedges as (
                            SELECT
                                user_id AS id1_temp,
                                'user_id' AS id1_type,
                                user_id AS id2_temp,
                                'user_id' AS id2_type,
                                MIN(timestamp) AS valid_at
                            FROM MATERIAL_RSORDERCANCELLED_4A77009B_1
                            
                            GROUP BY id1_temp, id2_temp)
                            SELECT id1_temp, id1_type, id2_temp, id2_type, valid_at FROM temporary_IDedges
                            WHERE
                                id1_temp IS NOT NULL
                                AND id1_type IS NOT NULL
                                AND id2_temp IS NOT NULL
                                AND id2_type IS NOT NULL
                        ;
                    
                
            
                
                    
                        INSERT INTO MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW (id1, id1_type, id2, id2_type, valid_at)
                            WITH temporary_IDedges as (
                            SELECT
                                user_id AS id1_temp,
                                'user_id' AS id1_type,
                                anonymous_id AS id2_temp,
                                'anonymous_id' AS id2_type,
                                MIN(timestamp) AS valid_at
                            FROM MATERIAL_RSORDERCANCELLED_4A77009B_1
                            
                            GROUP BY id1_temp, id2_temp)
                            SELECT id1_temp, id1_type, id2_temp, id2_type, valid_at FROM temporary_IDedges
                            WHERE
                                id1_temp IS NOT NULL
                                AND id1_type IS NOT NULL
                                AND id2_temp IS NOT NULL
                                AND id2_type IS NOT NULL
                        ;
                    
                
            
        
            
                
                    
                
            
                
                    
                        INSERT INTO MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW (id1, id1_type, id2, id2_type, valid_at)
                            WITH temporary_IDedges as (
                            SELECT
                                anonymous_id AS id1_temp,
                                'anonymous_id' AS id1_type,
                                anonymous_id AS id2_temp,
                                'anonymous_id' AS id2_type,
                                MIN(timestamp) AS valid_at
                            FROM MATERIAL_RSORDERCANCELLED_4A77009B_1
                            
                            GROUP BY id1_temp, id2_temp)
                            SELECT id1_temp, id1_type, id2_temp, id2_type, valid_at FROM temporary_IDedges
                            WHERE
                                id1_temp IS NOT NULL
                                AND id1_type IS NOT NULL
                                AND id2_temp IS NOT NULL
                                AND id2_type IS NOT NULL
                        ;
                    
                
            
        
    

        
    
        
            
    
        
            
                
                    
                        INSERT INTO MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW (id1, id1_type, id2, id2_type, valid_at)
                            WITH temporary_IDedges as (
                            SELECT
                                anonymous_id AS id1_temp,
                                'anonymous_id' AS id1_type,
                                anonymous_id AS id2_temp,
                                'anonymous_id' AS id2_type,
                                MIN(timestamp) AS valid_at
                            FROM MATERIAL_RSCARTUPDATE_E50A60EA_1
                            
                            GROUP BY id1_temp, id2_temp)
                            SELECT id1_temp, id1_type, id2_temp, id2_type, valid_at FROM temporary_IDedges
                            WHERE
                                id1_temp IS NOT NULL
                                AND id1_type IS NOT NULL
                                AND id2_temp IS NOT NULL
                                AND id2_type IS NOT NULL
                        ;
                    
                
            
        
    

        
    
        
            
    
        
            
                
                    
                        INSERT INTO MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW (id1, id1_type, id2, id2_type, valid_at)
                            WITH temporary_IDedges as (
                            SELECT
                                anonymous_id AS id1_temp,
                                'anonymous_id' AS id1_type,
                                anonymous_id AS id2_temp,
                                'anonymous_id' AS id2_type,
                                MIN(timestamp) AS valid_at
                            FROM MATERIAL_RSCARTCREATE_5B46D5D1_1
                            
                            GROUP BY id1_temp, id2_temp)
                            SELECT id1_temp, id1_type, id2_temp, id2_type, valid_at FROM temporary_IDedges
                            WHERE
                                id1_temp IS NOT NULL
                                AND id1_type IS NOT NULL
                                AND id2_temp IS NOT NULL
                                AND id2_type IS NOT NULL
                        ;
                    
                
            
        
    

        
    

    DELETE FROM MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW WHERE
        id1_type NOT IN (
    'anonymous_id', 'cart_token', 'email', 'user_id'
)
        OR id2_type NOT IN (
    'anonymous_id', 'cart_token', 'email', 'user_id'
);

        
            
            
                DELETE FROM MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW WHERE NOT (
    
    (
        id1_type != 'anonymous_id' OR
        NOT
        (
            
            id1 = ''
            
            
        )
    )

    AND 
    (
        id2_type != 'anonymous_id' OR
        NOT
        (
            
            id2 = ''
            
            
        )
    )

);
            
            
            
                DELETE FROM MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW WHERE NOT (
    
    (
        id1_type != 'anonymous_id' OR
        NOT
        (
            
            id1 = 'unknown'
            
            
        )
    )

    AND 
    (
        id2_type != 'anonymous_id' OR
        NOT
        (
            
            id2 = 'unknown'
            
            
        )
    )

);
            
            
            
                DELETE FROM MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW WHERE NOT (
    
    (
        id1_type != 'anonymous_id' OR
        NOT
        (
            
            id1 = 'NaN'
            
            
        )
    )

    AND 
    (
        id2_type != 'anonymous_id' OR
        NOT
        (
            
            id2 = 'NaN'
            
            
        )
    )

);
            
            
        
            
        
            
            
                DELETE FROM MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW WHERE NOT (
    
    (
        id1_type != 'email' OR
        
        (
            
            
             id1 regexp '.+@.+'
            
        )
    )

    AND 
    (
        id2_type != 'email' OR
        
        (
            
            
             id2 regexp '.+@.+'
            
        )
    )

);
            
            
            
                DELETE FROM MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW WHERE NOT (
    
    (
        id1_type != 'email' OR
        NOT
        (
            
            id1 = 'test@company.com'
            
            
        )
    )

    AND 
    (
        id2_type != 'email' OR
        NOT
        (
            
            id2 = 'test@company.com'
            
            
        )
    )

);
            
            
        
            
        

        
        
            CREATE OR REPLACE TABLE MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES AS 
            SELECT
                id1,
                id1_type,
                id2,
                id2_type,
                LEAST(
                    
    
    'rid' || left(sha1(concat(left(sha1('fe971b24-9572-4005-b22f-351e9c09274d' || NVL(id1,'')),32),NVL(id1_type,''))),32)
    
,
                    
    
    'rid' || left(sha1(concat(left(sha1('fe971b24-9572-4005-b22f-351e9c09274d' || NVL(id2,'')),32),NVL(id2_type,''))),32)
    

                ) AS user_main_id,
                MIN(valid_at) as valid_at
            FROM MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW
            WHERE
                id1 IS NOT NULL
                AND id1_type IS NOT NULL
                AND id2 IS NOT NULL
                AND id2_type IS NOT NULL
            GROUP BY id1, id2, id1_type, id2_type
        ;
        

        DROP TABLE MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES_RAW;

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_ACTIVE_MAPPING 
    (
        node_id varchar,
        node_id_type varchar,
        user_main_id varchar,
        valid_at timestamp,
        user_main_id_dist int,
        stitching_active int)
;
        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_FINISHED_MAPPING 
    (
        node_id varchar,
        node_id_type varchar,
        user_main_id varchar,
        valid_at timestamp,
        user_main_id_dist int,
        stitching_active int)
;

        INSERT INTO MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_ACTIVE_MAPPING (
            node_id,
            node_id_type,
            user_main_id,
            valid_at,
            user_main_id_dist,
            stitching_active
        )
        SELECT
            id1 AS node_id,
            id1_type AS node_id_type,
            user_main_id,
            valid_at,
            0 AS user_main_id_dist,
            0 AS stitching_active
        FROM
            MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES
        UNION ALL
        SELECT
            id2 AS node_id,
            id2_type AS node_id_type,
            user_main_id,
            valid_at,
            0 AS user_main_id_dist,
            0 AS stitching_active
        FROM
            MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_EDGES
        ;

        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_ACTIVE_MAPPING AS (
    SELECT
        node_id,
        node_id_type,
        user_main_id,
        valid_at,
        user_main_id_dist,
        0 AS stitching_active
    FROM (
        SELECT
            node_id,
            node_id_type,
            user_main_id,
            valid_at,
            user_main_id_dist,
            row_number() OVER (
                PARTITION BY node_id, node_id_type, user_main_id
                ORDER BY valid_at ASC, user_main_id_dist ASC
            ) AS row_number
        FROM MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_ACTIVE_MAPPING
    )
    WHERE row_number = 1
);

        

    /* Set which clusters are actively stitching */
    CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_ACTIVE_MAPPING AS (
    SELECT
        node_id,
        node_id_type,
        user_main_id,
        user_main_id_dist,
        valid_at,
        MAX(stitching_active) OVER (PARTITION BY user_main_id) AS stitching_active
    FROM (
        SELECT
            node_id,
            node_id_type,
            user_main_id,
            user_main_id_dist,
            valid_at,
            CASE
                WHEN min_user_main_id_node = max_user_main_id_node THEN 0
                ELSE 1
                END AS stitching_active
        FROM (
            SELECT
                node_id,
                node_id_type,
                user_main_id,
                user_main_id_dist,
                valid_at,
                MIN(user_main_id) OVER (PARTITION BY node_id, node_id_type) AS min_user_main_id_node,
                MAX(user_main_id) OVER (PARTITION BY node_id, node_id_type) AS max_user_main_id_node
            FROM MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_ACTIVE_MAPPING
        )
    )
);

    /* Insert converged clusters into finished table */
    INSERT INTO MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_FINISHED_MAPPING (
        node_id, node_id_type, user_main_id, valid_at, user_main_id_dist, stitching_active
    )
    SELECT
        node_id,
        node_id_type,
        user_main_id,
        valid_at,
        user_main_id_dist,
        stitching_active
    FROM MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_ACTIVE_MAPPING
    WHERE stitching_active = 0;

    /* Remove converged clusters from active table */
    DELETE FROM MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_ACTIVE_MAPPING WHERE stitching_active = 0;



        WHILE ((iter_var < max_iter_var) AND is_not_converged_var) DO 
    iter_var := iter_var + 1;

    /* Run 1 round of cluster merging as follows
    select distinct new_user_main_id AS user_main_id, node_id into table FROM
        partition mappings by user_main_id (clusters), select min node_user_main_id as new_user_main_id FROM
            partition mappings by node, select min user_main_id as node_user_main_id
    */
    
    CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_ACTIVE_MAPPING AS (
    SELECT DISTINCT
        node_id,
        node_id_type,
        CASE
            WHEN new_user_main_id < user_main_id THEN new_user_main_id
            ELSE user_main_id
            END AS user_main_id,
        CASE
            WHEN new_user_main_id < user_main_id THEN
                CASE
                    WHEN new_valid_at is NULL or valid_at is NULL THEN NULL
                        ELSE GREATEST(new_valid_at, valid_at)
                END
            ELSE valid_at
            END AS valid_at,
        CASE
            WHEN new_user_main_id < user_main_id THEN new_user_main_id_dist + 1
            ELSE user_main_id_dist
            END AS user_main_id_dist,
        0 AS stitching_active
    FROM (
        SELECT
            node_id,
            node_id_type,
            user_main_id,
            valid_at,
            user_main_id_dist,
            FIRST_VALUE(new_user_main_id) over(
                PARTITION BY user_main_id
                ORDER BY
                    new_user_main_id ASC,
                    new_valid_at ASC,
                    new_user_main_id_dist ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS new_user_main_id,
            FIRST_VALUE(new_valid_at) over(
                PARTITION BY user_main_id
                ORDER BY
                    new_user_main_id ASC,
                    new_valid_at ASC,
                    new_user_main_id_dist ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS new_valid_at,
            FIRST_VALUE(new_user_main_id_dist) over(
                PARTITION BY user_main_id
                ORDER BY
                    new_user_main_id ASC,
                    new_valid_at ASC,
                    new_user_main_id_dist ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS new_user_main_id_dist
        FROM (
            SELECT
                *,
                FIRST_VALUE(user_main_id) over(
                    PARTITION BY node_id, node_id_type
                    ORDER BY
                        user_main_id ASC,
                        valid_at ASC,
                        user_main_id_dist ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS new_user_main_id,
                FIRST_VALUE(valid_at) over(
                    PARTITION BY node_id, node_id_type
                    ORDER BY
                        user_main_id ASC,
                        valid_at ASC,
                        user_main_id_dist ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS new_valid_at,
                FIRST_VALUE(user_main_id_dist) over(
                    PARTITION BY node_id, node_id_type
                    ORDER BY
                        user_main_id ASC,
                        valid_at ASC,
                        user_main_id_dist ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS new_user_main_id_dist
            FROM
                MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_ACTIVE_MAPPING
        )
    )
    );

    

    /* Set which clusters are actively stitching */
    CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_ACTIVE_MAPPING AS (
    SELECT
        node_id,
        node_id_type,
        user_main_id,
        user_main_id_dist,
        valid_at,
        MAX(stitching_active) OVER (PARTITION BY user_main_id) AS stitching_active
    FROM (
        SELECT
            node_id,
            node_id_type,
            user_main_id,
            user_main_id_dist,
            valid_at,
            CASE
                WHEN min_user_main_id_node = max_user_main_id_node THEN 0
                ELSE 1
                END AS stitching_active
        FROM (
            SELECT
                node_id,
                node_id_type,
                user_main_id,
                user_main_id_dist,
                valid_at,
                MIN(user_main_id) OVER (PARTITION BY node_id, node_id_type) AS min_user_main_id_node,
                MAX(user_main_id) OVER (PARTITION BY node_id, node_id_type) AS max_user_main_id_node
            FROM MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_ACTIVE_MAPPING
        )
    )
);

    /* Insert converged clusters into finished table */
    INSERT INTO MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_FINISHED_MAPPING (
        node_id, node_id_type, user_main_id, valid_at, user_main_id_dist, stitching_active
    )
    SELECT
        node_id,
        node_id_type,
        user_main_id,
        valid_at,
        user_main_id_dist,
        stitching_active
    FROM MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_ACTIVE_MAPPING
    WHERE stitching_active = 0;

    /* Remove converged clusters from active table */
    DELETE FROM MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_ACTIVE_MAPPING WHERE stitching_active = 0;



    SELECT not_converged INTO :is_not_converged_var FROM (SELECT COUNT(*) > 0 AS not_converged FROM MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_ACTIVE_MAPPING);
 END WHILE;

        CREATE OR REPLACE TABLE MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_MAPPING AS SELECT node_id, node_id_type, user_main_id, valid_at, user_main_id_dist, stitching_active FROM MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_FINISHED_MAPPING UNION ALL SELECT node_id, node_id_type, user_main_id, valid_at, user_main_id_dist, stitching_active FROM MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_ACTIVE_MAPPING;

        CREATE OR REPLACE VIEW MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1 AS 
    WITH ranked_mappings AS (
        SELECT
            user_main_id,
            node_id AS other_id,
            node_id_type AS other_id_type,
            valid_at,
            row_number() OVER (
                PARTITION BY node_id, node_id_type, user_main_id
                ORDER BY
                 valid_at ASC
            ) AS row_number
        FROM MATERIAL_SHOPIFY_USER_ID_STITCHER_EF33ACB8_1_INTERNAL_MAPPING
    )
    SELECT
        user_main_id,
        other_id,
        other_id_type,
        valid_at
    FROM ranked_mappings
    WHERE row_number = 1
;

    END;
 END;
        
    

	END;

			
    
        
            
    
        
    
        
    
        
            
                DROP VIEW IF EXISTS MATERIAL_RSORDERCREATED_C9E04568_1;
            
        
    
        
            
                DROP VIEW IF EXISTS MATERIAL_RSPAGES_AE075D7C_1;
            
        
    
        
    
        
    
        
    
        
    
        
    
        
            
                DROP VIEW IF EXISTS MATERIAL_RSCARTUPDATE_E50A60EA_1;
            
        
    
        
            
                DROP VIEW IF EXISTS MATERIAL_RSORDERCANCELLED_4A77009B_1;
            
        
    
        
            
                DROP VIEW IF EXISTS MATERIAL_RSCARTCREATE_5B46D5D1_1;
            
        
    
        
            
                DROP VIEW IF EXISTS MATERIAL_RSTRACKS_59AABFAA_1;
            
        
    
        
            
                DROP VIEW IF EXISTS MATERIAL_RSIDENTIFIES_62307D86_1;
            
        
    

        
    
 
	
	END;
	