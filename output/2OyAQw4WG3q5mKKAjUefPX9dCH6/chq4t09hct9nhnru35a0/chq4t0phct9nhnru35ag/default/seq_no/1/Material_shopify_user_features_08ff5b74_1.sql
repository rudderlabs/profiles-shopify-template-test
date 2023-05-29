
	
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
                
            
        
    
        
    
        
    
        
    
        
    
        
    
        
            
                
                CREATE OR REPLACE VIEW MATERIAL_RSITEMSPURCHASEDEVERSKU_25BFE499_1 AS 
  select t.value['sku'] as SKU,products,token,ANONYMOUS_ID,USER_ID,TIMESTAMP,order_number,cart_token
    from (select * from Material_rsOrderCreated_c9e04568_1 ), table(flatten(parse_json(products))) t where products is not null

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
                
            
        
    
        
            
                
                CREATE OR REPLACE VIEW MATERIAL_RSITEMSPURCHASEDEVERCTE_DA75AA63_1 AS 
 SELECT * , row_number() OVER (PARTITION BY token ORDER BY timestamp DESC) AS rn
    FROM   Material_rsItemsPurchasedEverSku_25bfe499_1
  
;
                
            
        
    
        
    
        
            
                
                CREATE OR REPLACE VIEW MATERIAL_RSCARTLINEITEMS_073458D1_1 AS 

SELECT to_char(t.value['brand']) AS brand,
    t.value['discounted_price']::real AS discounted_price,
    to_char(t.value['gift_card']) AS gift_card,
    t.value['grams']::real AS grams,
    to_char(t.value['id']) AS id,
    to_char(t.value['key']) AS KEY,
    t.value['line_price']::real AS line_price,
    t.value['original_line_price']::real AS original_line_price,
    t.value['original_price']::real AS original_price,
    t.value['price']::real AS price,
    to_char(t.value['product_id']) AS product_id,
    to_char(t.value['properties']) AS properties,
    t.value['quantity']::real AS quantity,
    to_char(t.value['sku']) AS sku,
    to_char(t.value['taxable']) AS taxable,
    to_char(t.value['title']) AS title,
    t.value['total_discount']::real AS total_discount,
    to_char(t.value['variant']) AS _VARIANT_,
    products,
    anonymous_id,timestamp, token
FROM
(SELECT *
FROM
    (SELECT *,
            row_number() over(PARTITION BY anonymous_id, token
                                        ORDER BY timestamp DESC) AS rn
    FROM Material_rsCartUpdate_e50a60ea_1 where products is not null)
WHERE rn = 1), table(flatten(parse_json(products))) t
        
;
                
            
        
    
        
            
                
                CREATE OR REPLACE VIEW MATERIAL_RSORDERCREATEDORDERCANCELLED_1AE9978F_1 AS 
    select 
    a.ANONYMOUS_ID,
    a.user_id,
    a.total_price_usd,
    a.products,
    a.payment_details_credit_card_company,
    a.order_number,
    a.timestamp,
    a.cart_token,
    a.fulfillment_status,
    a.financial_status,
    b.anonymous_id as anonymous_id_order_cancelled,
    b.user_id as user_id_order_cancelled ,
    b.total_price_usd as total_price_usd_order_cancelled,
    b.order_number as order_number_order_cancelled,
    b.financial_status as financial_status_order_cancelled,
    b.cart_token as cart_token_order_cancelled,
    b.timestamp as timestamp_order_cancelled,
    b.products as products_order_cancelled 
    from Material_rsOrderCreated_c9e04568_1 a left join Material_rsOrderCancelled_4a77009b_1 b 
    on a.user_id = b.user_id and a.order_number = b.order_number
 
       
;
                
            
        
    
        
            
                
                CREATE OR REPLACE VIEW MATERIAL_RSCARTCREATEUNIONCARTUPDATE_9415C17C_1 AS 
    select anonymous_id,timestamp,token from Material_rsCartCreate_5b46d5d1_1
    union all 
    select anonymous_id,timestamp,token from Material_rsCartUpdate_e50a60ea_1

;
                
            
        
    
        
    
        
            
                
                CREATE OR REPLACE VIEW MATERIAL_RSITEMSPURCHASEDEVER_4B111E2F_1 AS 

    SELECT *
    FROM   Material_rsItemsPurchasedEverCte_da75aa63_1
    WHERE  rn = 1
    
;
                
            
        
    
        
            
                
                CREATE OR REPLACE VIEW MATERIAL_RSSESSIONTABLE_3F477D2A_1 AS 
    select * from Material_shopify_session_features_aca4e614_1

;
                
            
        
    

        
    

			/* Macros */
/* Declare and Define Macro for user defined functions */










/* endMacros */

/* Define macros */
    BEGIN

        
    
    CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (
        SELECT DISTINCT user_main_id FROM Material_shopify_user_id_stitcher_ef33acb8_1
    );

        /* Create input tables */
        
            
            
                
    
    
        CREATE TEMPORARY TABLE inputVarTable_rsCartCreate_5b4_1 AS (
            SELECT
                
        left(sha1(random()::text),32)
 AS input_row_id,
                COALESCE(NULL, Material_shopify_user_id_stitcher_ef33acb8_1_1.user_main_id) AS user_main_id,
                Material_rsCartCreate_5b46d5d1_1.*
            FROM
                Material_rsCartCreate_5b46d5d1_1
            
                        LEFT JOIN Material_shopify_user_id_stitcher_ef33acb8_1 AS Material_shopify_user_id_stitcher_ef33acb8_1_1
                    
                    
                        ON anonymous_id = Material_shopify_user_id_stitcher_ef33acb8_1_1.other_id
                        AND 'anonymous_id' = Material_shopify_user_id_stitcher_ef33acb8_1_1.other_id_type
                    
        );
    
    

            
            
        
            
            
                
    
    
        CREATE TEMPORARY TABLE inputVarTable_rsCartCreateUnionCartUpdate_941_1 AS (
            SELECT
                
        left(sha1(random()::text),32)
 AS input_row_id,
                COALESCE(NULL, Material_shopify_user_id_stitcher_ef33acb8_1_1.user_main_id) AS user_main_id,
                Material_rsCartCreateUnionCartUpdate_9415c17c_1.*
            FROM
                Material_rsCartCreateUnionCartUpdate_9415c17c_1
            
                        LEFT JOIN Material_shopify_user_id_stitcher_ef33acb8_1 AS Material_shopify_user_id_stitcher_ef33acb8_1_1
                    
                    
                        ON anonymous_id = Material_shopify_user_id_stitcher_ef33acb8_1_1.other_id
                        AND 'anonymous_id' = Material_shopify_user_id_stitcher_ef33acb8_1_1.other_id_type
                    
        );
    
    

            
            
        
            
            
                
    
    
        CREATE TEMPORARY TABLE inputVarTable_rsCartLineItems_073_1 AS (
            SELECT
                
        left(sha1(random()::text),32)
 AS input_row_id,
                COALESCE(NULL, Material_shopify_user_id_stitcher_ef33acb8_1_1.user_main_id) AS user_main_id,
                Material_rsCartLineItems_073458d1_1.*
            FROM
                Material_rsCartLineItems_073458d1_1
            
                        LEFT JOIN Material_shopify_user_id_stitcher_ef33acb8_1 AS Material_shopify_user_id_stitcher_ef33acb8_1_1
                    
                    
                        ON anonymous_id = Material_shopify_user_id_stitcher_ef33acb8_1_1.other_id
                        AND 'anonymous_id' = Material_shopify_user_id_stitcher_ef33acb8_1_1.other_id_type
                    
        );
    
    

            
            
        
            
            
                
    
    
        CREATE TEMPORARY TABLE inputVarTable_rsCartUpdate_e50_1 AS (
            SELECT
                
        left(sha1(random()::text),32)
 AS input_row_id,
                COALESCE(NULL, Material_shopify_user_id_stitcher_ef33acb8_1_1.user_main_id) AS user_main_id,
                Material_rsCartUpdate_e50a60ea_1.*
            FROM
                Material_rsCartUpdate_e50a60ea_1
            
                        LEFT JOIN Material_shopify_user_id_stitcher_ef33acb8_1 AS Material_shopify_user_id_stitcher_ef33acb8_1_1
                    
                    
                        ON anonymous_id = Material_shopify_user_id_stitcher_ef33acb8_1_1.other_id
                        AND 'anonymous_id' = Material_shopify_user_id_stitcher_ef33acb8_1_1.other_id_type
                    
        );
    
    

            
            
        
            
            
                
    
    
        CREATE TEMPORARY TABLE inputVarTable_rsIdentifies_623_1 AS (
            SELECT
                
        left(sha1(random()::text),32)
 AS input_row_id,
                COALESCE(NULL, Material_shopify_user_id_stitcher_ef33acb8_1_1.user_main_id, Material_shopify_user_id_stitcher_ef33acb8_1_2.user_main_id, Material_shopify_user_id_stitcher_ef33acb8_1_3.user_main_id) AS user_main_id,
                Material_rsIdentifies_62307d86_1.*
            FROM
                Material_rsIdentifies_62307d86_1
            
                        LEFT JOIN Material_shopify_user_id_stitcher_ef33acb8_1 AS Material_shopify_user_id_stitcher_ef33acb8_1_1
                    
                    
                        ON user_id = Material_shopify_user_id_stitcher_ef33acb8_1_1.other_id
                        AND 'user_id' = Material_shopify_user_id_stitcher_ef33acb8_1_1.other_id_type
                    
                        LEFT JOIN Material_shopify_user_id_stitcher_ef33acb8_1 AS Material_shopify_user_id_stitcher_ef33acb8_1_2
                    
                    
                        ON anonymous_id = Material_shopify_user_id_stitcher_ef33acb8_1_2.other_id
                        AND 'anonymous_id' = Material_shopify_user_id_stitcher_ef33acb8_1_2.other_id_type
                    
                        LEFT JOIN Material_shopify_user_id_stitcher_ef33acb8_1 AS Material_shopify_user_id_stitcher_ef33acb8_1_3
                    
                    
                        ON lower(email) = Material_shopify_user_id_stitcher_ef33acb8_1_3.other_id
                        AND 'email' = Material_shopify_user_id_stitcher_ef33acb8_1_3.other_id_type
                    
        );
    
    

            
            
        
            
            
                
    
    
        CREATE TEMPORARY TABLE inputVarTable_rsItemsPurchasedEver_4b1_1 AS (
            SELECT
                
        left(sha1(random()::text),32)
 AS input_row_id,
                COALESCE(NULL, Material_shopify_user_id_stitcher_ef33acb8_1_1.user_main_id, Material_shopify_user_id_stitcher_ef33acb8_1_2.user_main_id) AS user_main_id,
                Material_rsItemsPurchasedEver_4b111e2f_1.*
            FROM
                Material_rsItemsPurchasedEver_4b111e2f_1
            
                        LEFT JOIN Material_shopify_user_id_stitcher_ef33acb8_1 AS Material_shopify_user_id_stitcher_ef33acb8_1_1
                    
                    
                        ON user_id = Material_shopify_user_id_stitcher_ef33acb8_1_1.other_id
                        AND 'user_id' = Material_shopify_user_id_stitcher_ef33acb8_1_1.other_id_type
                    
                        LEFT JOIN Material_shopify_user_id_stitcher_ef33acb8_1 AS Material_shopify_user_id_stitcher_ef33acb8_1_2
                    
                    
                        ON anonymous_id = Material_shopify_user_id_stitcher_ef33acb8_1_2.other_id
                        AND 'anonymous_id' = Material_shopify_user_id_stitcher_ef33acb8_1_2.other_id_type
                    
        );
    
    

            
            
        
            
            
                
    
    
        CREATE TEMPORARY TABLE inputVarTable_rsOrderCancelled_4a7_1 AS (
            SELECT
                
        left(sha1(random()::text),32)
 AS input_row_id,
                COALESCE(NULL, Material_shopify_user_id_stitcher_ef33acb8_1_1.user_main_id, Material_shopify_user_id_stitcher_ef33acb8_1_2.user_main_id) AS user_main_id,
                Material_rsOrderCancelled_4a77009b_1.*
            FROM
                Material_rsOrderCancelled_4a77009b_1
            
                        LEFT JOIN Material_shopify_user_id_stitcher_ef33acb8_1 AS Material_shopify_user_id_stitcher_ef33acb8_1_1
                    
                    
                        ON user_id = Material_shopify_user_id_stitcher_ef33acb8_1_1.other_id
                        AND 'user_id' = Material_shopify_user_id_stitcher_ef33acb8_1_1.other_id_type
                    
                        LEFT JOIN Material_shopify_user_id_stitcher_ef33acb8_1 AS Material_shopify_user_id_stitcher_ef33acb8_1_2
                    
                    
                        ON anonymous_id = Material_shopify_user_id_stitcher_ef33acb8_1_2.other_id
                        AND 'anonymous_id' = Material_shopify_user_id_stitcher_ef33acb8_1_2.other_id_type
                    
        );
    
    

            
            
        
            
            
                
    
    
        CREATE TEMPORARY TABLE inputVarTable_rsOrderCreated_c9e_1 AS (
            SELECT
                
        left(sha1(random()::text),32)
 AS input_row_id,
                COALESCE(NULL, Material_shopify_user_id_stitcher_ef33acb8_1_1.user_main_id, Material_shopify_user_id_stitcher_ef33acb8_1_2.user_main_id) AS user_main_id,
                Material_rsOrderCreated_c9e04568_1.*
            FROM
                Material_rsOrderCreated_c9e04568_1
            
                        LEFT JOIN Material_shopify_user_id_stitcher_ef33acb8_1 AS Material_shopify_user_id_stitcher_ef33acb8_1_1
                    
                    
                        ON user_id = Material_shopify_user_id_stitcher_ef33acb8_1_1.other_id
                        AND 'user_id' = Material_shopify_user_id_stitcher_ef33acb8_1_1.other_id_type
                    
                        LEFT JOIN Material_shopify_user_id_stitcher_ef33acb8_1 AS Material_shopify_user_id_stitcher_ef33acb8_1_2
                    
                    
                        ON anonymous_id = Material_shopify_user_id_stitcher_ef33acb8_1_2.other_id
                        AND 'anonymous_id' = Material_shopify_user_id_stitcher_ef33acb8_1_2.other_id_type
                    
        );
    
    

            
            
        
            
            
                
    
    
        CREATE TEMPORARY TABLE inputVarTable_rsOrderCreatedOrderCancelled_1ae_1 AS (
            SELECT
                
        left(sha1(random()::text),32)
 AS input_row_id,
                COALESCE(NULL, Material_shopify_user_id_stitcher_ef33acb8_1_1.user_main_id, Material_shopify_user_id_stitcher_ef33acb8_1_2.user_main_id, Material_shopify_user_id_stitcher_ef33acb8_1_3.user_main_id) AS user_main_id,
                Material_rsOrderCreatedOrderCancelled_1ae9978f_1.*
            FROM
                Material_rsOrderCreatedOrderCancelled_1ae9978f_1
            
                        LEFT JOIN Material_shopify_user_id_stitcher_ef33acb8_1 AS Material_shopify_user_id_stitcher_ef33acb8_1_1
                    
                    
                        ON user_id = Material_shopify_user_id_stitcher_ef33acb8_1_1.other_id
                        AND 'user_id' = Material_shopify_user_id_stitcher_ef33acb8_1_1.other_id_type
                    
                        LEFT JOIN Material_shopify_user_id_stitcher_ef33acb8_1 AS Material_shopify_user_id_stitcher_ef33acb8_1_2
                    
                    
                        ON anonymous_id = Material_shopify_user_id_stitcher_ef33acb8_1_2.other_id
                        AND 'anonymous_id' = Material_shopify_user_id_stitcher_ef33acb8_1_2.other_id_type
                    
                        LEFT JOIN Material_shopify_user_id_stitcher_ef33acb8_1 AS Material_shopify_user_id_stitcher_ef33acb8_1_3
                    
                    
                        ON cart_token = Material_shopify_user_id_stitcher_ef33acb8_1_3.other_id
                        AND 'cart_token' = Material_shopify_user_id_stitcher_ef33acb8_1_3.other_id_type
                    
        );
    
    

            
            
        
            
            
                
    
    
        CREATE TEMPORARY TABLE inputVarTable_rsPages_ae0_1 AS (
            SELECT
                
        left(sha1(random()::text),32)
 AS input_row_id,
                COALESCE(NULL, Material_shopify_user_id_stitcher_ef33acb8_1_1.user_main_id, Material_shopify_user_id_stitcher_ef33acb8_1_2.user_main_id, Material_shopify_user_id_stitcher_ef33acb8_1_3.user_main_id) AS user_main_id,
                Material_rsPages_ae075d7c_1.*
            FROM
                Material_rsPages_ae075d7c_1
            
                        LEFT JOIN Material_shopify_user_id_stitcher_ef33acb8_1 AS Material_shopify_user_id_stitcher_ef33acb8_1_1
                    
                    
                        ON user_id = Material_shopify_user_id_stitcher_ef33acb8_1_1.other_id
                        AND 'user_id' = Material_shopify_user_id_stitcher_ef33acb8_1_1.other_id_type
                    
                        LEFT JOIN Material_shopify_user_id_stitcher_ef33acb8_1 AS Material_shopify_user_id_stitcher_ef33acb8_1_2
                    
                    
                        ON anonymous_id = Material_shopify_user_id_stitcher_ef33acb8_1_2.other_id
                        AND 'anonymous_id' = Material_shopify_user_id_stitcher_ef33acb8_1_2.other_id_type
                    
                        LEFT JOIN Material_shopify_user_id_stitcher_ef33acb8_1 AS Material_shopify_user_id_stitcher_ef33acb8_1_3
                    
                    
                        ON concat(coalesce(anonymous_id, 'null'), coalesce(to_char(context_session_id), 'null')) = Material_shopify_user_id_stitcher_ef33acb8_1_3.other_id
                        AND 'session_id' = Material_shopify_user_id_stitcher_ef33acb8_1_3.other_id_type
                    
        );
    
    

            
            
        
            
            
                
    
    
        CREATE TEMPORARY TABLE inputVarTable_rsSessionTable_3f4_1 AS (
            SELECT
                
        left(sha1(random()::text),32)
 AS input_row_id,
                COALESCE(NULL, Material_shopify_user_id_stitcher_ef33acb8_1_1.user_main_id, Material_shopify_user_id_stitcher_ef33acb8_1_2.user_main_id) AS user_main_id,
                Material_rsSessionTable_3f477d2a_1.*
            FROM
                Material_rsSessionTable_3f477d2a_1
            
                        LEFT JOIN Material_shopify_user_id_stitcher_ef33acb8_1 AS Material_shopify_user_id_stitcher_ef33acb8_1_1
                    
                    
                        ON user_id = Material_shopify_user_id_stitcher_ef33acb8_1_1.other_id
                        AND 'user_id' = Material_shopify_user_id_stitcher_ef33acb8_1_1.other_id_type
                    
                        LEFT JOIN Material_shopify_user_id_stitcher_ef33acb8_1 AS Material_shopify_user_id_stitcher_ef33acb8_1_2
                    
                    
                        ON anonymous_id = Material_shopify_user_id_stitcher_ef33acb8_1_2.other_id
                        AND 'anonymous_id' = Material_shopify_user_id_stitcher_ef33acb8_1_2.other_id_type
                    
        );
    
    

            
            
        
            
            
                
    
    
        CREATE TEMPORARY TABLE inputVarTable_rsTracks_59a_1 AS (
            SELECT
                
        left(sha1(random()::text),32)
 AS input_row_id,
                COALESCE(NULL, Material_shopify_user_id_stitcher_ef33acb8_1_1.user_main_id, Material_shopify_user_id_stitcher_ef33acb8_1_2.user_main_id) AS user_main_id,
                Material_rsTracks_59aabfaa_1.*
            FROM
                Material_rsTracks_59aabfaa_1
            
                        LEFT JOIN Material_shopify_user_id_stitcher_ef33acb8_1 AS Material_shopify_user_id_stitcher_ef33acb8_1_1
                    
                    
                        ON user_id = Material_shopify_user_id_stitcher_ef33acb8_1_1.other_id
                        AND 'user_id' = Material_shopify_user_id_stitcher_ef33acb8_1_1.other_id_type
                    
                        LEFT JOIN Material_shopify_user_id_stitcher_ef33acb8_1 AS Material_shopify_user_id_stitcher_ef33acb8_1_2
                    
                    
                        ON anonymous_id = Material_shopify_user_id_stitcher_ef33acb8_1_2.other_id
                        AND 'anonymous_id' = Material_shopify_user_id_stitcher_ef33acb8_1_2.other_id_type
                    
        );
    
    

            
            
        
            
            
            
        

        /* Create entityvars */
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_MAX_TIMESTAMP_TRACKS AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (max_timestamp_tracks) AS max_timestamp_tracks
                FROM (
            SELECT
                user_main_id,
                max(timestamp)
	
	 AS max_timestamp_tracks
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsTracks_59a_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_MAX_TIMESTAMP_TRACKS USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_MAX_TIMESTAMP_TRACKS;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_MAX_TIMESTAMP_PAGES AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (max_timestamp_pages) AS max_timestamp_pages
                FROM (
            SELECT
                user_main_id,
                max(timestamp)
	
	 AS max_timestamp_pages
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsPages_ae0_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_MAX_TIMESTAMP_PAGES USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_MAX_TIMESTAMP_PAGES;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_MAX_TIMESTAMP_BW_TRACKS_PAGES AS (
            
                SELECT * FROM (
            SELECT
                user_main_id,
                CASE WHEN max_timestamp_tracks>=max_timestamp_pages THEN max_timestamp_tracks ELSE max_timestamp_pages END
	
	 AS max_timestamp_bw_tracks_pages
            FROM entityVarTable_user_04f_1
            
            WHERE user_main_id IS NOT NULL
            
            
        )
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_MAX_TIMESTAMP_BW_TRACKS_PAGES USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_MAX_TIMESTAMP_BW_TRACKS_PAGES;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_DAYS_SINCE_LAST_SEEN AS (
            
                SELECT * FROM (
            SELECT
                user_main_id,
                 datediff(day, date(max_timestamp_bw_tracks_pages),date('2023-05-29 06:34:49')) 
	
	 AS days_since_last_seen
            FROM entityVarTable_user_04f_1
            
            WHERE user_main_id IS NOT NULL
            
            
        )
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_DAYS_SINCE_LAST_SEEN USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_DAYS_SINCE_LAST_SEEN;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_IS_CHURNED_7_DAYS AS (
            
                SELECT * FROM (
            SELECT
                user_main_id,
                case when days_since_last_seen > 7 then 1 else 0 end
	
	 AS is_churned_7_days
            FROM entityVarTable_user_04f_1
            
            WHERE user_main_id IS NOT NULL
            
            
        )
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_IS_CHURNED_7_DAYS USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_IS_CHURNED_7_DAYS;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_MAX_TIMESTAMP_CART_UPDATE AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (max_timestamp_cart_update) AS max_timestamp_cart_update
                FROM (
            SELECT
                user_main_id,
                max(timestamp)
	
	 AS max_timestamp_cart_update
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsCartUpdate_e50_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_MAX_TIMESTAMP_CART_UPDATE USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_MAX_TIMESTAMP_CART_UPDATE;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_MAX_TIMESTAMP_CART_CREATE AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (max_timestamp_cart_create) AS max_timestamp_cart_create
                FROM (
            SELECT
                user_main_id,
                max(timestamp)
	
	 AS max_timestamp_cart_create
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsCartCreate_5b4_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_MAX_TIMESTAMP_CART_CREATE USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_MAX_TIMESTAMP_CART_CREATE;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_MAX_TIMESTAMP_BW_CART_CREATE_UPDATE AS (
            
                SELECT * FROM (
            SELECT
                user_main_id,
                CASE WHEN max_timestamp_cart_update>=max_timestamp_cart_create THEN max_timestamp_cart_update ELSE max_timestamp_cart_create end
	
	 AS max_timestamp_bw_cart_create_update
            FROM entityVarTable_user_04f_1
            
            WHERE user_main_id IS NOT NULL
            
            
        )
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_MAX_TIMESTAMP_BW_CART_CREATE_UPDATE USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_MAX_TIMESTAMP_BW_CART_CREATE_UPDATE;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_DAYS_SINCE_LAST_CART_ADD AS (
            
                SELECT * FROM (
            SELECT
                user_main_id,
                 datediff(day, date(max_timestamp_bw_cart_create_update), date('2023-05-29 06:34:49')) 
	
	 AS days_since_last_cart_add
            FROM entityVarTable_user_04f_1
            
            WHERE user_main_id IS NOT NULL
            
            
        )
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_DAYS_SINCE_LAST_CART_ADD USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_DAYS_SINCE_LAST_CART_ADD;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_REFUND AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (total_refund) AS total_refund
                FROM (
            SELECT
                user_main_id,
                sum(total_price_usd ::real)
	
	 AS total_refund
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsOrderCancelled_4a7_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND financial_status in ('paid','refunded','partially_refunded')
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_REFUND USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_REFUND;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_REFUND_COUNT AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (refund_count) AS refund_count
                FROM (
            SELECT
                user_main_id,
                count(*)
	
	 AS refund_count
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsOrderCancelled_4a7_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND financial_status in ('paid','refunded','partially_refunded')
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_REFUND_COUNT USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_REFUND_COUNT;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_MAX_TIMESTAMP_ORDER_CREATED AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (max_timestamp_order_created) AS max_timestamp_order_created
                FROM (
            SELECT
                user_main_id,
                max(timestamp)
	
	 AS max_timestamp_order_created
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsOrderCreated_c9e_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_MAX_TIMESTAMP_ORDER_CREATED USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_MAX_TIMESTAMP_ORDER_CREATED;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_DAYS_SINCE_LAST_PURCHASE AS (
            
                SELECT * FROM (
            SELECT
                user_main_id,
                 datediff(day, date(max_timestamp_order_created), date('2023-05-29 06:34:49'))
	
	 AS days_since_last_purchase
            FROM entityVarTable_user_04f_1
            
            WHERE user_main_id IS NOT NULL
            
            
        )
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_DAYS_SINCE_LAST_PURCHASE USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_DAYS_SINCE_LAST_PURCHASE;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_MIN_TIMESTAMP_ORDER_CREATED AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (min_timestamp_order_created) AS min_timestamp_order_created
                FROM (
            SELECT
                user_main_id,
                min(timestamp)
	
	 AS min_timestamp_order_created
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsOrderCreated_c9e_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_MIN_TIMESTAMP_ORDER_CREATED USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_MIN_TIMESTAMP_ORDER_CREATED;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_DAYS_SINCE_FIRST_PURCHASE AS (
            
                SELECT * FROM (
            SELECT
                user_main_id,
                 datediff(day, date(min_timestamp_order_created), date('2023-05-29 06:34:49'))
	
	 AS days_since_first_purchase
            FROM entityVarTable_user_04f_1
            
            WHERE user_main_id IS NOT NULL
            
            
        )
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_DAYS_SINCE_FIRST_PURCHASE USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_DAYS_SINCE_FIRST_PURCHASE;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_HAS_CREDIT_CARD AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (has_credit_card) AS has_credit_card
                FROM (
            SELECT
                user_main_id,
                max(case when lower(payment_details_credit_card_company) in ('visa','american express','mastercard') then 1 else 0 end)
	
	 AS has_credit_card
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsOrderCreated_c9e_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_HAS_CREDIT_CARD USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_HAS_CREDIT_CARD;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_AVG_UNITS_PER_TRANSACTION AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (avg_units_per_transaction) AS avg_units_per_transaction
                FROM (
            SELECT
                user_main_id,
                avg(array_size( parse_json(products) )::real)
	
	 AS avg_units_per_transaction
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsOrderCreated_c9e_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_AVG_UNITS_PER_TRANSACTION USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_AVG_UNITS_PER_TRANSACTION;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_AVG_TRANSACTION_VALUE AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (avg_transaction_value) AS avg_transaction_value
                FROM (
            SELECT
                user_main_id,
                avg( total_price_usd ::real)
	
	 AS avg_transaction_value
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsOrderCreated_c9e_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_AVG_TRANSACTION_VALUE USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_AVG_TRANSACTION_VALUE;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_HIGHEST_TRANSACTION_VALUE AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (highest_transaction_value) AS highest_transaction_value
                FROM (
            SELECT
                user_main_id,
                max(total_price_usd ::real)
	
	 AS highest_transaction_value
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsOrderCreated_c9e_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_HIGHEST_TRANSACTION_VALUE USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_HIGHEST_TRANSACTION_VALUE;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_MEDIAN_TRANSACTION_VALUE AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (median_transaction_value) AS median_transaction_value
                FROM (
            SELECT
                user_main_id,
                median( total_price_usd ::real)
	
	 AS median_transaction_value
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsOrderCreated_c9e_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_MEDIAN_TRANSACTION_VALUE USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_MEDIAN_TRANSACTION_VALUE;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_TRANSACTIONS AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (total_transactions) AS total_transactions
                FROM (
            SELECT
                user_main_id,
                count(*)
	
	 AS total_transactions
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsOrderCreated_c9e_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_TRANSACTIONS USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_TRANSACTIONS;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_REFUND_IN_PAST_1_DAYS AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (total_refund_in_past_1_days) AS total_refund_in_past_1_days
                FROM (
            SELECT
                user_main_id,
                sum(total_price_usd::real)
	
	 AS total_refund_in_past_1_days
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsOrderCancelled_4a7_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND  datediff(day,date(timestamp),date('2023-05-29 06:34:49'))<=1  and financial_status in ('paid','refunded','partially_refunded')
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_REFUND_IN_PAST_1_DAYS USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_REFUND_IN_PAST_1_DAYS;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_REFUND_IN_PAST_7_DAYS AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (total_refund_in_past_7_days) AS total_refund_in_past_7_days
                FROM (
            SELECT
                user_main_id,
                sum(total_price_usd::real)
	
	 AS total_refund_in_past_7_days
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsOrderCancelled_4a7_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND  datediff(day,date(timestamp),date('2023-05-29 06:34:49'))<=7  and financial_status in ('paid','refunded','partially_refunded')
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_REFUND_IN_PAST_7_DAYS USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_REFUND_IN_PAST_7_DAYS;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_DAYS_SINCE_ACCOUNT_CREATION AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (days_since_account_creation) AS days_since_account_creation
                FROM (
            SELECT
                user_main_id,
                datediff(days,date(min(timestamp)),date('2023-05-29 06:34:49'))
	
	 AS days_since_account_creation
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsIdentifies_623_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_DAYS_SINCE_ACCOUNT_CREATION USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_DAYS_SINCE_ACCOUNT_CREATION;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_HAS_MOBILE_APP AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (has_mobile_app) AS has_mobile_app
                FROM (
            SELECT
                user_main_id,
                max(case when lower(context_device_type) in ('android', 'ios') then 1 else 0 end)
	
	 AS has_mobile_app
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsIdentifies_623_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_HAS_MOBILE_APP USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_HAS_MOBILE_APP;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_STATE AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (state) AS state
                FROM (
            SELECT
                user_main_id,
                first_value(state)
	OVER (
		
    -- todo: fix this
	PARTITION BY user_main_id

		
	ORDER BY timestamp desc

		
	)
	 AS state
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsIdentifies_623_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND state is not null and state!=''
            
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_STATE USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_STATE;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_COUNTRY AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (country) AS country
                FROM (
            SELECT
                user_main_id,
                first_value(address_country)
	OVER (
		
    -- todo: fix this
	PARTITION BY user_main_id

		
	ORDER BY timestamp desc

		
	)
	 AS country
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsIdentifies_623_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND address_country is not null and address_country!=''
            
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_COUNTRY USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_COUNTRY;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_FIRST_NAME AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (first_name) AS first_name
                FROM (
            SELECT
                user_main_id,
                first_value(first_name)
	OVER (
		
    -- todo: fix this
	PARTITION BY user_main_id

		
	ORDER BY timestamp desc

		
	)
	 AS first_name
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsIdentifies_623_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND first_name is not null and first_name!=''
            
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_FIRST_NAME USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_FIRST_NAME;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_LAST_NAME AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (last_name) AS last_name
                FROM (
            SELECT
                user_main_id,
                first_value(last_name)
	OVER (
		
    -- todo: fix this
	PARTITION BY user_main_id

		
	ORDER BY timestamp desc

		
	)
	 AS last_name
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsIdentifies_623_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND last_name is not null and last_name!=''
            
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_LAST_NAME USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_LAST_NAME;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_CURRENCY AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (currency) AS currency
                FROM (
            SELECT
                user_main_id,
                first_value(currency)
	OVER (
		
    -- todo: fix this
	PARTITION BY user_main_id

		
	ORDER BY timestamp desc

		
	)
	 AS currency
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsIdentifies_623_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND currency is not null and currency!=''
            
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_CURRENCY USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_CURRENCY;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_DEVICE_NAME AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (device_name) AS device_name
                FROM (
            SELECT
                user_main_id,
                first_value(context_device_name)
	OVER (
		
    -- todo: fix this
	PARTITION BY user_main_id

		
	ORDER BY timestamp desc

		
	)
	 AS device_name
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsIdentifies_623_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND context_device_name is not null and context_device_name!=''
            
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_DEVICE_NAME USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_DEVICE_NAME;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_DEVICE_TYPE AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (device_type) AS device_type
                FROM (
            SELECT
                user_main_id,
                first_value(context_device_type)
	OVER (
		
    -- todo: fix this
	PARTITION BY user_main_id

		
	ORDER BY timestamp desc

		
	)
	 AS device_type
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsIdentifies_623_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND context_device_type is not null and context_device_type!=''
            
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_DEVICE_TYPE USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_DEVICE_TYPE;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_CAMPAIGN_SOURCES AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (campaign_sources) AS campaign_sources
                FROM (
            SELECT
                user_main_id,
                array_agg( context_campaign_source )
	
	 AS campaign_sources
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsIdentifies_623_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_CAMPAIGN_SOURCES USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_CAMPAIGN_SOURCES;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_IS_ACTIVE_ON_WEBSITE AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (is_active_on_website) AS is_active_on_website
                FROM (
            SELECT
                user_main_id,
                max(case when lower(context_device_type) like '%pc' then 1 else 0 end)
	
	 AS is_active_on_website
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsIdentifies_623_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_IS_ACTIVE_ON_WEBSITE USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_IS_ACTIVE_ON_WEBSITE;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_DEVICE_MANUFACTURER AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (device_manufacturer) AS device_manufacturer
                FROM (
            SELECT
                user_main_id,
                first_value(context_device_manufacturer)
	OVER (
		
    -- todo: fix this
	PARTITION BY user_main_id

		
	ORDER BY timestamp desc

		
	)
	 AS device_manufacturer
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsIdentifies_623_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND context_device_manufacturer is not null and context_device_manufacturer!=''
            
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_DEVICE_MANUFACTURER USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_DEVICE_MANUFACTURER;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_ACTIVE_DAYS_IN_PAST_7_DAYS AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (active_days_in_past_7_days) AS active_days_in_past_7_days
                FROM (
            SELECT
                user_main_id,
                count(distinct date(session_start_time))
	
	 AS active_days_in_past_7_days
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsSessionTable_3f4_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND datediff(day,date(session_start_time),date('2023-05-29 06:34:49')) <= 7
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_ACTIVE_DAYS_IN_PAST_7_DAYS USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_ACTIVE_DAYS_IN_PAST_7_DAYS;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_ACTIVE_DAYS_IN_PAST_365_DAYS AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (active_days_in_past_365_days) AS active_days_in_past_365_days
                FROM (
            SELECT
                user_main_id,
                count(distinct date(session_start_time))
	
	 AS active_days_in_past_365_days
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsSessionTable_3f4_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND datediff(day,date(session_start_time),date('2023-05-29 06:34:49')) <= 365
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_ACTIVE_DAYS_IN_PAST_365_DAYS USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_ACTIVE_DAYS_IN_PAST_365_DAYS;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_SESSIONS_TILL_DATE AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (total_sessions_till_date) AS total_sessions_till_date
                FROM (
            SELECT
                user_main_id,
                count(*)
	
	 AS total_sessions_till_date
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsSessionTable_3f4_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_SESSIONS_TILL_DATE USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_SESSIONS_TILL_DATE;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_SESSIONS_LAST_WEEK AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (total_sessions_last_week) AS total_sessions_last_week
                FROM (
            SELECT
                user_main_id,
                count(*)
	
	 AS total_sessions_last_week
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsSessionTable_3f4_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND  datediff(day, date(session_start_time), date('2023-05-29 06:34:49')) between 0 and 7 
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_SESSIONS_LAST_WEEK USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_SESSIONS_LAST_WEEK;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_SESSIONS_90_DAYS AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (total_sessions_90_days) AS total_sessions_90_days
                FROM (
            SELECT
                user_main_id,
                count(*)
	
	 AS total_sessions_90_days
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsSessionTable_3f4_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND  datediff(day, date(session_start_time), date('2023-05-29 06:34:49')) between 0 and 90 
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_SESSIONS_90_DAYS USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_SESSIONS_90_DAYS;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_SESSIONS_365_DAYS AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (total_sessions_365_days) AS total_sessions_365_days
                FROM (
            SELECT
                user_main_id,
                count(*)
	
	 AS total_sessions_365_days
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsSessionTable_3f4_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND  datediff(day, date(session_start_time), date('2023-05-29 06:34:49')) between 0 and 365 
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_SESSIONS_365_DAYS USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_SESSIONS_365_DAYS;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_AVG_SESSION_LENGTH_IN_SEC_OVERALL AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (avg_session_length_in_sec_overall) AS avg_session_length_in_sec_overall
                FROM (
            SELECT
                user_main_id,
                avg(session_length)
	
	 AS avg_session_length_in_sec_overall
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsSessionTable_3f4_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_AVG_SESSION_LENGTH_IN_SEC_OVERALL USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_AVG_SESSION_LENGTH_IN_SEC_OVERALL;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_AVG_SESSION_LENGTH_IN_SEC_LAST_WEEK AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (avg_session_length_in_sec_last_week) AS avg_session_length_in_sec_last_week
                FROM (
            SELECT
                user_main_id,
                avg(session_length)
	
	 AS avg_session_length_in_sec_last_week
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsSessionTable_3f4_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND  datediff(day, date(session_start_time), date('2023-05-29 06:34:49')) between 0 and 7 
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_AVG_SESSION_LENGTH_IN_SEC_LAST_WEEK USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_AVG_SESSION_LENGTH_IN_SEC_LAST_WEEK;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_AVG_SESSION_LENGTH_IN_SEC_365_DAYS AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (avg_session_length_in_sec_365_days) AS avg_session_length_in_sec_365_days
                FROM (
            SELECT
                user_main_id,
                avg(session_length)
	
	 AS avg_session_length_in_sec_365_days
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsSessionTable_3f4_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND  datediff(day, date(session_start_time), date('2023-05-29 06:34:49'))<=365 
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_AVG_SESSION_LENGTH_IN_SEC_365_DAYS USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_AVG_SESSION_LENGTH_IN_SEC_365_DAYS;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_FIRST_SEEN_DATE AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (first_seen_date) AS first_seen_date
                FROM (
            SELECT
                user_main_id,
                min(date(session_start_time))
	
	 AS first_seen_date
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsSessionTable_3f4_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_FIRST_SEEN_DATE USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_FIRST_SEEN_DATE;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_LAST_SEEN_DATE AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (last_seen_date) AS last_seen_date
                FROM (
            SELECT
                user_main_id,
                max(date(session_end_time))
	
	 AS last_seen_date
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsSessionTable_3f4_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_LAST_SEEN_DATE USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_LAST_SEEN_DATE;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_CARTS_IN_PAST_1_DAYS AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (carts_in_past_1_days) AS carts_in_past_1_days
                FROM (
            SELECT
                user_main_id,
                count(distinct token)
	
	 AS carts_in_past_1_days
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsCartCreateUnionCartUpdate_941_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND  datediff(day,date(timestamp),date('2023-05-29 06:34:49'))  <= 1 
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_CARTS_IN_PAST_1_DAYS USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_CARTS_IN_PAST_1_DAYS;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_CARTS_IN_PAST_7_DAYS AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (carts_in_past_7_days) AS carts_in_past_7_days
                FROM (
            SELECT
                user_main_id,
                count(distinct token)
	
	 AS carts_in_past_7_days
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsCartCreateUnionCartUpdate_941_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND  datediff(day,date(timestamp),date('2023-05-29 06:34:49'))  <= 7 
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_CARTS_IN_PAST_7_DAYS USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_CARTS_IN_PAST_7_DAYS;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_CARTS_IN_PAST_365_DAYS AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (carts_in_past_365_days) AS carts_in_past_365_days
                FROM (
            SELECT
                user_main_id,
                count(distinct token)
	
	 AS carts_in_past_365_days
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsCartCreateUnionCartUpdate_941_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND  datediff(day,date(timestamp),date('2023-05-29 06:34:49'))  <= 365 
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_CARTS_IN_PAST_365_DAYS USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_CARTS_IN_PAST_365_DAYS;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_CARTS AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (total_carts) AS total_carts
                FROM (
            SELECT
                user_main_id,
                count(distinct token)
	
	 AS total_carts
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsCartCreateUnionCartUpdate_941_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_CARTS USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_CARTS;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_LAST_TRANSACTION_VALUE AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (last_transaction_value) AS last_transaction_value
                FROM (
            SELECT
                user_main_id,
                first_value(total_price_usd)
	OVER (
		
    -- todo: fix this
	PARTITION BY user_main_id

		
	ORDER BY case when TOTAL_PRICE_USD is not null then 2 else 1 end desc, timestamp desc

		
	)
	 AS last_transaction_value
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsOrderCreated_c9e_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_LAST_TRANSACTION_VALUE USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_LAST_TRANSACTION_VALUE;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_PRODUCTS_ADDED AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (total_products_added) AS total_products_added
                FROM (
            SELECT
                user_main_id,
                array_agg(distinct product_id)
	
	 AS total_products_added
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsCartLineItems_073_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_PRODUCTS_ADDED USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TOTAL_PRODUCTS_ADDED;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_PRODUCTS_ADDED_IN_PAST_1_DAYS AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (products_added_in_past_1_days) AS products_added_in_past_1_days
                FROM (
            SELECT
                user_main_id,
                array_agg(distinct product_id)
	
	 AS products_added_in_past_1_days
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsCartLineItems_073_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND  datediff(day, date(timestamp), date('2023-05-29 06:34:49')) <= 1 
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_PRODUCTS_ADDED_IN_PAST_1_DAYS USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_PRODUCTS_ADDED_IN_PAST_1_DAYS;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_PRODUCTS_ADDED_IN_PAST_7_DAYS AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (products_added_in_past_7_days) AS products_added_in_past_7_days
                FROM (
            SELECT
                user_main_id,
                array_agg(distinct product_id)
	
	 AS products_added_in_past_7_days
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsCartLineItems_073_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND  datediff(day, date(timestamp), date('2023-05-29 06:34:49')) <= 7 
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_PRODUCTS_ADDED_IN_PAST_7_DAYS USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_PRODUCTS_ADDED_IN_PAST_7_DAYS;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_PRODUCTS_ADDED_IN_PAST_365_DAYS AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (products_added_in_past_365_days) AS products_added_in_past_365_days
                FROM (
            SELECT
                user_main_id,
                array_agg(distinct product_id)
	
	 AS products_added_in_past_365_days
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsCartLineItems_073_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND  datediff(day, date(timestamp), date('2023-05-29 06:34:49')) <= 365 
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_PRODUCTS_ADDED_IN_PAST_365_DAYS USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_PRODUCTS_ADDED_IN_PAST_365_DAYS;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_LATEST_CART_ID AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (latest_cart_id) AS latest_cart_id
                FROM (
            SELECT
                user_main_id,
                first_value(token)
	OVER (
		
    -- todo: fix this
	PARTITION BY user_main_id

		
	ORDER BY timestamp desc

		
	)
	 AS latest_cart_id
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsCartLineItems_073_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_LATEST_CART_ID USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_LATEST_CART_ID;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_LAST_CART_VALUE_IN_DOLLARS AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (last_cart_value_in_dollars) AS last_cart_value_in_dollars
                FROM (
            SELECT
                user_main_id,
                sum(line_price)
	
	 AS last_cart_value_in_dollars
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsCartLineItems_073_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND token=latest_cart_id
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_LAST_CART_VALUE_IN_DOLLARS USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_LAST_CART_VALUE_IN_DOLLARS;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_LAST_CART_STATUS AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (last_cart_status) AS last_cart_status
                FROM (
            SELECT
                user_main_id,
                min(coalesce(fulfillment_status, financial_status, 'abandoned'))
	
	 AS last_cart_status
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsOrderCreatedOrderCancelled_1ae_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_LAST_CART_STATUS USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_LAST_CART_STATUS;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TRANSACTIONS_IN_PAST_1_DAYS AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (transactions_in_past_1_days) AS transactions_in_past_1_days
                FROM (
            SELECT
                user_main_id,
                count(*)::real
	
	 AS transactions_in_past_1_days
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsOrderCreatedOrderCancelled_1ae_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND  datediff(day,date(timestamp),date('2023-05-29 06:34:49'))<=1 
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TRANSACTIONS_IN_PAST_1_DAYS USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TRANSACTIONS_IN_PAST_1_DAYS;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TRANSACTIONS_IN_PAST_90_DAYS AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (transactions_in_past_90_days) AS transactions_in_past_90_days
                FROM (
            SELECT
                user_main_id,
                count(*)::real
	
	 AS transactions_in_past_90_days
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsOrderCreatedOrderCancelled_1ae_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND  datediff(day,date(timestamp),date('2023-05-29 06:34:49'))<=90 
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TRANSACTIONS_IN_PAST_90_DAYS USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TRANSACTIONS_IN_PAST_90_DAYS;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TRANSACTIONS_IN_PAST_365_DAYS AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (transactions_in_past_365_days) AS transactions_in_past_365_days
                FROM (
            SELECT
                user_main_id,
                count(*)::real
	
	 AS transactions_in_past_365_days
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsOrderCreatedOrderCancelled_1ae_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND  datediff(day,date(timestamp),date('2023-05-29 06:34:49'))<=365 
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TRANSACTIONS_IN_PAST_365_DAYS USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_TRANSACTIONS_IN_PAST_365_DAYS;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_NET_AMT_SPENT_IN_PAST_90_DAYS AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (net_amt_spent_in_past_90_days) AS net_amt_spent_in_past_90_days
                FROM (
            SELECT
                user_main_id,
                sum(total_price_usd::real) - coalesce(sum(total_price_usd_order_cancelled::real), 0)
	
	 AS net_amt_spent_in_past_90_days
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsOrderCreatedOrderCancelled_1ae_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND  datediff(day,date(timestamp),date('2023-05-29 06:34:49'))<=90 
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_NET_AMT_SPENT_IN_PAST_90_DAYS USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_NET_AMT_SPENT_IN_PAST_90_DAYS;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_NET_AMT_SPENT_IN_PAST_365_DAYS AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (net_amt_spent_in_past_365_days) AS net_amt_spent_in_past_365_days
                FROM (
            SELECT
                user_main_id,
                sum(total_price_usd::real) - coalesce(sum(total_price_usd_order_cancelled::real), 0)
	
	 AS net_amt_spent_in_past_365_days
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsOrderCreatedOrderCancelled_1ae_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND  datediff(day,date(timestamp),date('2023-05-29 06:34:49'))<=365 
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_NET_AMT_SPENT_IN_PAST_365_DAYS USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_NET_AMT_SPENT_IN_PAST_365_DAYS;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_NET_AMT_SPENT_IN_PAST_1_DAYS AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (net_amt_spent_in_past_1_days) AS net_amt_spent_in_past_1_days
                FROM (
            SELECT
                user_main_id,
                sum(total_price_usd::real) - coalesce(sum(total_price_usd_order_cancelled::real), 0)
	
	 AS net_amt_spent_in_past_1_days
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsOrderCreatedOrderCancelled_1ae_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            AND  datediff(day,date(timestamp),date('2023-05-29 06:34:49'))<=1 
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_NET_AMT_SPENT_IN_PAST_1_DAYS USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_NET_AMT_SPENT_IN_PAST_1_DAYS;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_NET_AMT_SPENT_IN_PAST AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (net_amt_spent_in_past) AS net_amt_spent_in_past
                FROM (
            SELECT
                user_main_id,
                sum(total_price_usd::real) - coalesce(sum(total_price_usd_order_cancelled::real), 0)
	
	 AS net_amt_spent_in_past
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsOrderCreatedOrderCancelled_1ae_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_NET_AMT_SPENT_IN_PAST USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_NET_AMT_SPENT_IN_PAST;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_GROSS_AMT_SPENT_IN_PAST AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (gross_amt_spent_in_past) AS gross_amt_spent_in_past
                FROM (
            SELECT
                user_main_id,
                sum(total_price_usd::real)
	
	 AS gross_amt_spent_in_past
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsOrderCreatedOrderCancelled_1ae_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_GROSS_AMT_SPENT_IN_PAST USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_GROSS_AMT_SPENT_IN_PAST;

        /* Handle default setting */
        

    

            
            
        
            
                
    
        
        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_ITEMS_PURCHASED_EVER AS (
            
                SELECT
                    user_main_id,
                    ANY_VALUE (items_purchased_ever) AS items_purchased_ever
                FROM (
            SELECT
                user_main_id,
                array_agg(sku)
	
	 AS items_purchased_ever
            FROM entityVarTable_user_04f_1
            
            RIGHT JOIN inputVarTable_rsItemsPurchasedEver_4b1_1 USING (user_main_id)
            
            WHERE user_main_id IS NOT NULL
            
            
            GROUP BY user_main_id
            
        )
                GROUP BY user_main_id
            
        );

        
      
          
              /* Join with single entityvar table and replace old table */
              CREATE TEMPORARY TABLE entityVarTable_user_04f_1New AS (
                  SELECT *
                  FROM
                      entityVarTable_user_04f_1
                      LEFT JOIN MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_ITEMS_PURCHASED_EVER USING (user_main_id)
              );
              DROP TABLE entityVarTable_user_04f_1;
              
              CREATE OR REPLACE TEMPORARY TABLE ENTITYVARTABLE_USER_04F_1 AS (SELECT * FROM entityVarTable_user_04f_1New);
              DROP TABLE entityVarTable_user_04f_1New;
          
      
  

        DROP TABLE IF EXISTS MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1_INTERNAL_EV_ITEMS_PURCHASED_EVER;

        /* Handle default setting */
        

    

            
            
        

        /* Create output table */
        
        CREATE OR REPLACE TABLE MATERIAL_SHOPIFY_USER_FEATURES_08FF5B74_1 AS 
            SELECT user_main_id
                
                , '2023-05-29T06:34:49.347741Z' AS valid_at
                
            
                , days_since_last_seen
            
                , is_churned_7_days
            
                , days_since_last_cart_add
            
                , total_refund
            
                , refund_count
            
                , days_since_last_purchase
            
                , days_since_first_purchase
            
                , has_credit_card
            
                , avg_units_per_transaction
            
                , avg_transaction_value
            
                , highest_transaction_value
            
                , median_transaction_value
            
                , total_transactions
            
                , total_refund_in_past_1_days
            
                , total_refund_in_past_7_days
            
                , days_since_account_creation
            
                , has_mobile_app
            
                , state
            
                , country
            
                , first_name
            
                , last_name
            
                , currency
            
                , device_type
            
                , device_name
            
                , campaign_sources
            
                , is_active_on_website
            
                , device_manufacturer
            
                , active_days_in_past_7_days
            
                , active_days_in_past_365_days
            
                , total_sessions_till_date
            
                , total_sessions_last_week
            
                , avg_session_length_in_sec_overall
            
                , avg_session_length_in_sec_last_week
            
                , avg_session_length_in_sec_365_days
            
                , first_seen_date
            
                , last_seen_date
            
                , carts_in_past_1_days
            
                , carts_in_past_7_days
            
                , carts_in_past_365_days
            
                , total_carts
            
                , last_transaction_value
            
                , total_products_added
            
                , products_added_in_past_1_days
            
                , products_added_in_past_7_days
            
                , products_added_in_past_365_days
            
                , total_sessions_90_days
            
                , total_sessions_365_days
            
                , last_cart_status
            
                , last_cart_value_in_dollars
            
                , transactions_in_past_1_days
            
                , transactions_in_past_90_days
            
                , transactions_in_past_365_days
            
                , net_amt_spent_in_past_1_days
            
                , net_amt_spent_in_past_90_days
            
                , net_amt_spent_in_past_365_days
            
                , net_amt_spent_in_past
            
                , gross_amt_spent_in_past
            
                , items_purchased_ever
            
            FROM entityVarTable_user_04f_1
        ;

        

        /* Drop temp tables */
        DROP TABLE entityVarTable_user_04f_1;
        
        
            
            DROP TABLE inputVarTable_rsCartCreate_5b4_1;
            
        
            
            DROP TABLE inputVarTable_rsCartCreateUnionCartUpdate_941_1;
            
        
            
            DROP TABLE inputVarTable_rsCartLineItems_073_1;
            
        
            
            DROP TABLE inputVarTable_rsCartUpdate_e50_1;
            
        
            
            DROP TABLE inputVarTable_rsIdentifies_623_1;
            
        
            
            DROP TABLE inputVarTable_rsItemsPurchasedEver_4b1_1;
            
        
            
            DROP TABLE inputVarTable_rsOrderCancelled_4a7_1;
            
        
            
            DROP TABLE inputVarTable_rsOrderCreated_c9e_1;
            
        
            
            DROP TABLE inputVarTable_rsOrderCreatedOrderCancelled_1ae_1;
            
        
            
            DROP TABLE inputVarTable_rsPages_ae0_1;
            
        
            
            DROP TABLE inputVarTable_rsSessionTable_3f4_1;
            
        
            
            DROP TABLE inputVarTable_rsTracks_59a_1;
            
        
            
        
        
    
	END;

			
    
        
            
    
        
    
        
    
        
            
                DROP VIEW IF EXISTS MATERIAL_RSORDERCREATED_C9E04568_1;
            
        
    
        
            
                DROP VIEW IF EXISTS MATERIAL_RSPAGES_AE075D7C_1;
            
        
    
        
    
        
    
        
    
        
    
        
    
        
            
                DROP VIEW IF EXISTS MATERIAL_RSITEMSPURCHASEDEVERSKU_25BFE499_1;
            
        
    
        
            
                DROP VIEW IF EXISTS MATERIAL_RSCARTUPDATE_E50A60EA_1;
            
        
    
        
            
                DROP VIEW IF EXISTS MATERIAL_RSORDERCANCELLED_4A77009B_1;
            
        
    
        
            
                DROP VIEW IF EXISTS MATERIAL_RSCARTCREATE_5B46D5D1_1;
            
        
    
        
            
                DROP VIEW IF EXISTS MATERIAL_RSTRACKS_59AABFAA_1;
            
        
    
        
            
                DROP VIEW IF EXISTS MATERIAL_RSIDENTIFIES_62307D86_1;
            
        
    
        
            
                DROP VIEW IF EXISTS MATERIAL_RSITEMSPURCHASEDEVERCTE_DA75AA63_1;
            
        
    
        
    
        
            
                DROP VIEW IF EXISTS MATERIAL_RSCARTLINEITEMS_073458D1_1;
            
        
    
        
            
                DROP VIEW IF EXISTS MATERIAL_RSORDERCREATEDORDERCANCELLED_1AE9978F_1;
            
        
    
        
            
                DROP VIEW IF EXISTS MATERIAL_RSCARTCREATEUNIONCARTUPDATE_9415C17C_1;
            
        
    
        
    
        
            
                DROP VIEW IF EXISTS MATERIAL_RSITEMSPURCHASEDEVER_4B111E2F_1;
            
        
    
        
            
                DROP VIEW IF EXISTS MATERIAL_RSSESSIONTABLE_3F477D2A_1;
            
        
    

        
    
 
	
	END;
	