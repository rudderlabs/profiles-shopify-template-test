
	
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

    
    CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_EDGES_RAW 
        (
            id1 varchar,
            id1_type varchar,
            id2 varchar,
            id2_type varchar,
            valid_at timestamp)
    ;

    
        
            
    
        
            
                
            
                
            
                
            
                
            
        
            
                
            
                
            
                
            
                
            
        
            
                
            
                
            
                
            
                
            
        
            
                
            
                
            
                
            
                
                    
                        INSERT INTO MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_EDGES_RAW (id1, id1_type, id2, id2_type, valid_at)
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
                    
                
            
        
    

        
    

    DELETE FROM MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_EDGES_RAW WHERE
        id1_type NOT IN (
    'session_id', 'session_main_id'
)
        OR id2_type NOT IN (
    'session_id', 'session_main_id'
);

        
            
        
            
        

        
        
            CREATE OR REPLACE TABLE MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_EDGES AS 
            SELECT
                id1,
                id1_type,
                id2,
                id2_type,
                LEAST(
                    
    
    'rid' || left(sha1(concat(left(sha1('fe971b24-9572-4005-b22f-351e9c09274d' || NVL(id1,'')),32),NVL(id1_type,''))),32)
    
,
                    
    
    'rid' || left(sha1(concat(left(sha1('fe971b24-9572-4005-b22f-351e9c09274d' || NVL(id2,'')),32),NVL(id2_type,''))),32)
    

                ) AS session_main_id,
                MIN(valid_at) as valid_at
            FROM MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_EDGES_RAW
            WHERE
                id1 IS NOT NULL
                AND id1_type IS NOT NULL
                AND id2 IS NOT NULL
                AND id2_type IS NOT NULL
            GROUP BY id1, id2, id1_type, id2_type
        ;
        

        DROP TABLE MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_EDGES_RAW;

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_ACTIVE_MAPPING 
    (
        node_id varchar,
        node_id_type varchar,
        session_main_id varchar,
        valid_at timestamp,
        session_main_id_dist int,
        stitching_active int)
;
        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_FINISHED_MAPPING 
    (
        node_id varchar,
        node_id_type varchar,
        session_main_id varchar,
        valid_at timestamp,
        session_main_id_dist int,
        stitching_active int)
;

        INSERT INTO MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_ACTIVE_MAPPING (
            node_id,
            node_id_type,
            session_main_id,
            valid_at,
            session_main_id_dist,
            stitching_active
        )
        SELECT
            id1 AS node_id,
            id1_type AS node_id_type,
            session_main_id,
            valid_at,
            0 AS session_main_id_dist,
            0 AS stitching_active
        FROM
            MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_EDGES
        UNION ALL
        SELECT
            id2 AS node_id,
            id2_type AS node_id_type,
            session_main_id,
            valid_at,
            0 AS session_main_id_dist,
            0 AS stitching_active
        FROM
            MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_EDGES
        ;

        

        CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_ACTIVE_MAPPING AS (
    SELECT
        node_id,
        node_id_type,
        session_main_id,
        valid_at,
        session_main_id_dist,
        0 AS stitching_active
    FROM (
        SELECT
            node_id,
            node_id_type,
            session_main_id,
            valid_at,
            session_main_id_dist,
            row_number() OVER (
                PARTITION BY node_id, node_id_type, session_main_id
                ORDER BY valid_at ASC, session_main_id_dist ASC
            ) AS row_number
        FROM MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_ACTIVE_MAPPING
    )
    WHERE row_number = 1
);

        

    /* Set which clusters are actively stitching */
    CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_ACTIVE_MAPPING AS (
    SELECT
        node_id,
        node_id_type,
        session_main_id,
        session_main_id_dist,
        valid_at,
        MAX(stitching_active) OVER (PARTITION BY session_main_id) AS stitching_active
    FROM (
        SELECT
            node_id,
            node_id_type,
            session_main_id,
            session_main_id_dist,
            valid_at,
            CASE
                WHEN min_session_main_id_node = max_session_main_id_node THEN 0
                ELSE 1
                END AS stitching_active
        FROM (
            SELECT
                node_id,
                node_id_type,
                session_main_id,
                session_main_id_dist,
                valid_at,
                MIN(session_main_id) OVER (PARTITION BY node_id, node_id_type) AS min_session_main_id_node,
                MAX(session_main_id) OVER (PARTITION BY node_id, node_id_type) AS max_session_main_id_node
            FROM MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_ACTIVE_MAPPING
        )
    )
);

    /* Insert converged clusters into finished table */
    INSERT INTO MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_FINISHED_MAPPING (
        node_id, node_id_type, session_main_id, valid_at, session_main_id_dist, stitching_active
    )
    SELECT
        node_id,
        node_id_type,
        session_main_id,
        valid_at,
        session_main_id_dist,
        stitching_active
    FROM MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_ACTIVE_MAPPING
    WHERE stitching_active = 0;

    /* Remove converged clusters from active table */
    DELETE FROM MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_ACTIVE_MAPPING WHERE stitching_active = 0;



        WHILE ((iter_var < max_iter_var) AND is_not_converged_var) DO 
    iter_var := iter_var + 1;

    /* Run 1 round of cluster merging as follows
    select distinct new_session_main_id AS session_main_id, node_id into table FROM
        partition mappings by session_main_id (clusters), select min node_session_main_id as new_session_main_id FROM
            partition mappings by node, select min session_main_id as node_session_main_id
    */
    
    CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_ACTIVE_MAPPING AS (
    SELECT DISTINCT
        node_id,
        node_id_type,
        CASE
            WHEN new_session_main_id < session_main_id THEN new_session_main_id
            ELSE session_main_id
            END AS session_main_id,
        CASE
            WHEN new_session_main_id < session_main_id THEN
                CASE
                    WHEN new_valid_at is NULL or valid_at is NULL THEN NULL
                        ELSE GREATEST(new_valid_at, valid_at)
                END
            ELSE valid_at
            END AS valid_at,
        CASE
            WHEN new_session_main_id < session_main_id THEN new_session_main_id_dist + 1
            ELSE session_main_id_dist
            END AS session_main_id_dist,
        0 AS stitching_active
    FROM (
        SELECT
            node_id,
            node_id_type,
            session_main_id,
            valid_at,
            session_main_id_dist,
            FIRST_VALUE(new_session_main_id) over(
                PARTITION BY session_main_id
                ORDER BY
                    new_session_main_id ASC,
                    new_valid_at ASC,
                    new_session_main_id_dist ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS new_session_main_id,
            FIRST_VALUE(new_valid_at) over(
                PARTITION BY session_main_id
                ORDER BY
                    new_session_main_id ASC,
                    new_valid_at ASC,
                    new_session_main_id_dist ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS new_valid_at,
            FIRST_VALUE(new_session_main_id_dist) over(
                PARTITION BY session_main_id
                ORDER BY
                    new_session_main_id ASC,
                    new_valid_at ASC,
                    new_session_main_id_dist ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS new_session_main_id_dist
        FROM (
            SELECT
                *,
                FIRST_VALUE(session_main_id) over(
                    PARTITION BY node_id, node_id_type
                    ORDER BY
                        session_main_id ASC,
                        valid_at ASC,
                        session_main_id_dist ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS new_session_main_id,
                FIRST_VALUE(valid_at) over(
                    PARTITION BY node_id, node_id_type
                    ORDER BY
                        session_main_id ASC,
                        valid_at ASC,
                        session_main_id_dist ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS new_valid_at,
                FIRST_VALUE(session_main_id_dist) over(
                    PARTITION BY node_id, node_id_type
                    ORDER BY
                        session_main_id ASC,
                        valid_at ASC,
                        session_main_id_dist ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS new_session_main_id_dist
            FROM
                MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_ACTIVE_MAPPING
        )
    )
    );

    

    /* Set which clusters are actively stitching */
    CREATE OR REPLACE TEMPORARY TABLE MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_ACTIVE_MAPPING AS (
    SELECT
        node_id,
        node_id_type,
        session_main_id,
        session_main_id_dist,
        valid_at,
        MAX(stitching_active) OVER (PARTITION BY session_main_id) AS stitching_active
    FROM (
        SELECT
            node_id,
            node_id_type,
            session_main_id,
            session_main_id_dist,
            valid_at,
            CASE
                WHEN min_session_main_id_node = max_session_main_id_node THEN 0
                ELSE 1
                END AS stitching_active
        FROM (
            SELECT
                node_id,
                node_id_type,
                session_main_id,
                session_main_id_dist,
                valid_at,
                MIN(session_main_id) OVER (PARTITION BY node_id, node_id_type) AS min_session_main_id_node,
                MAX(session_main_id) OVER (PARTITION BY node_id, node_id_type) AS max_session_main_id_node
            FROM MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_ACTIVE_MAPPING
        )
    )
);

    /* Insert converged clusters into finished table */
    INSERT INTO MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_FINISHED_MAPPING (
        node_id, node_id_type, session_main_id, valid_at, session_main_id_dist, stitching_active
    )
    SELECT
        node_id,
        node_id_type,
        session_main_id,
        valid_at,
        session_main_id_dist,
        stitching_active
    FROM MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_ACTIVE_MAPPING
    WHERE stitching_active = 0;

    /* Remove converged clusters from active table */
    DELETE FROM MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_ACTIVE_MAPPING WHERE stitching_active = 0;



    SELECT not_converged INTO :is_not_converged_var FROM (SELECT COUNT(*) > 0 AS not_converged FROM MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_ACTIVE_MAPPING);
 END WHILE;

        CREATE OR REPLACE TABLE MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_MAPPING AS SELECT node_id, node_id_type, session_main_id, valid_at, session_main_id_dist, stitching_active FROM MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_FINISHED_MAPPING UNION ALL SELECT node_id, node_id_type, session_main_id, valid_at, session_main_id_dist, stitching_active FROM MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_ACTIVE_MAPPING;

        CREATE OR REPLACE VIEW MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1 AS 
    WITH ranked_mappings AS (
        SELECT
            session_main_id,
            node_id AS other_id,
            node_id_type AS other_id_type,
            valid_at,
            row_number() OVER (
                PARTITION BY node_id, node_id_type, session_main_id
                ORDER BY
                 valid_at ASC
            ) AS row_number
        FROM MATERIAL_SHOPIFY_SESSION_ID_STITCHER_A47A3FC3_1_INTERNAL_MAPPING
    )
    SELECT
        session_main_id,
        other_id,
        other_id_type,
        valid_at
    FROM ranked_mappings
    WHERE row_number = 1
;

    END;
 END;
        
    

	END;

			
    
        
            
    
        
    
        
            
                DROP VIEW IF EXISTS MATERIAL_RSPAGES_AE075D7C_1;
            
        
    

        
    
 
	
	END;
	