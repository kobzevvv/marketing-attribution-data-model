WITH 
    test_data AS (
            SELECT 
                1                                                                            AS event_id,
                TIMESTAMP('2023-03-29 10:00:00 UTC')                                         AS timestamp,
                'prospect_001'                                                               AS prospect_id,
                
                ARRAY<STRUCT<key STRING, value STRING>>[
                    STRUCT('page_location', '/home'),
                    STRUCT('page_title', 'Home Page'),
                    STRUCT('marketing_source_name', 'Google'),
                    STRUCT('marketing_source_medium', 'CPC'),
                    STRUCT('marketing_source_campaign', 'Spring_Promo'),
                    STRUCT('marketing_source_term', 'running+shoes')
                ]                                                                            AS features_map,

                'GA4'                                                                        AS datasource_name

        UNION ALL

            SELECT 
                2                                                                            AS event_id,
                TIMESTAMP('2023-03-29 10:05:00 UTC')                                         AS timestamp,
                'prospect_002'                                                               AS prospect_id,

                ARRAY<STRUCT<key STRING, value STRING>>[
                    STRUCT('event_name', 'purchase'),
                    STRUCT('transaction_id', 'txn_12345'),
                    STRUCT('value', '100.00'),
                    STRUCT('marketing_source_name', 'Facebook'),
                    STRUCT('marketing_source_medium', 'Social'),
                    STRUCT('marketing_source_campaign', '2023_Launch'),
                    STRUCT('marketing_source_term', 'luxury+sneakers')
                ]                                                                            AS features_map,

                'GA4'                                                                        AS datasource_name

        UNION ALL

            SELECT 
                3                                                                            AS event_id,
                TIMESTAMP('2023-03-29 11:00:00 UTC')                                         AS timestamp,
                'prospect_003'                                                               AS prospect_id,

                ARRAY<STRUCT<key STRING, value STRING>>[
                    STRUCT('stage', 'Opportunity Created')
                ]                                                                            AS features_map,

                'Salesforce'                                                                 AS datasource_name

        UNION ALL

            SELECT 
                4                                                                            AS event_id,
                TIMESTAMP('2023-03-30 09:00:00 UTC')                                         AS timestamp,
                'prospect_003'                                                               AS prospect_id,

                ARRAY<STRUCT<key STRING, value STRING>>[
                    STRUCT('stage', 'Sales Qualified')
                ]                                                                            AS features_map,

                'Salesforce'                                                                 AS datasource_name

        UNION ALL

            SELECT 
                5                                                                            AS event_id,
                TIMESTAMP('2023-04-01 14:00:00 UTC')                                         AS timestamp,

                'prospect_003'                                                               AS prospect_id,

                ARRAY<STRUCT<key STRING, value STRING>>[
                    STRUCT('stage', 'Close Won')
                ]                                                                            AS features_map,
                'Salesforce'                                                                 AS datasource_name

        UNION ALL

            SELECT 
                6                                                                            AS event_id,
                TIMESTAMP('2023-04-02 16:00:00 UTC')                                         AS timestamp,
                'prospect_004'                                                               AS prospect_id,

                ARRAY<STRUCT<key STRING, value STRING>>[
                    STRUCT('report_topic', 'Digital Marketing Trends'),
                    STRUCT('attendee_count', '150'),
                    STRUCT('feedback_average', '4.5'),
                    STRUCT('marketing_source_name', 'LinkedIn'),
                    STRUCT('marketing_source_medium', 'Social'),
                    STRUCT('marketing_source_campaign', 'Tech_Conference_2023'),
                    STRUCT('marketing_source_term', 'digital+transformation')
                ]                                                                            AS features_map,

                'GSheets'                                                                    AS datasource_name
    )

SELECT * FROM test_data