WITH 
    _events_data AS (
            SELECT 
                1                                                                               AS event_id, 
                '2023-03-29 10:00:00'                                                           AS event_datetime, 
                'prospect_001'                                                                  AS prospect_id, 
                [   ('page_location', '/home'), 
                    ('page_title', 'Home Page'), 
                    ('marketing_source_name', 'Google'), 
                    ('marketing_source_medium', 'CPC'), 
                    ('marketing_source_campaign', 'Spring_Promo'), 
                    ('marketing_source_term', 'running+shoes')
                ]                                                                               AS features, 
                'GA4'                                                                           AS datasource_name

        UNION ALL

            SELECT 
                2, 
                '2023-03-29 10:05:00', 
                'prospect_002', 
                [   ('event_name', 'purchase'), 
                    ('transaction_id', 'txn_12345'), 
                    ('value', '100.00'), 
                    ('marketing_source_name', 'Facebook'), 
                    ('marketing_source_medium', 'Social'), 
                    ('marketing_source_campaign', '2023_Launch'), 
                    ('marketing_source_term', 'luxury+sneakers')
                ], 
                'GA4'

        UNION ALL

            SELECT 
                3, 
                '2023-03-29 11:00:00', 
                'prospect_003', 
                [   ('stage', 'Opportunity Created')
                ], 
                'Salesforce'

        UNION ALL

            SELECT 
                4, 
                '2023-03-30 09:00:00', 
                'prospect_003', 
                [   ('stage', 'Sales Qualified')
                ], 
                'Salesforce'

        UNION ALL

            SELECT 
                5, 
                '2023-04-01 14:00:00', 
                'prospect_003', 
                [   ('stage', 'Close Won')
                ], 
                'Salesforce'

        UNION ALL

            SELECT 
                6, 
                '2023-04-02 16:00:00', 
                'prospect_004', 
                [   ('report_topic', 'Digital Marketing Trends'), 
                    ('attendee_count', '150'), 
                    ('feedback_average', '4.5'), 
                    ('marketing_source_name', 'LinkedIn'), 
                    ('marketing_source_medium', 'Social'), 
                    ('marketing_source_campaign', 'Tech_Conference_2023'), 
                    ('marketing_source_term', 'digital+transformation')
                ], 
                'GSheets'
    )

--
    SELECT 
        event_id,
        toDateTime(event_datetime)                                                       AS event_datetime,
        prospect_id,

        cast(
                (features.1, features.2)                                                    
            as 
                Map(LowCardinality(String), String))                                    AS features_map,

        datasource_name

    FROM _events_data