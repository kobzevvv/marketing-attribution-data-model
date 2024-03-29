with

-- input
    intermediate_google_analytics as (
        select * from {{ ref('intermediate_google_analytics') }}
    ),

    stg_gsheets_referring_domains_to_exclude as (
        select * from {{ ref('stg_gsheets_referring_domains_to_exclude') }}
    ),

    stg_gsheets_referring_domains_to_filter as (
        select * from {{ ref('stg_gsheets_referring_domains_to_filter') }}
    ),

    -- LOGIC -----------------------------------------------------------------------
    unwanted_referrals as (
        select
            referring_domain
        from
            stg_gsheets_referring_domains_to_exclude
    ),

    ghost_spam_domains as (
        select
            referring_domain
        from
            stg_gsheets_referring_domains_to_filter
    ),

    ga4_necessary_columns as (
        select
            ga_unique_id,
            traffic_source_name,
            traffic_source_medium,
            traffic_source_source,
            device_category,
            geo_continent,
            geo_country,
            link_url,
            link_domain,
            form_budget,
            search_term,
            page_utr,
            click_text,
            click_url,
            page_location,
            entrances,
            banner_name,
            ga_session_number,
            page_title,
            gclid,
            link_text,
            campaign_id,
            session_engaged,
            engagement_time_msec,
            file_name,
            ignore_referrer,
            outbound,
            page_referrer,
            ga_session_id,
            engaged_session_event,
            percent_scrolled,
            disco_assignee,
            event_timestamp,
            event_date,
            event_datetime,
            event_name,
            ga_client_id,
            user_first_touch_timestamp,
            user_type,
            user_first_touch_datetime,
            page_location_clean,
            if(collected_traffic_source_manual_source != '',
                collected_traffic_source_manual_source, source)                                     as source,
            if(collected_traffic_source_manual_medium != '',
                collected_traffic_source_manual_medium, medium)                                     as medium,
            if(collected_traffic_source_manual_campaign_name != '', 
                collected_traffic_source_manual_campaign_name, campaign)                            as campaign,
            if(collected_traffic_source_manual_term != '',
                collected_traffic_source_manual_term, term)                                         as term,
            if(collected_traffic_source_manual_content != '', 
                collected_traffic_source_manual_content, content)                                   as content
        from
            intermediate_google_analytics
    ),

    cleaning_traffic_params as (
        with
            source in unwanted_referrals                                                            as unwanted_referrals_clause
        select
            * except(
                source,
                medium,
                campaign),
                
            if(unwanted_referrals_clause, '', source)                                               as source_cleaned,
            if(unwanted_referrals_clause, '', medium)                                               as medium_cleaned,
            if(unwanted_referrals_clause, '', campaign)                                             as campaign_cleaned
        from
            ga4_necessary_columns
    ),

    channel_grouping as (
        with      
            source_cleaned = '' and medium_cleaned = '' and campaign_cleaned = ''
                and term = '' and content = ''                                                      as direct_clause,
            
            match(source_cleaned, '^(google|bing)$')
                and match(medium_cleaned, '^(.*cp.*|ppc|paid.*)$')                                  as search_paid_clause,
            
            match(source_cleaned, '^(twitter|facebook|fb|instagram|ig|linkedin|pinterest)$')
                and match(medium_cleaned, '^(.*cp.*|ppc|paid.*|social_paid)$')                      as social_paid_clause,
            
            match(medium_cleaned, '^(display|banner|expandable|interstitial|cpm)$')                 as display_clause,
            
            match(medium_cleaned, '^(.*cp.*|ppc|paid.*)$')                                          as other_paid_clause,
            
            match(source_cleaned, concat('^.*(twitter|^t\.co|facebook|instagram|LinkedIn|',
                'linkedin|lnkd\.in|pinterest).*'))
                or match(medium_cleaned, concat('^(social|social_network',
                '|social-network|social_media|social-media|sm)$'))                                  as social_organic_clause,
            
            match(source_cleaned, '^(google|bing|yahoo|baidu|duckduckgo|yandex|ask)$')
                or medium_cleaned = 'organic'                                                       as search_organic_clause,
            
            match(source_cleaned, '^(email|mail|e-mail|e_mail|e mail|mail\.google\.com)$')
                or match(medium_cleaned, '^(email|mail|e-mail|e_mail|e mail)$')                     as email_clause,
            
            match(source_cleaned, '(g2|sourceforge|capterra)')                                      as listings_clause,
            
            medium_cleaned = 'referral'                                                             as referral_clause
        select
            *,   
            multiIf(
                direct_clause, 'direct',
                listings_clause, 'listings',
                search_paid_clause, 'search_paid',
                social_paid_clause, 'social_paid',
                display_clause, 'display',
                other_paid_clause, 'other_paid',
                search_organic_clause, 'search_organic',
                social_organic_clause, 'social_organic',
                email_clause, 'email',
                referral_clause, 'referral',
                '(other)')                                                                          as channel
        from
            cleaning_traffic_params
    )

-- final
