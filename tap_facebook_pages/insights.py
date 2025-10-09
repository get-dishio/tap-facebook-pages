from tap_facebook_pages.streams import PageInsights, PostInsights

# TODO The insights tables can be further split if the insights with _unique postfix get their dedicated tables

INSIGHT_STREAMS = [
    # PAGE INSIGHTS
    {"class": PageInsights, "name": "page_insight_engagement", "metrics": [
        "page_post_engagements",
    ]},

    # REMOVED: page_insight_consumptions (all fields deprecated)
    # REMOVED: page_insight_feedback (all fields deprecated)

    {"class": PageInsights, "name": "page_insight_fans", "metrics": [
        # Fans family is at-risk by Nov 15, 2025 -> plan to migrate to follows
        "page_fans",
        "page_fans_city",
        "page_fans_country",
        "page_fans_locale",
        "page_fan_adds_by_paid_non_paid_unique",
    ]},

    # TODO (Nov 15, 2025): migrate all *impressions* to *views*
    {"class": PageInsights, "name": "page_insight_impressions", "metrics": [
        "page_impressions",
        "page_impressions_unique",
        "page_impressions_paid",
        "page_impressions_paid_unique",
        # REMOVED NOW: page_impressions_organic_v2, page_impressions_organic_unique_v2
    ]},
    
    # REMOVED NOW: page_insight_impressions_by_category/location (organic-by-* deprecated combinations)

    {"class": PageInsights, "name": "page_insight_post", "metrics": [
        "page_posts_impressions",
        "page_posts_impressions_unique",
        "page_posts_impressions_paid",
        "page_posts_impressions_paid_unique",
        # REMOVED NOW: page_posts_impressions_organic, page_posts_impressions_organic_unique,
        # REMOVED NOW: page_posts_served_impressions_organic_unique (organic reach/impr deprecations)
        "page_posts_impressions_viral",
        "page_posts_impressions_viral_unique",
        "page_posts_impressions_nonviral",
        "page_posts_impressions_nonviral_unique",
    ]},

    # REMOVED: page_insight_reactions (legacy reactions namespace)

    {"class": PageInsights, "name": "page_insight_demographics", "metrics": [
        # Fans-based breakdowns (at-risk by Nov 15, 2025)
        "page_fans",
        "page_fans_locale",
        "page_fans_city",
        "page_fans_country",
    ]},

    {"class": PageInsights, "name": "page_insight_video_views", "metrics": [
        "page_video_views",
        "page_video_views_paid",
        "page_video_views_organic",
        "page_video_views_by_paid_non_paid",
    ]},
    {"class": PageInsights, "name": "page_insight_video_views_2", "metrics": [
        "page_video_views_autoplayed",
        "page_video_views_click_to_play",
        "page_video_views_unique",
        "page_video_repeat_views",
        "page_video_view_time",
    ]},

    {"class": PageInsights, "name": "page_insight_video_complete_views", "metrics": [
        "page_video_complete_views_30s",
        "page_video_complete_views_30s_paid",
        "page_video_complete_views_30s_organic",
        "page_video_complete_views_30s_autoplayed",
        "page_video_complete_views_30s_click_to_play",
        "page_video_complete_views_30s_unique",
        "page_video_complete_views_30s_repeat_views",
    ]},

    {"class": PageInsights, "name": "page_insight_views", "metrics": [
        "page_views_total"
    ]},

    {"class": PageInsights, "name": "page_insight_video_ad_break", "metrics": [
        "page_daily_video_ad_break_ad_impressions_by_crosspost_status",
        "page_daily_video_ad_break_cpm_by_crosspost_status",
        "page_daily_video_ad_break_earnings_by_crosspost_status",
    ]},

    # POST INSIGHTS
    {"class": PostInsights, "name": "post_insight_engagement", "metrics": [
        # REMOVED NOW: post_engaged_users, post_negative_feedback*, *_by_type* (deprecated)
        "post_clicks",
        "post_clicks_unique",
        "post_clicks_by_type",
        "post_clicks_by_type_unique",
    ]},

    # TODO (Nov 15, 2025): migrate all *post_impressions* to *post views*
    {"class": PostInsights, "name": "post_insight_impressions", "metrics": [
        "post_impressions",
        "post_impressions_unique",
        "post_impressions_paid",
        "post_impressions_paid_unique",
        "post_impressions_fan",
        "post_impressions_fan_unique",
        "post_impressions_viral",
        "post_impressions_viral_unique",
        "post_impressions_nonviral",
        "post_impressions_nonviral_unique",
        "post_impressions_by_story_type",
        "post_impressions_by_story_type_unique",
        # REMOVED NOW: post_impressions_organic, post_impressions_organic_unique
    ]},

    # REMOVED: post_insight_reactions (legacy per-type totals deprecated)

    {"class": PostInsights, "name": "post_insight_video", "metrics": [
        "post_video_avg_time_watched",
        "post_video_retention_graph",
        "post_video_retention_graph_clicked_to_play",
        "post_video_retention_graph_autoplayed",
        "post_video_length",
    ]},

    {"class": PostInsights, "name": "post_insight_video_views", "metrics": [
        "post_video_views_organic",
        "post_video_views_organic_unique",
        "post_video_views_paid",
        "post_video_views_paid_unique",
        "post_video_views",
        "post_video_views_unique",
        "post_video_views_autoplayed",
        "post_video_views_clicked_to_play",
        "post_video_views_sound_on",
        "post_video_view_time",
        "post_video_view_time_organic",
    ]},

    {"class": PostInsights, "name": "post_insight_video_complete_views", "metrics": [
        "post_video_complete_views_organic",
        "post_video_complete_views_organic_unique",
        "post_video_complete_views_paid",
        "post_video_complete_views_paid_unique",
    ]},

    {"class": PostInsights, "name": "post_insight_video_by_sec", "metrics": [
        "post_video_views_15s",
        "post_video_views_60s_excludes_shorter",
        # REMOVED NOW: all post_video_views_10s* fields
    ]},

    {"class": PostInsights, "name": "post_insight_video_by_category", "metrics": [
        "post_video_view_time_by_age_bucket_and_gender",
        "post_video_view_time_by_region_id",
        "post_video_views_by_distribution_type",
        "post_video_view_time_by_distribution_type",
        "post_video_view_time_by_country_id",
    ]},

    {"class": PostInsights, "name": "post_insight_activity", "metrics": [
        "post_activity_by_action_type",
        "post_activity_by_action_type_unique",
    ]},

    {"class": PostInsights, "name": "post_insight_video_ad_break", "metrics": [
        "post_video_ad_break_ad_impressions",
        "post_video_ad_break_earnings",
        "post_video_ad_break_ad_cpm",
    ]},
]
