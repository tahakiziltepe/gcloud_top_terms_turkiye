from google.cloud import bigquery
import google.auth
from datetime import datetime
_, project = google.auth.default()

client = bigquery.Client(project=project)

def query(query: str):
    return client.query(query)

def get_top_terms_of_week_turkiye():
    query = """
        set @@query_label = 'project:top_terms,project_query:top_terms_of_week_turkiye';
        with max_week as (
            select max(week) max_week_column 
            from `bigquery-public-data.google_trends.international_top_terms` 
            where country_name = 'Turkey' 
                and refresh_date >= date_sub(current_date('Europe/Istanbul'), interval 7 day)
        )
        , terms_of_week as (
            SELECT 'terms_of_week' as label, refresh_date as `date`, term, `rank` 
            FROM `bigquery-public-data.google_trends.international_top_terms` a
            join max_week m ON a.week = m.max_week_column
            where country_name = 'Turkey' and `rank` = 1
                and refresh_date >= date_sub(current_date('Europe/Istanbul'), interval 7 day)
            group by all
        )
        select * from terms_of_week
        order by `date`
        """
    result = client.query(query)
    return [dict(row) for row in result]

def get_top_terms_of_yesterday_turkiye():
    query = """
        set @@query_label = 'project:top_terms,project_query:top_terms_of_yesterday_turkiye';
        with terms_of_yesterday as (
            SELECT 'terms_of_yesterday' as label, refresh_date as `date`, term, `rank` 
            FROM `bigquery-public-data.google_trends.international_top_terms` a
            where country_name = 'Turkey' and `rank` <= 5 
                and refresh_date = date_sub(current_date('Europe/Istanbul'), interval 1 day)
            group by all
        )
        select * from terms_of_yesterday
        order by `rank`
        """
    result = client.query(query)
    return [dict(row) for row in result]


def get_top_terms_of_turkiye():
    query = """
        set @@query_label = 'project:top_terms,project_query:top_terms_of_turkiye';
        with max_week as (
            select max(week) max_week_column 
            from `bigquery-public-data.google_trends.international_top_terms` 
            where country_name = 'Turkey' 
            and refresh_date >= date_sub(current_date('Europe/Istanbul'), interval 7 day)
        )
        , terms_of_week as (
            SELECT 'terms_of_week' as label, refresh_date as `date`, term, `rank` 
            FROM `bigquery-public-data.google_trends.international_top_terms` a
            join max_week m ON a.week = m.max_week_column
            where country_name = 'Turkey' and `rank` = 1
            and refresh_date >= date_sub(current_date('Europe/Istanbul'), interval 7 day)
            group by all
        )
        , terms_of_yesterday as (
            SELECT 'terms_of_yesterday' as label, refresh_date as `date`, term, `rank` 
            FROM `bigquery-public-data.google_trends.international_top_terms` a
            where country_name = 'Turkey' and `rank` <= 5 
                and refresh_date = date_sub(current_date('Europe/Istanbul'), interval 1 day)
            group by all
        )
        select * from terms_of_week
        union all 
        select * from terms_of_yesterday
        """
    result = client.query(query)
    return [dict(row) for row in result]

