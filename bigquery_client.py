from google.cloud import bigquery
import google.auth

_, project = google.auth.default()

client = bigquery.Client(project=project)

def query(query: str):
    return client.query(query)

def get_top_terms_of_week_turkiye():
    query = """
        with max_week as (
            select max(week) max_week_column from `bigquery-public-data.google_trends.international_top_terms` where country_name = 'Turkey'
        )
        , terms_of_week as (
            SELECT 'terms_of_week' as label, refresh_date as `date`, term, `rank` 
            FROM `bigquery-public-data.google_trends.international_top_terms` a
            join max_week m ON a.week = m.max_week_column
            where country_name = 'Turkey' and `rank` = 1
            group by all
            order by refresh_date
        )
        select * from terms_of_week
        """
    return client.query(query)

def get_top_terms_of_yesterday_turkiye():
    query = """
        with max_week as (
            select max(week) max_week_column from `bigquery-public-data.google_trends.international_top_terms` where country_name = 'Turkey'
        )
        , terms_of_yesterday as (
            SELECT 'terms_of_yesterday' as label, refresh_date as `date`, term, `rank` 
            FROM `bigquery-public-data.google_trends.international_top_terms` a
            join max_week m ON a.week = m.max_week_column
            where country_name = 'Turkey' and `rank` <= 5 and refresh_date = date_sub(current_date('Europe/Istanbul'), interval 1 day)
            group by all
            order by `rank`
        )
        select * from terms_of_yesterday
        """
    return client.query(query)


def get_top_terms_of_turkiye():
    query = """
        with max_week as (
            select max(week) max_week_column from `bigquery-public-data.google_trends.international_top_terms` where country_name = 'Turkey'
        )
        , terms_of_week as (
            SELECT 'terms_of_week' as label, refresh_date as `date`, term, `rank` 
            FROM `bigquery-public-data.google_trends.international_top_terms` a
            join max_week m ON a.week = m.max_week_column
            where country_name = 'Turkey' and `rank` = 1
            group by all
            order by refresh_date
        )
        , terms_of_yesterday as (
            SELECT 'terms_of_yesterday' as label, refresh_date as `date`, term, `rank` 
            FROM `bigquery-public-data.google_trends.international_top_terms` a
            join max_week m ON a.week = m.max_week_column
            where country_name = 'Turkey' and `rank` <= 5 and refresh_date = date_sub(current_date('Europe/Istanbul'), interval 1 day)
            group by all
            order by `rank`
        )
        select * from terms_of_week
        union all 
        select * from terms_of_yesterday
        """
    return client.query(query)