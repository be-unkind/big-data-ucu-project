import uvicorn
from dateutil import parser
from datetime import datetime, timedelta
import pandas as pd
from fastapi import FastAPI
from cassandra.cluster import Cluster
from fastapi.responses import HTMLResponse

app = FastAPI()

# CATEGORY A

# 1 precomputed: Return the aggregated statistics containing the number of created pages for each Wikipedia domain for each hour in the last 6 hours, excluding the last hour.
@app.get("/created_pages_statistics", tags=["Category A"])
def created_pages_statistics():
    current_time = datetime.now().replace(minute=0, second=0, microsecond=0)

    stats = []
    for hour in range(2, 8):
        start_time = current_time - timedelta(hours=hour)
        start_timestamp = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")

        # test
        # insert_q = f"INSERT INTO domain_stats (start_hour, end_hour, statistics) VALUES ('{start_timestamp}', '{start_timestamp}', NULL);"
        # session.execute(insert_q)

        query = f"SELECT start_hour, end_hour, statistics FROM domain_stats WHERE start_hour = '{start_timestamp}'"
        result = session.execute(query)

        stats.append([result[0].start_hour, result[0].end_hour, result[0].statistics])
    
    created_pages_data = pd.DataFrame(stats, columns=['time_start', 'time_end', 'statistics'])
    return HTMLResponse(content=created_pages_data.to_html(), status_code=200)

# 2 precomputed: Return the statistics about the number of pages created by bots for each of the domains for the last 6 hours, excluding the last hour.
@app.get("/bots_statistics", tags=["Category A"])
def bots_statistics():
    start_time = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=7)

    # test
    # insert_q = f"INSERT INTO domain_bot_stats (start_hour, end_hour, statistics) VALUES ('{start_time}', '{start_time}', NULL);"
    # session.execute(insert_q)
    
    query = f"SELECT start_hour, end_hour, statistics FROM domain_bot_stats WHERE start_hour = '{start_time}'"
    result = session.execute(query)
    
    bots_stats = [[l.start_hour, l.end_hour, l.statistics] for l in result]

    bots_data = pd.DataFrame(bots_stats, columns=['start_hour', 'end_hour', 'statistics'])
    return HTMLResponse(content=bots_data.to_html(), status_code=200)

# 3 precomputed: Return Top 20 users that created the most pages during the last 6 hours, excluding the last hour. 
# The response should contain user name, user id, start and end time, the list of the page titles, and the number of pages created
@app.get("/top_users_statistics", tags=["Category A"])
def top_users_statistics():
    start_time = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=7)

    # test
    # insert_q = f"INSERT INTO user_stats (start_hour, end_hour, statistics) VALUES ('{start_time}', '{start_time}', NULL);"
    # session.execute(insert_q)

    query = f"SELECT start_hour, end_hour, statistics FROM user_stats WHERE start_hour = '{start_time}';"
    result = session.execute(query)
    
    top_user_stats = [[l.start_hour, l.end_hour, l.statistics] for l in result]

    top_user_data = pd.DataFrame(top_user_stats, columns=['start_hour', 'end_hour', 'statistics'])
    return HTMLResponse(content=top_user_data.to_html(), status_code=200)

# CATEGORY B

# 1 ad-hoc: Return the list of existing domains for which pages were created.
@app.get("/get_domains", tags=["Category B"])
def get_domains():

    query = f"SELECT DISTINCT domain FROM domain_table;"
    result = session.execute(query)
    
    domains = [[l.domain] for l in result]

    created_pages_data = pd.DataFrame(domains, columns=['domain'])
    return HTMLResponse(content=created_pages_data.to_html(), status_code=200)

# 2 ad-hoc: Return all the pages which were created by the user with a specified user_id
@app.get("/get_pages_created_by_user/{user_id}", tags=["Category B"])
def get_pages_created_by_user(user_id: int):

    query = f"SELECT * FROM user_table WHERE user_id = {user_id};"
    result = session.execute(query)
    
    created_pages = [[l.user_id, l.page_id, l.page_title] for l in result]

    created_pages_data = pd.DataFrame(created_pages, columns=['user_id', 'page_id', 'page_title'])
    return HTMLResponse(content=created_pages_data.to_html(), status_code=200)

# 3 ad-hoc: Return the number of articles created for a specified domain.
@app.get("/get_articles_number_for_domain/{domain}", tags=["Category B"])
def get_pages_created_by_user(domain: str):

    query = f"SELECT COUNT(*) AS page_count FROM domain_table WHERE domain = '{domain}';"
    result = session.execute(query)
    
    page_count = [result[0].page_count]

    created_pages_data = pd.DataFrame(page_count, columns=['page_count'])
    return HTMLResponse(content=created_pages_data.to_html(), status_code=200)

# 4 ad-hoc: Return the page with the specified page_id
@app.get("/get_page_by_id/{page_id}", tags=["Category B"])
def get_pages_created_by_user(page_id: int):

    query = f"SELECT * FROM page_table WHERE page_id = {page_id};"
    result = session.execute(query)
    
    pages_by_id = [[l.page_id, l.page_title] for l in result]

    pages_data = pd.DataFrame(pages_by_id, columns=['page_id', 'page_title'])
    return HTMLResponse(content=pages_data.to_html(), status_code=200)

# 5 ad-hoc: Return the id, name, and the number of created pages of all the users who created at least one page in a specified time range. 
@app.get("/get_users_active_in_time_range/{start_time}/{end_time}", tags=["Category B"])
def get_users_active_in_time_range(start_time, end_time):

    start_timestamp = parser.parse(start_time)
    end_timestamp = parser.parse(end_time)

    query = f"SELECT user_id, user_text, COUNT(page_id) AS created_pages_count FROM user_time_table WHERE rev_timestamp >= '{start_timestamp}' AND rev_timestamp <= '{end_timestamp}' GROUP BY rev_timestamp, user_id, user_text ALLOW FILTERING;"
    result = session.execute(query)
    
    user_activity_info = [[l.user_id, l.user_text, l.created_pages_count] for l in result]

    pages_data = pd.DataFrame(user_activity_info, columns=['user_id', 'user_text', 'created_pages_count'])
    return HTMLResponse(content=pages_data.to_html(), status_code=200)



if __name__ == "__main__":
    cluster = Cluster(['cassandra-node'], port=9042)
    session = cluster.connect()
    session.set_keyspace('wiki_data')

    uvicorn.run(app, host="0.0.0.0", port=8000)
