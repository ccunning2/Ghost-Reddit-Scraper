import praw
import sqlite3
import datetime
import os

# praw = python reddit api wrapper.
# You need a client id and secret, user agent can be anything
#Sert these environment variables in your bashrc or bash_profile
CLIENT_ID = os.environ['CLIENT_ID']

CLIENT_SECRET = os.environ['CLIENT_SECRET']

# path to sqlite3 db
DB_PATH = os.environ['DB_PATH']


#The subreddits to scrape
SUBREDDITS = ['politics'
,'pcmasterrace'
,'android'
,'apple'
,'worldpolitics'
,'MachineLearning'
,'geek'
,'LateStageCapitalism'
,'linux'
,'Cyberpunk'
,'tech'
,'tech'
,'Anarchism'
,'DataHoarder'
,'DataScience'
,'Anarcho_Capitalism'
,'lostgeneration'
,'BasicIncome'
,'kotakuinaction'
,'lewronggeneration'
,'androidwear'
,'samsung'
,'KeepOurNetFree'
,'HelloInternet'
,'neoliberal'
,'CapitalismVSocialism'
]



def scrapeSubreddit(reddit, subreddit_name, db, cursor):
    subreddit = reddit.subreddit(subreddit_name)
    post_query = f"INSERT INTO {subreddit_name.upper()}_POSTS(TITLE, URL, AUTHOR, SCORE, PERMALINK, NUM_COMMENTS) VALUES (?,?,?,?,?,?)"
    comment_query = f"INSERT INTO {subreddit_name.upper()}_COMMENTS(AUTHOR, POST_ID, PERMALINK, CONTROVERSIALITY, SCORE) VALUES (?,?,?,?,?)"
    topPosts = subreddit.top('month')
    print(f"Beginning scrape of {subreddit_name}")
    for post in topPosts:
        try:
            title = post.title
            url = post.url
            author = post.author.name
            score = post.score
            permalink = post.permalink
            numcomments = post.num_comments
        except:
            continue
        if postExists(subreddit_name, cursor, (title, author, url,)):
            print(f'Post exists: TITLE {title}')
            continue
        print(f'Scraping title: {title}')
        try:
            cursor.execute(post_query, (title, url, author,
                                        score, permalink, numcomments))
        except:
            continue
        db.commit()
        post_id = cursor.lastrowid
        post.comments.replace_more(limit=None)
        for comment in post.comments.list():
            try:
                comment_author = comment.author.name
            except:
                continue
            comment_permalink = comment.permalink
            controversiality = comment.controversiality
            comment_score = comment.score
            if commentExists(subreddit_name, cursor, (comment_author, permalink,)):
                print(f"Comment already exists {permalink}")
                continue
            cursor.execute(comment_query, (comment_author, post_id,
                                           comment_permalink, controversiality, comment_score))
            db.commit()
            print(f'Added comment:{comment_permalink}')
    print(f"Finished scrape of {subreddit_name}")

def createIfNotExist(tableName, cursor):
    ddl_posts_table =  f"""
    CREATE TABLE IF NOT EXISTS \"{tableName.upper()}_POSTS\" (
    \"ID\"    INTEGER UNIQUE,
    \"TITLE\" TEXT,
    \"URL\"   TEXT,
    \"AUTHOR\"    TEXT,
    \"SCORE\" NUMERIC,
    \"PERMALINK\" TEXT,
    \"NUM_COMMENTS\"  INTEGER,
    PRIMARY KEY(\"ID\")) """
    ddl_comments_table = f"""
    CREATE TABLE IF NOT EXISTS \"{tableName.upper()}_COMMENTS\" (
    \"ID\"    INTEGER,
    \"POST_ID\"   INTEGER,
    \"AUTHOR\"    TEXT,
    \"PERMALINK\" TEXT,
    \"SCORE\" TEXT,
    \"CONTROVERSIALITY\"  INTEGER,
    FOREIGN KEY(\"POST_ID\") REFERENCES \"{tableName.upper()}_POSTS\"(\"ID\"),
    PRIMARY KEY(\"ID\")) """
    cursor.execute(ddl_posts_table)
    cursor.execute(ddl_comments_table)
    print(f"Created tables for {tableName}")


#fieldTuple will be (title, author, url)
def postExists(tableName, cursor, fieldTuple):
    query = f"select count(*) from {tableName.upper()}_POSTS where TITLE = ? AND AUTHOR = ? AND URL = ?"
    return cursor.execute(query, fieldTuple).fetchone()[0] > 0

#fieldTuple will be (author, permalink)
def commentExists(tableName, cursor, fieldTuple):
    query = f"select count(*) from {tableName.upper()}_COMMENTS where AUTHOR = ? AND PERMALINK = ?"
    return cursor.execute(query, fieldTuple).fetchone()[0] > 0




#Get our reddit instance
reddit = praw.Reddit(client_id=CLIENT_ID,
                     client_secret=CLIENT_SECRET, user_agent='sublime')



db = sqlite3.connect(DB_PATH)




cursor = db.cursor()
print('starting:')
print(datetime.datetime.now())

for subreddit_name in SUBREDDITS:
    createIfNotExist(subreddit_name, cursor)
    scrapeSubreddit(reddit, subreddit_name, db, cursor)

db.close()

print('ending:')
print(datetime.datetime.now())