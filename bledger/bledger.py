import json
from time import sleep
import argparse
import sys

import praw
from praw.exceptions import PRAWException
from prawcore import Requestor
import pandas as pd
import dataset

class JSONRequestor(Requestor):
    '''
    Modified from ReadTheDocs
    '''
    def request(self, *args, **kwargs):
        response = super().request(*args, **kwargs)
        #print(json.dumps(response.json(), indent=4))
        return response.json()

def read_creds(filename):
    '''
    Returns a json structure formatted as:
    {
        "client_id": "...",
        "client_secret": "..."
    }

    These values can be found at
    https://www.reddit.com/prefs/apps
    for your app.
    '''
    with open(filename, 'r') as f:
        return json.loads(f.read())

def save_recent_posts(reddit, db_filename):
    '''
    Save last 1000 posts of r/borrow to a csv file:
    (See https://stackoverflow.com/questions/53988619/praw-6-get-all-submission-of-a-subreddit for more info on limitations)

    timestamp, author, title
    '''
    rborrow = reddit.subreddit('borrow')
    data = { 'timestamp': [], 'author': [], 'title': [] }
    # limit=None gives access to 1000 most recent posts
    for submission in rborrow.new(limit=None):
        print(submission.created_utc, submission.author, submission.title)
        data['timestamp'].append(submission.created_utc)
        data['author'].append(submission.author)
        data['title'].append(submission.title)

    db = pd.DataFrame.from_dict(data)
    print('Saving database to ' + db_filename)
    db.to_csv(db_filename, index=False)

def load_db(csv_filename):
    return pd.read_csv(csv_filename)

# Just use dataset api
def archive_posts(db_filename):
    ...

def get_submission_json(submission):
    keys = ['author', 'created_utc', 'id', 'name', 'selftext', 'title', 'url',
            'permalink', 'link_flair_text']

    obj = {
        key: getattr(submission, key) for key in keys
    }

    # Initially returns a Reddit object, which can't be converted to a string
    obj['author'] = obj['author'].name
    # Store as int rather than float
    obj['created_utc'] = int(obj['created_utc'])
    return obj

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('credentials', help='Path to JSON credential file for the Reddit API')
    parser.add_argument('database', help='Path to the SQL database for storing posts')
    args = parser.parse_args()

    creds = read_creds(args.credentials)

    reddit = praw.Reddit(
        client_id=creds['client_id'], 
        client_secret=creds['client_secret'],
        user_agent='r/borrow_scraper (by u/plmr)'
    )

    # Open db here
    db = dataset.connect('sqlite:///' + args.database)

    '''
    Need to write a post archiver as submissions arrive
    '''

    #save_recent_posts(reddit, 'borrow.csv')
    # Setup a new post stream

    # TODO
    # - Refresh token
    table = db['posts']
    failures = 0
    while failures < 3:
        try:
            # TODO Ideally, if failures was greater than 1, we would synchronize (any) missed
            # posts
            for post in reddit.subreddit('borrow').stream.submissions(skip_existing=True):
                # Any simple way to extract raw API json?
                #print(json.dumps(post))
                data = get_submission_json(post)
                print(data['title'])
                #print(data)
                # Private API, but whatever
                table.insert({'timestamp': data['created_utc'], 'post': json.dumps(data)})
            failures = 0
        except PRAWException as e:
            failures += 1
            print('PRAW failed for the following reason:')
            print(e)
            print('Waiting 5 minutes, and trying again.')
            sleep(5*60)
        except KeyboardInterrupt:
            sys.exit(0)

if __name__ == '__main__':
    main()
