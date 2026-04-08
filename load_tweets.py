#!/usr/bin/python3

# imports
import sqlalchemy
import os
import datetime
import zipfile
import io
import json

################################################################################
# helper functions
################################################################################


def remove_nulls(s):
    r'''
    Postgres doesn't support strings with the null character \x00 in them, but twitter does.
    This helper function replaces the null characters with an escaped version so that they can be loaded into postgres.
    Technically, this means the data in postgres won't be an exact match of the data in twitter,
    and there is no way to get the original twitter data back from the data in postgres.

    The null character is extremely rarely used in real world text (approx. 1 in 1 billion tweets),
    and so this isn't too big of a deal.
    A more correct implementation, however, would be to *escape* the null characters rather than remove them.
    This isn't hard to do in python, but it is a bit of a pain to do with the JSON/COPY commands for the denormalized data.
    Since our goal is for the normalized/denormalized versions of the data to match exactly,
    we're not going to escape the strings for the normalized data.

    >>> remove_nulls('\x00')
    ''
    >>> remove_nulls('hello\x00 world')
    'hello world'
    '''
    if s is None:
        return None
    else:
        return s.replace('\x00','').replace('\\u0000','')


def get_id_urls(url, connection):
    '''
    Given a url, return the corresponding id in the urls table.
    If no row exists for the url, then one is inserted automatically.

    NOTE:
    This function cannot be tested with standard python testing tools because it interacts with the db.
    '''
    sql = sqlalchemy.sql.text('''
    insert into urls
        (url)
        values
        (:url)
    on conflict do nothing
    returning id_urls
    ;
    ''')
    res = connection.execute(sql,{'url':url}).first()

    if res is None:
        sql = sqlalchemy.sql.text('''
        select id_urls
        from urls
        where
            url=:url
        ''')
        res = connection.execute(sql,{'url':url}).first()

    id_urls = res[0]
    return id_urls


def insert_tweet(connection,tweet):
    '''
    Insert the tweet into the database.

    Args:
        connection: a sqlalchemy connection to the postgresql db
        tweet: a dictionary representing the json tweet object
    '''

    with connection.begin():

        sql = sqlalchemy.sql.text('''
        select id_tweets
        from tweets
        where id_tweets = :id_tweets
        ''')
        res = connection.execute(sql, {'id_tweets': tweet['id']}).first()
        if res is not None:
            return

        ########################################
        # insert into the users table
        ########################################
        if tweet['user']['url'] is None:
            user_id_urls = None
        else:
            user_id_urls = get_id_urls(tweet['user']['url'], connection)

        sql = sqlalchemy.sql.text('''
        insert into users (
            id_users,
            created_at,
            updated_at,
            id_urls,
            friends_count,
            listed_count,
            favourites_count,
            statuses_count,
            protected,
            verified,
            screen_name,
            name,
            location,
            description,
            withheld_in_countries
        )
        values (
            :id_users,
            :created_at,
            :updated_at,
            :id_urls,
            :friends_count,
            :listed_count,
            :favourites_count,
            :statuses_count,
            :protected,
            :verified,
            :screen_name,
            :name,
            :location,
            :description,
            :withheld_in_countries
        )
        on conflict (id_users) do update set
            created_at = excluded.created_at,
            updated_at = excluded.updated_at,
            id_urls = excluded.id_urls,
            friends_count = excluded.friends_count,
            listed_count = excluded.listed_count,
            favourites_count = excluded.favourites_count,
            statuses_count = excluded.statuses_count,
            protected = excluded.protected,
            verified = excluded.verified,
            screen_name = excluded.screen_name,
            name = excluded.name,
            location = excluded.location,
            description = excluded.description,
            withheld_in_countries = excluded.withheld_in_countries
        where users.updated_at is null or excluded.updated_at > users.updated_at
        ;
        ''')
        connection.execute(sql, {
            'id_users': tweet['user']['id'],
            'created_at': tweet['user'].get('created_at'),
            'updated_at': tweet.get('created_at'),
            'id_urls': user_id_urls,
            'friends_count': tweet['user'].get('friends_count'),
            'listed_count': tweet['user'].get('listed_count'),
            'favourites_count': tweet['user'].get('favourites_count'),
            'statuses_count': tweet['user'].get('statuses_count'),
            'protected': tweet['user'].get('protected'),
            'verified': tweet['user'].get('verified'),
            'screen_name': remove_nulls(tweet['user'].get('screen_name')),
            'name': remove_nulls(tweet['user'].get('name')),
            'location': remove_nulls(tweet['user'].get('location')),
            'description': remove_nulls(tweet['user'].get('description')),
            'withheld_in_countries': tweet['user'].get('withheld_in_countries'),
        })

        ########################################
        # insert into the tweets table
        ########################################

        geo_str = None
        geo_coords = None
        try:
            geo_coords = str(tweet['geo']['coordinates'][0]) + ' ' + str(tweet['geo']['coordinates'][1])
            geo_str = 'POINT'
        except (TypeError, KeyError):
            try:
                poly = tweet['place']['bounding_box']['coordinates'][0]
                geo_coords = '(('
                for i, point in enumerate(poly):
                    if i > 0:
                        geo_coords += ','
                    geo_coords += str(point[0]) + ' ' + str(point[1])
                if poly[0] != poly[-1]:
                    geo_coords += ',' + str(poly[0][0]) + ' ' + str(poly[0][1])
                geo_coords += '))'
                geo_str = 'POLYGON'
            except (KeyError, TypeError, IndexError):
                geo_str = None
                geo_coords = None

        try:
            text = tweet['extended_tweet']['full_text']
        except KeyError:
            text = tweet.get('text')

        try:
            country_code = tweet['place']['country_code'].lower()
        except TypeError:
            country_code = None

        if country_code == 'us':
            state_code = tweet['place']['full_name'].split(',')[-1].strip().lower()
            if len(state_code) > 2:
                state_code = None
        else:
            state_code = None

        try:
            place_name = tweet['place']['full_name']
        except TypeError:
            place_name = None

        if tweet.get('in_reply_to_user_id', None) is not None:
            sql = sqlalchemy.sql.text('''
            insert into users (id_users, screen_name)
            values (:id_users, :screen_name)
            on conflict do nothing
            ;
            ''')
            connection.execute(sql, {
                'id_users': tweet['in_reply_to_user_id'],
                'screen_name': remove_nulls(tweet.get('in_reply_to_screen_name')),
            })

        sql = sqlalchemy.sql.text('''
        insert into tweets (
            id_tweets,
            id_users,
            created_at,
            in_reply_to_status_id,
            in_reply_to_user_id,
            quoted_status_id,
            retweet_count,
            favorite_count,
            quote_count,
            withheld_copyright,
            withheld_in_countries,
            source,
            text,
            country_code,
            state_code,
            lang,
            place_name,
            geo
        )
        values (
            :id_tweets,
            :id_users,
            :created_at,
            :in_reply_to_status_id,
            :in_reply_to_user_id,
            :quoted_status_id,
            :retweet_count,
            :favorite_count,
            :quote_count,
            :withheld_copyright,
            :withheld_in_countries,
            :source,
            :text,
            :country_code,
            :state_code,
            :lang,
            :place_name,
            case
                when :geo_str = 'POINT' then ST_GeomFromText('POINT(' || :geo_coords || ')', 4326)
                when :geo_str = 'POLYGON' then ST_GeomFromText('POLYGON' || :geo_coords, 4326)
                else null
            end
        )
        ;
        ''')
        connection.execute(sql, {
            'id_tweets': tweet['id'],
            'id_users': tweet['user']['id'],
            'created_at': tweet.get('created_at'),
            'in_reply_to_status_id': tweet.get('in_reply_to_status_id'),
            'in_reply_to_user_id': tweet.get('in_reply_to_user_id'),
            'quoted_status_id': tweet.get('quoted_status_id'),
            'retweet_count': tweet.get('retweet_count'),
            'favorite_count': tweet.get('favorite_count'),
            'quote_count': tweet.get('quote_count'),
            'withheld_copyright': tweet.get('withheld_copyright'),
            'withheld_in_countries': tweet.get('withheld_in_countries'),
            'source': remove_nulls(tweet.get('source')),
            'text': remove_nulls(text),
            'country_code': country_code,
            'state_code': state_code,
            'lang': tweet.get('lang'),
            'place_name': remove_nulls(place_name),
            'geo_str': geo_str,
            'geo_coords': geo_coords,
        })

        ########################################
        # insert into the tweet_urls table
        ########################################

        try:
            urls = tweet['extended_tweet']['entities']['urls']
        except KeyError:
            urls = tweet['entities']['urls']

        for url in urls:
            expanded_url = url.get('expanded_url')
            if expanded_url is None:
                continue

            id_urls = get_id_urls(expanded_url, connection)

            sql = sqlalchemy.sql.text('''
            insert into tweet_urls (id_tweets, id_urls)
            values (:id_tweets, :id_urls)
            on conflict do nothing
            ;
            ''')
            connection.execute(sql, {
                'id_tweets': tweet['id'],
                'id_urls': id_urls,
            })

        ########################################
        # insert into the tweet_mentions table
        ########################################

        try:
            mentions = tweet['extended_tweet']['entities']['user_mentions']
        except KeyError:
            mentions = tweet['entities']['user_mentions']

        for mention in mentions:
            sql = sqlalchemy.sql.text('''
            insert into users (id_users, screen_name, name)
            values (:id_users, :screen_name, :name)
            on conflict do nothing
            ;
            ''')
            connection.execute(sql, {
                'id_users': mention['id'],
                'screen_name': remove_nulls(mention.get('screen_name')),
                'name': remove_nulls(mention.get('name')),
            })

            sql = sqlalchemy.sql.text('''
            insert into tweet_mentions (id_tweets, id_users)
            values (:id_tweets, :id_users)
            on conflict do nothing
            ;
            ''')
            connection.execute(sql, {
                'id_tweets': tweet['id'],
                'id_users': mention['id'],
            })

        ########################################
        # insert into the tweet_tags table
        ########################################

        try:
            hashtags = tweet['extended_tweet']['entities']['hashtags']
            cashtags = tweet['extended_tweet']['entities']['symbols']
        except KeyError:
            hashtags = tweet['entities']['hashtags']
            cashtags = tweet['entities']['symbols']

        tags = [ '#' + hashtag['text'] for hashtag in hashtags ] + [ '$' + cashtag['text'] for cashtag in cashtags ]

        for tag in tags:
            sql = sqlalchemy.sql.text('''
            insert into tweet_tags (id_tweets, tag)
            values (:id_tweets, :tag)
            on conflict do nothing
            ;
            ''')
            connection.execute(sql, {
                'id_tweets': tweet['id'],
                'tag': remove_nulls(tag),
            })

        ########################################
        # insert into the tweet_media table
        ########################################

        try:
            media = tweet['extended_tweet']['extended_entities']['media']
        except KeyError:
            try:
                media = tweet['extended_entities']['media']
            except KeyError:
                media = []

        for medium in media:
            media_url = medium.get('media_url')
            if media_url is None:
                continue

            id_urls = get_id_urls(media_url, connection)
            sql = sqlalchemy.sql.text('''
            insert into tweet_media (id_tweets, id_urls, type)
            values (:id_tweets, :id_urls, :type)
            on conflict do nothing
            ;
            ''')
            connection.execute(sql, {
                'id_tweets': tweet['id'],
                'id_urls': id_urls,
                'type': medium.get('type'),
            })

################################################################################
# main functions
################################################################################

if __name__ == '__main__':
    
    # process command line args
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--db',required=True)
    parser.add_argument('--inputs',nargs='+',required=True)
    parser.add_argument('--print_every',type=int,default=1000)
    args = parser.parse_args()

    # create database connection
    engine = sqlalchemy.create_engine(args.db, connect_args={
        'application_name': 'load_tweets.py',
        })
    connection = engine.connect()

    # loop through the input file
    # NOTE:
    # we reverse sort the filenames because this results in fewer updates to the users table,
    # which prevents excessive dead tuples and autovacuums
    for filename in sorted(args.inputs, reverse=True):
        with zipfile.ZipFile(filename, 'r') as archive:
            print(datetime.datetime.now(),filename)
            for subfilename in sorted(archive.namelist(), reverse=True):
                with io.TextIOWrapper(archive.open(subfilename)) as f:
                    for i,line in enumerate(f):

                        # load and insert the tweet
                        tweet = json.loads(line)
                        insert_tweet(connection,tweet)

                        # print message
                        if i%args.print_every==0:
                            print(datetime.datetime.now(),filename,subfilename,'i=',i,'id=',tweet['id'])
