import json
from datetime import datetime
import os
from collections import Counter


def process_jsonl(file_path):
    """
    Process a JSONL file containing Twitter data and collect various statistics.

    Args:
        file_path (str): The path to the JSONL file containing Twitter data.

    Returns:
        tuple: Contains the following elements:
            - dict: Monthly tweets (keys are month strings, values are sets of tweet IDs)
            - int: Total number of tweets
            - set: Unique authors
            - Counter: Tweet count by language
            - datetime: First tweet date
            - datetime: Last tweet date
    """
    monthly_tweets = {}
    total_tweets = 0
    unique_authors = set()
    tweets_by_language = Counter()
    first_tweet_date = None
    last_tweet_date = None

    with open(file_path, 'r') as file:
        for line in file:
            try:
                tweet = json.loads(line)
                tweet_id = tweet.get('id')
                created_at = tweet.get('created_at')
                author_id = tweet.get('author_id')
                lang = tweet.get('lang')

                if tweet_id and created_at:
                    # Process tweet date
                    date = datetime.strptime(created_at, '%Y-%m-%dT%H:%M:%S.%fZ')
                    if first_tweet_date is None or date < first_tweet_date:
                        first_tweet_date = date
                    if last_tweet_date is None or date > last_tweet_date:
                        last_tweet_date = date

                    # Process monthly tweets
                    month_key = f"tweet_ids_{date.year}_{date.month:02d}"
                    monthly_tweets.setdefault(month_key, set()).add(tweet_id)

                    # Update statistics
                    total_tweets += 1
                    if author_id:
                        unique_authors.add(author_id)
                    if lang:
                        tweets_by_language[lang] += 1

            except json.JSONDecodeError:
                print(f"Skipping invalid JSON line: {line}")
            except KeyError:
                print(f"Skipping tweet with missing required fields: {line}")

    return monthly_tweets, total_tweets, unique_authors, tweets_by_language, first_tweet_date, last_tweet_date


def write_monthly_files(monthly_tweets, output_dir):
    """
    Write tweet IDs to separate files for each month.

    Args:
        monthly_tweets (dict): A dictionary where keys are month strings (YYYY-MM)
                               and values are sets of tweet IDs for that month.
        output_dir (str): The directory where the output files will be written.
    """
    os.makedirs(output_dir, exist_ok=True)

    for month, tweet_ids in monthly_tweets.items():
        output_file = os.path.join(output_dir, f"{month}.txt")
        with open(output_file, 'w') as f:
            for tweet_id in tweet_ids:
                f.write(f"{tweet_id}\n")


def write_statistics(output_dir, total_tweets, unique_authors, tweets_by_language, first_tweet_date, last_tweet_date):
    """
    Write various statistics to files.

    Args:
        output_dir (str): The directory where the output files will be written.
        total_tweets (int): Total number of tweets processed.
        unique_authors (set): Set of unique author IDs.
        tweets_by_language (Counter): Counter of tweets by language.
        first_tweet_date (datetime): Date of the earliest tweet.
        last_tweet_date (datetime): Date of the latest tweet.
    """
    os.makedirs(output_dir, exist_ok=True)

    # Write general statistics
    with open(os.path.join(output_dir, 'general_stats.txt'), 'w') as f:
        f.write(f"Total tweets: {total_tweets}\n")
        f.write(f"Total unique authors: {len(unique_authors)}\n")
        f.write(f"First tweet date: {first_tweet_date.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Last tweet date: {last_tweet_date.strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Write language statistics
    with open(os.path.join(output_dir, 'tweets_by_language.txt'), 'w') as f:
        for lang, count in tweets_by_language.most_common():
            f.write(f"{lang}: {count}\n")


def main():
    """
    Main function to process the JSONL file, write monthly tweet ID files,
    and generate statistics.
    """
    # TODO: Replace with the actual path to your JSONL file
    input_file = 'datasets/******.jsonl'
    output_dir = 'datasets/******'

    print("Processing JSONL file...")
    monthly_tweets, total_tweets, unique_authors, tweets_by_language, first_tweet_date, last_tweet_date = process_jsonl(
        input_file)

    print("Writing monthly files...")
    write_monthly_files(monthly_tweets, os.path.join(output_dir, 'monthly_tweet_ids'))

    print("Writing statistics...")
    write_statistics(output_dir, total_tweets, unique_authors, tweets_by_language, first_tweet_date, last_tweet_date)

    print("Processing complete!")


if __name__ == "__main__":
    main()
