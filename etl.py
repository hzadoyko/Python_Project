from main import extract_keywords, make_hashtags, read_file
import click
import sqlite3
import os
from os import path

DATABASE = "keywords.db"

# Function that loads keywords into a SQLite database
def load_keywords(keywords, score, hashtags):
    """Load keywords, hashtags and their scores into a SQLite database."""

    db_exists = False

    #If path to the database exists, set db_exists to True
    if path.exists(DATABASE):
        db_exists = True

    conn = sqlite3.connect(DATABASE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS keywords
                 (keyword TEXT, score REAL, hashtags TEXT)''')
    for keyword, score, hashtags in zip(keywords, score, hashtags):
        c.execute("INSERT INTO keywords VALUES (?, ?, ?)", (keyword, score, hashtags))
    conn.commit()
    conn.close()
    return db_exists

# Function that collects keywords from a file and loads them into the database
def collect_extract(filename):
    """Collect keywords from a file and load them into the database."""
    keywords = []
    score = []
    text = read_file(filename)
    extracted_keyword_score = extract_keywords(text)
    for keyword_score in extracted_keyword_score:
        keywords.append(keyword_score[0])
        score.append(keyword_score[1])
    # Pass keywords to make_hashtags function
    hashtags = make_hashtags(extracted_keyword_score)
    return keywords, score, hashtags

# Function that extracts keywords from a file and loads them into the database
def extract_and_load(filename):
    """Extract keywords from a file and load them into the database."""
    keywords, score, hashtags = collect_extract(filename)
    status = load_keywords(keywords, score, hashtags)
    return status

# Function that queries the database for keywords, hashtags, and scores
def query_database(order_by = "score", limit = 10):
    """Query the database for keywords, hashtags, and scores."""
    conn = sqlite3.connect(DATABASE)
    c = conn.cursor()
    c.execute(f"SELECT * FROM keywords ORDER BY {order_by} DESC LIMIT {limit}")
    results = c.fetchall()
    conn.close()
    return results

@click.group()
def cli():
    """An ETL CLI"""

# Command to extract keywords from a file and load them into a database
@cli.command("etl")
@click.argument("filename", default = "sample.txt")
def etl(filename):
    """Extract keywords from a file and load them into a database"""

    path_to_db = path.abspath(DATABASE)
    click.echo(
        click.style(
            f"Running ETL to extract keywords from {filename} and load them into database at {path_to_db}",
            fg = "blue"
            )
    )
    result = extract_and_load(filename)
    if result:
        click.echo(click.style("Database already exists", fg = "yellow"))
    else:
        click.echo(click.style("Database created", fg = "green"))

# Command to query the database for keywords, hashtags, and scores
@cli.command("query")
@click.option("--order_by", default = "score", help = "Order by score or keyword")
@click.option("--limit", default = 10, help = "Limit the number of results")
def query(order_by, limit):
    """Query the database for keywords, hashtags, and scores"""

    results = query_database(order_by, limit)
    for result in results:
        print(
            click.style(result[0], fg = "blue"),
            click.style(result[1], fg = "green"),
            click.style(result[2], fg = "cyan")
        )

# Command to delete the database file
@cli.command("delete")
def delete():
    """Delete the database"""

    if path.exists(DATABASE):
        path_to_db = path.abspath(DATABASE)
        click.echo(click.style(f"Deleting database {path_to_db}", fg = "red"))
        os.remove(DATABASE)
    else:
        # If database does not exist, print message
        click.echo(click.style("Database does not exist", fg = "yellow"))

if __name__ == "__main__":
    cli()