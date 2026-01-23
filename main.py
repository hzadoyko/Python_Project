import yake
import click

# Function to read a file
def read_file(filename):
    with open(filename, 'r', encoding='utf-8') as myfile:
        return myfile.read()
    
# Function to extract keywords
def extract_keywords(text):
    kw_extractor = yake.KeywordExtractor()
    keywords = kw_extractor.extract_keywords(text)
    return keywords

# Function that creates hashtags
def make_hashtags(keywords):
    hashtags = []
    for keyword in keywords:
        hashtags.append("#" + keyword[0].replace(" ", ""))
    return hashtags

# CLI setup using Click
@click.group()
def cli():
    """A CLI tool for extracting keywords and generating hashtags from text files."""

@cli.command("extract")
@click.argument("filename", default="sample.txt")

def hashtagscli(filename):
    """Extract keywords and generate hashtags from a text file."""
    text = read_file(filename)
    keywords = extract_keywords(text)
    hashtags = make_hashtags(keywords)
    click.echo(hashtags)

if __name__ == "__main__":
    cli()