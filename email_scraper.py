from bs4 import BeautifulSoup
from pip import main
import requests
import requests.exceptions
import pandas as pd
from urllib.parse import urlsplit
from collections import deque
import re
from googlesearch import search


"""
An app that scrapes for emails for names in a CSV file by finding the top n websites from a google search for each person's name, then crawling all links inside those websites, saving all found emails until all links inside the websites have been visited, and returning it as a csv file with all names and the found emails

Note:
since the app scrapes for all emails it finds on the website, it also collects emails that might be for a person other than the one it is looking for (perhaps journalists emails or colleagues).
The user can find the correct email by looking for the email that contains a the name of the person they are looking for (or a variation of that name)

A future version of this app could do that step for the person and have a "most likely" email, in addition to all the other emails that are less likely
"""


def run():
    data = read_csv('names.csv', 'Candidate Name')

    # create data frame to store output
    df = pd.DataFrame(columns=['Candidate Name', "emails"])

    new_urls = deque()

    for i, name in enumerate(data["Candidate Name"]):
        print(f' \n ========= Finding emails for: {name}! ========= \n')
        
        processed_urls = set()
        emails = set()
        s = requests.Session()
        s.max_redirects = 100 

        try:
            find_emails(df, new_urls, i, name, processed_urls, emails, s)
        except:
            print(f'There was an error finding emails for {name}')

    df.to_csv('output_files/result.csv')
    print(" \n \n DONE!")


def find_emails(df, new_urls, i, name, processed_urls, emails, s):
    for url in get_urls(name, n = 3, language='en'):
        new_urls.append(str(url)) 


            # process urls one by one until we exhaust the queue
    while len(new_urls):
        url = new_urls.popleft()
        processed_urls.add(url)

                # extract base url to resolve relative links
        parts = urlsplit(url)
        base_url = "{0.scheme}://{0.netloc}".format(parts)
        path = url[:url.rfind('/')+1] if '/' in parts.path else url

        if "twitter" not in base_url:
            print("Processing %s" % url)
                    # get url's content
            try:
                response = s.get(url)
            except (requests.exceptions.MissingSchema, requests.exceptions.ConnectionError):
                        # ignore pages with errors
                continue

            new_emails = set(re.findall(r"[a-z0-9\.\-+_]+@[a-z0-9\.\-+_]+\.[a-z]+", response.text, re.I))
            emails.update(new_emails)
                    
            soup = BeautifulSoup(response.text, features="html.parser")

            link = find_urls(base_url, path, soup)

            if link not in new_urls and link not in processed_urls:
                new_urls.append(link)

    email_list = [email for email in emails if(len(email) < 30)]
    df.loc[i, ["Candidate Name"]] = name
    df.at[i, ["emails"]] = str(email_list)

    print(f'+++++++++++++ Found {len(email_list)} emails for {name} +++++++++++++')

def find_urls(base_url, path, soup):
    # find and process all the anchors in the document
    for anchor in soup.find_all("a"):
    # extract link url from the anchor
        link = anchor.attrs["href"] if "href" in anchor.attrs else ''
    
    # resolve relative links
    if link.startswith('/'):
        link = base_url + link
    elif not link.startswith('http'):
        link = path + link
    return link

def get_urls(tag, n, language):
    """Generates a list of top n results from a google search for a tag in a certain language

    Args:
        tag (string): tag to be searched
        n (integer): number of urls to fetch
        language (string): language for search results

    Returns:
        list: list of top n urls from search results
    """
    urls = [url for url in search(tag, num_results = n, lang=language)][:n]
    return urls


def read_csv(file_name, *use_cols):
    """reads certain columns from a CSV file

    Args:
        file_name (String): path to CSV file

    Returns:
        dataframe: dataframe of information from CSV file
    """
    data = pd.read_csv(f'{file_name}', usecols= list(use_cols))

    # TODO: remove this after testing
    # data_small = data.tail(3)
    return data


if __name__ == '__main__':
    run()