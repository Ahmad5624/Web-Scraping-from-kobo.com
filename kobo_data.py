import csv
import pandas as pd  # a dataframe
import asyncio  # asyncio is a library in Python used for writing concurrent code using the async/await syntax.
from playwright.async_api import async_playwright  # An automation library developed by Microsoft which is good for
# asynchronous running.
from selectolax.parser import HTMLParser  # A fast html5 parser, much better than bs4 but less flexible
import re  # regular expression for data cleaning
import json  # json for data parsing
from datetime import datetime  # A library which is used for record the time
import time
OUTPUT_FILE_NAME = 'Kobo_data.csv'  # output file name, I usually use the csv file data saving because it's much
# faster than other databases when saving records in it, you can convert the csv to xlsx format after scraping.

Header_file = [
    'Main Link',
    'Region',
    'Retailer',
    'Isbn13',
    'Asin',
    'Title Name',
    'Authors',
    'Sales Rank',
    'List Price',
    'Price',
    'Old Price',
    'Currency',
    'Synopsis',
    'Rating',
    'Number of Pages',
    'Hours to Read',
    'Total Words',
    'Availability',
    'eBook Details',
    'Time Stamp'
]


# Write to the csv file
def write_to_file(rows):
    with open(OUTPUT_FILE_NAME, 'a', encoding='utf-8-sig', newline="") as file:
        writer = csv.writer(file)
        writer.writerows(rows)


# A function which launches the chrome first and then process on links.
async def process_links(links, queue, playwright):
    browser = await playwright.chromium.launch(headless=False, channel='chrome')
    try:
        page = await browser.new_page()
        viewport_size = {"width": 1920, "height": 1080}
        await page.set_viewport_size(viewport_size)

        for link in links:
            await process_link(link, page)
            await queue.put(None)  # Single link processing completion

        print("Processed", len(links), "links")
    finally:
        await browser.close()


async def process_link(link, page):
    # Get to the website
    await page.goto(link, timeout=600000)
    await page.wait_for_load_state("networkidle", timeout=150000)
    # Scroll to the bottom of the page
    # Scrape the Data
    await Over_all_Scrape_Data(link, page)  # Await here
    await asyncio.sleep(5)


# A main function for data scraping.
async def Over_all_Scrape_Data(link, page):
    try:
        content = await page.content()
        # Create a Selectolax object
        root = HTMLParser(content)

        # Parse the json for the other data which is not showing in the tags.
        Name, Pages, Region, Isbn13, Asin, Availability = await Json_Parsing(page)
        # Extract the author names
        author_Name = ', '.join([a.text() for a in root.css('span.visible-contributors a.contributor-name')])

        # Synopsis
        Synopsis = root.css_first("div.synopsis-description").text()
        # Rating
        try:
            Rating = root.css_first("span#average-rating-value span:nth-child(1)").text()
        except:
            Rating = 'Null'
        # Rankings
        Rankings = '\n'.join([
            f"{li.css_first('span.rank').text(strip=True)} in {', '.join(a.text(strip=True) for a in li.css('a.rankingAnchor'))}"
            for li in root.css('ul.category-rankings > li')])

        # Hours of Read
        try:
            Hours_to_read = "'" + root.css_first("div.book-stats div.column:nth-child(2) strong").text()
        except:
            Hours_to_read = ''

        # Total Words
        try:
            Total_words = root.css_first("div.book-stats div.column:nth-child(3) strong").text()
        except:
            Total_words = ''

        # Retailer
        Retailer = root.css_first("div.rich-header-secondary-link-bar img.country-flag").attributes.get('title')

        # Prices
        List_Price, Price, Currency, old_Price = await Prices(root)

        # eBook Details
        eBook_details = root.css_first("div.bookitem-secondary-metadata ul")
        # Extract and join the text from li elements without gaps
        eBook_details = '\n'.join(li.text(strip=True) for li in eBook_details.css('li'))

        # Get timestamp
        time_stamp = await get_the_timestamp()

        # Join the names with a comma
        print("1.Main Link:", link)
        print("......")
        print("2.Region:", Region)
        print("......")
        print("3.Retailer:", Retailer)
        print("......")
        print("4.Isbn13:", Isbn13)
        print("......")
        print("5.Asin:", Asin)
        print("......")
        print("6.Title Name:", Name)
        print("......")
        print("7.Authors:", author_Name)
        print("......")
        print("8.Sales Rank:", Rankings)
        print("......")
        print("9.List Price:", List_Price)
        print("......")
        print("10.Price:", Price)
        print("......")
        print("11.Old Price:", old_Price)
        print("......")
        print("12.Currency:", Currency)
        print("......")
        print("13.Synopsis:", Synopsis)
        print("......")
        print("14.Rating:", Rating)
        print("......")
        print("15.Number of Pages:", Pages)
        print("......")
        print("16.Hours to Read:", Hours_to_read)
        print("......")
        print("17.Total Words:", Total_words)
        print("......")
        print("18.Availability:", Availability)
        print("......")
        print("19.eBook Details:", eBook_details)
        print('......')
        print("20.Time Stamp:", time_stamp)
        print('......')
        output_result = ([link] + [Region] + [Retailer] + [Isbn13] + [Asin] + [Name] + [author_Name] + [Rankings] +
                         [List_Price] + [Price] + [old_Price] + [Currency] + [Synopsis] + [Rating] + [Pages] +
                         [Hours_to_read] + [Total_words] + [Availability] + [eBook_details] + [time_stamp])
        write_to_file([output_result])
        print("######################")
    except:
        Region = ''
        Retailer = ''
        Isbn13 = ''
        Asin = ''
        Name = ''
        author_Name = ''
        Rankings = ''
        List_Price = ''
        Price = ''
        old_Price = ''
        Currency = ''
        Synopsis = ''
        Rating = ''
        Pages = ''
        Hours_to_read = ''
        Total_words = ''
        Availability = ''
        eBook_details = ''
        # Get timestamp
        time_stamp = await get_the_timestamp()

        output_result = ([link] + [Region] + [Retailer] + [Isbn13] + [Asin] + [Name] + [author_Name] + [Rankings] +
                         [List_Price] + [Price] + [old_Price] + [Currency] + [Synopsis] + [Rating] + [Pages] +
                         [Hours_to_read] + [Total_words] + [Availability] + [eBook_details] + [time_stamp])
        write_to_file([output_result])
        print("Data not found!")


# json parsing from the html on the website for: Name, Pages, Region, Isbn13, Asin, Availability
async def Json_Parsing(page):
    Asin = ''
    json_element = await page.query_selector("div.RatingAndReviewWidget > script[type='application/ld+json']")
    json_data = await json_element.text_content()
    # Remove the "description" field using regex
    parsed_json = re.sub(r',\s*"description":\s*".*?"(?=,\s*"image")', '', json_data, flags=re.DOTALL)
    parsed_Data = json.loads(parsed_json)
    # Name
    Name = parsed_Data['name']
    # Total Pages
    try:
       Pages = parsed_Data['numberOfPages']
    except:
        Pages = ''

    # Region
    Region = parsed_Data['workExample']['potentialAction']['expectsAcceptanceOf']['eligibleRegion']['name'].upper()
    # ISBN
    Isbn13 = "'" + parsed_Data['workExample']['isbn']
    # Availability
    Availability = 'In Stock' if 'InStock' in parsed_Data['workExample']['potentialAction']['expectsAcceptanceOf'][
        'availability'] else 'Out of Stock'
    return Name, Pages, Region, Isbn13, Asin, Availability


# Get the prices from html
async def Prices(root):
    content = root.css_first("div[data-kobo-gizmo='ItemDetailActions']").attributes.get("data-kobo-gizmo-config")
    List_Price = ''
    Price = json.loads(content)['priceDetails']['displayPrice']
    Currency = json.loads(content)['priceDetails']['displayCurrency']
    old_Price = json.loads(content)['priceDetails']['wasPrice']
    return List_Price, Price, Currency, old_Price


# get the timestamp
async def get_the_timestamp():
    now = datetime.now()
    # Format the time in the desired format
    time_stamp = "'" + now.strftime('%Y-%m-%d %I:%M:%S %p').lower()
    return time_stamp


# A main function
async def main():
    excel_data_df = pd.read_csv('Kobo_ISBN.csv')
    links = excel_data_df['isbn13'].tolist()
    print("Total Links: ", len(links))

    max_concurrent_browsers = 100  # set the value for browser running, for example: if there are total 1000 links,
    # if you set the '500' value then 2 browsers will be run, and if you set '100' then 10 browsers will asynchronously run.

    chunk_size = max_concurrent_browsers
    async with async_playwright() as p:
        queue = asyncio.Queue()
        # Start the link processing coroutines
        coroutines = [
            process_links(links[i:i + chunk_size], queue, p)
            for i in range(0, len(links), chunk_size)
        ]
        # Start consuming from the queue to track link processing completion
        consuming = asyncio.ensure_future(queue.get())
        coroutines.append(consuming)
        # Wait for all coroutines to complete
        await asyncio.gather(*coroutines)


if __name__ == "__main__":
    write_to_file([Header_file])
    asyncio.run(main())
