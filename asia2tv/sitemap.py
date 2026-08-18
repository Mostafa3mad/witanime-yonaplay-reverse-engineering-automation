from bs4 import BeautifulSoup
from curl_cffi import requests
from tqdm import tqdm

session = requests.Session()




def get_request(url:str):

    response = session.get( url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        return soup

    else:
        return None



def main ():
    url = 'https://ww1.asia2tv.pw/category/asian-drama/'

    for i in tqdm(range(1, 58), desc="Scraping"):
        page_url = f"{url}page/{i}/"
        soup = get_request(page_url)
        if soup:
            movies = soup.select("div.box-item")

            for movie in movies:
                link = movie.select_one(".postmovie-photo a[href]")

                if link:
                    url = link["href"]
                    title = link.get("title")

                    print(title)
                    print(url)
                    print("-" * 50)
                    with open("asia2tv_sitemap.txt", "a", encoding="utf-8") as f:
                        f.write(f"{url}\n")

if __name__ == "__main__":
    main()