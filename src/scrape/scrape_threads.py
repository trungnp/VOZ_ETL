import re
import time
from functools import reduce

from bs4 import BeautifulSoup as bs
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

from src.scrape.scrape_users import scrape_user_tooltip_selenium
from src.scrape.get_source_page import get_source_page

host = 'https://voz.vn'

def get_main_threads(url):
    '''Get 20 main threads, ignore sticky threads'''
    soup = get_source_page(url)
    all_threads = soup.find('div', class_='structItemContainer-group js-threadList')
    main_threads = all_threads.findAll('div', {'data-author': True})

    return main_threads


def scrape_threads(page_number: int, f=33, ext='', no_of_threads=20):
    """ Extract data of threads from page {page_number} """
    url = f'{host}/f/{f}/page-{page_number}{ext}'
    result = []
    main_threads = get_main_threads(url)[:no_of_threads]
    for thread in main_threads:
        # print(thread)
        title = thread.find(class_='structItem-title').find('a', {'data-tp-primary': 'on'})
        data = {
            'thread_id': int(title['href'].split(".")[-1][:-1]),
            'title': title.get_text().strip(),
            'author_name': thread.get('data-author'),
            'author_id': int(thread.find('a', class_="username")['href'].split(".")[-1][:-1]),
            'reply_count': thread.find('dl', class_='pairs pairs--justified').find('dd').get_text(),
            'view_count': thread.find('dl', class_='pairs pairs--justified structItem-minor').find('dd').get_text(),
            'created_at': thread.find('time', class_='u-dt').get('title').replace('at', '')
        }
        if thread.find_all(class_="u-srOnly"):
            data['status'] = [i.get_text() for i in thread.find_all(class_="u-srOnly")]
            data['status'].append('Open') if 'Locked' not in data['status'] else None
        else:
            data['status'] = ['Open']

        result.append(data)
    return result


# extract_threads(1)


def scrape_threads_range(from_page: int, to_page: int, f=33, ext=''):
    """
    Extract all threads in f33 from a range of {from_page} to {to_page}
    """
    # Get all threads from the range
    threads_extracted = [scrape_threads(page_number=page, f=f, ext=ext) for page in range(from_page, to_page + 1)]
    # Flatten result to a 2d list
    return reduce(lambda x, y: x + y, threads_extracted)


def scrape_top_threads(order='reply_count', last_days=7, direction='desc', top=10, f=33):
    """Find {top} threads within {last_days} then sort by {order} and {direction}"""
    if order not in ['view_count', 'reply_count']:
        order = 'reply_count'
    if direction not in ['desc', 'asc']:
        direction = 'desc'

    data = []
    ceiling_page = -(top // -20)
    for page in range(1, ceiling_page + 1):
        data.extend(
            scrape_threads(page_number=page, f=f,
                            ext=f'?last_days={last_days}&order={order}&direction={direction}',
                            no_of_threads=min(top, 20)))
        top -= 20

    return data


# print(get_top_threads(top=30))

def analyze_thread(driver):
    """
    This function does 2 tasks:
        1/ Extract all comments along with comments' info
        2/ Get users (comments' authors)  info from tooltip boxes
    """
    link_to_thread = driver.current_url
    action_chain = ActionChains(driver)
    thread_content = bs(driver.page_source, 'html.parser')
    comment_drivers = driver.find_elements(By.CSS_SELECTOR,
                                           "article[class='message message--post js-post js-inlineModContainer  ']")
    comments = thread_content.find_all('article', {'class': 'message message--post js-post js-inlineModContainer'})
    extracted_page = link_to_thread.split('/')[-1]
    start_comment = 1 if (extracted_page == '') or (extracted_page == "page-1") else 0
    result = []
    # Get comments' data
    for i in range(start_comment, len(comments)):
        comment = comments[i]
        data = {}
        author = comment.get('data-author')
        user_id = comment.find("a", class_="username")['href'].split(".")[-1][:-1]
        data['user_id'] = int(user_id)
        data['username'] = author
        reply = comment.find(class_='bbWrapper')
        reply.find('span').extract() if reply.find('span') else None
        reply.find('blockquote').extract() if reply.find('blockquote') else None
        reply.find('a').extract() if reply.find('a') else None
        reply.find('i').extract() if reply.find('i') else None
        data['reply'] = re.sub(r'[\t\n]', '', reply.get_text())
        data['created_at'] = comment.find(class_="message-attribution-main listInline").find("time").get(
            "title").replace("at", "")
        data['love'] = 0
        data['brick'] = 0
        comment_driver = comment_drivers[i]
        # Move mouse pointer to author name and hover for 1 second
        elm = comment_driver.find_element(By.CSS_SELECTOR, "h4[class='message-name']").find_element(By.TAG_NAME, 'a')
        action_chain.move_to_element(elm).pause(1).perform()
        try:
            # Find reaction link text
            driver.find_element(By.CSS_SELECTOR, "div[class='overlay-content']")
            # Move to the link text if appeared
            action_chain.send_keys(Keys.ESCAPE).perform()
            time.sleep(3)
        except:
            pass
        finally:
            try:
                react_bar = comment_driver.find_element(By.CSS_SELECTOR,
                                                        "a[class='reactionsBar-link']")
                action_chain.move_to_element(react_bar).click().perform()
                WebDriverWait(driver, 60).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[class='overlay-container is-active']"))
                )
                try:
                    love_raw = driver.find_element(By.CSS_SELECTOR, "a[id='reaction-1']").get_attribute("textContent")
                    data['love'] = int(re.sub('\D', '', love_raw))
                except:
                    pass

                try:
                    brick_raw = driver.find_element(By.CSS_SELECTOR, "a[id='reaction-2']").get_attribute("textContent")
                    data['brick'] = int(re.sub('\D', '', brick_raw))
                except:
                    pass

            except:
                pass
            # Send a keystroke to close tooltip box
            action_chain.send_keys(Keys.ESCAPE).perform()

        result.append(data)

    # Get users infor
    tooltips = driver.find_elements(By.CSS_SELECTOR, 'div[role="tooltip"]')
    user_info = [scrape_user_tooltip_selenium(tooltip) for tooltip in tooltips]

    return result, user_info

