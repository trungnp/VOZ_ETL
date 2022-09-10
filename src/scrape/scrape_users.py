import re
from datetime import datetime

from selenium.webdriver.common.by import By

from src.scrape.get_source_page import get_source_page

host = 'https://voz.vn'


def scrape_user_tooltip_selenium(tooltip):
    '''Get user info from tooltip box'''
    try:
        u_data = tooltip.find_element(By.CLASS_NAME, "memberTooltip")
    except Exception as e:
        #print(e)
        return None
    data = {}
    user = u_data.find_element(By.CLASS_NAME, "username")
    data['user_id'] = int(user.get_attribute("data-user-id"))
    data['username'] = user.get_attribute("textContent").strip()
    data['title'] = u_data.find_element(By.CLASS_NAME, "userTitle").get_attribute("textContent")
    loc_raw = u_data.find_element(By.CLASS_NAME, "memberTooltip-blurb").get_attribute("textContent")
    loc = re.sub(r'[\t\n]', '', loc_raw)
    data['location'] = loc.split("From")[-1].strip() if "From" in loc else "Namek"
    jd_ls = u_data.find_elements(By.CLASS_NAME, "u-dt")
    data['join_date'] = jd_ls[0].get_attribute("title").replace("at", "")
    now = datetime.now().strftime("%b %d, %Y %I:%M %p")
    data['last_seen'] = jd_ls[1].get_attribute("title").replace("at", "") if len(jd_ls) == 2 else now
    stats = u_data.find_element(By.CLASS_NAME, "memberTooltip-stats").find_elements(By.TAG_NAME, 'dd')
    for i, v in enumerate(["message", "reaction", "point"]):
        data[v] = int(re.sub(r'[\t\n,]', '', stats[i].get_attribute("textContent")))
    data['created_at'] = now

    return data


def scrape_user_data(user_id):
    '''Get user info from the user profile page'''
    url = f'{host}/u/{user_id}'
    soup = get_source_page(url)
    result = {
        'user_id': user_id,
        'username': '',
        'title': '',
        'last_seen': '',
        'location': '',
        'join_date': '',
        'message': '',
        'reaction': '',
        'point': ''
    }

    if soup.find(attrs={'data-template': 'error'}) or soup.find(attrs={'data-template': 'login'}):
        pass
    else:
        result['username'] = soup.find('span', class_='username').get_text()
        loc = soup.find('div', class_='memberHeader-blurb')
        result['location'] = loc.find('a', href=True).get_text() if loc.find('a', href=True) else ''
        mod = soup.find('h1', class_='memberHeader-name').find('span', class_='username--staff username--moderator')
        title = soup.find(class_='userTitle').get_text()
        if mod:
            banner = soup.find(class_='memberHeader-banners').get_text().strip()
            title = f'{title} | {banner}'
        result['title'] = title
        result['last_seen'] = soup.find('time', class_='u-dt').get('title').replace('at', '')
        result['join_date'] = soup.find_all('div', class_='memberHeader-blurb')[1].find('dd').get_text()
        score = soup.find('div', class_='pairJustifier').find_all('dl')
        result['message'] = score[0].find('a', href=True).get_text().strip()
        result['reaction'] = score[1].find('dd').get_text().strip()
        result['point'] = score[2].find('dd').get_text().strip()
    return result

