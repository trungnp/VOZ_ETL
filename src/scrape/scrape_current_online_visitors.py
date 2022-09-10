from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from src.scrape.scrape_users import scrape_user_tooltip_selenium
from src.scrape.get_source_page import get_source_selenium

host = 'https://voz.vn'


def get_current_online_members():
    '''Get users info, who are members and are online'''
    page = 1
    users_data = []
    while True:
        url = f"{host}/online/?type=member&page={page}"
        driver = get_source_selenium(url)
        action_chain = ActionChains(driver)
        users = driver.find_elements(By.CLASS_NAME, "username")
        for user in users:
            try:
                action_chain.move_to_element(user).pause(1).perform()
            except:
                pass
            # print(user.text)
        tooltips = driver.find_elements(By.CSS_SELECTOR, 'div[role="tooltip"]')
        users_data.extend([scrape_user_tooltip_selenium(tooltip) for tooltip in tooltips])
        try:
            element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.LINK_TEXT, "Next"))
            )
            page += 1
        except:
            print("Next button not found")
            break
        finally:
            driver.quit()

    return users_data

# get_current_online_users(find_all=False)