import undetected_chromedriver as uc
from bs4 import BeautifulSoup as bs
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait


def get_source_page(url: str):
    '''
        Get source of url by beautifulsoup
    :param url: any url
    :return: beautifulsoup
    '''

    driver = uc.Chrome(use_subprocess=True)

    print(url)
    driver.get(url)
    try:
        elem = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CLASS_NAME, "p-body-inner"))
        )
        soup = bs(driver.page_source, 'html.parser')
        # driver.quit()
    finally:
        driver.quit()

    return soup

# get_source_page('https://voz.vn')


def get_source_selenium(url: str):
    # Send request by using undetected_chromedriver
    driver = uc.Chrome(use_subprocess=True)
    print(url)
    driver.get(url)
    try:
        elem = WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.CLASS_NAME, "p-body-inner"))
        )
        return driver
    except Exception as e:
        print(e)



