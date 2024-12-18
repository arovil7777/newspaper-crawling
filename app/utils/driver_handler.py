from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from fake_useragent import UserAgent


class DriverUtils:
    @classmethod
    def get_driver(self):
        options = webdriver.ChromeOptions()
        ua = UserAgent()

        options.add_argument("user-agent=" + ua.chrome)
        options.add_argument("--no-sandbox")
        options.add_argument("--disabled-dev-shm-usage")
        options.add_argument("--incognito")
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")

        service = webdriver.chrome.service.Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)

        return driver
