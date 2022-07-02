import scrapy
from scrapy.crawler import CrawlerProcess
import re

############################### ОЧИСТКА #############################
"""
    "Читай-город" - страшный сайт. Настолько страшный, что при запросе описания он возвращает его вместе с html-разметкой,
    чего не делает ни один другой сайт из тех, что я пробовал парсить.
    Как? Почему? В чем смысл подобной жестокости? Не знаю. Но ответ пришлось чистить.
    Функции 1-3 вырывают описание из html-разметки (в теории можно обойтись только 1, но 2 и 3 на подстраховке),
    а clear_description() чистит от мусора само описание.
"""
bad_start = '<div itemprop="description">\n'
bad_end = '\n                </div>'
rep = {'<br>': ' ', '\n': ' ', '\r': '', '<br>\n': ' ',
       '<br>\r': '', '<br>\r.': '', '<br>\r\n': '', '\xa0': ''}
regex = re.compile(
    '((.|\n)+\"description\">\\n\s{3,})(.+?)(\\n\s{3,}</div>(.|\n)+)')
subst = "\\3"


def get_description(get_input):
    get_input = f"{get_input}"
    try:
        result = re.search(regex, get_input).group(3)
    except AttributeError:
        result = get_description_b(get_input)
    result = clear_description(result)
    return result


def get_description_b(get_input):
    try:
        result = re.sub(regex, subst, get_input, flags=re.MULTILINE)
    except:
        result = re.sub(regex, subst, get_input)

    if result.count(bad_start) > 0 or result.count(bad_end) > 0:
        return get_description_c(result)
    else:
        return result


def get_description_c(desc):
    sI = desc.find(bad_start) + 30
    eI = desc.find(bad_end)
    text = desc[sI:eI].strip()
    return text


def clear_description(text):
    for key in rep.keys():
        text = text.replace(key, rep[key])
    return text.strip()
#####################################################################

############################### ПАВУК ###############################


class BookSpider(scrapy.Spider):
    name = 'cg_cat'
    allowed_domains = ['chitai-gorod.ru']
    start_urls = ['https://www.chitai-gorod.ru/catalog/books/priklyucheniya-9680/',
                  'https://www.chitai-gorod.ru/catalog/books/klassicheskaya_i_sovremennaya_proza-9665/',
                  'https://www.chitai-gorod.ru/catalog/books/detektiv_boyevik_triller-9685/',
                  'https://www.chitai-gorod.ru/catalog/books/fantastika_fentezi-9692/',
                  'https://www.chitai-gorod.ru/catalog/books/romantika-9698/']

    checked_pages = []

    def parse(self, response, **kwargs):
        for link in response.css('div.product-card__info a::attr(href)'):
            yield response.follow(link, callback=self.parse_book)

        next_page = self.get_next_page(response)
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def parse_book(self, response):
        yield {
            'author': response.css('a.product__author::text').get().strip(),
            'name': response.css('h1.product__title::text').get().strip(),
            'genre': ', '.join(response.css('li.breadcrumbs__item a::attr(title)').getall()[-2:]),
            'description': get_description(response.css('div.product__description').get())
        }

    """
        Писавшие "Читай-Город" демоны решили, что элемент с тэгом NEXT_PAGE должен отвечать и за следующую, и за предыдущую
        страницы. Так что зачастую запрос возвращает список из 2 элементов. Но не всегда...
    """

    def get_next_page(self, response):
        if len(self.checked_pages) > 5:
            self.checked_pages.clear()
        self.checked_pages.append(response)
        next_page = response.css(
            'div.pagination a#navigation_1_next_page::attr(href)').getall()
        if len(next_page) == 1:
            res = None if next_page[0] in self.checked_pages else next_page[0]
        else:
            res = next_page[1]
        return res
###########################№ КОНЕЦ ПАВУКА ###########################


def main():
    process = CrawlerProcess(settings={
        "FEEDS": {
            "cg_cat.csv": {"format": "csv"},
        },
    })

    process.crawl(BookSpider)
    process.start()
    import pandas
    return pandas.read_csv('cg_cat.csv')


def get_parsed_data():
    return main()


if __name__ == '__main__':
    main()
