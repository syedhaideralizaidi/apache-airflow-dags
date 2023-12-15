import csv
import datetime
import datetime as dt
from datetime import datetime as dt
import scrapy


class MercadoagrarioSpider(scrapy.Spider):
    name = "MercadoagrarioSpider"
    allowed_domains = ["mercadoagrario.com"]
    start_urls = [
        f"https://mercadoagrario.com/?s=syngenta&post_type=product&dgwt_wcas=1"
    ]
    custom_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
    }
    products = []

    def parse(self, response, **kwargs):
        print(response.url)
        products = response.css("li.product")
        if products:
            for product in products:
                today = dt.today()
                listing = dict()
                listing["seller"] = "mercadoagrario"
                listing["region"] = "LATAM"
                listing["country"] = "Argentina"
                listing["currency"] = "$"
                listing["created_at"] = today.strftime("%d-%m-%Y")
                listing["domain"] = "https://www.mercadoagrario.com"
                listing["company_id"] = kwargs.get("company_id")
                listing["keyword"] = kwargs.get("keyword")
                listing["created_at"] = today.strftime("%d-%m-%Y")
                listing["url"] = product.css(
                    "a.woocommerce-LoopProduct-link.woocommerce-loop-product__link::attr(href)"
                ).get()
                listing["price"] = (
                    product.css(
                        "a.woocommerce-LoopProduct-link.woocommerce-loop-product__link"
                    )
                    .css("span.woocommerce-Price-amount.amount bdi::text")
                    .get()
                    .replace(",", ".")
                    if product.css(
                        "a.woocommerce-LoopProduct-link.woocommerce-loop-product__link"
                    )
                    .css("span.woocommerce-Price-amount.amount bdi::text")
                    .get()
                    else ""
                )
                listing["product"] = (
                    product.css("h2.woocommerce-loop-product__title::text")
                    .get()
                    .strip()
                    if product.css("h2.woocommerce-loop-product__title::text").get()
                    else ""
                )
                listing["pic"] = ""
                if listing["url"]:
                    yield scrapy.Request(
                        listing["url"],
                        callback=self.pic,
                        meta={
                            "listing": listing,
                        },
                        headers=self.custom_headers,
                        dont_filter=True,
                    )
                else:
                    self.products.append(listing)
            next_page = response.css("a.next.page-numbers::attr(href)").get()
            if next_page:
                yield scrapy.Request(
                    next_page,
                    callback=self.parse,
                    cb_kwargs=kwargs,
                    headers=self.custom_headers,
                    dont_filter=True,
                )

    def pic(self, response):
        response.meta["listing"]["pic"] = ",".join(
            [
                v
                for v in response.css(
                "figure.woocommerce-product-gallery__wrapper > div a::attr(href)"
            ).getall()
            ]
        )
        self.products.append(response.meta["listing"])

    def close(self, reason):

        unique_list_of_dicts = []
        seen_dicts = set()

        for d in self.products:
        # Convert the dictionary to a frozenset to make it hashable
            frozen_dict = frozenset(d.items())

        # If the frozen_dict is not in seen_dicts, add it to the new list
        if frozen_dict not in seen_dicts:
            seen_dicts.add(frozen_dict)
            unique_list_of_dicts.append(d)

        now = datetime.datetime.now()
        date_now = now.strftime("%Y-%m-%d")
        file = "/home/ahsan/airflow/dags/scraper" + "abc" + "_" + date_now + ".csv"
        csv_col = [
            "product",
            "price",
            "url",
            "region",
            "seller",
            "currency",
            "country",
            "domain",
            "created_at",
            "pic",
            "company_id",
            "keyword",
        ]
        csvfile = open(file, "w", newline="", encoding="utf-8")
        writer = csv.DictWriter(csvfile, fieldnames=csv_col)
        if csvfile.tell() == 0:
            writer.writeheader()
            csvfile.flush()

        for item in unique_list_of_dicts:
            ite = dict()
            ite["product"] = item["product"]
            ite["price"] = item["price"]
            ite["url"] = item["url"]
            ite["region"] = item["region"]
            ite["seller"] = item["seller"]
            ite["country"] = item["country"]
            ite["currency"] = item["currency"]
            ite["domain"] = item["domain"]
            ite["created_at"] = item["created_at"]
            ite["pic"] = item["pic"]
            ite["company_id"] = item["company_id"]
            ite["keyword"] = item["keyword"]
            writer.writerow(ite)
            csvfile.flush()


