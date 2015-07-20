# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


class LocationsPipeline(object):
    def __init__(self, path='locations.csv'):
        self.path = path

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            path=crawler.settings.get('LOCATIONS_OUTPUT', 'locations.csv')
        )

    def process_item(self, item, spider):
        self.file.write(
            "{0}\t{1}\t{2}\n".format(
                item['date'],
                item['geoname_id'],
                item['count']
            )
        )

        return item

    def open_spider(self, spider):
        self.file = open(self.path, 'w')

    def close_spider(self, spider):
        self.file.close()
