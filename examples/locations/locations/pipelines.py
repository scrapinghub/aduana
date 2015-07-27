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
            '\t'.join(
                map(str, [item['date'],
                          item['geoname_id'],
                          spider.geo_names.name(item['geoname_id']),
                          item['count']]))
            + '\n'
        )

        return item

    def open_spider(self, spider):
        self.file = open(self.path, 'a')

    def close_spider(self, spider):
        self.file.close()
