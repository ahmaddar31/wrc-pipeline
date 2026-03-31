import scrapy


class DecisionItem(scrapy.Item):
    source = scrapy.Field()
    body = scrapy.Field()
    body_id = scrapy.Field()
    identifier = scrapy.Field()
    title = scrapy.Field()
    description = scrapy.Field()
    published_date = scrapy.Field()
    published_date_iso = scrapy.Field()
    partition_date = scrapy.Field()
    search_url = scrapy.Field()
    detail_url = scrapy.Field()
    file_url = scrapy.Field()
    file_type = scrapy.Field()
    local_tmp_path = scrapy.Field()
    object_storage_path = scrapy.Field()
    file_hash = scrapy.Field()
    # Transient fields — used internally between spider and pipeline, not stored in Mongo
    html_content = scrapy.Field()
    downloaded_file_path = scrapy.Field()