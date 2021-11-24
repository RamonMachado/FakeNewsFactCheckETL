class Article:
    def __init__(self, title, subtitle, text, publishDate, modifiedDate, author, publisher):
        self.title = title
        self.subtitle = subtitle
        self.text = text
        self.publish_date = publishDate
        self.modified_date = modifiedDate
        self.author = author
        self.publisher = publisher

    def print(self):
        print('\n<<<<<<< Article >>>>>>>\n==== Publisher: {}\n==== Author: {}\n==== Publish Date: {}\n==== Modified Date: {}\n==== Title: {}\n==== Subtitle: {}\n==== Text: {}\n'.format(self.publisher, self.author, self.publish_date,
              self.modified_date, self.title, self.subtitle, self.text))

    def attributes(self):
        return ["publisher", "author", "publish_date", "modified_date", "title", "subtitle", "text"]

    def toList(self):
        return list(map(lambda attr: getattr(self, attr), self.attributes()))
