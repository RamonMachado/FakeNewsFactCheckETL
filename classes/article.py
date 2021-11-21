class Article:
    def __init__(self, title, subtitle, text, publishDate, modifiedDate, author, publisher):
        self.title = title
        self.subtitle = subtitle
        self.text = text
        self.publishDate = publishDate
        self.modifiedDate = modifiedDate
        self.author = author
        self.publisher = publisher

    def print(self):
        print('\n<<<<<<< Article >>>>>>>\n==== Publisher: {}\n==== Author: {}\n==== Publish Date: {}\n==== Modified Date: {}\n==== Title: {}\n==== Subtitle: {}\n==== Text: {}\n'.format(self.publisher, self.author, self.publishDate,
              self.modifiedDate, self.title, self.subtitle, self.text))
