import csv


class UnicodeCSVReader(object):
    """
    Expect an iterator that returns unicode strings which will be
    parsed by csv module

    """

    def __init__(self, f):
        """
        Assumes utf-8 encoding and ignores all decoding errors.

        f ... an iterator (maybe file object opened in text mode)
        """
        # build a standard csv reader, that works on utf-8 strings
        self.reader = csv.reader(
            (line.encode("utf-8", errors="ignore") for line in f))

    def __iter__(self):
        """
        return an iterator over f
        """
        return self

    def next(self):
        """
        return next row frow csv.reader, each cell as unicode again
        """
        return [cell.decode('utf-8') for cell in self.reader.next()]


class UnicodeCSVWriter(object):
    """
    Writes unicode csv rows as utf-8 into file.
    """

    def __init__(self, f):
        """
        f ... an open file object that expects byte strings as input.
        """
        self.writer = csv.writer(f)

    def writerow(self, row):
        """
        encode each cell value as utf-8 and write to writer as usual
        """
        self.writer.writerow([cell.encode('utf-8') for cell in row])


def safe_unicode(value, encoding='utf-8'):
    if isinstance(value, unicode):
        return value
    elif isinstance(value, basestring):
        try:
            value = unicode(value, encoding)
        except (UnicodeDecodeError):
            value = value.decode('utf-8', 'replace')
        return value
