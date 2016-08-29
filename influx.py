from pyparsing import QuotedString, Word, nums, delimitedList, Optional, \
    printables, Regex, Group, Suppress, ParseException


integer = Regex(r"(?P<integer>\d+)i")
real = Regex(r"[+-]?\d+(\.\d+)?")
number = Word(nums)
key = Regex(r"[a-zA-Z][a-zA-Z0-9_]*")
quoted = QuotedString('"')
blob = Regex(r"[^ ,]+")
LINE = key("key") + Optional(Suppress(",") + Group(delimitedList(Group(key +
                                                                 Suppress("=")
                                                                 + (blob ^ quoted))))("tags")) +\
    Group(delimitedList(Group(key + Suppress("=") + (integer ^ real ^ quoted))))("values") +\
    number("ts")


def readflux(reader):
    for line in reader:
        try:
            l = LINE.parseString(line)
        except ParseException as p:
            print(line)
            print(" " * p.col + "^")
            raise p
        else:
            print(l[1].resultsName)
            yield l


if __name__ == '__main__':

    import sys

    for line in readflux(open(sys.argv[1], 'r')):
        print(line)
