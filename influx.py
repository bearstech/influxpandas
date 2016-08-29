from collections import defaultdict

from pyparsing import QuotedString, Word, nums, delimitedList, Optional, \
    Regex, Group, Suppress, ParseException


integer = Regex(r"\d+i").setParseAction(lambda s, locs, toks:
                                        int(toks[0][:-1]))
real = Regex(r"[+-]?\d+(\.\d+)?")("real").setParseAction(lambda s, locs, toks:
                                                         float(toks[0]))
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
            yield l['key'], int(l['ts']),\
                dict((a[0], a[1]) for a in l['values']),\
                dict((a[0], a[1]) for a in l['tags'])


def orderByKey(flux):
    box = defaultdict(list)
    for line in flux:
        box[line[0]].append(line)
    return box


if __name__ == '__main__':

    import sys

    b = orderByKey(readflux(open(sys.argv[1], 'r')))
    for k, v in b.items():
        print()
        print(k)
        for vv in v:
            print("\t", vv[1:])
