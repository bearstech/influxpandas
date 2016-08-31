import pandas as pd

import influx


def load(path):
    "Load the influxdb content of the file, at path"
    d = dict()
    for name, values in \
            influx.orderByKey(influx.readflux(open(path, 'r'))).items():
        idx, v = influx.df(values)
        d[name] = pd.DataFrame(v, index=[pd.Timestamp(a) for a in idx])
    return d


if __name__ == "__main__":
    import sys

    infl = influx.orderByKey(influx.readflux(open(sys.argv[1], 'r')))

    if len(sys.argv) == 2:
        print(" ".join(infl.keys()))
    else:
        print(infl[sys.argv[2]])
