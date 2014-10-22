
class TVD:
    STATE_TAG = 0
    STATE_VALUE = 1
    def __init__(self, delimiter='=', eov='|'):
        self.delimiter = delimiter
        self.eov = eov

    def decode(self, indata):
        state = self.STATE_TAG
        tag, value = '', ''
        for cc in indata:
            if state == self.STATE_TAG:
                if cc == self.delimiter:
                    state = self.STATE_VALUE
                else:
                    tag += cc
            elif state == self.STATE_VALUE:
                if cc == self.eov:
                    yield tag, value
                    state = self.STATE_TAG
                    tag, value = '', ''
                else:
                    value += cc
        if len(tag):
            yield tag, value

if __name__ == '__main__':
    tvd = TVD(delimiter='=', eov=';')
    for tt, vv in tvd.decode('channel=technology;ctype=article;flash=15.0.0;show=techtalk'):
        print tt, vv
