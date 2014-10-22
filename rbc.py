
def rbc(fin, size=100000):
    DELIMITER = ','
    QUOTE = '"'
    STATE_ITEM = 0
    STATE_STRING = 1
    STATE_POSSIBLE_EXIT_STRING = 2

    cols = list()
    state = STATE_ITEM
    item = ''
    while True:
        ndata = fin.read(size)
        if not ndata:
            if len(cols):
                yield cols
            return
        # print dcdata
        for cc in ndata:
            if state == STATE_ITEM:
                if cc == DELIMITER:
                    cols.append(item)
                    item = ''
                elif cc == QUOTE:
                    state = STATE_STRING
                elif cc == '\n':
                    cols.append(item)
                    item = ''
                    yield cols
                    cols = list()
                else:
                    item += cc
            elif state == STATE_STRING:
                if cc == QUOTE:
                    state = STATE_POSSIBLE_EXIT_STRING
                else:
                    item += cc
            elif state == STATE_POSSIBLE_EXIT_STRING:
                if cc == '\n' or cc == DELIMITER:
                    cols.append(item)
                    item = ''
                    state = STATE_ITEM
                else:
                    item += QUOTE + cc
                    state = STATE_STRING
