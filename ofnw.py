import sys
import logging
import shutil

from optparse import OptionParser

logger = logging.getLogger('impressions')
logging.basicConfig()
logger.setLevel(logging.INFO)


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-o", "--out-file", dest="out_file", default=None)
    options, args = parser.parse_args()

    if not options.out_file:
        logger.error("must specify an out-file")
        sys.exit(1)

    fpo = open(options.out_file, 'w')
    shutil.copyfileobj(sys.stdin, fpo)
