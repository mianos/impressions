# Multi part parallel S3 upload
import gevent.monkey
gevent.monkey.patch_thread()

from boto.s3.connection import S3Connection
from cStringIO import StringIO
import zlib

gevent.monkey.patch_all()

import gevent.pool

class RanRows:
    text = """Lorem,ipsum,dolor,sit,amet,,consectetur,adipiscing,elit.,Fusce,est,velit,,finibus,ac,cursus,id,,finibus,vitae,ex.,Proin,finibus,est,diam,,vel,tincidunt,erat,tincidunt,molestie.,Nunc,eu,elementum,nulla.,Aliquam,vel,suscipit,felis,,vel,ullamcorper,arcu.,Vestibulum,ante,ipsum,primis,in,faucibus,orci,luctus,et,ultrices,posuere,cubilia,Curae;,Donec,et,finibus,nibh.,Interdum,et,malesuada,fames,ac,ante,ipsum,primis,in,faucibus.,Vivamus,justo,leo,,pretium,in,massa,id,,hendrerit,lobortis,tellus\n"""

    def __init__(self, rows):
        self.rows = rows

    def get_data(self):
        for ii in xrange(self.rows):
            yield self.text



class S3MP():
    minblock = 5242880

    def __init__(self):
        conn = S3Connection()
    #                        host='s3-ap-southeast-2.amazonaws.com')
        bucket = conn.get_bucket('ffx-bia')
        self.oblock = ''
        self.mp = bucket.initiate_multipart_upload('dev-nz/bproc/t1.gz')
        self.part = 1
        self.pool = gevent.pool.Pool(5)

    @staticmethod
    def sendr(mp, part, iob):
        print "sending block", part
        mp.upload_part_from_file(iob, part)

    def add(self, block):
        self.oblock += block
        while len(self.oblock) >= self.minblock:
            self.pool.spawn(self.sendr, self.mp, self.part, StringIO(self.oblock[:self.minblock]))
            # self.mp.upload_part_from_file(StringIO(self.oblock[:self.minblock]), self.part)
            self.oblock = self.oblock[self.minblock:]
            self.part += 1

    def flush(self):
        self.pool.join()
        # self.mp.upload_part_from_file(StringIO(self.oblock), self.part)
        self.sendr(self.mp, self.part, StringIO(self.oblock))
        for part in self.mp:
            print part.part_number, part.size
        self.mp.complete_upload()
        return self.part



def upload():
    s3mp = S3MP()
    ranrows = RanRows(3000000)
    compressor = zlib.compressobj(1, zlib.DEFLATED, zlib.MAX_WBITS | 16)
    for ii in ranrows.get_data():
        s3mp.add(compressor.compress(ii))
    s3mp.add(compressor.flush())
    s3mp.flush()


if __name__ == '__main__':
    upload()
