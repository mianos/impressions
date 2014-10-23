#!/usr/bin/python
import os
import re
from optparse import OptionParser
import gevent.monkey

gevent.monkey.patch_thread()

import boto
import gcs_oauth2_boto_plugin  # noqa don't check with flake. This is needed for boto to use oauth
GOOGLE_STORAGE = 'gs'

gevent.monkey.patch_all()

import gevent
import gevent.subprocess
import gevent.pool

import tv
from rbc import rbc


def writer(fpo, src_uri, bsize=1000000):
    gsock = src_uri.get_key()
    while True:
        aa = gsock.read(bsize)
        if aa == '':
            break
        fpo.write(aa)
    fpo.close()


class Csv:
    class CsvTableEntry:
        def __init__(self, column_names, fpo):
            self.wrote_header = False
            self.column_names = column_names
            self.fpo = fpo

        def header_row(self):
            return ','.join(self.column_names)

    def __init__(self):
        self.tables = dict()

    def close_all(self):
        map(lambda ii: ii.fpo.close(), self.tables.itervalues())

    def add_table(self, table_name, column_names, fpo):
        self.tables[table_name] = self.CsvTableEntry(column_names, fpo)

    def write_csv(self, table_name, row):
        csvt = self.tables[table_name]
        fpo = csvt.fpo
        if not csvt.wrote_header:
            fpo.write(csvt.header_row() + '\n')
            csvt.wrote_header = True
        fpo.write(','.join(row) + '\n')


ssvals = tv.TVD(delimiter='=', eov=';')


def DecodeTsv(field, cols, col_data):
    for keyval in ssvals.decode(col_data):
        yield cols + list(keyval)


def DecodePsv(field, cols, col_data):
    for val in col_data.split('|'):
        yield cols + [val]

sub_field_descriptors = {
    'CustomTargeting': {
        'decoder': DecodeTsv,
        'keys': ['KeyPart', 'TimeUsec2'],
        'additional_fields': ['LineItemId']},
    'AudienceSegmentIds': {
        'decoder': DecodePsv,
        'keys': ['KeyPart', 'TimeUsec2'],
        'additional_fields': ['LineItemId']},
    'TargetedCustomCriteria': {
        'decoder': DecodeTsv,
        'keys': ['KeyPart', 'TimeUsec2'],
        'additional_fields': ['LineItemId']}
}


def field_for_fde(fde):
    fields = list(fde['keys'])
    fields.extend(fde['additional_fields'])
    if fde['decoder'] == DecodeTsv:
        fields.extend(['key', 'value'])
    elif fde['decoder'] == DecodePsv:
        fields.append('value')
    else:
        print "Invalid decoder", fde['decoder']
        raise Exception
    return fields

primary_table = 'NetworkClicks'


def tname_to_fpo(tname, fname_base):
    fname = os.path.join('out', tname + '_' + fname_base + '.csv.gz')
    sub = gevent.subprocess.Popen(["-c", "gzip | python ofnw.py -o '%s'" % fname], shell=True, stdin=gevent.subprocess.PIPE)
    return sub.stdin


def reader(fpi, fname_base):
    csvtn = Csv()
    for tname, fde in sub_field_descriptors.iteritems():
        csvtn.add_table(tname, field_for_fde(fde), tname_to_fpo(tname, fname_base))

    for field_number, vv in enumerate(rbc(fpi)):
        if field_number == 0:
            header = vv
            map_fieldname_to_column = dict((field, col) for col, field in enumerate(header) if field not in sub_field_descriptors)
            subfield_map_colnum_to_fieldname = dict((col, field) for col, field in enumerate(header) if field in sub_field_descriptors)
            fields_in_primary = [fieldnum for fieldnum in xrange(len(header)) if fieldnum not in subfield_map_colnum_to_fieldname]
            csvtn.add_table(primary_table,
                            [ii for ii in header if ii not in subfield_map_colnum_to_fieldname],
                            tname_to_fpo(primary_table, fname_base))
        else:
            for col, field in subfield_map_colnum_to_fieldname.iteritems():
                col_data = vv[col]
                if not col_data:
                    continue
                sfd = sub_field_descriptors[field]
                cols = [vv[map_fieldname_to_column[kk]] for kk in sfd['keys'] + sfd['additional_fields']]
                for pivot in sfd['decoder'](field, cols, col_data):
                    csvtn.write_csv(field, pivot)
            csvtn.write_csv("NetworkClicks", [vv[ff] for ff in fields_in_primary])
    csvtn.close_all()


def process_file(bucket, fname, out_filename):
    print "started on", fname
    src_uri = boto.storage_uri(bucket + '/' + fname, GOOGLE_STORAGE)
    sub = gevent.subprocess.Popen('gunzip', stdin=gevent.subprocess.PIPE, stdout=gevent.subprocess.PIPE)
    rr = gevent.Greenlet(writer, sub.stdin, src_uri)
    ww = gevent.Greenlet(reader, sub.stdout, out_filename)
    rr.start()
    ww.start()
    gevent.wait([rr, ww])


def c_filenames(bucket):
    uri = boto.storage_uri(bucket + '/', GOOGLE_STORAGE)
    fnmap = dict()
    for obj in uri.get_bucket():
        fnmap[obj.name] = re.match('(.*)_(\d+)_(\d+)_(\d+).gz', obj.name).groups()
    return fnmap

if __name__ == '__main__':
    # not needed for a single project specified in .boto
    #    LOCAL_FILE = 'file'
    #    project_id = 'dfp-tests'
    #    uri = boto.storage_uri('', GOOGLE_STORAGE)
    #    header_values = {"x-goog-project-id": project_id}
    parser = OptionParser()
    parser.add_option("-t", "--threads", dest="threads", default=5)
    parser.add_option("-b", "--bucket", dest="bucket", default='ffx')
    options, args = parser.parse_args()

    fnmap = c_filenames(options.bucket)
    pool = gevent.pool.Pool(options.threads)
    for fname, fpart in fnmap.iteritems():
        pool.spawn(process_file, options.bucket, fname, '_'.join(fpart[ii] for ii in xrange(1, 4)))
    gevent.wait()
