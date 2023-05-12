
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import shlex


class MorningProgramms(MRJob):
    def mapper(self, _, line):
        if line.startswith('prog_code'):
                return
        result = re.findall(r'(?:[^,"\\\\]|\\\\.|"(?:\\.|[^"])*")+', line)
        result = [re.sub(r'^"|"$|\\\\(.)', lambda m: m.group(1) or '', element) for element in result]
        if 70000 <= int(result[4]) < 90000:
            if ('Talk' in result[2] ) or ('Politics' in result[2]) or ('Spanish' in result[2]) or ('Community' in result[2]) \
                    or ('Martial arts' in result[2]):
                if ('j' in str(result[1]).lower()) or ('q'in str(result[1]).lower() )or ('z' in str(result[1].lower())):
                    yield (result[1], result[2]), result[3]

    def reducer(self, key, val):
        dates = set([])
        keys = list(key)
        genres = keys[1].split(",")
        for d in val:
            dates.add(d)
        yield key,(len(dates) , len(genres))

if __name__ == '__main__':
    MorningProgramms.run()
