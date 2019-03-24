# coding=utf-8
# Salario promedio por sector económico
from mrjob.job import MRJob

class MRWordFrequencyCount(MRJob):
    def mapper(self, _, line):
        line = line.split(',')
        if not line[1].isdigit() or not line[2].isdigit(): return
        yield line[1], int(line[2])

    def reducer(self, key, values):
        values = [x for x in values]
        yield key, (sum(values) / len(values))

if __name__ == '__main__':
    MRWordFrequencyCount.run()